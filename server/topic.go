package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"shelley.exe.dev/db/generated"
	"shelley.exe.dev/llm"
)

type TopicConfig struct {
	Name    string
	ModelID string
}

type Topic struct {
	Name         string
	Config       TopicConfig
	Conversation *generated.Conversation
	Manager      *ConversationManager
	WSHub        *WSHub
	PromptQueue  *PromptQueue

	server        *Server
	logger        *slog.Logger
	runtimeCtx    context.Context
	runtimeCancel context.CancelFunc

	turnMu   sync.Mutex
	turnDone chan struct{}

	approvalMu       sync.Mutex
	pendingApprovals map[string]chan workspaceApprovalResponse
}

type TopicManager struct {
	mu                     sync.Mutex
	topics                 map[string]*Topic
	topicsByConversationID map[string]*Topic
	server                 *Server
	logger                 *slog.Logger
}

func NewTopicManager(server *Server, logger *slog.Logger) *TopicManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &TopicManager{
		topics:                 make(map[string]*Topic),
		topicsByConversationID: make(map[string]*Topic),
		server:                 server,
		logger:                 logger,
	}
}

func (tm *TopicManager) GetOrCreateTopic(ctx context.Context, topicName string) (*Topic, topicConversationState, error) {
	tm.mu.Lock()
	if topic, ok := tm.topics[topicName]; ok {
		tm.mu.Unlock()
		return topic, topicConversationExisting, nil
	}
	tm.mu.Unlock()

	conversation, state, err := tm.server.getOrCreateTopicConversation(ctx, topicName)
	if err != nil {
		return nil, topicConversationExisting, err
	}

	manager, err := tm.server.getOrCreateConversationManager(ctx, conversation.ConversationID, "")
	if err != nil {
		return nil, topicConversationExisting, err
	}

	modelID := conversationModelID(*conversation, tm.server.defaultTopicModelID())
	topic := newTopic(tm.server, manager, conversation, TopicConfig{
		Name:    topicName,
		ModelID: modelID,
	})
	topic.start()

	tm.mu.Lock()
	defer tm.mu.Unlock()
	if existing, ok := tm.topics[topicName]; ok {
		topic.Close()
		return existing, topicConversationExisting, nil
	}
	tm.topics[topicName] = topic
	tm.topicsByConversationID[conversation.ConversationID] = topic
	return topic, state, nil
}

func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.topics[name]
}

func (tm *TopicManager) GetTopicByConversationID(conversationID string) *Topic {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.topicsByConversationID[conversationID]
}

func (tm *TopicManager) TopicClientCount(topicName string) int {
	tm.mu.Lock()
	topic := tm.topics[topicName]
	tm.mu.Unlock()
	if topic == nil {
		return 0
	}
	return topic.ClientCount()
}

func (tm *TopicManager) RemoveTopicRuntime(topicName string) {
	tm.mu.Lock()
	topic := tm.topics[topicName]
	if topic != nil {
		delete(tm.topics, topicName)
		delete(tm.topicsByConversationID, topic.Conversation.ConversationID)
	}
	tm.mu.Unlock()

	if topic != nil {
		topic.Close()
	}
}

func (tm *TopicManager) RenameTopic(conversationID, topicName string, conversation *generated.Conversation) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topic := tm.topicsByConversationID[conversationID]
	if topic == nil {
		return
	}

	delete(tm.topics, topic.Name)
	topic.Name = topicName
	topic.Config.Name = topicName
	if conversation != nil {
		topic.Conversation = conversation
	}
	tm.topics[topicName] = topic
}

func newTopic(server *Server, manager *ConversationManager, conversation *generated.Conversation, cfg TopicConfig) *Topic {
	logger := server.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("topic", cfg.Name, "conversationID", conversation.ConversationID)

	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())

	return &Topic{
		Name:             cfg.Name,
		Config:           cfg,
		Conversation:     conversation,
		Manager:          manager,
		WSHub:            NewWSHub(),
		PromptQueue:      NewPromptQueue(),
		server:           server,
		logger:           logger,
		runtimeCtx:       runtimeCtx,
		runtimeCancel:    runtimeCancel,
		pendingApprovals: make(map[string]chan workspaceApprovalResponse),
	}
}

func (t *Topic) start() {
	go t.forwardStream()
	go t.drainPrompts()
}

func (t *Topic) Close() {
	t.runtimeCancel()
	t.WSHub.Close()
}

func (t *Topic) ClientCount() int {
	return t.WSHub.Size()
}

func (t *Topic) IsBusy() bool {
	t.turnMu.Lock()
	turnActive := t.turnDone != nil
	t.turnMu.Unlock()
	return turnActive || t.Manager.IsAgentWorking() || t.PromptQueue.Len() > 0
}

func (t *Topic) EnqueuePrompt(text, senderID string) bool {
	queued := t.IsBusy()
	t.PromptQueue.Enqueue(QueuedPrompt{
		Text:     text,
		SenderID: senderID,
		QueuedAt: time.Now(),
	})
	if queued {
		t.WSHub.Broadcast(workspaceWSMessage{Type: "system", Data: "queued prompt"})
	}
	return queued
}

func (t *Topic) forwardStream() {
	lastSequenceID, err := t.server.latestSequenceID(t.runtimeCtx, t.Conversation.ConversationID)
	if err != nil {
		t.logger.Error("Failed to get latest sequence id for topic stream", "error", err)
		return
	}

	next := t.Manager.subpub.Subscribe(t.runtimeCtx, lastSequenceID)
	toolTitles := make(map[string]string)

	for {
		streamData, ok := next()
		if !ok {
			return
		}

		messages, turnComplete := translateWorkspaceWSMessages(toolTitles, streamData)
		for _, msg := range messages {
			t.WSHub.Broadcast(msg)
		}
		if turnComplete || (streamData.ConversationState != nil && !streamData.ConversationState.Working) {
			t.completeTurn()
		}
	}
}

func (t *Topic) drainPrompts() {
	for {
		prompt, ok := t.PromptQueue.WaitForNext(t.runtimeCtx)
		if !ok {
			return
		}

		waitCh := t.beginTurn()
		if err := t.refreshWorkspaceTools(t.runtimeCtx); err != nil {
			t.abortTurn()
			t.WSHub.Broadcast(workspaceWSMessage{Type: "error", Data: err.Error()})
			continue
		}

		t.WSHub.Broadcast(workspaceWSMessage{Type: "system", Data: "thinking..."})

		modelID := conversationModelID(*t.Conversation, t.Config.ModelID)
		llmService, err := t.server.llmManager.GetService(modelID)
		if err != nil {
			t.abortTurn()
			t.WSHub.Broadcast(workspaceWSMessage{Type: "error", Data: err.Error()})
			continue
		}

		userMessage := llm.Message{
			Role: llm.MessageRoleUser,
			Content: []llm.Content{
				{Type: llm.ContentTypeText, Text: prompt.Text},
			},
		}

		if _, err := t.Manager.AcceptUserMessage(t.runtimeCtx, llmService, modelID, userMessage); err != nil {
			t.abortTurn()
			t.WSHub.Broadcast(workspaceWSMessage{Type: "error", Data: err.Error()})
			continue
		}

		if !t.waitForTurnEnd(waitCh) {
			return
		}
	}
}

func (t *Topic) refreshWorkspaceTools(ctx context.Context) error {
	workspaceTools, err := t.server.buildTopicWorkspaceTools(ctx, t.Name)
	if err != nil {
		t.logger.Error("Failed to refresh workspace tools", "error", err)
		return err
	}
	t.Manager.SetExtraTools(workspaceTools)
	return nil
}

func (t *Topic) beginTurn() <-chan struct{} {
	t.turnMu.Lock()
	defer t.turnMu.Unlock()
	t.turnDone = make(chan struct{})
	return t.turnDone
}

func (t *Topic) completeTurn() {
	t.turnMu.Lock()
	defer t.turnMu.Unlock()
	if t.turnDone != nil {
		close(t.turnDone)
		t.turnDone = nil
	}
}

func (t *Topic) abortTurn() {
	t.turnMu.Lock()
	defer t.turnMu.Unlock()
	if t.turnDone != nil {
		close(t.turnDone)
		t.turnDone = nil
	}
}

func (t *Topic) waitForTurnEnd(waitCh <-chan struct{}) bool {
	select {
	case <-t.runtimeCtx.Done():
		return false
	case <-waitCh:
		return true
	}
}
