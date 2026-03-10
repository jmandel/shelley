package server

import (
	"context"
	"fmt"
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

	metaMu    sync.Mutex
	promptSeq int64
	eventSeq  int64

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
	return turnActive || t.Manager.IsAgentWorking() || t.PromptQueue.Len() > 0 || t.PromptQueue.ActivePromptID() != ""
}

func (t *Topic) EnqueuePrompt(promptID, text, senderID string) QueuedPrompt {
	if promptID == "" {
		promptID = t.nextPromptID()
	}
	queued := t.IsBusy()
	prompt := QueuedPrompt{
		PromptID: promptID,
		Text:     text,
		SenderID: senderID,
		QueuedAt: time.Now().UTC(),
	}
	position := t.PromptQueue.Enqueue(prompt)
	t.broadcastQueueEvent(workspaceWSMessage{
		Type:     "prompt_status",
		PromptID: prompt.PromptID,
		Status:   string(PromptStatusAccepted),
		Position: position,
		SubmittedBy: &workspaceSubjectRef{
			Kind: "participant",
			ID:   prompt.SenderID,
		},
	})
	if queued {
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:     "prompt_status",
			PromptID: prompt.PromptID,
			Status:   string(PromptStatusQueued),
			Position: position,
			SubmittedBy: &workspaceSubjectRef{
				Kind: "participant",
				ID:   prompt.SenderID,
			},
		})
	}
	t.broadcastQueueSnapshot()
	return prompt
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
			if msg.PromptID == "" {
				msg.PromptID = t.PromptQueue.ActivePromptID()
			}
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
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:     "prompt_status",
			PromptID: prompt.PromptID,
			Status:   string(PromptStatusStarted),
			SubmittedBy: &workspaceSubjectRef{
				Kind: "participant",
				ID:   prompt.SenderID,
			},
		})
		t.broadcastQueueSnapshot()
		if err := t.refreshWorkspaceTools(t.runtimeCtx); err != nil {
			t.abortTurn()
			t.WSHub.Broadcast(workspaceWSMessage{Type: "error", Data: err.Error()})
			continue
		}

		t.WSHub.Broadcast(workspaceWSMessage{Type: "system", Data: "thinking...", PromptID: prompt.PromptID})

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

func (t *Topic) QueueSnapshot() workspaceQueueSnapshot {
	snapshot := t.PromptQueue.Snapshot()
	resp := workspaceQueueSnapshot{
		SessionID:      t.Conversation.ConversationID,
		ActivePromptID: t.PromptQueue.ActivePromptID(),
		Entries:        make([]workspaceQueueEntry, 0, len(snapshot.Entries)),
	}
	for i, prompt := range snapshot.Entries {
		resp.Entries = append(resp.Entries, workspaceQueueEntry{
			PromptID:  prompt.PromptID,
			Status:    string(prompt.Status),
			Text:      prompt.Text,
			CreatedAt: prompt.QueuedAt.Format(time.RFC3339),
			Position:  i + 1,
			SubmittedBy: workspaceSubjectRef{
				Kind: "participant",
				ID:   prompt.SenderID,
			},
		})
	}
	return resp
}

func (t *Topic) CancelQueuedPrompt(promptID, senderID string) error {
	removed, _, err := t.PromptQueue.Cancel(promptID, senderID)
	if err != nil {
		return err
	}
	t.broadcastQueueEvent(workspaceWSMessage{
		Type:     "prompt_status",
		PromptID: removed.PromptID,
		Status:   string(PromptStatusCancelled),
		SubmittedBy: &workspaceSubjectRef{
			Kind: "participant",
			ID:   removed.SenderID,
		},
	})
	t.broadcastQueueEvent(workspaceWSMessage{
		Type:     "queue_entry_removed",
		PromptID: removed.PromptID,
		Reason:   "cancelled_by_submitter",
	})
	t.broadcastQueueSnapshot()
	return nil
}

func (t *Topic) ClearQueuedPromptsForSender(senderID string) []string {
	removed := t.PromptQueue.CancelMine(senderID)
	if len(removed) == 0 {
		return nil
	}
	removedIDs := make([]string, 0, len(removed))
	for _, prompt := range removed {
		removedIDs = append(removedIDs, prompt.PromptID)
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:     "prompt_status",
			PromptID: prompt.PromptID,
			Status:   string(PromptStatusCancelled),
			SubmittedBy: &workspaceSubjectRef{
				Kind: "participant",
				ID:   prompt.SenderID,
			},
		})
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:     "queue_entry_removed",
			PromptID: prompt.PromptID,
			Reason:   "cancelled_by_submitter",
		})
	}
	t.broadcastQueueSnapshot()
	return removedIDs
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
	if completed, ok := t.PromptQueue.CompleteActive(PromptStatusCompleted); ok {
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:     "prompt_status",
			PromptID: completed.PromptID,
			Status:   string(PromptStatusCompleted),
			SubmittedBy: &workspaceSubjectRef{
				Kind: "participant",
				ID:   completed.SenderID,
			},
		})
		t.broadcastQueueSnapshot()
	}
	t.turnMu.Lock()
	defer t.turnMu.Unlock()
	if t.turnDone != nil {
		close(t.turnDone)
		t.turnDone = nil
	}
}

func (t *Topic) abortTurn() {
	if failed, ok := t.PromptQueue.CompleteActive(PromptStatusFailed); ok {
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:     "prompt_status",
			PromptID: failed.PromptID,
			Status:   string(PromptStatusFailed),
			SubmittedBy: &workspaceSubjectRef{
				Kind: "participant",
				ID:   failed.SenderID,
			},
		})
		t.broadcastQueueSnapshot()
	}
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

func (t *Topic) nextPromptID() string {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	t.promptSeq++
	return fmt.Sprintf("p_%s_%d", t.Conversation.ConversationID, t.promptSeq)
}

func (t *Topic) nextEventMeta() (string, string) {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	t.eventSeq++
	return fmt.Sprintf("e_%s_%d", t.Conversation.ConversationID, t.eventSeq), time.Now().UTC().Format(time.RFC3339)
}

func (t *Topic) broadcastQueueEvent(msg workspaceWSMessage) {
	msg.EventID, msg.Timestamp = t.nextEventMeta()
	t.WSHub.Broadcast(msg)
}

func (t *Topic) broadcastQueueSnapshot() {
	snapshot := t.QueueSnapshot()
	eventID, timestamp := t.nextEventMeta()
	t.WSHub.Broadcast(workspaceWSMessage{
		Type:           "queue_snapshot",
		EventID:        eventID,
		Timestamp:      timestamp,
		SessionID:      snapshot.SessionID,
		ActivePromptID: snapshot.ActivePromptID,
		Entries:        snapshot.Entries,
	})
}
