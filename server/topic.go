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
	injectSeq int64
	eventSeq  int64

	injectMu       sync.Mutex
	pendingInjects map[string]workspacePendingInject

	turnStatusMu      sync.Mutex
	pendingTurnStatus map[string]string

	approvalMu       sync.Mutex
	pendingApprovals map[string]chan workspaceApprovalResponse
}

type workspacePendingInject struct {
	InjectID string
	PromptID string
	SenderID string
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

	workspaceTools, err := tm.server.buildTopicWorkspaceTools(ctx, topicName)
	if err != nil {
		return nil, topicConversationExisting, err
	}

	manager, err := tm.server.getOrCreateConversationManagerConfigured(ctx, conversation.ConversationID, "", func(cm *ConversationManager) error {
		cm.SetExtraTools(workspaceTools)
		return nil
	})
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
		Name:              cfg.Name,
		Config:            cfg,
		Conversation:      conversation,
		Manager:           manager,
		WSHub:             NewWSHub(),
		PromptQueue:       NewPromptQueue(),
		server:            server,
		logger:            logger,
		runtimeCtx:        runtimeCtx,
		runtimeCancel:     runtimeCancel,
		pendingInjects:    make(map[string]workspacePendingInject),
		pendingTurnStatus: make(map[string]string),
		pendingApprovals:  make(map[string]chan workspaceApprovalResponse),
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

func (t *Topic) EnqueuePrompt(promptID, text, senderID string, position *int) QueuedPrompt {
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
	queuePosition := 0
	if position != nil && *position == 0 {
		queuePosition = t.PromptQueue.EnqueueFront(prompt)
	} else {
		queuePosition = t.PromptQueue.Enqueue(prompt)
	}
	t.broadcastQueueEvent(workspaceWSMessage{
		Type:        "prompt_status",
		PromptID:    prompt.PromptID,
		Status:      string(PromptStatusAccepted),
		Data:        prompt.Text,
		Position:    queuePosition,
		SubmittedBy: workspaceParticipantRef(prompt.SenderID),
	})
	if queued {
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:        "prompt_status",
			PromptID:    prompt.PromptID,
			Status:      string(PromptStatusQueued),
			Position:    queuePosition,
			SubmittedBy: workspaceParticipantRef(prompt.SenderID),
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
	translator := newWorkspaceTranslatorState()

	for {
		streamData, ok := next()
		if !ok {
			return
		}

		messages, turnComplete := translateWorkspaceWSMessages(translator, streamData)
		doneStatus := ""
		for _, msg := range messages {
			if msg.PromptID == "" {
				msg.PromptID = t.PromptQueue.ActivePromptID()
			}
			if msg.Type == "done" {
				doneStatus = msg.Status
			}
			t.broadcastWSMessage(msg)
		}
		if turnComplete || (streamData.ConversationState != nil && !streamData.ConversationState.Working) {
			t.completeTurn(doneStatus)
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
			Type:        "prompt_status",
			PromptID:    prompt.PromptID,
			Status:      string(PromptStatusStarted),
			SubmittedBy: workspaceParticipantRef(prompt.SenderID),
		})
		t.broadcastQueueSnapshot()
		if err := t.refreshWorkspaceTools(t.runtimeCtx); err != nil {
			t.abortTurn()
			t.broadcastWSMessage(workspaceWSMessage{Type: "error", Data: err.Error()})
			continue
		}

		t.broadcastWSMessage(workspaceWSMessage{Type: "system", Data: "thinking...", PromptID: prompt.PromptID})

		modelID := conversationModelID(*t.Conversation, t.Config.ModelID)
		llmService, err := t.server.llmManager.GetService(modelID)
		if err != nil {
			t.abortTurn()
			t.broadcastWSMessage(workspaceWSMessage{Type: "error", Data: err.Error()})
			continue
		}

		userMessage := llm.Message{
			Role: llm.MessageRoleUser,
			Content: []llm.Content{
				{Type: llm.ContentTypeText, Text: prompt.Text},
			},
		}

		userData := workspacePromptUserData{
			SubmittedBy: workspaceParticipantRef(prompt.SenderID),
		}
		if _, err := t.Manager.AcceptUserMessageWithMetadata(t.runtimeCtx, llmService, modelID, userMessage, userData, nil); err != nil {
			t.abortTurn()
			t.broadcastWSMessage(workspaceWSMessage{Type: "error", Data: err.Error()})
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
		resp.Entries = append(resp.Entries, workspaceQueueEntryFromPrompt(prompt, i+1))
	}
	return resp
}

func (t *Topic) UpdateQueuedPrompt(promptID, senderID, text string) error {
	updated, position, err := t.PromptQueue.Update(promptID, senderID, text)
	if err != nil {
		return err
	}
	t.broadcastQueueEvent(workspaceWSMessage{
		Type:        "queue_entry_updated",
		PromptID:    updated.PromptID,
		Data:        updated.Text,
		Position:    position,
		SubmittedBy: workspaceParticipantRef(updated.SenderID),
	})
	t.broadcastQueueSnapshot()
	return nil
}

func (t *Topic) MoveQueuedPrompt(promptID, senderID, direction string) error {
	moved, position, err := t.PromptQueue.Move(promptID, senderID, direction)
	if err != nil {
		return err
	}
	t.broadcastQueueEvent(workspaceWSMessage{
		Type:        "queue_entry_moved",
		PromptID:    moved.PromptID,
		Direction:   direction,
		Position:    position,
		SubmittedBy: workspaceParticipantRef(moved.SenderID),
	})
	t.broadcastQueueSnapshot()
	return nil
}

func (t *Topic) CancelQueuedPrompt(promptID, senderID string) error {
	removed, _, err := t.PromptQueue.Cancel(promptID, senderID)
	if err != nil {
		return err
	}
	t.broadcastQueueEvent(workspaceWSMessage{
		Type:        "prompt_status",
		PromptID:    removed.PromptID,
		Status:      string(PromptStatusCancelled),
		SubmittedBy: workspaceParticipantRef(removed.SenderID),
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
			Type:        "prompt_status",
			PromptID:    prompt.PromptID,
			Status:      string(PromptStatusCancelled),
			SubmittedBy: workspaceParticipantRef(prompt.SenderID),
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

func (t *Topic) completeTurn(doneStatus string) {
	activePrompt, hasActive := t.PromptQueue.Active()
	if doneStatus == "" && hasActive {
		doneStatus = t.consumePendingTurnStatus(activePrompt.PromptID)
	}
	status := PromptStatusCompleted
	rejectReason := "turn_ended_before_delivery"
	switch doneStatus {
	case "failed":
		status = PromptStatusFailed
	case "cancelled", "interrupted":
		status = PromptStatusCancelled
		if doneStatus == "interrupted" {
			rejectReason = "turn_interrupted"
		}
	}
	if completed, ok := t.PromptQueue.CompleteActive(status); ok {
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:        "prompt_status",
			PromptID:    completed.PromptID,
			Status:      string(status),
			SubmittedBy: workspaceParticipantRef(completed.SenderID),
		})
		t.rejectPendingInjectsForPrompt(completed.PromptID, rejectReason)
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
			Type:        "prompt_status",
			PromptID:    failed.PromptID,
			Status:      string(PromptStatusFailed),
			SubmittedBy: workspaceParticipantRef(failed.SenderID),
		})
		t.rejectPendingInjectsForPrompt(failed.PromptID, "turn_failed_before_delivery")
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

func (t *Topic) HasActiveTurn() bool {
	if !t.Manager.IsAgentWorking() {
		return false
	}
	_, ok := t.PromptQueue.Active()
	return ok
}

func (t *Topic) InjectMessage(injectID, text, senderID string) (workspaceWSMessage, error) {
	activePrompt, ok := t.PromptQueue.Active()
	if !ok || !t.Manager.IsAgentWorking() {
		return workspaceWSMessage{
			Type:        "inject_status",
			InjectID:    injectID,
			PromptID:    activePrompt.PromptID,
			Status:      "rejected",
			Reason:      "no_active_turn",
			SubmittedBy: workspaceParticipantRef(senderID),
		}, fmt.Errorf("no active turn")
	}
	if injectID == "" {
		injectID = t.nextInjectID()
	}

	userData := workspacePromptUserData{
		SubmittedBy: workspaceParticipantRef(senderID),
	}
	message := llm.Message{
		Role: llm.MessageRoleUser,
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: text},
		},
	}

	modelID := conversationModelID(*t.Conversation, t.Config.ModelID)
	llmService, err := t.server.llmManager.GetService(modelID)
	if err != nil {
		return workspaceWSMessage{}, err
	}

	t.injectMu.Lock()
	t.pendingInjects[injectID] = workspacePendingInject{
		InjectID: injectID,
		PromptID: activePrompt.PromptID,
		SenderID: senderID,
	}
	t.injectMu.Unlock()

	_, err = t.Manager.AcceptUserMessageWithMetadata(t.runtimeCtx, llmService, modelID, message, userData, func() {
		t.markInjectDelivered(injectID)
	})
	if err != nil {
		t.rejectInject(injectID, "inject_failed")
		return workspaceWSMessage{}, err
	}

	accepted := workspaceWSMessage{
		Type:        "inject_status",
		InjectID:    injectID,
		PromptID:    activePrompt.PromptID,
		Status:      "accepted",
		SubmittedBy: workspaceParticipantRef(senderID),
	}
	t.broadcastQueueEvent(accepted)
	return accepted, nil
}

func (t *Topic) InterruptTurn(reason, senderID string) (workspaceWSMessage, error) {
	activePrompt, ok := t.PromptQueue.Active()
	if !ok || !t.Manager.IsAgentWorking() {
		return workspaceWSMessage{}, fmt.Errorf("no active turn")
	}
	t.rememberPendingTurnStatus(activePrompt.PromptID, "interrupted")

	doneMeta := workspaceDoneUserData{
		Status:        "interrupted",
		Reason:        reason,
		InterruptedBy: workspaceParticipantRef(senderID),
	}
	if err := t.Manager.CancelConversationWithMetadata(t.runtimeCtx, doneMeta); err != nil {
		return workspaceWSMessage{}, err
	}
	return workspaceWSMessage{
		Type:          "done",
		PromptID:      activePrompt.PromptID,
		Status:        "interrupted",
		Reason:        reason,
		InterruptedBy: workspaceParticipantRef(senderID),
	}, nil
}

func (t *Topic) nextPromptID() string {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	t.promptSeq++
	return fmt.Sprintf("p_%s_%d", t.Conversation.ConversationID, t.promptSeq)
}

func (t *Topic) nextInjectID() string {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	t.injectSeq++
	return fmt.Sprintf("inj_%s_%d", t.Conversation.ConversationID, t.injectSeq)
}

func (t *Topic) nextEventMeta() (string, string) {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	t.eventSeq++
	return fmt.Sprintf("e_%s_%d", t.Conversation.ConversationID, t.eventSeq), time.Now().UTC().Format(time.RFC3339)
}

func (t *Topic) broadcastQueueEvent(msg workspaceWSMessage) {
	t.broadcastWSMessage(msg)
}

func (t *Topic) broadcastQueueSnapshot() {
	snapshot := t.QueueSnapshot()
	t.broadcastWSMessage(workspaceWSMessage{
		Type:           "queue_snapshot",
		SessionID:      snapshot.SessionID,
		ActivePromptID: snapshot.ActivePromptID,
		Entries:        snapshot.Entries,
	})
}

func (t *Topic) broadcastWSMessage(msg workspaceWSMessage) {
	t.WSHub.Broadcast(t.stampWSMessage(msg))
}

func (t *Topic) sendWSMessage(ctx context.Context, outCh chan<- workspaceWSMessage, msg workspaceWSMessage) bool {
	return sendWorkspaceWSMessage(ctx, outCh, t.stampWSMessage(msg))
}

func (t *Topic) stampWSMessage(msg workspaceWSMessage) workspaceWSMessage {
	if msg.Type == "connected" {
		return msg
	}
	if msg.EventID == "" {
		msg.EventID, msg.Timestamp = t.nextEventMeta()
	}
	return msg
}

func (t *Topic) markInjectDelivered(injectID string) {
	t.injectMu.Lock()
	pending, ok := t.pendingInjects[injectID]
	if ok {
		delete(t.pendingInjects, injectID)
	}
	t.injectMu.Unlock()

	if !ok {
		return
	}

	t.broadcastQueueEvent(workspaceWSMessage{
		Type:        "inject_status",
		InjectID:    injectID,
		PromptID:    pending.PromptID,
		Status:      "delivered",
		SubmittedBy: workspaceParticipantRef(pending.SenderID),
	})
}

func (t *Topic) rejectInject(injectID, reason string) {
	t.injectMu.Lock()
	pending, ok := t.pendingInjects[injectID]
	if ok {
		delete(t.pendingInjects, injectID)
	}
	t.injectMu.Unlock()

	if !ok {
		return
	}

	t.broadcastQueueEvent(workspaceWSMessage{
		Type:        "inject_status",
		InjectID:    injectID,
		PromptID:    pending.PromptID,
		Status:      "rejected",
		Reason:      reason,
		SubmittedBy: workspaceParticipantRef(pending.SenderID),
	})
}

func (t *Topic) rejectPendingInjectsForPrompt(promptID, reason string) {
	t.injectMu.Lock()
	rejected := make([]workspacePendingInject, 0)
	for injectID, pending := range t.pendingInjects {
		if pending.PromptID != promptID {
			continue
		}
		rejected = append(rejected, pending)
		delete(t.pendingInjects, injectID)
	}
	t.injectMu.Unlock()

	for _, pending := range rejected {
		t.broadcastQueueEvent(workspaceWSMessage{
			Type:        "inject_status",
			InjectID:    pending.InjectID,
			PromptID:    pending.PromptID,
			Status:      "rejected",
			Reason:      reason,
			SubmittedBy: workspaceParticipantRef(pending.SenderID),
		})
	}
}

func (t *Topic) rememberPendingTurnStatus(promptID, status string) {
	if promptID == "" || status == "" {
		return
	}
	t.turnStatusMu.Lock()
	t.pendingTurnStatus[promptID] = status
	t.turnStatusMu.Unlock()
}

func (t *Topic) consumePendingTurnStatus(promptID string) string {
	if promptID == "" {
		return ""
	}
	t.turnStatusMu.Lock()
	status := t.pendingTurnStatus[promptID]
	delete(t.pendingTurnStatus, promptID)
	t.turnStatusMu.Unlock()
	return status
}

func workspaceQueueEntryFromPrompt(prompt QueuedPrompt, position int) workspaceQueueEntry {
	return workspaceQueueEntry{
		PromptID:  prompt.PromptID,
		Status:    string(prompt.Status),
		Text:      prompt.Text,
		CreatedAt: prompt.QueuedAt.Format(time.RFC3339),
		Position:  position,
		SubmittedBy: workspaceSubjectRef{
			Kind: "participant",
			ID:   prompt.SenderID,
		},
	}
}
