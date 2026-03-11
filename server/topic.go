package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	dbpkg "shelley.exe.dev/db"
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

	runOutcomeMu      sync.Mutex
	pendingRunOutcome map[string]workspaceDoneUserData

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

type topicTurnState struct {
	activePrompt    QueuedPrompt
	hasActivePrompt bool
	managerWorking  bool
	turnInFlight    bool
	queuedCount     int
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
	if err := topic.seedPromptSequence(ctx); err != nil {
		topic.Close()
		return nil, topicConversationExisting, err
	}
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
		pendingRunOutcome: make(map[string]workspaceDoneUserData),
		pendingApprovals:  make(map[string]chan workspaceApprovalResponse),
	}
}

func (t *Topic) start() {
	go t.forwardStream()
	go t.drainPrompts()
}

func (t *Topic) seedPromptSequence(ctx context.Context) error {
	messages, err := t.server.db.ListMessages(ctx, t.Conversation.ConversationID)
	if err != nil {
		return err
	}

	var maxPromptSeq int64
	for _, msg := range messages {
		promptMeta, ok := parseWorkspacePromptUserData(msg.UserData)
		if !ok {
			continue
		}
		seq, ok := workspacePromptSequence(promptMeta.RunID, t.Conversation.ConversationID)
		if ok && seq > maxPromptSeq {
			maxPromptSeq = seq
		}
	}

	t.metaMu.Lock()
	t.promptSeq = maxPromptSeq
	t.metaMu.Unlock()
	return nil
}

func workspacePromptSequence(runID, conversationID string) (int64, bool) {
	prefix := "p_" + conversationID + "_"
	if !strings.HasPrefix(runID, prefix) {
		return 0, false
	}
	seq, err := strconv.ParseInt(strings.TrimPrefix(runID, prefix), 10, 64)
	if err != nil || seq < 0 {
		return 0, false
	}
	return seq, true
}

func (t *Topic) Close() {
	t.runtimeCancel()
	t.WSHub.Close()
}

func (t *Topic) ClientCount() int {
	return t.WSHub.Size()
}

func (t *Topic) IsBusy() bool {
	state := t.currentTurnState("busy_check")
	return state.turnInFlight || state.managerWorking || state.queuedCount > 0 || state.hasActivePrompt
}

func (t *Topic) EnqueuePrompt(promptID, text string, submittedBy workspaceSubjectRef, position *int) QueuedPrompt {
	if promptID == "" {
		promptID = t.nextPromptID()
	}
	prompt := QueuedPrompt{
		PromptID:    promptID,
		Text:        text,
		SubmittedBy: submittedBy,
		QueuedAt:    time.Now().UTC(),
	}
	state := t.currentTurnState("submit")
	startIfIdle := !state.turnInFlight && !state.managerWorking
	submitted, started, queuePosition := t.PromptQueue.Submit(prompt, position != nil && *position == 0, startIfIdle)
	if started {
		t.broadcastTopicState("run_started")
		return submitted
	}

	run := workspaceRunStateFromPrompt(submitted, string(PromptStatusQueued), queuePosition)
	t.broadcastRunUpdated(run)
	t.broadcastTopicState("run_queued")
	return submitted
}

func (t *Topic) ActiveRun(source string) *workspaceRunState {
	state := t.currentTurnState(source)
	if state.hasActivePrompt && (state.turnInFlight || state.managerWorking) {
		return &workspaceRunState{
			RunID:         state.activePrompt.PromptID,
			State:         "running",
			Interruptible: state.managerWorking,
			SubmittedBy:   workspaceParticipantRef(state.activePrompt.SubmittedBy),
		}
	}
	return nil
}

func (t *Topic) TopicState(source string) workspaceTopicState {
	snapshot := t.PromptQueue.Snapshot()
	queue := make([]workspaceRunState, 0, len(snapshot.Entries))
	for i, prompt := range snapshot.Entries {
		queue = append(queue, workspaceRunStateFromPrompt(prompt, string(PromptStatusQueued), i+1))
	}
	return workspaceTopicState{
		Name:      t.Name,
		ActiveRun: t.ActiveRun(source),
		Queue:     queue,
	}
}

func (t *Topic) TopicStateMessage(source string) workspaceWSMessage {
	topicState := t.TopicState(source)
	return workspaceWSMessage{
		Type:      "topic_state",
		ActiveRun: topicState.ActiveRun,
		Queue:     topicState.Queue,
	}
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

		messages, outcome, turnComplete := translateWorkspaceWSMessages(translator, streamData)
		for _, msg := range messages {
			if msg.RunID == "" {
				msg.RunID = t.PromptQueue.ActivePromptID()
			}
			t.broadcastWSMessage(msg)
		}
		if turnComplete {
			t.completeTurn(outcome)
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
		t.broadcastRunUpdated(workspaceRunState{
			RunID:         prompt.PromptID,
			State:         "running",
			Text:          prompt.Text,
			SubmittedBy:   workspaceParticipantRef(prompt.SubmittedBy),
			Interruptible: false,
		})
		t.broadcastTopicState("run_started")
		if err := t.refreshWorkspaceTools(t.runtimeCtx); err != nil {
			t.abortTurn()
			t.broadcastWSMessage(workspaceWSMessage{Type: "error", Data: err.Error()})
			continue
		}

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
			RunID:       prompt.PromptID,
			SubmittedBy: workspaceParticipantRef(prompt.SubmittedBy),
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

func (t *Topic) UpdateQueuedPrompt(promptID, senderID, text string) error {
	updated, position, err := t.PromptQueue.Update(promptID, senderID, text)
	if err != nil {
		return err
	}
	t.broadcastRunUpdated(workspaceRunStateFromPrompt(updated, string(PromptStatusQueued), position))
	t.broadcastTopicState("run_updated")
	return nil
}

func (t *Topic) MoveQueuedPrompt(promptID, senderID, direction string) error {
	moved, position, err := t.PromptQueue.Move(promptID, senderID, direction)
	if err != nil {
		return err
	}
	t.broadcastRunUpdated(workspaceRunStateFromPrompt(moved, string(PromptStatusQueued), position))
	t.broadcastTopicState("run_moved")
	return nil
}

func (t *Topic) CancelQueuedPrompt(promptID, senderID string) error {
	removed, _, err := t.PromptQueue.Cancel(promptID, senderID)
	if err != nil {
		return err
	}
	run := workspaceRunStateFromPrompt(removed, string(PromptStatusCancelled), 0)
	run.Reason = "cancelled_by_submitter"
	t.broadcastRunUpdated(run)
	t.broadcastTopicState("run_cancelled")
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
		run := workspaceRunStateFromPrompt(prompt, string(PromptStatusCancelled), 0)
		run.Reason = "cancelled_by_submitter"
		t.broadcastRunUpdated(run)
	}
	t.broadcastTopicState("queue_cleared")
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

func (t *Topic) completeTurn(outcome workspaceDoneUserData) {
	activePrompt, hasActive := t.PromptQueue.Active()
	if outcome.Status == "" && hasActive {
		outcome = t.consumePendingRunOutcome(activePrompt.PromptID)
	}
	if outcome.Status == "" {
		outcome.Status = "completed"
	}
	status := PromptStatusCompleted
	switch outcome.Status {
	case "failed":
		status = PromptStatusFailed
	case "cancelled", "interrupted":
		status = PromptStatusCancelled
	}
	if completed, ok := t.PromptQueue.CompleteActive(status); ok {
		run := workspaceRunStateFromPrompt(completed, string(status), 0)
		run.Reason = outcome.Reason
		run.InterruptedBy = outcome.InterruptedBy
		t.broadcastRunUpdated(run)
		t.broadcastTopicState("run_completed")
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
		t.broadcastRunUpdated(workspaceRunStateFromPrompt(failed, string(PromptStatusFailed), 0))
		t.broadcastTopicState("run_failed")
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
	state := t.currentTurnState("has_active_turn")
	return state.managerWorking && state.hasActivePrompt
}

func (t *Topic) InjectMessage(injectID, text string, submittedBy workspaceSubjectRef) (workspaceWSMessage, error) {
	state := t.currentTurnState("inject")
	if !state.hasActivePrompt || !state.managerWorking {
		return workspaceWSMessage{
			Type:     "inject_status",
			InjectID: injectID,
			RunID:    state.activePrompt.PromptID,
			Status:   "rejected",
			Reason:   "no_active_run",
		}, fmt.Errorf("no active turn")
	}
	activePrompt := state.activePrompt
	if injectID == "" {
		injectID = t.nextInjectID()
	}

	userData := workspacePromptUserData{
		RunID:       activePrompt.PromptID,
		SubmittedBy: workspaceParticipantRef(submittedBy),
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

	_, err = t.Manager.AcceptUserMessageWithMetadata(t.runtimeCtx, llmService, modelID, message, userData, nil)
	if err != nil {
		return workspaceWSMessage{
			Type:     "inject_status",
			InjectID: injectID,
			RunID:    activePrompt.PromptID,
			Status:   "rejected",
			Reason:   "inject_failed",
		}, err
	}

	return workspaceWSMessage{
		Type:     "inject_status",
		InjectID: injectID,
		RunID:    activePrompt.PromptID,
		Status:   "accepted",
	}, nil
}

func (t *Topic) InterruptTurn(reason string, interruptedBy workspaceSubjectRef) (workspaceWSMessage, error) {
	state := t.currentTurnState("interrupt")
	if !state.hasActivePrompt || !state.managerWorking {
		return workspaceWSMessage{
			Type:   "interrupt_status",
			RunID:  state.activePrompt.PromptID,
			Status: "rejected",
			Reason: "no_active_run",
		}, fmt.Errorf("no active turn")
	}
	activePrompt := state.activePrompt
	t.rememberPendingRunOutcome(activePrompt.PromptID, workspaceDoneUserData{
		Status:        "interrupted",
		Reason:        reason,
		InterruptedBy: workspaceParticipantRef(interruptedBy),
	})

	doneMeta := workspaceDoneUserData{
		Status:        "interrupted",
		Reason:        reason,
		InterruptedBy: workspaceParticipantRef(interruptedBy),
	}
	if err := t.Manager.CancelConversationWithMetadata(t.runtimeCtx, doneMeta); err != nil {
		return workspaceWSMessage{}, err
	}
	return workspaceWSMessage{
		Type:   "interrupt_status",
		RunID:  activePrompt.PromptID,
		Status: "accepted",
	}, nil
}

func (t *Topic) nextPromptID() string {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()
	t.promptSeq++
	return fmt.Sprintf("p_%s_%d", t.Conversation.ConversationID, t.promptSeq)
}

func (t *Topic) currentTurnState(source string) topicTurnState {
	t.reconcileStaleActivePrompt(source)

	activePrompt, hasActivePrompt := t.PromptQueue.Active()

	t.turnMu.Lock()
	turnInFlight := t.turnDone != nil
	t.turnMu.Unlock()

	return topicTurnState{
		activePrompt:    activePrompt,
		hasActivePrompt: hasActivePrompt,
		managerWorking:  t.Manager.IsAgentWorking(),
		turnInFlight:    turnInFlight,
		queuedCount:     t.PromptQueue.Len(),
	}
}

func (t *Topic) reconcileStaleActivePrompt(source string) {
	activePrompt, hasActivePrompt := t.PromptQueue.Active()
	if !hasActivePrompt || t.Manager.IsAgentWorking() {
		return
	}

	outcome, ok := t.staleTurnOutcome(activePrompt.PromptID)
	if !ok {
		return
	}
	t.logger.Warn(
		"reconciling stale active prompt",
		"source", source,
		"topic", t.Name,
		"conversationID", t.Conversation.ConversationID,
		"promptID", activePrompt.PromptID,
		"status", outcome.Status,
	)
	t.completeTurn(outcome)
}

func (t *Topic) staleTurnOutcome(promptID string) (workspaceDoneUserData, bool) {
	if outcome := t.consumePendingRunOutcome(promptID); outcome.Status != "" || outcome.Reason != "" || outcome.InterruptedBy != nil {
		return outcome, true
	}

	messages, err := t.server.db.ListMessages(context.Background(), t.Conversation.ConversationID)
	if err != nil || len(messages) == 0 {
		return workspaceDoneUserData{}, false
	}

	var promptUserSeq int64
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		if msg.Type != string(dbpkg.MessageTypeUser) {
			continue
		}
		promptMeta, ok := parseWorkspacePromptUserData(msg.UserData)
		if ok && promptMeta.RunID == promptID {
			promptUserSeq = msg.SequenceID
			break
		}
	}
	if promptUserSeq == 0 {
		return workspaceDoneUserData{}, false
	}

	latest := messages[len(messages)-1]
	if latest.SequenceID <= promptUserSeq {
		return workspaceDoneUserData{}, false
	}

	if latest.Type == string(dbpkg.MessageTypeError) {
		return workspaceDoneUserData{Status: "failed"}, true
	}

	doneMeta, hasDoneMeta := parseWorkspaceDoneUserData(latest.UserData)
	if hasDoneMeta && doneMeta.Status != "" {
		return doneMeta, true
	}

	if latest.LlmData == nil {
		return workspaceDoneUserData{}, false
	}

	var llmMsg llm.Message
	if err := json.Unmarshal([]byte(*latest.LlmData), &llmMsg); err != nil {
		return workspaceDoneUserData{}, false
	}
	if !llmMsg.EndOfTurn {
		return workspaceDoneUserData{}, false
	}
	if llmMessageText(llmMsg) == "[Operation cancelled]" {
		return workspaceDoneUserData{Status: "cancelled"}, true
	}
	return workspaceDoneUserData{Status: "completed"}, true
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

func (t *Topic) broadcastRunUpdated(run workspaceRunState) {
	t.broadcastWSMessage(workspaceWSMessage{
		Type:          "run_updated",
		RunID:         run.RunID,
		State:         run.State,
		Text:          run.Text,
		Position:      run.Position,
		Reason:        run.Reason,
		SubmittedBy:   run.SubmittedBy,
		InterruptedBy: run.InterruptedBy,
	})
}

func (t *Topic) broadcastTopicState(source string) {
	t.broadcastWSMessage(t.TopicStateMessage(source))
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

func (t *Topic) rememberPendingRunOutcome(promptID string, outcome workspaceDoneUserData) {
	if promptID == "" || (outcome.Status == "" && outcome.Reason == "" && outcome.InterruptedBy == nil) {
		return
	}
	t.runOutcomeMu.Lock()
	t.pendingRunOutcome[promptID] = outcome
	t.runOutcomeMu.Unlock()
}

func (t *Topic) consumePendingRunOutcome(promptID string) workspaceDoneUserData {
	if promptID == "" {
		return workspaceDoneUserData{}
	}
	t.runOutcomeMu.Lock()
	outcome := t.pendingRunOutcome[promptID]
	delete(t.pendingRunOutcome, promptID)
	t.runOutcomeMu.Unlock()
	return outcome
}

func workspaceRunStateFromPrompt(prompt QueuedPrompt, state string, position int) workspaceRunState {
	return workspaceRunState{
		RunID:       prompt.PromptID,
		State:       state,
		Text:        prompt.Text,
		CreatedAt:   prompt.QueuedAt.Format(time.RFC3339),
		Position:    position,
		SubmittedBy: workspaceParticipantRef(prompt.SubmittedBy),
	}
}

func workspaceApprovalActor(submittedBy workspaceSubjectRef, fallback string) string {
	if submittedBy.DisplayName != "" {
		return submittedBy.DisplayName
	}
	if submittedBy.ID != "" {
		return submittedBy.ID
	}
	return fallback
}
