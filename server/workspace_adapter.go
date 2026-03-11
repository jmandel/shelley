package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	dbpkg "shelley.exe.dev/db"
	"shelley.exe.dev/db/generated"
	"shelley.exe.dev/llm"
	"shelley.exe.dev/slug"
)

type workspaceTopicInfo struct {
	Name       string              `json:"name"`
	Clients    int                 `json:"clients"`
	ActiveRun  *workspaceRunState  `json:"activeRun,omitempty"`
	QueuedCount int                `json:"queuedCount"`
	LogSize    int64               `json:"logSize"`
	Events     string              `json:"events,omitempty"`
	CreatedAt  string              `json:"createdAt"`
}

type workspaceTopicState struct {
	Name      string              `json:"name"`
	ActiveRun *workspaceRunState  `json:"activeRun,omitempty"`
	Queue     []workspaceRunState `json:"queue,omitempty"`
	CreatedAt string              `json:"createdAt,omitempty"`
	Events    string              `json:"events,omitempty"`
}

type workspaceRunState struct {
	RunID         string               `json:"runId"`
	State         string               `json:"state"`
	Text          string               `json:"text,omitempty"`
	CreatedAt     string               `json:"createdAt,omitempty"`
	Position      int                  `json:"position,omitempty"`
	Reason        string               `json:"reason,omitempty"`
	Interruptible bool                 `json:"interruptible,omitempty"`
	SubmittedBy   *workspaceSubjectRef `json:"submittedBy,omitempty"`
	InterruptedBy *workspaceSubjectRef `json:"interruptedBy,omitempty"`
}

type workspaceTopicCreateRequest struct {
	Name string `json:"name"`
}

type workspaceWSMessage struct {
	Type            string                `json:"type"`
	Data            string                `json:"data,omitempty"`
	Text            string                `json:"text,omitempty"`
	Topic           string                `json:"topic,omitempty"`
	ProtocolVersion string                `json:"protocolVersion,omitempty"`
	Replay          bool                  `json:"replay,omitempty"`
	EventID         string                `json:"eventId,omitempty"`
	Timestamp       string                `json:"timestamp,omitempty"`
	Actor           *workspaceSubjectRef  `json:"actor,omitempty"`
	RunID           string                `json:"runId,omitempty"`
	Role            string                `json:"role,omitempty"`
	State           string                `json:"state,omitempty"`
	InjectID        string                `json:"injectId,omitempty"`
	Position        int                   `json:"position,omitempty"`
	Reason          string                `json:"reason,omitempty"`
	Removed         []string              `json:"removed,omitempty"`
	ToolCallID      string                `json:"toolCallId,omitempty"`
	Title           string                `json:"title,omitempty"`
	Kind            string                `json:"kind,omitempty"`
	Status          string                `json:"status,omitempty"`
	Tool            string                `json:"tool,omitempty"`
	RawInput        json.RawMessage       `json:"rawInput,omitempty"`
	Action          string                `json:"action,omitempty"`
	Approvers       []string              `json:"approvers,omitempty"`
	Approved        bool                  `json:"approved,omitempty"`
	Approver        string                `json:"approver,omitempty"`
	SubmittedBy     *workspaceSubjectRef  `json:"submittedBy,omitempty"`
	InterruptedBy   *workspaceSubjectRef  `json:"interruptedBy,omitempty"`
	ActiveRun       *workspaceRunState    `json:"activeRun,omitempty"`
	Queue           []workspaceRunState   `json:"queue,omitempty"`
}

type workspacePromptMessage struct {
	Type       string `json:"type"`
	Data       string `json:"data,omitempty"`
	InjectID   string `json:"injectId,omitempty"`
	Position   *int   `json:"position,omitempty"`
	Reason     string `json:"reason,omitempty"`
	ToolCallID string `json:"toolCallId,omitempty"`
	Approved   bool   `json:"approved,omitempty"`
	Approver   string `json:"approver,omitempty"`
}

type workspaceSubjectRef struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName,omitempty"`
}

type workspaceQueueClearResponse struct {
	Removed []string `json:"removed"`
}

type workspaceQueueUpdateRequest struct {
	Data string `json:"data"`
}

type workspaceQueueMoveRequest struct {
	Direction string `json:"direction"`
}

type workspaceInjectRequest struct {
	Data string `json:"data"`
}

type workspaceInterruptRequest struct {
	Reason string `json:"reason"`
}

type topicConversationState int

const (
	topicConversationExisting topicConversationState = iota
	topicConversationCreated
	topicConversationRestored
)

func (s *Server) handleWorkspaceHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topics, err := s.workspaceTopicNames(r.Context())
	if err != nil {
		s.logger.Error("Failed to list topics for health", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":        "ok",
		"mode":          "workspace",
		"workspaceName": s.workspaceName,
		"hasApiKey":     s.defaultTopicModelID() != "",
		"topics":        topics,
	})
}

func (s *Server) handleWorkspaceTopics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleWorkspaceTopicsList(w, r)
	case http.MethodPost:
		s.handleWorkspaceTopicsCreate(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceTopicsList(w http.ResponseWriter, r *http.Request) {
	topics, err := s.workspaceTopics(r.Context(), r)
	if err != nil {
		s.logger.Error("Failed to list workspace topics", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(topics)
}

func (s *Server) handleWorkspaceTopicsCreate(w http.ResponseWriter, r *http.Request) {
	var req workspaceTopicCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	topicName, err := sanitizeTopicName(req.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	topic, state, err := s.topicManager.GetOrCreateTopic(r.Context(), topicName)
	if err != nil {
		s.logger.Error("Failed to create workspace topic", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if state == topicConversationCreated || state == topicConversationRestored {
		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: topic.Conversation,
		})
	}
	if state == topicConversationExisting {
		http.Error(w, fmt.Sprintf("topic already exists: %s", topicName), http.StatusConflict)
		return
	}

	info, err := s.workspaceTopicInfo(r.Context(), r, topicName, *topic.Conversation)
	if err != nil {
		s.logger.Error("Failed to build workspace topic info", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(info)
}

func (s *Server) handleWorkspaceTopic(w http.ResponseWriter, r *http.Request) {
	topicName, err := sanitizeTopicName(r.PathValue("name"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		topic, err := s.resolveWorkspaceTopicRuntime(r.Context(), topicName)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.Error(w, "Topic not found", http.StatusNotFound)
				return
			}
			s.logger.Error("Failed to get workspace topic", "topic", topicName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.workspaceTopicState(r, topic))
	case http.MethodDelete:
		conversation, err := s.getActiveTopicConversation(r.Context(), topicName)
		if err != nil {
			s.logger.Error("Failed to look up workspace topic for delete", "topic", topicName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if conversation == nil {
			http.Error(w, "Topic not found", http.StatusNotFound)
			return
		}

		archivedConversation, err := s.db.ArchiveConversation(r.Context(), conversation.ConversationID)
		if err != nil {
			s.logger.Error("Failed to archive workspace topic", "topic", topicName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: archivedConversation,
		})
		s.topicManager.RemoveTopicRuntime(topicName)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"name":   topicName,
			"status": "archived",
		})
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceTopicWSByName(w http.ResponseWriter, r *http.Request) {
	s.handleWorkspaceTopicWSForName(w, r, r.PathValue("name"))
}

func (s *Server) handleWorkspaceTopicWSForName(w http.ResponseWriter, r *http.Request, rawTopicName string) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	principal, ok, err := s.workspacePrincipalFromRequest(r)
	if err != nil {
		http.Error(w, "invalid authorization token", http.StatusUnauthorized)
		return
	}
	if !ok {
		http.Error(w, "authorization required", http.StatusUnauthorized)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		s.logger.Error("Failed to accept workspace websocket", "error", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	outCh := make(chan workspaceWSMessage, 64)
	go s.workspaceTopicWriter(ctx, cancel, conn, outCh)

	topicName, err := sanitizeTopicName(rawTopicName)
	if err != nil {
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
		return
	}

	topic, state, err := s.topicManager.GetOrCreateTopic(ctx, topicName)
	if err != nil {
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
		return
	}

	connectionID := fmt.Sprintf("%s-%d", topicName, time.Now().UnixNano())
	submittedBy := workspaceSubjectFromPrincipal(principal)
	topic.WSHub.Add(connectionID, outCh, cancel)
	defer topic.WSHub.Remove(connectionID)

	topic.sendWSMessage(ctx, outCh, workspaceWSMessage{
		Type:            "connected",
		Topic:           topicName,
		ProtocolVersion: workspaceProtocolVersion,
		Replay:          true,
	})

	topicStateMsg := topic.TopicStateMessage("connect")
	topicStateMsg.Replay = true
	topic.sendWSMessage(ctx, outCh, topicStateMsg)

	if state == topicConversationCreated {
		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: topic.Conversation,
		})
	}
	if state == topicConversationRestored {
		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: topic.Conversation,
		})
	}
	replayMessages, err := s.replayWorkspaceTopicMessages(ctx, topic.Conversation.ConversationID)
	if err != nil {
		topic.sendWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: "failed to replay topic history"})
	} else {
		for _, replayMsg := range replayMessages {
			replayMsg.Replay = true
			topic.sendWSMessage(ctx, outCh, replayMsg)
		}
	}

	for {
		var msg workspacePromptMessage
		if err := wsjson.Read(ctx, conn, &msg); err != nil {
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure && ctx.Err() == nil {
				s.logger.Debug("Workspace websocket read failed", "topic", topicName, "error", err)
			}
			return
		}

		switch msg.Type {
		case "prompt":
			prompt := strings.TrimSpace(msg.Data)
			if prompt == "" {
				continue
			}
			if msg.Position != nil && *msg.Position != 0 {
				topic.sendWSMessage(ctx, outCh, workspaceWSMessage{
					Type: "error",
					Data: "position must be 0 when provided",
				})
				continue
			}
			topic.EnqueuePrompt("", prompt, submittedBy, msg.Position)
		case "inject":
			injectText := strings.TrimSpace(msg.Data)
			if injectText == "" {
				topic.sendWSMessage(ctx, outCh, workspaceWSMessage{
					Type:   "inject_status",
					Status: "rejected",
					Reason: "empty_inject",
				})
				continue
			}
			if status, err := topic.InjectMessage("", injectText, submittedBy); err != nil {
				if status.Type != "" {
					topic.sendWSMessage(ctx, outCh, status)
					if status.Status == "rejected" {
						continue
					}
				}
				topic.sendWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
			} else {
				topic.sendWSMessage(ctx, outCh, status)
			}
		case "interrupt":
			reason := strings.TrimSpace(msg.Reason)
			if reason == "" {
				topic.sendWSMessage(ctx, outCh, workspaceWSMessage{Type: "interrupt_status", Status: "rejected", Reason: "missing_reason"})
				continue
			}
			if status, err := topic.InterruptTurn(reason, submittedBy); err != nil {
				if status.Type != "" {
					topic.sendWSMessage(ctx, outCh, status)
					if status.Status == "rejected" {
						continue
					}
				}
				topic.sendWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
			} else {
				topic.sendWSMessage(ctx, outCh, status)
			}
		case "approval_response":
			if msg.ToolCallID == "" {
				continue
			}
			topic.ResolveApprovalResponse(workspaceApprovalResponse{
				ToolCallID: msg.ToolCallID,
				Approved:   msg.Approved,
				Approver:   workspaceApprovalActor(submittedBy, strings.TrimSpace(msg.Approver)),
			})
		}
	}
}

func (s *Server) handleWorkspaceTopicQueueEntry(w http.ResponseWriter, r *http.Request) {
	topicName, err := sanitizeTopicName(r.PathValue("name"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	runID := strings.TrimSpace(r.PathValue("run"))
	if runID == "" {
		http.Error(w, "runId is required", http.StatusBadRequest)
		return
	}

	topic, err := s.resolveWorkspaceTopicRuntime(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Topic not found", http.StatusNotFound)
			return
		}
		s.logger.Error("Failed to resolve workspace topic runtime for queue delete", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodDelete:
		principal, ok := s.requireWorkspacePrincipal(w, r)
		if !ok {
			return
		}
		if err := topic.CancelQueuedPrompt(runID, principal.Subject); err != nil {
			writeWorkspaceQueueMutationError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPatch:
		var req workspaceQueueUpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		text := strings.TrimSpace(req.Data)
		if text == "" {
			http.Error(w, "data is required", http.StatusBadRequest)
			return
		}
		principal, ok := s.requireWorkspacePrincipal(w, r)
		if !ok {
			return
		}
		if err := topic.UpdateQueuedPrompt(runID, principal.Subject, text); err != nil {
			writeWorkspaceQueueMutationError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.workspaceTopicState(r, topic))
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceTopicQueueClearMine(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topicName, err := sanitizeTopicName(r.PathValue("name"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	topic, err := s.resolveWorkspaceTopicRuntime(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Topic not found", http.StatusNotFound)
			return
		}
		s.logger.Error("Failed to resolve workspace topic runtime for queue clear", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	principal, ok := s.requireWorkspacePrincipal(w, r)
	if !ok {
		return
	}

	removed := topic.ClearQueuedPromptsForSender(principal.Subject)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workspaceQueueClearResponse{Removed: removed})
}

func (s *Server) handleWorkspaceTopicQueueMove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topicName, err := sanitizeTopicName(r.PathValue("name"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	runID := strings.TrimSpace(r.PathValue("run"))
	if runID == "" {
		http.Error(w, "runId is required", http.StatusBadRequest)
		return
	}

	var req workspaceQueueMoveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	req.Direction = strings.TrimSpace(req.Direction)
	if req.Direction == "" {
		http.Error(w, "direction is required", http.StatusBadRequest)
		return
	}

	topic, err := s.resolveWorkspaceTopicRuntime(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Topic not found", http.StatusNotFound)
			return
		}
		s.logger.Error("Failed to resolve workspace topic runtime for queue move", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	principal, ok := s.requireWorkspacePrincipal(w, r)
	if !ok {
		return
	}

	if err := topic.MoveQueuedPrompt(runID, principal.Subject, req.Direction); err != nil {
		writeWorkspaceQueueMutationError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.workspaceTopicState(r, topic))
}

func (s *Server) handleWorkspaceTopicInject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topicName, err := sanitizeTopicName(r.PathValue("name"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req workspaceInjectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	req.Data = strings.TrimSpace(req.Data)
	if req.Data == "" {
		http.Error(w, "data is required", http.StatusBadRequest)
		return
	}

	topic, err := s.resolveWorkspaceTopicRuntime(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Topic not found", http.StatusNotFound)
			return
		}
		s.logger.Error("Failed to resolve workspace topic runtime for inject", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	injectID := topic.nextInjectID()
	principal, ok := s.requireWorkspacePrincipal(w, r)
	if !ok {
		return
	}

	accepted, err := topic.InjectMessage(injectID, req.Data, workspaceSubjectFromPrincipal(principal))
	if err != nil {
		if accepted.Status == "rejected" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(accepted)
			return
		}
		s.logger.Error("Failed to inject into topic", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"injectId": accepted.InjectID,
		"runId":    accepted.RunID,
		"status":   accepted.Status,
	})
}

func (s *Server) handleWorkspaceTopicInterrupt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topicName, err := sanitizeTopicName(r.PathValue("name"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req workspaceInterruptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Reason == "" {
		http.Error(w, "reason is required", http.StatusBadRequest)
		return
	}

	topic, err := s.resolveWorkspaceTopicRuntime(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Topic not found", http.StatusNotFound)
			return
		}
		s.logger.Error("Failed to resolve workspace topic runtime for interrupt", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	principal, ok := s.requireWorkspacePrincipal(w, r)
	if !ok {
		return
	}

	status, err := topic.InterruptTurn(req.Reason, workspaceSubjectFromPrincipal(principal))
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *Server) resolveWorkspaceTopicRuntime(ctx context.Context, topicName string) (*Topic, error) {
	conversation, err := s.getActiveTopicConversation(ctx, topicName)
	if err != nil {
		return nil, err
	}
	if conversation == nil {
		return nil, sql.ErrNoRows
	}
	topic, _, err := s.topicManager.GetOrCreateTopic(ctx, topicName)
	return topic, err
}

func (s *Server) replayWorkspaceTopicMessages(ctx context.Context, conversationID string) ([]workspaceWSMessage, error) {
	var records []generated.Message
	if err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		records, err = q.ListMessages(ctx, conversationID)
		return err
	}); err != nil {
		return nil, err
	}

	translator := newWorkspaceTranslatorState()
	messages := make([]workspaceWSMessage, 0, len(records))
	for _, record := range records {
		translated, _, _ := translateWorkspaceWSMessagesForAPIMessage(translator, APIMessage{
			Type:     record.Type,
			LlmData:  record.LlmData,
			UserData: record.UserData,
		})
		messages = append(messages, translated...)
	}
	return messages, nil
}

func (s *Server) workspaceTopics(ctx context.Context, r *http.Request) ([]workspaceTopicInfo, error) {
	topicRecords, err := s.listActiveTopicRecords(ctx)
	if err != nil {
		return nil, err
	}

	topics := make([]workspaceTopicInfo, 0, len(topicRecords))
	for _, topicRecord := range topicRecords {
		conversation, err := s.db.GetConversationByID(ctx, topicRecord.ConversationID)
		if err != nil {
			return nil, err
		}
		info, err := s.workspaceTopicInfo(ctx, r, topicRecord.TopicName, *conversation)
		if err != nil {
			return nil, err
		}
		topics = append(topics, info)
	}
	return topics, nil
}

func (s *Server) workspaceTopicNames(ctx context.Context) ([]string, error) {
	topicRecords, err := s.listActiveTopicRecords(ctx)
	if err != nil {
		return nil, err
	}

	topics := make([]string, 0, len(topicRecords))
	for _, topicRecord := range topicRecords {
		topics = append(topics, topicRecord.TopicName)
	}
	return topics, nil
}

func (s *Server) workspaceTopicInfo(ctx context.Context, r *http.Request, topicName string, conversation generated.Conversation) (workspaceTopicInfo, error) {
	logSize, err := s.countMessagesInConversation(ctx, conversation.ConversationID)
	if err != nil {
		return workspaceTopicInfo{}, err
	}

	clients := 0
	var activeRun *workspaceRunState
	queuedCount := 0
	if topic := s.topicManager.GetTopic(topicName); topic != nil {
		clients = topic.ClientCount()
		state := topic.TopicState("topic_info")
		activeRun = state.ActiveRun
		queuedCount = len(state.Queue)
	}

	createdAt := conversation.CreatedAt
	if topicRecord, err := s.lookupTopicRecord(ctx, topicName); err != nil {
		return workspaceTopicInfo{}, err
	} else if topicRecord != nil {
		createdAt = topicRecord.CreatedAt
	}

	return workspaceTopicInfo{
		Name:        topicName,
		Clients:     clients,
		ActiveRun:   activeRun,
		QueuedCount: queuedCount,
		LogSize:     logSize,
		Events:      workspaceTopicEventsURL(r, topicName),
		CreatedAt:   createdAt.Format(time.RFC3339),
	}, nil
}

func (s *Server) workspaceTopicState(r *http.Request, topic *Topic) workspaceTopicState {
	state := topic.TopicState("topic_get")
	state.CreatedAt = topic.Conversation.CreatedAt.Format(time.RFC3339)
	state.Events = workspaceTopicEventsURL(r, topic.Name)
	return state
}

func (s *Server) getActiveTopicConversation(ctx context.Context, topicName string) (*generated.Conversation, error) {
	conversation, err := s.lookupTopicConversation(ctx, topicName)
	if err != nil || conversation == nil {
		return conversation, err
	}
	if conversation.Archived {
		return nil, nil
	}
	return conversation, nil
}

func (s *Server) getOrCreateTopicConversation(ctx context.Context, topicName string) (*generated.Conversation, topicConversationState, error) {
	conversation, err := s.lookupTopicConversation(ctx, topicName)
	if err != nil {
		return nil, topicConversationExisting, err
	}
	if conversation != nil {
		if conversation.Archived {
			restored, err := s.db.UnarchiveConversation(ctx, conversation.ConversationID)
			if err != nil {
				return nil, topicConversationExisting, err
			}
			return restored, topicConversationRestored, nil
		}
		return conversation, topicConversationExisting, nil
	}

	modelID := s.defaultTopicModelID()
	var modelPtr *string
	if modelID != "" {
		modelPtr = &modelID
	}
	cwd := s.workspaceRoot
	var cwdPtr *string
	if cwd != "" {
		cwdPtr = &cwd
	}

	created, err := s.db.CreateConversation(ctx, &topicName, true, cwdPtr, modelPtr)
	if err != nil {
		// Another request may have created it between lookup and insert.
		existing, lookupErr := s.lookupTopicConversation(ctx, topicName)
		if lookupErr == nil && existing != nil {
			if existing.Archived {
				restored, unarchiveErr := s.db.UnarchiveConversation(ctx, existing.ConversationID)
				if unarchiveErr != nil {
					return nil, topicConversationExisting, unarchiveErr
				}
				return restored, topicConversationRestored, nil
			}
			return existing, topicConversationExisting, nil
		}
		return nil, topicConversationExisting, err
	}

	if _, err := s.ensureTopicRecord(ctx, topicName, created.ConversationID); err != nil {
		return nil, topicConversationExisting, err
	}

	return created, topicConversationCreated, nil
}

func (s *Server) lookupTopicConversation(ctx context.Context, topicName string) (*generated.Conversation, error) {
	topicRecord, err := s.lookupTopicRecord(ctx, topicName)
	if err != nil {
		return nil, err
	}
	if topicRecord != nil {
		return s.db.GetConversationByID(ctx, topicRecord.ConversationID)
	}

	conversation, err := s.lookupLegacyTopicConversation(ctx, topicName)
	if err != nil || conversation == nil {
		return conversation, err
	}
	if _, err := s.ensureTopicRecord(ctx, topicName, conversation.ConversationID); err != nil {
		return nil, err
	}
	return conversation, nil
}

func (s *Server) lookupLegacyTopicConversation(ctx context.Context, topicName string) (*generated.Conversation, error) {
	var conversation generated.Conversation
	err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		conversation, err = q.GetConversationBySlug(ctx, &topicName)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &conversation, nil
}

func (s *Server) lookupTopicRecord(ctx context.Context, topicName string) (*generated.Topic, error) {
	var topic generated.Topic
	err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		topic, err = q.GetTopic(ctx, topicName)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &topic, nil
}

func (s *Server) lookupTopicRecordByConversationID(ctx context.Context, conversationID string) (*generated.Topic, error) {
	var topic generated.Topic
	err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		topic, err = q.GetTopicByConversationID(ctx, conversationID)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &topic, nil
}

func (s *Server) ensureTopicRecord(ctx context.Context, topicName, conversationID string) (*generated.Topic, error) {
	topicRecord, err := s.lookupTopicRecord(ctx, topicName)
	if err != nil {
		return nil, err
	}
	if topicRecord != nil {
		return topicRecord, nil
	}

	var created generated.Topic
	err = s.db.QueriesTx(ctx, func(q *generated.Queries) error {
		var err error
		created, err = q.CreateTopic(ctx, generated.CreateTopicParams{
			TopicName:      topicName,
			ConversationID: conversationID,
		})
		return err
	})
	if err == nil {
		return &created, nil
	}

	topicRecord, lookupErr := s.lookupTopicRecord(ctx, topicName)
	if lookupErr == nil && topicRecord != nil {
		return topicRecord, nil
	}

	topicRecord, lookupErr = s.lookupTopicRecordByConversationID(ctx, conversationID)
	if lookupErr == nil && topicRecord != nil {
		return topicRecord, nil
	}

	return nil, err
}

func (s *Server) listActiveTopicRecords(ctx context.Context) ([]generated.Topic, error) {
	var topics []generated.Topic
	err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		topics, err = q.ListActiveTopics(ctx)
		return err
	})
	return topics, err
}

func (s *Server) getOrCreateTopicByConversationID(ctx context.Context, conversationID string) (*Topic, bool, error) {
	if topic := s.topicManager.GetTopicByConversationID(conversationID); topic != nil {
		return topic, true, nil
	}

	topicRecord, err := s.lookupTopicRecordByConversationID(ctx, conversationID)
	if err != nil {
		return nil, false, err
	}
	if topicRecord == nil {
		return nil, false, nil
	}

	topic, _, err := s.topicManager.GetOrCreateTopic(ctx, topicRecord.TopicName)
	if err != nil {
		return nil, false, err
	}
	return topic, true, nil
}

func (s *Server) latestSequenceID(ctx context.Context, conversationID string) (int64, error) {
	var latest generated.Message
	err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		latest, err = q.GetLatestMessage(ctx, conversationID)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	return latest.SequenceID, nil
}

func (s *Server) countMessagesInConversation(ctx context.Context, conversationID string) (int64, error) {
	var count int64
	err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		count, err = q.CountMessagesInConversation(ctx, conversationID)
		return err
	})
	return count, err
}

func (s *Server) isConversationWorking(conversationID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	manager, ok := s.activeConversations[conversationID]
	return ok && manager.IsAgentWorking()
}

func (s *Server) defaultTopicModelID() string {
	if s.defaultModel != "" {
		return s.defaultModel
	}
	if s.predictableOnly {
		return "predictable"
	}
	models := s.llmManager.GetAvailableModels()
	if len(models) > 0 {
		return models[0]
	}
	return ""
}

func (s *Server) workspaceTopicWriter(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, outCh <-chan workspaceWSMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-outCh:
			if err := wsjson.Write(ctx, conn, msg); err != nil {
				cancel()
				return
			}
		}
	}
}

// toolKindFromName maps tool names to ACP-aligned kind categories.
func toolKindFromName(toolName string) string {
	switch toolName {
	case "bash", "computer":
		return "execute"
	case "read", "cat", "head", "tail":
		return "read"
	case "edit", "write", "notebook_edit":
		return "edit"
	case "glob", "grep", "find", "search":
		return "search"
	case "browser", "web_search", "web_fetch", "fetch":
		return "fetch"
	case "think":
		return "think"
	default:
		return "other"
	}
}

func translateWorkspaceWSMessages(translator *workspaceTranslatorState, streamData StreamResponse) ([]workspaceWSMessage, workspaceDoneUserData, bool) {
	var messages []workspaceWSMessage
	outcome := workspaceDoneUserData{}
	turnComplete := false
	for _, msg := range streamData.Messages {
		translated, msgOutcome, msgTurnComplete := translateWorkspaceWSMessagesForAPIMessage(translator, msg)
		messages = append(messages, translated...)
		if msgTurnComplete {
			turnComplete = true
			if msgOutcome.Status != "" || msgOutcome.Reason != "" || msgOutcome.InterruptedBy != nil {
				outcome = msgOutcome
			}
		}
	}
	return messages, outcome, turnComplete
}

func translateWorkspaceWSMessagesForAPIMessage(translator *workspaceTranslatorState, msg APIMessage) ([]workspaceWSMessage, workspaceDoneUserData, bool) {
	if msg.LlmData == nil {
		return nil, workspaceDoneUserData{}, false
	}

	var llmMsg llm.Message
	if err := json.Unmarshal([]byte(*msg.LlmData), &llmMsg); err != nil {
		return nil, workspaceDoneUserData{}, false
	}

	messages := make([]workspaceWSMessage, 0)

	switch msg.Type {
	case string(dbpkg.MessageTypeAgent):
		doneMeta, hasDoneMeta := parseWorkspaceDoneUserData(msg.UserData)
		runID := translator.CurrentRunID()
		outcome := workspaceDoneUserData{Status: "completed"}
		if hasDoneMeta && doneMeta.Status != "" {
			outcome = doneMeta
		} else if llmMessageText(llmMsg) == "[Operation cancelled]" {
			outcome.Status = "cancelled"
		}
		for _, content := range llmMsg.Content {
			switch content.Type {
			case llm.ContentTypeText:
				if llmMsg.EndOfTurn && content.Text == "[Operation cancelled]" {
					continue
				}
				if content.Text != "" {
					messages = append(messages, workspaceWSMessage{
						Type:  "message",
						Role:  "assistant",
						RunID: runID,
						Text:  content.Text,
					})
				}
			case llm.ContentTypeToolUse:
				translator.NoteToolCall(content.ID, content.ToolName)
				messages = append(messages, workspaceWSMessage{
					Type:       "tool_call",
					RunID:      runID,
					ToolCallID: content.ID,
					Title:      content.ToolName,
					Kind:       toolKindFromName(content.ToolName),
					Status:     "pending",
					RawInput:   content.ToolInput,
				})
			}
		}
		if llmMsg.EndOfTurn {
			translator.SetCurrentRunID("")
			return messages, outcome, true
		}
	case string(dbpkg.MessageTypeUser):
		promptMeta, hasPromptMeta := parseWorkspacePromptUserData(msg.UserData)
		if hasPromptMeta {
			translator.SetCurrentRunID(promptMeta.RunID)
		}
		runID := translator.CurrentRunID()
		for _, content := range llmMsg.Content {
			switch content.Type {
			case llm.ContentTypeText:
				if content.Text != "" {
					userMsg := workspaceWSMessage{
						Type:  "message",
						Role:  "user",
						RunID: runID,
						Text:  content.Text,
					}
					if hasPromptMeta {
						userMsg.SubmittedBy = promptMeta.SubmittedBy
					}
					messages = append(messages, userMsg)
				}
			case llm.ContentTypeToolResult:
				status := "completed"
				if content.ToolError {
					status = "failed"
				}
				title := translator.ToolTitle(content.ToolUseID)
				messages = append(messages, workspaceWSMessage{
					Type:       "tool_update",
					RunID:      runID,
					ToolCallID: content.ToolUseID,
					Title:      title,
					Status:     status,
					Data:       llmToolResultText(content.ToolResult),
				})
			}
		}
	case string(dbpkg.MessageTypeTool):
		for _, content := range llmMsg.Content {
			if content.Type != llm.ContentTypeToolResult {
				continue
			}
			status := "completed"
			if content.ToolError {
				status = "failed"
			}
			title := translator.ToolTitle(content.ToolUseID)
			messages = append(messages, workspaceWSMessage{
				Type:       "tool_update",
				RunID:      translator.CurrentRunID(),
				ToolCallID: content.ToolUseID,
				Title:      title,
				Status:     status,
				Data:       llmToolResultText(content.ToolResult),
			})
		}
	case string(dbpkg.MessageTypeError):
		messages = append(messages, workspaceWSMessage{
			Type: "error",
			Data: llmMessageText(llmMsg),
		})
	}

	return messages, workspaceDoneUserData{}, false
}

func sendWorkspaceWSMessage(ctx context.Context, outCh chan<- workspaceWSMessage, msg workspaceWSMessage) bool {
	select {
	case outCh <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

func writeWorkspaceQueueMutationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueuedPromptNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, ErrQueuedPromptNotOwned):
		http.Error(w, err.Error(), http.StatusForbidden)
	case errors.Is(err, ErrQueuedPromptNotCancellable):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, ErrQueuedPromptInvalidMove):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func llmMessageText(message llm.Message) string {
	var parts []string
	for _, content := range message.Content {
		if content.Type == llm.ContentTypeText && content.Text != "" {
			parts = append(parts, content.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func llmToolResultText(contents []llm.Content) string {
	var parts []string
	for _, content := range contents {
		if content.Type == llm.ContentTypeText && content.Text != "" {
			parts = append(parts, content.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func conversationModelID(conversation generated.Conversation, fallback string) string {
	if conversation.Model != nil && *conversation.Model != "" {
		return *conversation.Model
	}
	return fallback
}

func sanitizeTopicName(name string) (string, error) {
	sanitized := slug.Sanitize(name)
	if sanitized == "" {
		return "", fmt.Errorf("topic name is required")
	}
	return sanitized, nil
}

func workspaceTopicEventsURL(r *http.Request, topicName string) string {
	scheme := "ws"
	if r.TLS != nil {
		scheme = "wss"
	}
	return fmt.Sprintf("%s://%s/ws/topics/%s/events", scheme, r.Host, url.PathEscape(topicName))
}

func defaultWorkspaceName() string {
	if name := strings.TrimSpace(os.Getenv("WORKSPACE_NAME")); name != "" {
		return slug.Sanitize(name)
	}
	wd, err := os.Getwd()
	if err == nil {
		if base := slug.Sanitize(filepath.Base(wd)); base != "" {
			return base
		}
	}
	return "workspace"
}
