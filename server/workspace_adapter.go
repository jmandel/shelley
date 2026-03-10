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
	Name      string `json:"name"`
	SessionID string `json:"sessionId"`
	Clients   int    `json:"clients"`
	Busy      bool   `json:"busy"`
	LogSize   int64  `json:"logSize"`
	ACP       string `json:"acp"`
	CreatedAt string `json:"createdAt"`
}

type workspaceTopicCreateRequest struct {
	Name string `json:"name"`
}

type workspaceWSMessage struct {
	Type       string   `json:"type"`
	Data       string   `json:"data,omitempty"`
	Topic      string   `json:"topic,omitempty"`
	SessionID  string   `json:"sessionId,omitempty"`
	ToolCallID string   `json:"toolCallId,omitempty"`
	Title      string   `json:"title,omitempty"`
	Kind       string   `json:"kind,omitempty"`
	Status     string   `json:"status,omitempty"`
	Tool       string   `json:"tool,omitempty"`
	Action     string   `json:"action,omitempty"`
	Approvers  []string `json:"approvers,omitempty"`
	Approved   bool     `json:"approved,omitempty"`
	Approver   string   `json:"approver,omitempty"`
}

type workspacePromptMessage struct {
	Type       string `json:"type"`
	Data       string `json:"data,omitempty"`
	ToolCallID string `json:"toolCallId,omitempty"`
	Approved   bool   `json:"approved,omitempty"`
	Approver   string `json:"approver,omitempty"`
}

type workspaceManagerInfo struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	ACP       string `json:"acp"`
	API       string `json:"api,omitempty"`
	CreatedAt string `json:"createdAt"`
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

func (s *Server) handleWorkspaceManager(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]workspaceManagerInfo{s.workspaceManagerInfo(r)})
	case http.MethodPost:
		var req struct {
			Name   string   `json:"name"`
			Topics []string `json:"topics"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if req.Name == "" {
			http.Error(w, "name required", http.StatusBadRequest)
			return
		}
		if req.Name != s.workspaceName {
			http.Error(w, "single-workspace server: name does not match running workspace", http.StatusConflict)
			return
		}
		for _, topic := range req.Topics {
			topicName, err := sanitizeTopicName(topic)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if _, _, err := s.topicManager.GetOrCreateTopic(r.Context(), topicName); err != nil {
				s.logger.Error("Failed to pre-create topic from workspace manager request", "topic", topicName, "error", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		resp := s.workspaceManagerInfo(r)
		type workspaceCreateResponse struct {
			workspaceManagerInfo
			Topics []string `json:"topics,omitempty"`
		}
		json.NewEncoder(w).Encode(workspaceCreateResponse{
			workspaceManagerInfo: resp,
			Topics:               req.Topics,
		})
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceManagerWorkspace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.PathValue("name") != s.workspaceName {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.workspaceManagerInfo(r))
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
		conversation, err := s.getActiveTopicConversation(r.Context(), topicName)
		if err != nil {
			s.logger.Error("Failed to get workspace topic", "topic", topicName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if conversation == nil {
			http.Error(w, "Topic not found", http.StatusNotFound)
			return
		}

		info, err := s.workspaceTopicInfo(r.Context(), r, topicName, *conversation)
		if err != nil {
			s.logger.Error("Failed to build workspace topic info", "topic", topicName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info)
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

func (s *Server) handleWorkspaceTopicQueryWS(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("session")
	if topicName == "" {
		topicName = r.URL.Query().Get("topic")
	}
	if topicName == "" {
		topicName = "general"
	}
	s.handleWorkspaceTopicWSForName(w, r, topicName)
}

func (s *Server) handleWorkspaceTopicWSByName(w http.ResponseWriter, r *http.Request) {
	s.handleWorkspaceTopicWSForName(w, r, r.PathValue("name"))
}

func (s *Server) handleWorkspaceTopicWS(w http.ResponseWriter, r *http.Request) {
	s.handleWorkspaceTopicWSForName(w, r, r.PathValue("topic"))
}

func (s *Server) handleWorkspaceTopicWSForName(w http.ResponseWriter, r *http.Request, rawTopicName string) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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

	if state == topicConversationCreated {
		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: topic.Conversation,
		})
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "system", Data: "starting agent..."})
	}
	if state == topicConversationRestored {
		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: topic.Conversation,
		})
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "system", Data: "restoring archived topic..."})
	}

	clientID := fmt.Sprintf("%s-%d", topicName, time.Now().UnixNano())
	topic.WSHub.Add(clientID, outCh, cancel)
	defer topic.WSHub.Remove(clientID)

	sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{
		Type:      "connected",
		Topic:     topicName,
		SessionID: topic.Conversation.ConversationID,
	})

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
			topic.EnqueuePrompt(prompt, clientID)
		case "approval_response":
			if msg.ToolCallID == "" {
				continue
			}
			topic.ResolveApprovalResponse(workspaceApprovalResponse{
				ToolCallID: msg.ToolCallID,
				Approved:   msg.Approved,
				Approver:   strings.TrimSpace(msg.Approver),
			})
		}
	}
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
	busy := s.isConversationWorking(conversation.ConversationID)
	if topic := s.topicManager.GetTopic(topicName); topic != nil {
		clients = topic.ClientCount()
		busy = topic.IsBusy()
	}

	createdAt := conversation.CreatedAt
	if topicRecord, err := s.lookupTopicRecord(ctx, topicName); err != nil {
		return workspaceTopicInfo{}, err
	} else if topicRecord != nil {
		createdAt = topicRecord.CreatedAt
	}

	return workspaceTopicInfo{
		Name:      topicName,
		SessionID: conversation.ConversationID,
		Clients:   clients,
		Busy:      busy,
		LogSize:   logSize,
		ACP:       workspaceTopicACPURL(r, topicName),
		CreatedAt: createdAt.Format(time.RFC3339),
	}, nil
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

func translateWorkspaceWSMessages(toolTitles map[string]string, streamData StreamResponse) ([]workspaceWSMessage, bool) {
	var messages []workspaceWSMessage
	turnComplete := false
	for _, msg := range streamData.Messages {
		translated, msgTurnComplete := translateWorkspaceWSMessagesForAPIMessage(toolTitles, msg)
		messages = append(messages, translated...)
		if msgTurnComplete {
			turnComplete = true
		}
	}
	return messages, turnComplete
}

func translateWorkspaceWSMessagesForAPIMessage(toolTitles map[string]string, msg APIMessage) ([]workspaceWSMessage, bool) {
	if msg.LlmData == nil {
		return nil, false
	}

	var llmMsg llm.Message
	if err := json.Unmarshal([]byte(*msg.LlmData), &llmMsg); err != nil {
		return nil, false
	}

	messages := make([]workspaceWSMessage, 0)

	switch msg.Type {
	case string(dbpkg.MessageTypeAgent):
		for _, content := range llmMsg.Content {
			switch content.Type {
			case llm.ContentTypeText:
				messages = append(messages, workspaceWSMessage{Type: "text", Data: content.Text})
			case llm.ContentTypeToolUse:
				toolTitles[content.ID] = content.ToolName
				messages = append(messages, workspaceWSMessage{
					Type:       "tool_call",
					ToolCallID: content.ID,
					Title:      content.ToolName,
					Kind:       content.ToolName,
					Status:     "pending",
				})
			}
		}
		if llmMsg.EndOfTurn {
			messages = append(messages, workspaceWSMessage{Type: "done"})
			return messages, true
		}
	case string(dbpkg.MessageTypeUser), string(dbpkg.MessageTypeTool):
		for _, content := range llmMsg.Content {
			if content.Type != llm.ContentTypeToolResult {
				continue
			}
			status := "completed"
			if content.ToolError {
				status = "failed"
			}
			title := toolTitles[content.ToolUseID]
			if title == "" {
				title = content.ToolUseID
			}
			messages = append(messages, workspaceWSMessage{
				Type:       "tool_update",
				ToolCallID: content.ToolUseID,
				Title:      title,
				Status:     status,
			})
		}
	case string(dbpkg.MessageTypeError):
		messages = append(messages, workspaceWSMessage{
			Type: "error",
			Data: llmMessageText(llmMsg),
		})
	}

	return messages, false
}

func sendWorkspaceWSMessage(ctx context.Context, outCh chan<- workspaceWSMessage, msg workspaceWSMessage) bool {
	select {
	case outCh <- msg:
		return true
	case <-ctx.Done():
		return false
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

func workspaceTopicACPURL(r *http.Request, topicName string) string {
	scheme := "ws"
	if r.TLS != nil {
		scheme = "wss"
	}
	return fmt.Sprintf("%s://%s/acp/%s", scheme, r.Host, url.PathEscape(topicName))
}

func workspaceCanonicalAPIBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s/ws", scheme, r.Host)
}

func workspaceLegacyACPBaseURL(r *http.Request) string {
	scheme := "ws"
	if r.TLS != nil {
		scheme = "wss"
	}
	return fmt.Sprintf("%s://%s/acp", scheme, r.Host)
}

func (s *Server) workspaceManagerInfo(r *http.Request) workspaceManagerInfo {
	return workspaceManagerInfo{
		Name:      s.workspaceName,
		Status:    "running",
		ACP:       workspaceLegacyACPBaseURL(r),
		API:       workspaceCanonicalAPIBaseURL(r),
		CreatedAt: s.startedAt.Format(time.RFC3339),
	}
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
