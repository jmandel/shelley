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
	Type       string `json:"type"`
	Data       string `json:"data,omitempty"`
	Topic      string `json:"topic,omitempty"`
	SessionID  string `json:"sessionId,omitempty"`
	ToolCallID string `json:"toolCallId,omitempty"`
	Title      string `json:"title,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Status     string `json:"status,omitempty"`
}

type workspacePromptMessage struct {
	Type string `json:"type"`
	Data string `json:"data,omitempty"`
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
			if _, _, err := s.getOrCreateTopicConversation(r.Context(), topicName); err != nil && !errors.Is(err, errTopicAlreadyExists) {
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

	conversation, state, err := s.getOrCreateTopicConversation(r.Context(), topicName)
	if err != nil {
		if errors.Is(err, errTopicAlreadyExists) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		s.logger.Error("Failed to create workspace topic", "topic", topicName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if state == topicConversationCreated || state == topicConversationRestored {
		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: conversation,
		})
	}
	if state == topicConversationExisting {
		http.Error(w, fmt.Sprintf("%s: %s", errTopicAlreadyExists, topicName), http.StatusConflict)
		return
	}

	info, err := s.workspaceTopicInfo(r.Context(), r, *conversation)
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

		info, err := s.workspaceTopicInfo(r.Context(), r, *conversation)
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

	conversation, state, err := s.getOrCreateTopicConversation(ctx, topicName)
	if err != nil {
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
		return
	}

	if state == topicConversationCreated {
		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: conversation,
		})
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "system", Data: "starting agent..."})
	}
	if state == topicConversationRestored {
		go s.publishConversationListUpdate(ConversationListUpdate{
			Type:         "update",
			Conversation: conversation,
		})
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "system", Data: "restoring archived topic..."})
	}

	manager, err := s.getOrCreateConversationManager(ctx, conversation.ConversationID, "")
	if err != nil {
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
		return
	}

	s.incrementTopicClientCount(topicName)
	defer s.decrementTopicClientCount(topicName)

	sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{
		Type:      "connected",
		Topic:     topicName,
		SessionID: conversation.ConversationID,
	})

	lastSequenceID, err := s.latestSequenceID(ctx, conversation.ConversationID)
	if err != nil {
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
		return
	}

	next := manager.subpub.Subscribe(ctx, lastSequenceID)
	toolTitles := make(map[string]string)
	go func() {
		for {
			streamData, ok := next()
			if !ok {
				return
			}
			s.emitWorkspaceWSMessages(ctx, outCh, toolTitles, streamData)
		}
	}()

	for {
		var msg workspacePromptMessage
		if err := wsjson.Read(ctx, conn, &msg); err != nil {
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure && ctx.Err() == nil {
				s.logger.Debug("Workspace websocket read failed", "topic", topicName, "error", err)
			}
			return
		}

		if msg.Type != "prompt" {
			continue
		}
		prompt := strings.TrimSpace(msg.Data)
		if prompt == "" {
			continue
		}

		modelID := conversationModelID(*conversation, s.defaultTopicModelID())
		llmService, err := s.llmManager.GetService(modelID)
		if err != nil {
			sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
			continue
		}

		busy := manager.IsAgentWorking()
		if busy {
			sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "system", Data: "queued prompt"})
		} else {
			sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "system", Data: "thinking..."})
		}

		userMessage := llm.Message{
			Role: llm.MessageRoleUser,
			Content: []llm.Content{
				{Type: llm.ContentTypeText, Text: prompt},
			},
		}
		if _, err := manager.AcceptUserMessage(ctx, llmService, modelID, userMessage); err != nil {
			sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "error", Data: err.Error()})
		}
	}
}

var errTopicAlreadyExists = errors.New("topic already exists")

func (s *Server) workspaceTopics(ctx context.Context, r *http.Request) ([]workspaceTopicInfo, error) {
	conversations, err := s.db.ListConversations(ctx, 5000, 0)
	if err != nil {
		return nil, err
	}

	topics := make([]workspaceTopicInfo, 0, len(conversations))
	for _, conversation := range conversations {
		if conversation.Slug == nil || conversation.Archived {
			continue
		}
		info, err := s.workspaceTopicInfo(ctx, r, conversation)
		if err != nil {
			return nil, err
		}
		topics = append(topics, info)
	}
	return topics, nil
}

func (s *Server) workspaceTopicNames(ctx context.Context) ([]string, error) {
	conversations, err := s.db.ListConversations(ctx, 5000, 0)
	if err != nil {
		return nil, err
	}

	topics := make([]string, 0, len(conversations))
	for _, conversation := range conversations {
		if conversation.Slug == nil || conversation.Archived {
			continue
		}
		topics = append(topics, *conversation.Slug)
	}
	return topics, nil
}

func (s *Server) workspaceTopicInfo(ctx context.Context, r *http.Request, conversation generated.Conversation) (workspaceTopicInfo, error) {
	logSize, err := s.countMessagesInConversation(ctx, conversation.ConversationID)
	if err != nil {
		return workspaceTopicInfo{}, err
	}

	return workspaceTopicInfo{
		Name:      *conversation.Slug,
		SessionID: conversation.ConversationID,
		Clients:   s.topicClientCount(*conversation.Slug),
		Busy:      s.isConversationWorking(conversation.ConversationID),
		LogSize:   logSize,
		ACP:       workspaceTopicACPURL(r, *conversation.Slug),
		CreatedAt: conversation.CreatedAt.Format(time.RFC3339),
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

	created, err := s.db.CreateConversation(ctx, &topicName, true, nil, modelPtr)
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

	return created, topicConversationCreated, nil
}

func (s *Server) lookupTopicConversation(ctx context.Context, topicName string) (*generated.Conversation, error) {
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

func (s *Server) incrementTopicClientCount(topicName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topicClientCounts[topicName]++
}

func (s *Server) decrementTopicClientCount(topicName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.topicClientCounts[topicName] <= 1 {
		delete(s.topicClientCounts, topicName)
		return
	}
	s.topicClientCounts[topicName]--
}

func (s *Server) topicClientCount(topicName string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.topicClientCounts[topicName]
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

func (s *Server) emitWorkspaceWSMessages(ctx context.Context, outCh chan<- workspaceWSMessage, toolTitles map[string]string, streamData StreamResponse) {
	for _, msg := range streamData.Messages {
		s.emitWorkspaceWSMessagesForAPIMessage(ctx, outCh, toolTitles, msg)
	}
}

func (s *Server) emitWorkspaceWSMessagesForAPIMessage(ctx context.Context, outCh chan<- workspaceWSMessage, toolTitles map[string]string, msg APIMessage) {
	if msg.LlmData == nil {
		return
	}

	var llmMsg llm.Message
	if err := json.Unmarshal([]byte(*msg.LlmData), &llmMsg); err != nil {
		return
	}

	switch msg.Type {
	case string(dbpkg.MessageTypeAgent):
		for _, content := range llmMsg.Content {
			switch content.Type {
			case llm.ContentTypeText:
				sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "text", Data: content.Text})
			case llm.ContentTypeToolUse:
				toolTitles[content.ID] = content.ToolName
				sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{
					Type:       "tool_call",
					ToolCallID: content.ID,
					Title:      content.ToolName,
					Kind:       content.ToolName,
					Status:     "pending",
				})
			}
		}
		if llmMsg.EndOfTurn {
			sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{Type: "done"})
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
			title := toolTitles[content.ToolUseID]
			if title == "" {
				title = content.ToolUseID
			}
			sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{
				Type:       "tool_update",
				ToolCallID: content.ToolUseID,
				Title:      title,
				Status:     status,
			})
		}
	case string(dbpkg.MessageTypeError):
		sendWorkspaceWSMessage(ctx, outCh, workspaceWSMessage{
			Type: "error",
			Data: llmMessageText(llmMsg),
		})
	}
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

func workspaceBaseURLs(r *http.Request) (apiURL, acpURL string) {
	scheme := "http"
	wsScheme := "ws"
	if r.TLS != nil {
		scheme = "https"
		wsScheme = "wss"
	}
	return fmt.Sprintf("%s://%s", scheme, r.Host), fmt.Sprintf("%s://%s/acp", wsScheme, r.Host)
}

func (s *Server) workspaceManagerInfo(r *http.Request) workspaceManagerInfo {
	apiURL, acpURL := workspaceBaseURLs(r)
	return workspaceManagerInfo{
		Name:      s.workspaceName,
		Status:    "running",
		ACP:       acpURL,
		API:       apiURL,
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
