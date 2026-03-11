package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"shelley.exe.dev/db/generated"
)

type workspaceGrantInfo struct {
	GrantID   string          `json:"grantId"`
	Subject   string          `json:"subject"`
	Tools     []string        `json:"tools"`
	Access    string          `json:"access"`
	Approvers []string        `json:"approvers,omitempty"`
	Scope     json.RawMessage `json:"scope,omitempty"`
	CreatedAt string          `json:"createdAt"`
}

type workspaceToolInfo struct {
	Name          string                 `json:"name"`
	Description   string                 `json:"description,omitempty"`
	Protocol      string                 `json:"protocol"`
	Transport     json.RawMessage        `json:"transport,omitempty"`
	Tools         []workspaceActionInfo  `json:"tools,omitempty"`
	Provider      string                 `json:"provider,omitempty"`
	CredentialRef string                 `json:"credentialRef,omitempty"`
	Config        json.RawMessage        `json:"config,omitempty"`
	CreatedAt     string                 `json:"createdAt"`
	Grants        []workspaceGrantInfo   `json:"grants,omitempty"`
	Log           []workspaceToolLogInfo `json:"log,omitempty"`
}

type workspaceToolLogInfo struct {
	LogID          string `json:"logId"`
	TopicName      string `json:"topicName,omitempty"`
	Action         string `json:"action"`
	Subject        string `json:"subject"`
	AccessDecision string `json:"accessDecision"`
	ApprovedBy     string `json:"approvedBy,omitempty"`
	InputSummary   string `json:"inputSummary,omitempty"`
	CreatedAt      string `json:"createdAt"`
}

func (s *Server) handleWorkspaceTools(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleWorkspaceToolsList(w, r)
	case http.MethodPost:
		s.handleWorkspaceToolsCreate(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceToolsList(w http.ResponseWriter, r *http.Request) {
	tools, err := s.listWorkspaceToolInfos(r.Context())
	if err != nil {
		s.logger.Error("Failed to list workspace tools", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tools)
}

func (s *Server) handleWorkspaceToolsCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name          string          `json:"name"`
		Description   string          `json:"description"`
		Protocol      string          `json:"protocol"`
		Actions       json.RawMessage `json:"actions"`
		Tools         json.RawMessage `json:"tools"`
		Transport     json.RawMessage `json:"transport"`
		Provider      string          `json:"provider"`
		CredentialRef string          `json:"credentialRef"`
		Config        json.RawMessage `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	if req.Protocol == "" {
		req.Protocol = "mcp"
	}

	toolDefsRaw := req.Tools
	if len(toolDefsRaw) == 0 {
		toolDefsRaw = req.Actions
	}
	actionDefs, err := normalizeWorkspaceActionDefs(toolDefsRaw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	actionsJSON := json.RawMessage("[]")
	if len(actionDefs) > 0 {
		actionsJSON, err = json.Marshal(actionDefs)
	}
	if err != nil {
		http.Error(w, "Invalid actions", http.StatusBadRequest)
		return
	}
	var descriptionPtr, providerPtr, credentialRefPtr, configPtr *string
	if req.Description != "" {
		descriptionPtr = &req.Description
	}
	if req.Provider != "" {
		providerPtr = &req.Provider
	}
	if req.CredentialRef != "" {
		credentialRefPtr = &req.CredentialRef
	}
	configPayload := req.Config
	if len(req.Transport) > 0 {
		configPayload = req.Transport
	}
	if len(configPayload) > 0 {
		configText := string(configPayload)
		configPtr = &configText
	}

	var tool generated.WorkspaceTool
	err = s.db.QueriesTx(r.Context(), func(q *generated.Queries) error {
		var err error
		tool, err = q.CreateWorkspaceTool(r.Context(), generated.CreateWorkspaceToolParams{
			ToolID:        uuid.NewString(),
			Name:          req.Name,
			Description:   descriptionPtr,
			Protocol:      req.Protocol,
			Actions:       string(actionsJSON),
			Provider:      providerPtr,
			CredentialRef: credentialRefPtr,
			Config:        configPtr,
		})
		return err
	})
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			http.Error(w, "tool already exists", http.StatusConflict)
			return
		}
		s.logger.Error("Failed to create workspace tool", "name", req.Name, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	info, err := s.workspaceToolInfo(r.Context(), tool, false)
	if err != nil {
		s.logger.Error("Failed to build workspace tool response", "name", req.Name, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if err := s.refreshWorkspaceTopicsToolViews(r.Context()); err != nil {
		s.logger.Error("Failed to refresh workspace topic tool views", "name", req.Name, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(info)
}

func (s *Server) handleWorkspaceTool(w http.ResponseWriter, r *http.Request) {
	toolName := r.PathValue("tool")
	switch r.Method {
	case http.MethodGet:
		tool, err := s.lookupWorkspaceToolByName(r.Context(), toolName)
		if err != nil {
			s.logger.Error("Failed to get workspace tool", "tool", toolName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if tool == nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		info, err := s.workspaceToolInfo(r.Context(), *tool, true)
		if err != nil {
			s.logger.Error("Failed to build workspace tool response", "tool", toolName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info)
	case http.MethodDelete:
		tool, err := s.lookupWorkspaceToolByName(r.Context(), toolName)
		if err != nil {
			s.logger.Error("Failed to get workspace tool for delete", "tool", toolName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if tool == nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		if err := s.db.QueriesTx(r.Context(), func(q *generated.Queries) error {
			return q.DeleteWorkspaceToolByName(r.Context(), toolName)
		}); err != nil {
			s.logger.Error("Failed to delete workspace tool", "tool", toolName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if err := s.refreshWorkspaceTopicsToolViews(r.Context()); err != nil {
			s.logger.Error("Failed to refresh workspace topic tool views after delete", "tool", toolName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"name":   toolName,
			"status": "deleted",
		})
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceToolGrants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	toolName := r.PathValue("tool")
	tool, err := s.lookupWorkspaceToolByName(r.Context(), toolName)
	if err != nil {
		s.logger.Error("Failed to load workspace tool for grant", "tool", toolName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if tool == nil {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	var req struct {
		Subject   string          `json:"subject"`
		Actions   []string        `json:"actions"`
		Tools     []string        `json:"tools"`
		Access    string          `json:"access"`
		Approvers []string        `json:"approvers"`
		Scope     json.RawMessage `json:"scope"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	req.Subject = strings.TrimSpace(req.Subject)
	if req.Subject == "" {
		http.Error(w, "subject required", http.StatusBadRequest)
		return
	}
	if len(req.Tools) > 0 && len(req.Actions) == 0 {
		req.Actions = append([]string(nil), req.Tools...)
	}
	if len(req.Actions) == 0 {
		http.Error(w, "tools required", http.StatusBadRequest)
		return
	}
	if req.Access == "" {
		req.Access = "allowed"
	}
	if !isValidWorkspaceGrantAccess(req.Access) {
		http.Error(w, "invalid access", http.StatusBadRequest)
		return
	}

	toolActionDefs, err := decodeWorkspaceActionDefs(tool.Actions)
	if err != nil {
		s.logger.Error("Failed to decode workspace tool actions", "tool", toolName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	// When tools are pre-registered, validate grant actions are a subset.
	// When tools is empty (MCP discovery), accept any action names.
	if len(toolActionDefs) > 0 {
		toolActions := workspaceActionNames(toolActionDefs)
		for _, action := range req.Actions {
			if action == "*" {
				continue
			}
			if !containsString(toolActions, action) {
				http.Error(w, "unknown action for tool", http.StatusBadRequest)
				return
			}
		}
	}

	actionsJSON, err := json.Marshal(req.Actions)
	if err != nil {
		http.Error(w, "Invalid actions", http.StatusBadRequest)
		return
	}
	var approversPtr, scopePtr *string
	if len(req.Approvers) > 0 {
		approversJSON, err := json.Marshal(req.Approvers)
		if err != nil {
			http.Error(w, "Invalid approvers", http.StatusBadRequest)
			return
		}
		approversText := string(approversJSON)
		approversPtr = &approversText
	}
	if len(req.Scope) > 0 {
		scopeText := string(req.Scope)
		scopePtr = &scopeText
	}

	var grant generated.WorkspaceGrant
	err = s.db.QueriesTx(r.Context(), func(q *generated.Queries) error {
		var err error
		grant, err = q.CreateWorkspaceGrant(r.Context(), generated.CreateWorkspaceGrantParams{
			GrantID:   uuid.NewString(),
			ToolID:    tool.ToolID,
			Subject:   req.Subject,
			Actions:   string(actionsJSON),
			Access:    req.Access,
			Approvers: approversPtr,
			Scope:     scopePtr,
		})
		return err
	})
	if err != nil {
		s.logger.Error("Failed to create workspace grant", "tool", toolName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	info, err := workspaceGrantInfoFromRecord(grant)
	if err != nil {
		s.logger.Error("Failed to build workspace grant response", "tool", toolName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if err := s.refreshWorkspaceTopicsToolViews(r.Context()); err != nil {
		s.logger.Error("Failed to refresh workspace topic tool views after grant create", "tool", toolName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(info)
}

func (s *Server) handleWorkspaceToolGrant(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.db.QueriesTx(r.Context(), func(q *generated.Queries) error {
		return q.DeleteWorkspaceGrant(r.Context(), r.PathValue("grant"))
	}); err != nil {
		s.logger.Error("Failed to delete workspace grant", "grant", r.PathValue("grant"), "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if err := s.refreshWorkspaceTopicsToolViews(r.Context()); err != nil {
		s.logger.Error("Failed to refresh workspace topic tool views after grant delete", "grant", r.PathValue("grant"), "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"grantId": r.PathValue("grant"),
		"status":  "deleted",
	})
}

func (s *Server) listWorkspaceToolInfos(ctx context.Context) ([]workspaceToolInfo, error) {
	var tools []generated.WorkspaceTool
	if err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		tools, err = q.ListWorkspaceTools(ctx)
		return err
	}); err != nil {
		return nil, err
	}

	infos := make([]workspaceToolInfo, 0, len(tools))
	for _, tool := range tools {
		info, err := s.workspaceToolInfo(ctx, tool, false)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (s *Server) workspaceToolInfo(ctx context.Context, tool generated.WorkspaceTool, includeLog bool) (workspaceToolInfo, error) {
	var grants []generated.WorkspaceGrant
	if err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		grants, err = q.ListWorkspaceGrantsByToolID(ctx, tool.ToolID)
		return err
	}); err != nil {
		return workspaceToolInfo{}, err
	}

	var logs []generated.WorkspaceToolLog
	if includeLog {
		if err := s.db.Queries(ctx, func(q *generated.Queries) error {
			var err error
			logs, err = q.ListWorkspaceToolLogByToolID(ctx, tool.ToolID)
			return err
		}); err != nil {
			return workspaceToolInfo{}, err
		}
	}

	actionDefs, err := decodeWorkspaceActionDefs(tool.Actions)
	if err != nil {
		return workspaceToolInfo{}, err
	}

	info := workspaceToolInfo{
		Name:      tool.Name,
		Protocol:  tool.Protocol,
		Tools:     workspaceActionInfos(actionDefs),
		CreatedAt: tool.CreatedAt.Format(time.RFC3339),
		Grants:    make([]workspaceGrantInfo, 0, len(grants)),
	}
	if includeLog {
		info.Log = make([]workspaceToolLogInfo, 0, len(logs))
	}
	if tool.Description != nil {
		info.Description = *tool.Description
	}
	if tool.Provider != nil {
		info.Provider = *tool.Provider
	}
	if tool.CredentialRef != nil {
		info.CredentialRef = *tool.CredentialRef
	}
	if tool.Config != nil {
		info.Config = json.RawMessage(*tool.Config)
		info.Transport = workspaceTransportFromConfig(*tool.Config)
	}

	for _, grant := range grants {
		grantInfo, err := workspaceGrantInfoFromRecord(grant)
		if err != nil {
			return workspaceToolInfo{}, err
		}
		info.Grants = append(info.Grants, grantInfo)
	}
	for _, logEntry := range logs {
		info.Log = append(info.Log, workspaceToolLogInfoFromRecord(logEntry))
	}

	return info, nil
}

func workspaceGrantInfoFromRecord(grant generated.WorkspaceGrant) (workspaceGrantInfo, error) {
	actions, err := decodeJSONStringSlice(grant.Actions)
	if err != nil {
		return workspaceGrantInfo{}, err
	}

	info := workspaceGrantInfo{
		GrantID:   grant.GrantID,
		Subject:   grant.Subject,
		Tools:     actions,
		Access:    grant.Access,
		CreatedAt: grant.CreatedAt.Format(time.RFC3339),
	}
	if grant.Approvers != nil {
		approvers, err := decodeJSONStringSlice(*grant.Approvers)
		if err != nil {
			return workspaceGrantInfo{}, err
		}
		info.Approvers = approvers
	}
	if grant.Scope != nil {
		info.Scope = json.RawMessage(*grant.Scope)
	}
	return info, nil
}

func workspaceToolLogInfoFromRecord(logEntry generated.WorkspaceToolLog) workspaceToolLogInfo {
	info := workspaceToolLogInfo{
		LogID:          logEntry.LogID,
		Action:         logEntry.Action,
		Subject:        logEntry.Subject,
		AccessDecision: logEntry.AccessDecision,
		CreatedAt:      logEntry.CreatedAt.Format(time.RFC3339),
	}
	if logEntry.TopicName != nil {
		info.TopicName = *logEntry.TopicName
	}
	if logEntry.ApprovedBy != nil {
		info.ApprovedBy = *logEntry.ApprovedBy
	}
	if logEntry.InputSummary != nil {
		info.InputSummary = *logEntry.InputSummary
	}
	return info
}

func decodeJSONStringSlice(raw string) ([]string, error) {
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, err
	}
	return values, nil
}

func workspaceTransportFromConfig(raw string) json.RawMessage {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return json.RawMessage(trimmed)
	}
	if _, hasType := payload["type"]; hasType {
		normalized, err := json.Marshal(payload)
		if err != nil {
			return json.RawMessage(trimmed)
		}
		return normalized
	}

	transportType, _ := payload["transport"].(string)
	switch strings.ToLower(strings.TrimSpace(transportType)) {
	case "stdio":
		delete(payload, "transport")
		payload["type"] = "stdio"
	case "streamable_http", "streamable-http":
		delete(payload, "transport")
		payload["type"] = "streamable_http"
		if urlValue, ok := payload["endpoint"]; ok {
			payload["url"] = urlValue
			delete(payload, "endpoint")
		}
	default:
		return json.RawMessage(trimmed)
	}

	normalized, err := json.Marshal(payload)
	if err != nil {
		return json.RawMessage(trimmed)
	}
	return normalized
}

func (s *Server) refreshWorkspaceTopicsToolViews(ctx context.Context) error {
	var topicRecords []generated.Topic
	if err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		topicRecords, err = q.ListTopics(ctx)
		return err
	}); err != nil {
		return err
	}

	for _, topicRecord := range topicRecords {
		topic, _, err := s.topicManager.GetOrCreateTopic(ctx, topicRecord.TopicName)
		if err != nil {
			return err
		}
		if err := topic.refreshWorkspaceTools(ctx); err != nil {
			return err
		}
		if !topic.Manager.HasConversationEvents() {
			if err := topic.Manager.RefreshSystemPromptDisplayData(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) lookupWorkspaceToolByName(ctx context.Context, name string) (*generated.WorkspaceTool, error) {
	var tool generated.WorkspaceTool
	err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		tool, err = q.GetWorkspaceToolByName(ctx, name)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &tool, nil
}
