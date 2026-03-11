package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	"shelley.exe.dev/db/generated"
	"shelley.exe.dev/llm"
)

const (
	workspaceGrantAllowed          = "allowed"
	workspaceGrantApprovalRequired = "approval_required"
	workspaceGrantDenied           = "denied"
)

type workspaceToolInvocation struct {
	Action string          `json:"action"`
	Input  json.RawMessage `json:"input,omitempty"`
}

func isValidWorkspaceGrantAccess(access string) bool {
	switch access {
	case workspaceGrantAllowed, workspaceGrantApprovalRequired, workspaceGrantDenied:
		return true
	default:
		return false
	}
}

func (s *Server) buildTopicWorkspaceTools(ctx context.Context, topicName string) ([]*llm.Tool, error) {
	var toolRecords []generated.WorkspaceTool
	if err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		toolRecords, err = q.ListWorkspaceTools(ctx)
		return err
	}); err != nil {
		return nil, err
	}

	tools := make([]*llm.Tool, 0, len(toolRecords))
	for _, toolRecord := range toolRecords {
		runtimeTool, err := s.buildTopicWorkspaceTool(ctx, topicName, toolRecord)
		if err != nil {
			return nil, err
		}
		if runtimeTool != nil {
			tools = append(tools, runtimeTool)
		}
	}

	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})
	return tools, nil
}

func (s *Server) buildTopicWorkspaceTool(ctx context.Context, topicName string, toolRecord generated.WorkspaceTool) (*llm.Tool, error) {
	actionDefs, err := decodeWorkspaceActionDefs(toolRecord.Actions)
	if err != nil {
		return nil, err
	}

	var grantRecords []generated.WorkspaceGrant
	if err := s.db.Queries(ctx, func(q *generated.Queries) error {
		var err error
		grantRecords, err = q.ListWorkspaceGrantsByToolID(ctx, toolRecord.ToolID)
		return err
	}); err != nil {
		return nil, err
	}

	registeredActions := workspaceActionNames(actionDefs)

	actionPolicies, err := workspaceActionPolicies(grantRecords, topicName, registeredActions)
	if err != nil {
		return nil, err
	}
	approvalApprovers, err := workspaceActionApprovers(grantRecords, topicName, registeredActions)
	if err != nil {
		return nil, err
	}

	visibleActions := visibleWorkspaceActions(actionPolicies)
	if len(visibleActions) == 0 {
		return nil, nil
	}
	visibleActionDefs := visibleWorkspaceActionDefs(actionDefs, visibleActions)

	toolCopy := toolRecord
	policyCopy := cloneWorkspaceActionPolicies(actionPolicies)
	actionsCopy := append([]string(nil), registeredActions...)
	approversCopy := cloneWorkspaceActionApprovers(approvalApprovers)

	return &llm.Tool{
		Name:        "workspace_" + toolRecord.Name,
		Description: buildWorkspaceToolDescription(toolRecord.Description, visibleActionDefs),
		InputSchema: buildWorkspaceToolSchema(visibleActionDefs),
		Run: func(ctx context.Context, input json.RawMessage) llm.ToolOut {
			return s.runWorkspaceTool(ctx, topicName, toolCopy, actionsCopy, policyCopy, approversCopy, input)
		},
	}, nil
}

func buildWorkspaceToolDescription(description *string, actions []workspaceActionDef) string {
	base := "Workspace-managed external tool."
	if description != nil && strings.TrimSpace(*description) != "" {
		base = strings.TrimSpace(*description)
	}

	if len(actions) == 0 {
		return base
	}

	actionParts := make([]string, 0, len(actions))
	for _, action := range actions {
		if action.Description == "" {
			actionParts = append(actionParts, action.Name)
			continue
		}
		actionParts = append(actionParts, fmt.Sprintf("%s (%s)", action.Name, action.Description))
	}
	return fmt.Sprintf("%s Allowed actions for this topic: %s.", base, strings.Join(actionParts, ", "))
}

func buildWorkspaceToolSchema(actions []workspaceActionDef) json.RawMessage {
	actionNames := workspaceActionNames(actions)
	inputProperty := map[string]any{
		"type":                 "object",
		"description":          buildWorkspaceToolInputDescription(actions),
		"additionalProperties": true,
	}
	if len(actions) == 1 {
		inputSchema, err := workspaceActionSchemaAny(actions[0])
		if err != nil {
			panic(err)
		}
		if schemaMap, ok := inputSchema.(map[string]any); ok {
			if description := strings.TrimSpace(buildWorkspaceToolInputDescription(actions)); description != "" {
				if _, hasDescription := schemaMap["description"]; !hasDescription {
					schemaMap["description"] = description
				}
			}
			inputProperty = schemaMap
		}
	}
	schemaMap := map[string]any{
		"type":     "object",
		"required": []string{"action"},
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        actionNames,
				"description": "Workspace tool action to perform.",
			},
			"input": inputProperty,
		},
		"additionalProperties": false,
	}

	schema, err := json.Marshal(schemaMap)
	if err != nil {
		panic(err)
	}
	return schema
}

func buildWorkspaceToolInputDescription(actions []workspaceActionDef) string {
	if len(actions) == 0 {
		return "Tool-specific input payload."
	}
	if len(actions) == 1 {
		action := actions[0]
		summary := describeWorkspaceActionInput(action)
		if summary == "" {
			return "Tool-specific input payload."
		}
		return fmt.Sprintf("Input for action %s. %s", action.Name, summary)
	}

	parts := make([]string, 0, len(actions))
	for _, action := range actions {
		summary := describeWorkspaceActionInput(action)
		if summary == "" {
			parts = append(parts, fmt.Sprintf("%s: object input payload", action.Name))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: %s", action.Name, summary))
	}
	return "Action-specific input payload. " + strings.Join(parts, " ")
}

func describeWorkspaceActionInput(action workspaceActionDef) string {
	if len(action.InputSchema) == 0 {
		return ""
	}

	var schema map[string]any
	if err := json.Unmarshal(action.InputSchema, &schema); err != nil {
		return ""
	}

	properties, _ := schema["properties"].(map[string]any)
	if len(properties) == 0 {
		return "Accepts an object input."
	}

	propertyNames := make([]string, 0, len(properties))
	for name := range properties {
		propertyNames = append(propertyNames, name)
	}
	sort.Strings(propertyNames)

	requiredSet := make(map[string]struct{})
	if required, ok := schema["required"].([]any); ok {
		for _, value := range required {
			if name, ok := value.(string); ok {
				requiredSet[name] = struct{}{}
			}
		}
	}

	parts := make([]string, 0, len(propertyNames))
	for _, name := range propertyNames {
		label := name
		if _, ok := requiredSet[name]; ok {
			label += " (required)"
		}
		parts = append(parts, label)
	}

	return "Fields: " + strings.Join(parts, ", ") + "."
}

func (s *Server) runWorkspaceTool(ctx context.Context, topicName string, toolRecord generated.WorkspaceTool, registeredActions []string, actionPolicies map[string]string, approvalApprovers map[string][]string, input json.RawMessage) llm.ToolOut {
	var req workspaceToolInvocation
	if err := json.Unmarshal(input, &req); err != nil {
		return llm.ToolOut{Error: fmt.Errorf("invalid workspace tool input: %w", err)}
	}

	req.Action = strings.TrimSpace(req.Action)
	if req.Action == "" {
		return llm.ToolOut{Error: fmt.Errorf("workspace tool action required")}
	}
	if !containsString(registeredActions, req.Action) {
		return llm.ToolOut{Error: fmt.Errorf("unknown workspace tool action: %s", req.Action)}
	}

	subject := "agent:" + topicName
	access := actionPolicies[req.Action]
	decision := workspaceGrantDenied
	if access == workspaceGrantAllowed {
		decision = workspaceGrantAllowed
	}

	switch access {
	case workspaceGrantAllowed:
		if err := s.recordWorkspaceToolLog(ctx, toolRecord, topicName, req.Action, subject, decision, "", input); err != nil {
			return llm.ToolOut{Error: err}
		}
		return s.executeWorkspaceTool(ctx, toolRecord, req)
	case workspaceGrantApprovalRequired:
		approved, approver, err := s.requestWorkspaceToolApproval(ctx, topicName, toolRecord, req.Action, approvalApprovers[req.Action], input)
		if err != nil {
			return llm.ToolOut{Error: err}
		}
		if !approved {
			if err := s.recordWorkspaceToolLog(ctx, toolRecord, topicName, req.Action, subject, workspaceGrantDenied, approver, input); err != nil {
				return llm.ToolOut{Error: err}
			}
			return llm.ToolOut{Error: fmt.Errorf("approval denied for %s/%s", toolRecord.Name, req.Action)}
		}
		if err := s.recordWorkspaceToolLog(ctx, toolRecord, topicName, req.Action, subject, "approved", approver, input); err != nil {
			return llm.ToolOut{Error: err}
		}
		return s.executeWorkspaceTool(ctx, toolRecord, req)
	case workspaceGrantDenied:
		if err := s.recordWorkspaceToolLog(ctx, toolRecord, topicName, req.Action, subject, decision, "", input); err != nil {
			return llm.ToolOut{Error: err}
		}
		return llm.ToolOut{Error: fmt.Errorf("access denied for %s/%s", toolRecord.Name, req.Action)}
	default:
		if err := s.recordWorkspaceToolLog(ctx, toolRecord, topicName, req.Action, subject, decision, "", input); err != nil {
			return llm.ToolOut{Error: err}
		}
		return llm.ToolOut{Error: fmt.Errorf("no grant for %s/%s", toolRecord.Name, req.Action)}
	}
}

func (s *Server) executeWorkspaceTool(ctx context.Context, toolRecord generated.WorkspaceTool, req workspaceToolInvocation) llm.ToolOut {
	switch strings.ToLower(strings.TrimSpace(toolRecord.Protocol)) {
	case "mcp":
		return s.executeMCPWorkspaceTool(ctx, toolRecord, req)
	default:
		return llm.ToolOut{Error: fmt.Errorf("workspace tool protocol not implemented: %s", toolRecord.Protocol)}
	}
}

func (s *Server) recordWorkspaceToolLog(ctx context.Context, toolRecord generated.WorkspaceTool, topicName, action, subject, accessDecision, approvedBy string, input json.RawMessage) error {
	var topicNamePtr, approvedByPtr, inputSummaryPtr *string
	if topicName != "" {
		topicNamePtr = &topicName
	}
	if approvedBy != "" {
		approvedByPtr = &approvedBy
	}
	if summary := summarizeWorkspaceToolInput(input); summary != "" {
		inputSummaryPtr = &summary
	}

	return s.db.QueriesTx(ctx, func(q *generated.Queries) error {
		_, err := q.CreateWorkspaceToolLog(ctx, generated.CreateWorkspaceToolLogParams{
			LogID:          uuid.NewString(),
			ToolID:         toolRecord.ToolID,
			TopicName:      topicNamePtr,
			Action:         action,
			Subject:        subject,
			AccessDecision: accessDecision,
			ApprovedBy:     approvedByPtr,
			InputSummary:   inputSummaryPtr,
		})
		return err
	})
}

func summarizeWorkspaceToolInput(input json.RawMessage) string {
	if len(input) == 0 {
		return ""
	}
	const maxSummaryLen = 512
	summary := string(input)
	if len(summary) <= maxSummaryLen {
		return summary
	}
	return summary[:maxSummaryLen] + "..."
}

func workspaceActionPolicies(grantRecords []generated.WorkspaceGrant, topicName string, registeredActions []string) (map[string]string, error) {
	validActions := make(map[string]struct{}, len(registeredActions))
	for _, action := range registeredActions {
		validActions[action] = struct{}{}
	}

	policies := make(map[string]string)
	for _, grant := range grantRecords {
		if !workspaceGrantAppliesToTopic(grant.Subject, topicName) {
			continue
		}

		grantActions, err := decodeJSONStringSlice(grant.Actions)
		if err != nil {
			return nil, err
		}
		for _, action := range grantActions {
			// Wildcard grant applies to all registered actions
			if action == "*" {
				for _, regAction := range registeredActions {
					policies[regAction] = strongerWorkspaceAccess(policies[regAction], grant.Access)
				}
				continue
			}
			if _, ok := validActions[action]; !ok {
				continue
			}
			policies[action] = strongerWorkspaceAccess(policies[action], grant.Access)
		}
	}

	return policies, nil
}

func workspaceActionApprovers(grantRecords []generated.WorkspaceGrant, topicName string, registeredActions []string) (map[string][]string, error) {
	validActions := make(map[string]struct{}, len(registeredActions))
	for _, action := range registeredActions {
		validActions[action] = struct{}{}
	}

	approversByAction := make(map[string][]string)
	for _, grant := range grantRecords {
		if grant.Access != workspaceGrantApprovalRequired || !workspaceGrantAppliesToTopic(grant.Subject, topicName) {
			continue
		}

		grantActions, err := decodeJSONStringSlice(grant.Actions)
		if err != nil {
			return nil, err
		}
		var approvers []string
		if grant.Approvers != nil {
			approvers, err = decodeJSONStringSlice(*grant.Approvers)
			if err != nil {
				return nil, err
			}
		}
		for _, action := range grantActions {
			// Wildcard grant applies to all registered actions
			targetActions := []string{action}
			if action == "*" {
				targetActions = registeredActions
			}
			for _, targetAction := range targetActions {
				if _, ok := validActions[targetAction]; !ok {
					continue
				}
				for _, approver := range approvers {
					if !containsString(approversByAction[targetAction], approver) {
						approversByAction[targetAction] = append(approversByAction[targetAction], approver)
					}
				}
			}
		}
	}

	return approversByAction, nil
}

func workspaceGrantAppliesToTopic(subject, topicName string) bool {
	return subject == "agent:*" || subject == "agent:"+topicName
}

func strongerWorkspaceAccess(current, candidate string) string {
	if workspaceAccessRank(candidate) > workspaceAccessRank(current) {
		return candidate
	}
	return current
}

func workspaceAccessRank(access string) int {
	switch access {
	case workspaceGrantAllowed:
		return 1
	case workspaceGrantApprovalRequired:
		return 2
	case workspaceGrantDenied:
		return 3
	default:
		return 0
	}
}

func visibleWorkspaceActions(actionPolicies map[string]string) []string {
	actions := make([]string, 0, len(actionPolicies))
	for action, access := range actionPolicies {
		if access == workspaceGrantAllowed || access == workspaceGrantApprovalRequired {
			actions = append(actions, action)
		}
	}
	sort.Strings(actions)
	return actions
}

func cloneWorkspaceActionPolicies(actionPolicies map[string]string) map[string]string {
	if len(actionPolicies) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(actionPolicies))
	for action, access := range actionPolicies {
		cloned[action] = access
	}
	return cloned
}

func cloneWorkspaceActionApprovers(approversByAction map[string][]string) map[string][]string {
	if len(approversByAction) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(approversByAction))
	for action, approvers := range approversByAction {
		cloned[action] = append([]string(nil), approvers...)
	}
	return cloned
}

func (s *Server) requestWorkspaceToolApproval(ctx context.Context, topicName string, toolRecord generated.WorkspaceTool, action string, approvers []string, input json.RawMessage) (bool, string, error) {
	topic := s.topicManager.GetTopic(topicName)
	if topic == nil {
		return false, "", fmt.Errorf("topic runtime unavailable for approval: %s", topicName)
	}

	req := workspaceApprovalRequest{
		ToolCallID: uuid.NewString(),
		Tool:       toolRecord.Name,
		Action:     action,
		Summary:    summarizeWorkspaceToolInput(input),
		Approvers:  append([]string(nil), approvers...),
	}
	resp, approved := topic.RequestApproval(ctx, req)
	return approved, resp.Approver, nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
