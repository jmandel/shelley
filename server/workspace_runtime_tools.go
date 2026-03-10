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
	Action string `json:"action"`
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
	registeredActions, err := decodeJSONStringSlice(toolRecord.Actions)
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

	toolCopy := toolRecord
	policyCopy := cloneWorkspaceActionPolicies(actionPolicies)
	actionsCopy := append([]string(nil), registeredActions...)
	approversCopy := cloneWorkspaceActionApprovers(approvalApprovers)

	return &llm.Tool{
		Name:        "workspace_" + toolRecord.Name,
		Description: buildWorkspaceToolDescription(toolRecord.Description, visibleActions),
		InputSchema: buildWorkspaceToolSchema(visibleActions),
		Run: func(ctx context.Context, input json.RawMessage) llm.ToolOut {
			return s.runWorkspaceTool(ctx, topicName, toolCopy, actionsCopy, policyCopy, approversCopy, input)
		},
	}, nil
}

func buildWorkspaceToolDescription(description *string, actions []string) string {
	base := "Workspace-managed external tool."
	if description != nil && strings.TrimSpace(*description) != "" {
		base = strings.TrimSpace(*description)
	}
	return fmt.Sprintf("%s Allowed actions for this topic: %s.", base, strings.Join(actions, ", "))
}

func buildWorkspaceToolSchema(actions []string) json.RawMessage {
	schema, err := json.Marshal(map[string]any{
		"type":     "object",
		"required": []string{"action"},
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        actions,
				"description": "Workspace tool action to perform.",
			},
			"input": map[string]any{
				"type":                 "object",
				"description":          "Tool-specific input payload.",
				"additionalProperties": true,
			},
		},
		"additionalProperties": true,
	})
	if err != nil {
		panic(err)
	}
	return schema
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
		return llm.ToolOut{Error: fmt.Errorf("workspace tool execution not implemented for %s/%s", toolRecord.Name, req.Action)}
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
		return llm.ToolOut{Error: fmt.Errorf("workspace tool execution not implemented for %s/%s", toolRecord.Name, req.Action)}
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
			if _, ok := validActions[action]; !ok {
				continue
			}
			for _, approver := range approvers {
				if !containsString(approversByAction[action], approver) {
					approversByAction[action] = append(approversByAction[action], approver)
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
