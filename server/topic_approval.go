package server

import (
	"context"
	"slices"
	"time"
)

type workspaceApprovalRequest struct {
	ToolCallID string
	Tool       string
	Action     string
	Summary    string
	Approvers  []string
}

type workspaceApprovalResponse struct {
	ToolCallID string
	Approved   bool
	Approver   string
}

func (t *Topic) RequestApproval(ctx context.Context, req workspaceApprovalRequest) (workspaceApprovalResponse, bool) {
	if t.ClientCount() == 0 {
		return workspaceApprovalResponse{}, false
	}

	responseCh := make(chan workspaceApprovalResponse, 1)

	t.approvalMu.Lock()
	t.pendingApprovals[req.ToolCallID] = responseCh
	t.approvalMu.Unlock()

	defer func() {
		t.approvalMu.Lock()
		delete(t.pendingApprovals, req.ToolCallID)
		t.approvalMu.Unlock()
	}()

	t.broadcastWSMessage(workspaceWSMessage{
		Type:       "approval_request",
		ToolCallID: req.ToolCallID,
		Tool:       req.Tool,
		Action:     req.Action,
		Data:       req.Summary,
		Approvers:  append([]string(nil), req.Approvers...),
	})

	approvalCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		select {
		case <-approvalCtx.Done():
			return workspaceApprovalResponse{}, false
		case resp := <-responseCh:
			if !resp.Approved {
				return resp, false
			}
			if len(req.Approvers) > 0 && !slices.Contains(req.Approvers, resp.Approver) {
				return workspaceApprovalResponse{}, false
			}
			return resp, true
		}
	}
}

func (t *Topic) ResolveApprovalResponse(resp workspaceApprovalResponse) {
	t.approvalMu.Lock()
	responseCh := t.pendingApprovals[resp.ToolCallID]
	t.approvalMu.Unlock()

	if responseCh == nil {
		return
	}

	select {
	case responseCh <- resp:
	default:
	}
}
