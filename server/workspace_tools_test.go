package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"shelley.exe.dev/llm"
)

func TestWorkspaceToolsLifecycle(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/tools", bytes.NewBufferString(`{
		"name":"github",
		"description":"GitHub access",
		"protocol":"http",
		"actions":["read","write"],
		"provider":"alice@example.com",
		"config":{"baseUrl":"https://api.github.com"}
	}`))
	if err != nil {
		t.Fatalf("failed to build tool create request: %v", err)
	}
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("failed to create workspace tool: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from tool create, got %d", createResp.StatusCode)
	}

	var created workspaceToolInfo
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode tool create response: %v", err)
	}
	if created.Name != "github" || len(created.Actions) != 2 || created.Provider != "alice@example.com" {
		t.Fatalf("unexpected created tool: %#v", created)
	}

	listResp, err := http.Get(httpServer.URL + "/ws/tools")
	if err != nil {
		t.Fatalf("failed to list workspace tools: %v", err)
	}
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from tool list, got %d", listResp.StatusCode)
	}

	var tools []workspaceToolInfo
	if err := json.NewDecoder(listResp.Body).Decode(&tools); err != nil {
		t.Fatalf("failed to decode workspace tools list: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "github" {
		t.Fatalf("unexpected workspace tools list: %#v", tools)
	}

	getResp, err := http.Get(httpServer.URL + "/ws/tools/github")
	if err != nil {
		t.Fatalf("failed to get workspace tool: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from tool get, got %d", getResp.StatusCode)
	}

	var fetched workspaceToolInfo
	if err := json.NewDecoder(getResp.Body).Decode(&fetched); err != nil {
		t.Fatalf("failed to decode workspace tool get: %v", err)
	}
	if fetched.ToolID != created.ToolID {
		t.Fatalf("expected tool id %q, got %q", created.ToolID, fetched.ToolID)
	}

	grantReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/tools/github/grants", bytes.NewBufferString(`{
		"subject":"agent:*",
		"actions":["read"],
		"access":"allowed"
	}`))
	if err != nil {
		t.Fatalf("failed to build grant create request: %v", err)
	}
	grantReq.Header.Set("Content-Type", "application/json")

	grantResp, err := http.DefaultClient.Do(grantReq)
	if err != nil {
		t.Fatalf("failed to create workspace grant: %v", err)
	}
	defer grantResp.Body.Close()
	if grantResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from grant create, got %d", grantResp.StatusCode)
	}

	var grant workspaceGrantInfo
	if err := json.NewDecoder(grantResp.Body).Decode(&grant); err != nil {
		t.Fatalf("failed to decode grant create response: %v", err)
	}
	if grant.Subject != "agent:*" || len(grant.Actions) != 1 || grant.Actions[0] != "read" {
		t.Fatalf("unexpected created grant: %#v", grant)
	}

	getResp, err = http.Get(httpServer.URL + "/ws/tools/github")
	if err != nil {
		t.Fatalf("failed to refetch workspace tool: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from refetch tool, got %d", getResp.StatusCode)
	}
	if err := json.NewDecoder(getResp.Body).Decode(&fetched); err != nil {
		t.Fatalf("failed to decode refetched tool: %v", err)
	}
	if len(fetched.Grants) != 1 || fetched.Grants[0].GrantID != grant.GrantID {
		t.Fatalf("expected fetched tool to include grant, got %#v", fetched.Grants)
	}

	deleteGrantReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/ws/tools/github/grants/"+grant.GrantID, nil)
	if err != nil {
		t.Fatalf("failed to build grant delete request: %v", err)
	}
	deleteGrantResp, err := http.DefaultClient.Do(deleteGrantReq)
	if err != nil {
		t.Fatalf("failed to delete workspace grant: %v", err)
	}
	defer deleteGrantResp.Body.Close()
	if deleteGrantResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from grant delete, got %d", deleteGrantResp.StatusCode)
	}

	deleteToolReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/ws/tools/github", nil)
	if err != nil {
		t.Fatalf("failed to build tool delete request: %v", err)
	}
	deleteToolResp, err := http.DefaultClient.Do(deleteToolReq)
	if err != nil {
		t.Fatalf("failed to delete workspace tool: %v", err)
	}
	defer deleteToolResp.Body.Close()
	if deleteToolResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from tool delete, got %d", deleteToolResp.StatusCode)
	}

	notFoundResp, err := http.Get(httpServer.URL + "/ws/tools/github")
	if err != nil {
		t.Fatalf("failed to get deleted workspace tool: %v", err)
	}
	defer notFoundResp.Body.Close()
	if notFoundResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for deleted tool, got %d", notFoundResp.StatusCode)
	}
}

func TestWorkspaceToolsRejectDuplicateName(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/tools", bytes.NewBufferString(`{
			"name":"duplicate",
			"actions":["read"]
		}`))
		if err != nil {
			t.Fatalf("failed to build tool create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to create duplicate tool: %v", err)
		}
		defer resp.Body.Close()

		if i == 0 && resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected first duplicate create to succeed, got %d", resp.StatusCode)
		}
		if i == 1 && resp.StatusCode != http.StatusConflict {
			t.Fatalf("expected second duplicate create to conflict, got %d", resp.StatusCode)
		}
	}
}

func TestWorkspaceToolsRefreshActiveTopicTurns(t *testing.T) {
	server, _, predictable := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	sessionID := createWorkspaceTopic(t, httpServer.URL, "tool-refresh")

	sendTopicAPIChat(t, httpServer.URL, sessionID, "echo: before grant")
	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})
	if names := requestToolNames(predictable.GetLastRequest()); containsString(names, "workspace_github") {
		t.Fatalf("expected workspace tool to stay hidden before grants, got %#v", names)
	}

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"github",
		"description":"GitHub access",
		"actions":["read","write"]
	}`)
	createWorkspaceGrant(t, httpServer.URL, "github", `{
		"subject":"agent:*",
		"actions":["read"],
		"access":"allowed"
	}`)

	predictable.ClearRequests()
	sendTopicAPIChat(t, httpServer.URL, sessionID, "echo: after grant")
	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})
	if names := requestToolNames(predictable.GetLastRequest()); !containsString(names, "workspace_github") {
		t.Fatalf("expected granted workspace tool in request, got %#v", names)
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/ws/tools/github", nil)
	if err != nil {
		t.Fatalf("failed to build workspace tool delete request: %v", err)
	}
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("failed to delete workspace tool: %v", err)
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from workspace tool delete, got %d", deleteResp.StatusCode)
	}

	predictable.ClearRequests()
	sendTopicAPIChat(t, httpServer.URL, sessionID, "echo: after delete")
	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})
	if names := requestToolNames(predictable.GetLastRequest()); containsString(names, "workspace_github") {
		t.Fatalf("expected deleted workspace tool to disappear from request, got %#v", names)
	}
}

func TestWorkspaceToolsScopeGrantsToMatchingTopic(t *testing.T) {
	server, _, predictable := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	alphaSessionID := createWorkspaceTopic(t, httpServer.URL, "alpha")
	betaSessionID := createWorkspaceTopic(t, httpServer.URL, "beta")

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"gmail",
		"description":"Gmail access",
		"actions":["read","send"]
	}`)
	createWorkspaceGrant(t, httpServer.URL, "gmail", `{
		"subject":"agent:alpha",
		"actions":["read"],
		"access":"allowed"
	}`)

	predictable.ClearRequests()
	sendTopicAPIChat(t, httpServer.URL, alphaSessionID, "echo: alpha")
	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})
	if names := requestToolNames(predictable.GetLastRequest()); !containsString(names, "workspace_gmail") {
		t.Fatalf("expected topic-scoped workspace tool for alpha, got %#v", names)
	}

	predictable.ClearRequests()
	sendTopicAPIChat(t, httpServer.URL, betaSessionID, "echo: beta")
	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})
	if names := requestToolNames(predictable.GetLastRequest()); containsString(names, "workspace_gmail") {
		t.Fatalf("expected topic-scoped workspace tool to stay hidden from beta, got %#v", names)
	}
}

func TestWorkspaceToolsRejectInvalidGrantAccess(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"calendar",
		"actions":["read"]
	}`)

	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/tools/calendar/grants", bytes.NewBufferString(`{
		"subject":"agent:*",
		"actions":["read"],
		"access":"sometimes"
	}`))
	if err != nil {
		t.Fatalf("failed to build invalid grant request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send invalid grant request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 from invalid grant access, got %d", resp.StatusCode)
	}
}

func TestWorkspaceToolCallsAreLogged(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	sessionID := createWorkspaceTopic(t, httpServer.URL, "log-allowed")
	createWorkspaceTool(t, httpServer.URL, `{
		"name":"github",
		"actions":["read","write"]
	}`)
	createWorkspaceGrant(t, httpServer.URL, "github", `{
		"subject":"agent:*",
		"actions":["read"],
		"access":"allowed"
	}`)

	sendTopicAPIChat(t, httpServer.URL, sessionID, "workspace_tool: github read")

	var toolInfo workspaceToolInfo
	waitFor(t, 2*time.Second, func() bool {
		toolInfo = getWorkspaceToolInfo(t, httpServer.URL, "github")
		return len(toolInfo.Log) > 0
	})

	if toolInfo.Log[0].Action != "read" || toolInfo.Log[0].AccessDecision != workspaceGrantAllowed {
		t.Fatalf("unexpected workspace tool log entry: %#v", toolInfo.Log[0])
	}
	if toolInfo.Log[0].TopicName != "log-allowed" || toolInfo.Log[0].Subject != "agent:log-allowed" {
		t.Fatalf("unexpected workspace tool log scope: %#v", toolInfo.Log[0])
	}
}

func TestWorkspaceToolApprovalRequiredLogsDenied(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	sessionID := createWorkspaceTopic(t, httpServer.URL, "log-approval")
	createWorkspaceTool(t, httpServer.URL, `{
		"name":"gmail",
		"actions":["send"]
	}`)
	createWorkspaceGrant(t, httpServer.URL, "gmail", `{
		"subject":"agent:*",
		"actions":["send"],
		"access":"approval_required"
	}`)

	sendTopicAPIChat(t, httpServer.URL, sessionID, "workspace_tool: gmail send")

	var toolInfo workspaceToolInfo
	waitFor(t, 2*time.Second, func() bool {
		toolInfo = getWorkspaceToolInfo(t, httpServer.URL, "gmail")
		return len(toolInfo.Log) > 0
	})

	if toolInfo.Log[0].Action != "send" || toolInfo.Log[0].AccessDecision != workspaceGrantDenied {
		t.Fatalf("expected approval-required call to log denied, got %#v", toolInfo.Log[0])
	}
}

func TestWorkspaceToolApprovalResponseLogsApproved(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"gmail",
		"actions":["send"]
	}`)
	createWorkspaceGrant(t, httpServer.URL, "gmail", `{
		"subject":"agent:*",
		"actions":["send"],
		"access":"approval_required",
		"approvers":["alice@example.com"]
	}`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topic/approval-live"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial workspace websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, conn)

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{Type: "prompt", Data: "workspace_tool: gmail send"}); err != nil {
		t.Fatalf("failed to send approval prompt: %v", err)
	}

	var approvalRequest workspaceWSMessage
	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "approval_request" {
			approvalRequest = msg
			break
		}
	}

	if approvalRequest.Tool != "gmail" || approvalRequest.Action != "send" || approvalRequest.ToolCallID == "" {
		t.Fatalf("unexpected approval request: %#v", approvalRequest)
	}
	if len(approvalRequest.Approvers) != 1 || approvalRequest.Approvers[0] != "alice@example.com" {
		t.Fatalf("unexpected approval approvers: %#v", approvalRequest.Approvers)
	}

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{
		Type:       "approval_response",
		ToolCallID: approvalRequest.ToolCallID,
		Approved:   true,
		Approver:   "alice@example.com",
	}); err != nil {
		t.Fatalf("failed to send approval response: %v", err)
	}

	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "done" {
			break
		}
	}

	var toolInfo workspaceToolInfo
	waitFor(t, 2*time.Second, func() bool {
		toolInfo = getWorkspaceToolInfo(t, httpServer.URL, "gmail")
		return len(toolInfo.Log) > 0
	})

	if toolInfo.Log[0].Action != "send" || toolInfo.Log[0].AccessDecision != "approved" || toolInfo.Log[0].ApprovedBy != "alice@example.com" {
		t.Fatalf("expected approved audit log entry, got %#v", toolInfo.Log[0])
	}
}

func createWorkspaceTopic(t *testing.T, baseURL, topicName string) string {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/topics", bytes.NewBufferString(`{"name":"`+topicName+`"}`))
	if err != nil {
		t.Fatalf("failed to build topic create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create workspace topic: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from topic create, got %d", resp.StatusCode)
	}

	var created workspaceTopicInfo
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode topic create response: %v", err)
	}
	return created.SessionID
}

func createWorkspaceTool(t *testing.T, baseURL, rawJSON string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/ws/tools", bytes.NewBufferString(rawJSON))
	if err != nil {
		t.Fatalf("failed to build workspace tool create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create workspace tool: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from workspace tool create, got %d", resp.StatusCode)
	}
}

func createWorkspaceGrant(t *testing.T, baseURL, toolName, rawJSON string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/ws/tools/"+toolName+"/grants", bytes.NewBufferString(rawJSON))
	if err != nil {
		t.Fatalf("failed to build workspace grant create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create workspace grant: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from workspace grant create, got %d", resp.StatusCode)
	}
}

func sendTopicAPIChat(t *testing.T, baseURL, sessionID, message string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/conversation/"+sessionID+"/chat", bytes.NewBufferString(`{"message":"`+message+`","model":"predictable"}`))
	if err != nil {
		t.Fatalf("failed to build topic api chat request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send topic api chat request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 from topic api chat, got %d", resp.StatusCode)
	}
}

func requestToolNames(req *llm.Request) []string {
	if req == nil {
		return nil
	}
	names := make([]string, 0, len(req.Tools))
	for _, tool := range req.Tools {
		names = append(names, tool.Name)
	}
	return names
}

func getWorkspaceToolInfo(t *testing.T, baseURL, toolName string) workspaceToolInfo {
	t.Helper()

	resp, err := http.Get(baseURL + "/ws/tools/" + toolName)
	if err != nil {
		t.Fatalf("failed to fetch workspace tool info: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from workspace tool info, got %d", resp.StatusCode)
	}

	var info workspaceToolInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("failed to decode workspace tool info: %v", err)
	}
	return info
}
