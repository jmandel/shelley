package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"shelley.exe.dev/db"
	"shelley.exe.dev/db/generated"
	"shelley.exe.dev/llm"
)

const workspaceMCPStdioHelperEnv = "SHELLEY_WORKSPACE_MCP_STDIO_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(workspaceMCPStdioHelperEnv) != "" {
		runWorkspaceMCPStdioHelper()
		return
	}
	os.Exit(m.Run())
}

func runWorkspaceMCPStdioHelper() {
	server := newWorkspaceMCPTestServer(func(args map[string]any) (*mcp.CallToolResult, any, error) {
		name, _ := args["name"].(string)
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: "Hello " + name},
			},
		}, nil, nil
	})
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		os.Exit(1)
	}
}

func TestWorkspaceToolMCPStdioExecutesTextTool(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"greeter",
		"actions":["greet"],
		"config":{
			"transport":"stdio",
			"command":"`+os.Args[0]+`",
			"env":{"`+workspaceMCPStdioHelperEnv+`":"1"}
		}
	}`)
	createWorkspaceGrant(t, httpServer.URL, "greeter", `{
		"subject":"agent:*",
		"actions":["greet"],
		"access":"allowed"
	}`)

	tool := workspaceRuntimeTool(t, server, "alpha", "workspace_greeter")
	result := tool.Run(context.Background(), []byte(`{"action":"greet","input":{"name":"Shelley"}}`))
	if result.Error != nil {
		t.Fatalf("expected stdio mcp tool to succeed, got %v", result.Error)
	}
	if len(result.LLMContent) != 1 || result.LLMContent[0].Text != "Hello Shelley" {
		t.Fatalf("unexpected stdio mcp tool output: %#v", result.LLMContent)
	}
}

func TestWorkspaceToolMCPStdioResolvesCommandFromWorkspaceToolsDir(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	toolsDir := t.TempDir()
	binDir := filepath.Join(toolsDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("failed to create tools bin dir: %v", err)
	}
	helperPath := filepath.Join(binDir, "workspace-mcp-helper")
	copyExecutable(t, os.Args[0], helperPath)
	t.Setenv("WORKSPACE_TOOLS_DIR", toolsDir)

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"greeter",
		"actions":["greet"],
		"config":{
			"transport":"stdio",
			"command":"workspace-mcp-helper",
			"env":{"`+workspaceMCPStdioHelperEnv+`":"1"}
		}
	}`)
	createWorkspaceGrant(t, httpServer.URL, "greeter", `{
		"subject":"agent:*",
		"actions":["greet"],
		"access":"allowed"
	}`)

	tool := workspaceRuntimeTool(t, server, "alpha", "workspace_greeter")
	result := tool.Run(context.Background(), []byte(`{"action":"greet","input":{"name":"Shelley"}}`))
	if result.Error != nil {
		t.Fatalf("expected stdio mcp tool from workspace tools dir to succeed, got %v", result.Error)
	}
	if len(result.LLMContent) != 1 || result.LLMContent[0].Text != "Hello Shelley" {
		t.Fatalf("unexpected stdio mcp tool output: %#v", result.LLMContent)
	}
}

func TestWorkspaceToolMCPStdioBunFixtureFromWorkspace(t *testing.T) {
	server, _, _ := newTestServer(t)
	workspaceRoot := t.TempDir()
	if err := server.SetWorkspaceRoot(workspaceRoot); err != nil {
		t.Fatalf("failed to set workspace root: %v", err)
	}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	fixtureSource, err := filepath.Abs(filepath.Join("..", "..", "shelleymanager", "manager", "testdata", "hl7-jira-mcp.js"))
	if err != nil {
		t.Fatalf("failed to resolve fixture source: %v", err)
	}
	fixtureData, err := os.ReadFile(fixtureSource)
	if err != nil {
		t.Fatalf("failed to read fixture source: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(workspaceRoot, ".demo"), 0o755); err != nil {
		t.Fatalf("failed to create workspace fixture dir: %v", err)
	}
	fixturePath := filepath.Join(workspaceRoot, ".demo", "hl7-jira-mcp.js")
	if err := os.WriteFile(fixturePath, fixtureData, 0o755); err != nil {
		t.Fatalf("failed to write workspace fixture: %v", err)
	}

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"hl7-jira",
		"description":"Search realistic HL7 Jira fixture data",
		"protocol":"mcp",
		"transport":{
			"type":"stdio",
			"command":"bun",
			"args":["./.demo/hl7-jira-mcp.js"],
			"cwd":"."
		},
		"tools":[
			{
				"name":"jira.search",
				"description":"Search realistic HL7 Jira issues related to validation and FHIRPath behavior",
				"inputSchema":{
					"type":"object",
					"properties":{"query":{"type":"string"}},
					"required":["query"],
					"additionalProperties":false
				}
			}
		]
	}`)
	createWorkspaceGrant(t, httpServer.URL, "hl7-jira", `{
		"subject":"agent:*",
		"tools":["jira.search"],
		"access":"allowed"
	}`)

	tool := workspaceRuntimeTool(t, server, "alpha", "workspace_hl7-jira")
	result := tool.Run(context.Background(), []byte(`{"action":"jira.search","input":{"query":"validation error handling"}}`))
	if result.Error != nil {
		t.Fatalf("expected bun fixture mcp tool to succeed, got %v", result.Error)
	}
	if len(result.LLMContent) == 0 || !strings.Contains(result.LLMContent[0].Text, "FHIR-53953") {
		t.Fatalf("expected Jira fixture content in tool output, got %#v", result.LLMContent)
	}
}

func TestWorkspaceToolManagerProxyInvokesManagerEndpoint(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	managerCalls := 0
	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		managerCalls++
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization = %q", got)
		}
		if want := "/internal/namespaces/acme/workspaces/bp-ig-fix/tools/hl7-jira/invoke"; r.URL.Path != want {
			t.Fatalf("path = %q, want %q", r.URL.Path, want)
		}
		var req managerProxyInvokeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode invoke request: %v", err)
		}
		if req.Action != "jira.search" {
			t.Fatalf("action = %q", req.Action)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(managerProxyInvokeResponse{Content: "FHIR-53953\nFHIR-53960"})
	}))
	defer managerServer.Close()

	t.Setenv("WORKSPACE_MANAGER_INTERNAL_URL", managerServer.URL)
	t.Setenv("WORKSPACE_MANAGER_TOKEN", "test-token")
	t.Setenv("WORKSPACE_NAME", "bp-ig-fix")
	t.Setenv("WORKSPACE_NAMESPACE", "acme")

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"hl7-jira",
		"description":"Search realistic HL7 Jira fixture data",
		"protocol":"mcp",
		"transport":{
			"type":"manager_proxy"
		},
		"tools":[
			{
				"name":"jira.search",
				"description":"Search realistic HL7 Jira issues related to validation and FHIRPath behavior",
				"inputSchema":{
					"type":"object",
					"properties":{"query":{"type":"string"}},
					"required":["query"],
					"additionalProperties":false
				}
			}
		]
	}`)
	createWorkspaceGrant(t, httpServer.URL, "hl7-jira", `{
		"subject":"agent:*",
		"tools":["jira.search"],
		"access":"allowed"
	}`)

	tool := workspaceRuntimeTool(t, server, "alpha", "workspace_hl7-jira")
	result := tool.Run(context.Background(), []byte(`{"action":"jira.search","input":{"query":"validation error handling"}}`))
	if result.Error != nil {
		t.Fatalf("expected manager proxy mcp tool to succeed, got %v", result.Error)
	}
	if managerCalls != 1 {
		t.Fatalf("expected one manager invoke call, got %d", managerCalls)
	}
	if len(result.LLMContent) == 0 || !strings.Contains(result.LLMContent[0].Text, "FHIR-53953") {
		t.Fatalf("expected manager proxy tool output, got %#v", result.LLMContent)
	}
}

func TestWorkspaceToolMCPStreamableHTTPExecutesTextTool(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	mcpServer := newWorkspaceMCPTestServer(func(args map[string]any) (*mcp.CallToolResult, any, error) {
		name, _ := args["name"].(string)
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: "Hi " + name},
			},
			StructuredContent: map[string]any{"ok": true},
		}, nil, nil
	})
	transportServer := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{DisableLocalhostProtection: true}))
	defer transportServer.Close()

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"http-greeter",
		"actions":["greet"],
		"config":{
			"transport":"streamable_http",
			"endpoint":"`+transportServer.URL+`",
			"disableStandaloneSSE":true
		}
	}`)
	createWorkspaceGrant(t, httpServer.URL, "http-greeter", `{
		"subject":"agent:*",
		"actions":["greet"],
		"access":"allowed"
	}`)

	tool := workspaceRuntimeTool(t, server, "alpha", "workspace_http-greeter")
	result := tool.Run(context.Background(), []byte(`{"action":"greet","input":{"name":"Shelley"}}`))
	if result.Error != nil {
		t.Fatalf("expected streamable http mcp tool to succeed, got %v", result.Error)
	}
	if len(result.LLMContent) != 2 {
		t.Fatalf("expected text plus structured content, got %#v", result.LLMContent)
	}
	if result.LLMContent[0].Text != "Hi Shelley" {
		t.Fatalf("unexpected streamable http text output: %#v", result.LLMContent)
	}
	if result.LLMContent[1].Text != `{"ok":true}` {
		t.Fatalf("unexpected structured content fallback: %#v", result.LLMContent)
	}
}

func TestWorkspaceToolMCPRejectsNonObjectInput(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"greeter",
		"actions":["greet"],
		"config":{
			"transport":"stdio",
			"command":"`+os.Args[0]+`",
			"env":{"`+workspaceMCPStdioHelperEnv+`":"1"}
		}
	}`)
	createWorkspaceGrant(t, httpServer.URL, "greeter", `{
		"subject":"agent:*",
		"actions":["greet"],
		"access":"allowed"
	}`)

	tool := workspaceRuntimeTool(t, server, "alpha", "workspace_greeter")
	result := tool.Run(context.Background(), []byte(`{"action":"greet","input":["bad"]}`))
	if result.Error == nil {
		t.Fatal("expected invalid input payload to fail")
	}
}

func TestWorkspaceToolMCPStreamableHTTPEndToEndTopicTurn(t *testing.T) {
	server, database, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	mcpServer := newWorkspaceMCPTestServer(func(args map[string]any) (*mcp.CallToolResult, any, error) {
		name, _ := args["name"].(string)
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: "Hi " + name},
			},
		}, nil, nil
	})
	transportServer := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{DisableLocalhostProtection: true}))
	defer transportServer.Close()

	sessionID := createWorkspaceTopic(t, httpServer.URL, "mcp-e2e")
	createWorkspaceTool(t, httpServer.URL, `{
		"name":"http-greeter",
		"actions":["greet"],
		"config":{
			"transport":"streamable_http",
			"endpoint":"`+transportServer.URL+`",
			"disableStandaloneSSE":true
		}
	}`)
	createWorkspaceGrant(t, httpServer.URL, "http-greeter", `{
		"subject":"agent:*",
		"actions":["greet"],
		"access":"allowed"
	}`)

	sendTopicAPIChat(t, httpServer.URL, sessionID, `workspace_tool_json: http-greeter greet {"name":"Shelley"}`)

	waitFor(t, 2*time.Second, func() bool {
		return topicHasToolResultText(t, database, sessionID, "Hi Shelley")
	})
}

func TestWorkspaceToolMCPApprovalExecutesAfterApproval(t *testing.T) {
	server, database, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	mcpServer := newWorkspaceMCPTestServer(func(args map[string]any) (*mcp.CallToolResult, any, error) {
		name, _ := args["name"].(string)
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: "Approved hi " + name},
			},
		}, nil, nil
	})
	transportServer := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{DisableLocalhostProtection: true}))
	defer transportServer.Close()

	createWorkspaceTool(t, httpServer.URL, `{
		"name":"approval-greeter",
		"actions":["greet"],
		"config":{
			"transport":"streamable_http",
			"endpoint":"`+transportServer.URL+`",
			"disableStandaloneSSE":true
		}
	}`)
	createWorkspaceGrant(t, httpServer.URL, "approval-greeter", `{
		"subject":"agent:*",
		"actions":["greet"],
		"access":"approval_required",
		"approvers":["alice@example.com"]
	}`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + httpServer.URL[len("http"):] + "/ws/topic/mcp-approval"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial workspace websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, conn)
	sessionID := getWorkspaceTopicInfo(t, httpServer.URL+"/ws/topics/mcp-approval").SessionID

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{
		Type: "prompt",
		Data: `workspace_tool_json: approval-greeter greet {"name":"Shelley"}`,
	}); err != nil {
		t.Fatalf("failed to send approval workspace prompt: %v", err)
	}

	var (
		approvalRequest workspaceWSMessage
	)
	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "approval_request" {
			approvalRequest = msg
			break
		}
	}

	if approvalRequest.Tool != "approval-greeter" || approvalRequest.Action != "greet" || approvalRequest.ToolCallID == "" {
		t.Fatalf("unexpected approval request: %#v", approvalRequest)
	}

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{
		Type:       "approval_response",
		ToolCallID: approvalRequest.ToolCallID,
		Approved:   true,
		Approver:   "alice@example.com",
	}); err != nil {
		t.Fatalf("failed to send approval response: %v", err)
	}

	var toolCalled bool
	var toolUpdated bool
	var received []workspaceWSMessage
	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		received = append(received, msg)
		if msg.Type == "tool_call" && msg.Title == "workspace_approval-greeter" {
			toolCalled = true
		}
		if msg.Type == "tool_update" && msg.Title == "workspace_approval-greeter" && msg.Status == "completed" {
			toolUpdated = true
		}
		if msg.Type == "done" {
			break
		}
	}
	if !toolCalled {
		t.Fatalf("expected workspace tool call after approval, got %#v", received)
	}
	if !toolUpdated {
		t.Fatalf("expected workspace tool update after approval, got %#v", received)
	}

	waitFor(t, 2*time.Second, func() bool {
		return topicHasToolResultText(t, database, sessionID, "Approved hi Shelley")
	})
}

func newWorkspaceMCPTestServer(handler func(args map[string]any) (*mcp.CallToolResult, any, error)) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "workspace-test", Version: "v0.0.1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "greet"}, func(ctx context.Context, req *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
		return handler(args)
	})
	return server
}

func workspaceRuntimeTool(t *testing.T, server *Server, topicName, toolName string) *llm.Tool {
	t.Helper()

	tools, err := server.buildTopicWorkspaceTools(context.Background(), topicName)
	if err != nil {
		t.Fatalf("failed to build runtime workspace tools: %v", err)
	}
	for _, tool := range tools {
		if tool.Name == toolName {
			return tool
		}
	}
	t.Fatalf("workspace runtime tool %q not found in %#v", toolName, requestToolNames(&llm.Request{Tools: tools}))
	return nil
}

func topicHasToolResultText(t *testing.T, database *db.DB, conversationID, want string) bool {
	t.Helper()

	var messages []generated.Message
	err := database.Queries(context.Background(), func(q *generated.Queries) error {
		var qerr error
		messages, qerr = q.ListMessages(context.Background(), conversationID)
		return qerr
	})
	if err != nil {
		t.Fatalf("failed to list topic messages: %v", err)
	}

	for _, msg := range messages {
		if msg.Type != string(db.MessageTypeUser) || msg.LlmData == nil {
			continue
		}

		var llmMsg llm.Message
		if err := json.Unmarshal([]byte(*msg.LlmData), &llmMsg); err != nil {
			continue
		}
		for _, content := range llmMsg.Content {
			if content.Type != llm.ContentTypeToolResult {
				continue
			}
			for _, result := range content.ToolResult {
				if result.Type == llm.ContentTypeText && result.Text == want {
					return true
				}
			}
		}
	}
	return false
}

func copyExecutable(t *testing.T, src, dst string) {
	t.Helper()

	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("failed to open source executable: %v", err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatalf("failed to create destination executable: %v", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		t.Fatalf("failed to copy executable: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("failed to close destination executable: %v", err)
	}
}
