package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

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
