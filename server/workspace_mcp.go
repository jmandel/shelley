package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"shelley.exe.dev/db/generated"
	"shelley.exe.dev/llm"
)

type workspaceMCPConfig struct {
	Transport            string            `json:"transport"`
	Type                 string            `json:"type"`
	Command              string            `json:"command"`
	Args                 []string          `json:"args"`
	Env                  map[string]string `json:"env"`
	Cwd                  string            `json:"cwd"`
	Endpoint             string            `json:"endpoint"`
	URL                  string            `json:"url"`
	Headers              map[string]string `json:"headers"`
	DisableStandaloneSSE bool              `json:"disableStandaloneSSE"`
	MaxRetries           *int              `json:"maxRetries,omitempty"`
}

type workspaceMCPHeaderRoundTripper struct {
	base    http.RoundTripper
	headers http.Header
}

func (t *workspaceMCPHeaderRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	for name, values := range t.headers {
		for _, value := range values {
			clone.Header.Add(name, value)
		}
	}
	return t.base.RoundTrip(clone)
}

func (s *Server) executeMCPWorkspaceTool(ctx context.Context, toolRecord generated.WorkspaceTool, req workspaceToolInvocation) llm.ToolOut {
	cfg, err := decodeWorkspaceMCPConfig(toolRecord)
	if err != nil {
		return llm.ErrorToolOut(err)
	}

	transport, err := s.newWorkspaceMCPTransport(ctx, cfg)
	if err != nil {
		return llm.ErrorToolOut(err)
	}

	args, err := req.MCPArguments()
	if err != nil {
		return llm.ErrorToolOut(err)
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "shelley", Version: "workspace"}, nil)
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return llm.ErrorfToolOut("connect mcp tool %s: %v", toolRecord.Name, err)
	}
	defer session.Close()

	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      req.Action,
		Arguments: args,
	})
	if err != nil {
		return llm.ErrorfToolOut("call mcp tool %s/%s: %v", toolRecord.Name, req.Action, err)
	}
	if result == nil {
		return llm.ToolOut{LLMContent: []llm.Content{llm.StringContent("")}}
	}

	if result.IsError {
		return llm.ErrorfToolOut("mcp tool %s/%s failed: %s", toolRecord.Name, req.Action, summarizeMCPToolResult(result))
	}

	content, err := mcpToolResultToLLMContent(result)
	if err != nil {
		return llm.ErrorToolOut(err)
	}
	return llm.ToolOut{LLMContent: content}
}

func decodeWorkspaceMCPConfig(toolRecord generated.WorkspaceTool) (workspaceMCPConfig, error) {
	var cfg workspaceMCPConfig
	if toolRecord.Config == nil || strings.TrimSpace(*toolRecord.Config) == "" {
		return cfg, fmt.Errorf("workspace tool %s is missing mcp config", toolRecord.Name)
	}
	if err := json.Unmarshal([]byte(*toolRecord.Config), &cfg); err != nil {
		return cfg, fmt.Errorf("invalid mcp config for %s: %w", toolRecord.Name, err)
	}

	cfg.Transport = strings.ToLower(strings.TrimSpace(cfg.Transport))
	if cfg.Transport == "" {
		cfg.Transport = strings.ToLower(strings.TrimSpace(cfg.Type))
	}
	switch cfg.Transport {
	case "stdio":
		if strings.TrimSpace(cfg.Command) == "" {
			return cfg, fmt.Errorf("workspace tool %s stdio transport requires command", toolRecord.Name)
		}
	case "streamable_http", "streamable-http":
		cfg.Transport = "streamable_http"
		if strings.TrimSpace(cfg.Endpoint) == "" {
			cfg.Endpoint = strings.TrimSpace(cfg.URL)
		}
		if strings.TrimSpace(cfg.Endpoint) == "" {
			return cfg, fmt.Errorf("workspace tool %s streamable_http transport requires endpoint", toolRecord.Name)
		}
	default:
		return cfg, fmt.Errorf("workspace tool %s has unsupported mcp transport %q", toolRecord.Name, cfg.Transport)
	}
	return cfg, nil
}

func (s *Server) newWorkspaceMCPTransport(ctx context.Context, cfg workspaceMCPConfig) (mcp.Transport, error) {
	switch cfg.Transport {
	case "stdio":
		commandPath, err := s.resolveWorkspaceMCPCommand(cfg.Command)
		if err != nil {
			return nil, err
		}
		cmd := exec.CommandContext(ctx, commandPath, cfg.Args...)
		if cwd, err := s.workspaceMCPCwd(cfg.Cwd); err != nil {
			return nil, err
		} else if cwd != "" {
			cmd.Dir = cwd
		}
		if len(cfg.Env) > 0 {
			cmd.Env = mergedWorkspaceMCPEnv(os.Environ(), cfg.Env)
		}
		cmd.Stderr = os.Stderr
		return &mcp.CommandTransport{Command: cmd}, nil
	case "streamable_http":
		transport := &mcp.StreamableClientTransport{
			Endpoint:             cfg.Endpoint,
			DisableStandaloneSSE: cfg.DisableStandaloneSSE,
		}
		if cfg.MaxRetries != nil {
			transport.MaxRetries = *cfg.MaxRetries
		}
		if len(cfg.Headers) > 0 {
			transport.HTTPClient = &http.Client{
				Transport: &workspaceMCPHeaderRoundTripper{
					base:    http.DefaultTransport,
					headers: workspaceMCPHeaders(cfg.Headers),
				},
			}
		}
		return transport, nil
	default:
		return nil, fmt.Errorf("unsupported mcp transport %q", cfg.Transport)
	}
}

func (s *Server) resolveWorkspaceMCPCommand(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("missing mcp stdio command")
	}
	if filepath.IsAbs(raw) || strings.ContainsRune(raw, filepath.Separator) {
		return raw, nil
	}

	if toolsDir := strings.TrimSpace(os.Getenv("WORKSPACE_TOOLS_DIR")); toolsDir != "" {
		candidate := filepath.Join(toolsDir, "bin", raw)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}

	resolved, err := exec.LookPath(raw)
	if err != nil {
		return "", fmt.Errorf("resolve mcp stdio command %q: %w", raw, err)
	}
	return resolved, nil
}

func (s *Server) workspaceMCPCwd(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return s.workspaceRoot, nil
	}
	if filepath.IsAbs(raw) {
		return filepath.Clean(raw), nil
	}
	return filepath.Clean(filepath.Join(s.workspaceRoot, raw)), nil
}

func mergedWorkspaceMCPEnv(base []string, overrides map[string]string) []string {
	if len(overrides) == 0 {
		return base
	}

	seen := make(map[string]struct{}, len(overrides))
	env := make([]string, 0, len(base)+len(overrides))
	for _, entry := range base {
		name, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if value, exists := overrides[name]; exists {
			env = append(env, name+"="+value)
			seen[name] = struct{}{}
			continue
		}
		env = append(env, entry)
	}
	for name, value := range overrides {
		if _, ok := seen[name]; ok {
			continue
		}
		env = append(env, name+"="+value)
	}
	return env
}

func (req workspaceToolInvocation) MCPArguments() (map[string]any, error) {
	if len(req.Input) == 0 {
		return map[string]any{}, nil
	}
	trimmed := strings.TrimSpace(string(req.Input))
	if trimmed == "" || trimmed == "null" {
		return map[string]any{}, nil
	}

	var args map[string]any
	if err := json.Unmarshal(req.Input, &args); err != nil {
		return nil, fmt.Errorf("invalid workspace tool input payload: %w", err)
	}
	if args == nil {
		return map[string]any{}, nil
	}
	return args, nil
}

func mcpToolResultToLLMContent(result *mcp.CallToolResult) ([]llm.Content, error) {
	if result == nil {
		return []llm.Content{llm.StringContent("")}, nil
	}

	var content []llm.Content
	for _, item := range result.Content {
		converted, err := mcpContentToLLMContent(item)
		if err != nil {
			return nil, err
		}
		content = append(content, converted...)
	}

	if result.StructuredContent != nil {
		text, err := jsonText(result.StructuredContent)
		if err != nil {
			return nil, fmt.Errorf("marshal mcp structured content: %w", err)
		}
		content = append(content, llm.StringContent(text))
	}

	if len(content) == 0 {
		return []llm.Content{llm.StringContent("")}, nil
	}
	return content, nil
}

func mcpContentToLLMContent(content mcp.Content) ([]llm.Content, error) {
	switch v := content.(type) {
	case *mcp.TextContent:
		return []llm.Content{llm.StringContent(v.Text)}, nil
	case *mcp.EmbeddedResource:
		if v.Resource != nil && v.Resource.Text != "" {
			return []llm.Content{llm.StringContent(v.Resource.Text)}, nil
		}
	case *mcp.ResourceLink:
		text := strings.TrimSpace(strings.Join([]string{v.Title, v.Name, v.URI}, " "))
		if text != "" {
			return []llm.Content{llm.StringContent(text)}, nil
		}
	}

	text, err := jsonText(content)
	if err != nil {
		return nil, fmt.Errorf("marshal mcp content: %w", err)
	}
	return []llm.Content{llm.StringContent(text)}, nil
}

func summarizeMCPToolResult(result *mcp.CallToolResult) string {
	content, err := mcpToolResultToLLMContent(result)
	if err != nil {
		return err.Error()
	}
	texts := make([]string, 0, len(content))
	for _, item := range content {
		if item.Text != "" {
			texts = append(texts, item.Text)
		}
	}
	if len(texts) == 0 {
		return "no error details"
	}
	return strings.Join(texts, "\n")
}

func jsonText(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func workspaceMCPHeaders(src map[string]string) http.Header {
	headers := make(http.Header, len(src))
	for name, value := range src {
		headers.Set(name, value)
	}
	return headers
}
