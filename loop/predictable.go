package loop

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"shelley.exe.dev/llm"
)

// PredictableService is an LLM service that returns predictable responses for testing.
//
// To add new test patterns, update the Do() method directly by adding cases to the switch
// statement or new prefix checks. Do not extend or wrap this service - modify it in place.
// Available patterns include:
//   - "echo: <text>" - echoes the text back
//   - "bash: <command>" - triggers bash tool with command
//   - "think: <thoughts>" - returns response with extended thinking content
//   - "subagent: <slug> <prompt>" - triggers subagent tool
//   - "change_dir: <path>" - triggers change_dir tool
//   - "delay: <seconds>" - delays response by specified seconds
//   - "ws ..." - demo-oriented shorthand for local tools, MCP tools, and targeted delays
//   - See Do() method for complete list of supported patterns
type PredictableService struct {
	// TokenContextWindow size
	tokenContextWindow int
	mu                 sync.Mutex
	// Recent requests for testing inspection
	recentRequests []*llm.Request
	responseDelay  time.Duration
}

type wsDemoScript struct {
	PreDelay   time.Duration
	ToolDelay  time.Duration
	AfterDelay time.Duration
	Verb       string
	Text       string
	ToolName   string
	ToolAction string
	ToolInput  json.RawMessage
	AfterText  string
}

func (s wsDemoScript) usesTool() bool {
	switch s.Verb {
	case "bash", "validator", "publisher", "jira", "tool":
		return true
	default:
		return false
	}
}

const wsDemoLanguageGuide = `WS language quick guide

Use: ws [tags...]

Primary actions:
- text "..." or echo "..."
- bash "..."
- validator "path-or-args"
- publisher "path-or-args"
- jira "search terms"
- tool <tool-name> action <action-name> input '{"json":"value"}'

Timing tags:
- pause2 or pause 2
- toolpause3 or toolpause 3
- afterpause1 or afterpause 1
- aftertext "Done."

Examples:
- ws text "Thanks, what should we do next?"
- ws pause2 jira "Observation.component slicing"
- ws validator "input/fsh/BloodPressurePanel.fsh" toolpause3 aftertext "Validator finished."
- ws tool hl7-jira action jira.search input '{"query":"validator warning blood pressure slicing"}'

Rules:
- tags can appear in any order
- use exactly one primary action
- wrap multi-word values in quotes
- input must be valid JSON`

// NewPredictableService creates a new predictable LLM service
func NewPredictableService() *PredictableService {
	svc := &PredictableService{
		tokenContextWindow: 200000,
	}

	if delayEnv := os.Getenv("PREDICTABLE_DELAY_MS"); delayEnv != "" {
		if ms, err := strconv.Atoi(delayEnv); err == nil && ms > 0 {
			svc.responseDelay = time.Duration(ms) * time.Millisecond
		}
	}

	return svc
}

// TokenContextWindow returns the maximum token context window size
func (s *PredictableService) TokenContextWindow() int {
	return s.tokenContextWindow
}

// MaxImageDimension returns the maximum allowed image dimension.
func (s *PredictableService) MaxImageDimension() int {
	return 2000
}

// Do processes a request and returns a predictable response based on the input text
func (s *PredictableService) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	// Store request for testing inspection
	s.mu.Lock()
	delay := s.responseDelay
	s.recentRequests = append(s.recentRequests, req)
	// Keep only last 10 requests
	if len(s.recentRequests) > 10 {
		s.recentRequests = s.recentRequests[len(s.recentRequests)-10:]
	}
	s.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Calculate input token count based on the request content
	inputTokens := s.countRequestTokens(req)

	// Extract the text content from the last user message
	inputText, latestUserText, hasToolResult := predictableInputContext(req)

	// If the message is purely a tool result (no text), acknowledge it and end turn
	if hasToolResult && inputText == "" {
		if script, err := parseWSDemoScript(latestUserText); err == nil && script.usesTool() {
			if err := waitPredictableDelay(ctx, script.AfterDelay); err != nil {
				return nil, err
			}
			return s.makeResponse(script.AfterText, inputTokens), nil
		}
		return s.makeResponse("Done.", inputTokens), nil
	}

	// Handle input using case statements
	switch inputText {
	case "hello":
		return s.makeResponse("Well, hi there!", inputTokens), nil

	case "Hello":
		return s.makeResponse("Hello! I'm Shelley, your AI assistant. How can I help you today?", inputTokens), nil

	case "Create an example":
		return s.makeThinkingResponse("I'll create a simple example for you.", inputTokens), nil

	case "screenshot":
		// Trigger a screenshot of the current page
		return s.makeScreenshotToolResponse("", inputTokens), nil

	case "wide tables":
		return s.makeResponse(wideTablesMarkdown, inputTokens), nil

	case "tool smorgasbord":
		// Return a response with all tool types for testing
		return s.makeToolSmorgasbordResponse(inputTokens), nil

	case "echo: foo":
		return s.makeResponse("foo", inputTokens), nil

	case "patch fail":
		// Trigger a patch that will fail (file doesn't exist)
		return s.makePatchToolResponse("/nonexistent/file/that/does/not/exist.txt", inputTokens), nil

	case "patch success":
		// Trigger a patch that will succeed (using overwrite, which creates the file)
		return s.makePatchToolResponseOverwrite("/tmp/test-patch-success.txt", inputTokens), nil

	case "patch bad json":
		// Trigger a patch with malformed JSON (simulates Anthropic sending invalid JSON)
		return s.makeMalformedPatchToolResponse(inputTokens), nil

	case "maxTokens":
		// Simulate a max_tokens truncation
		return s.makeMaxTokensResponse("This is a truncated response that was cut off mid-sentence because the output token limit was", inputTokens), nil

	default:
		// Handle pattern-based inputs
		if strings.HasPrefix(inputText, "echo: ") {
			text := strings.TrimPrefix(inputText, "echo: ")
			return s.makeResponse(text, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "bash: ") {
			cmd := strings.TrimPrefix(inputText, "bash: ")
			return s.makeBashToolResponse(cmd, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "think: ") {
			thoughts := strings.TrimPrefix(inputText, "think: ")
			return s.makeThinkingResponse(thoughts, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "patch: ") {
			filePath := strings.TrimPrefix(inputText, "patch: ")
			return s.makePatchToolResponse(filePath, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "error: ") {
			errorMsg := strings.TrimPrefix(inputText, "error: ")
			return nil, fmt.Errorf("predictable error: %s", errorMsg)
		}

		if strings.HasPrefix(inputText, "screenshot: ") {
			selector := strings.TrimSpace(strings.TrimPrefix(inputText, "screenshot: "))
			return s.makeScreenshotToolResponse(selector, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "subagent: ") {
			// Format: "subagent: <slug> <prompt>"
			parts := strings.SplitN(strings.TrimPrefix(inputText, "subagent: "), " ", 2)
			slug := parts[0]
			prompt := "do the task"
			if len(parts) > 1 {
				prompt = parts[1]
			}
			return s.makeSubagentToolResponse(slug, prompt, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "markdown: ") {
			text := strings.TrimPrefix(inputText, "markdown: ")
			return s.makeResponse(text, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "change_dir: ") {
			path := strings.TrimPrefix(inputText, "change_dir: ")
			return s.makeChangeDirToolResponse(path, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "workspace_tool: ") {
			parts := strings.SplitN(strings.TrimPrefix(inputText, "workspace_tool: "), " ", 2)
			if len(parts) == 2 && s.requestHasTool(req, "workspace_"+parts[0]) {
				return s.makeWorkspaceToolResponse(parts[0], parts[1], nil, inputTokens), nil
			}
			return s.makeResponse("workspace tool unavailable", inputTokens), nil
		}

		if strings.HasPrefix(inputText, "workspace_tool_json: ") {
			parts := strings.SplitN(strings.TrimPrefix(inputText, "workspace_tool_json: "), " ", 3)
			if len(parts) == 3 && s.requestHasTool(req, "workspace_"+parts[0]) {
				input := json.RawMessage(parts[2])
				if !json.Valid(input) {
					return s.makeResponse("invalid workspace tool input json", inputTokens), nil
				}
				return s.makeWorkspaceToolResponse(parts[0], parts[1], input, inputTokens), nil
			}
			return s.makeResponse("workspace tool unavailable", inputTokens), nil
		}

		if strings.HasPrefix(inputText, "delay: ") {
			delayStr := strings.TrimPrefix(inputText, "delay: ")
			delaySeconds, err := strconv.ParseFloat(delayStr, 64)
			if err == nil && delaySeconds > 0 {
				delayDuration := time.Duration(delaySeconds * float64(time.Second))
				if err := waitPredictableDelay(ctx, delayDuration); err != nil {
					return nil, err
				}
			}
			return s.makeResponse(fmt.Sprintf("Delayed for %s seconds", delayStr), inputTokens), nil
		}

		if isWSDemoHelp(inputText) {
			return s.makeResponse(wsDemoLanguageGuide, inputTokens), nil
		}

		if strings.HasPrefix(inputText, "ws ") || strings.HasPrefix(inputText, "ws: ") || inputText == "ws" || inputText == "ws:" {
			script, err := parseWSDemoScript(inputText)
			if err != nil {
				return s.makeResponse(err.Error(), inputTokens), nil
			}
			return s.makeWSDemoResponse(ctx, req, script, inputTokens)
		}

		// Default response for undefined inputs
		return s.makeResponse("edit predictable.go to add a response for that one...", inputTokens), nil
	}
}

func predictableInputContext(req *llm.Request) (currentText, latestUserText string, hasToolResult bool) {
	if req == nil || len(req.Messages) == 0 {
		return "", "", false
	}

	lastMessage := req.Messages[len(req.Messages)-1]
	if lastMessage.Role == llm.MessageRoleUser {
		for _, content := range lastMessage.Content {
			if content.Type == llm.ContentTypeText {
				currentText = strings.TrimSpace(content.Text)
			} else if content.Type == llm.ContentTypeToolResult {
				hasToolResult = true
			}
		}
	}

	for i := len(req.Messages) - 1; i >= 0; i-- {
		msg := req.Messages[i]
		if msg.Role != llm.MessageRoleUser {
			continue
		}
		for j := len(msg.Content) - 1; j >= 0; j-- {
			content := msg.Content[j]
			if content.Type == llm.ContentTypeText {
				latestUserText = strings.TrimSpace(content.Text)
				if latestUserText != "" {
					return currentText, latestUserText, hasToolResult
				}
			}
		}
	}

	return currentText, latestUserText, hasToolResult
}

func isWSDemoHelp(inputText string) bool {
	switch strings.TrimSpace(inputText) {
	case "ws help", "ws: help", "ws tutorial", "ws: tutorial", "ws examples", "ws: examples":
		return true
	default:
		return false
	}
}

func parseWSDemoScript(inputText string) (wsDemoScript, error) {
	trimmed := strings.TrimSpace(inputText)
	args := ""
	switch {
	case trimmed == "ws" || trimmed == "ws:":
		return wsDemoScript{}, fmt.Errorf("ws usage: ws [pauseN|pause N] [toolpauseN|toolpause N] [afterpauseN|afterpause N] text|bash|validator|publisher|jira|tool ...")
	case strings.HasPrefix(trimmed, "ws:"):
		args = strings.TrimSpace(strings.TrimPrefix(trimmed, "ws:"))
	case strings.HasPrefix(trimmed, "ws "):
		args = strings.TrimSpace(strings.TrimPrefix(trimmed, "ws "))
	default:
		return wsDemoScript{}, fmt.Errorf("not a ws command")
	}

	if args == "" {
		return wsDemoScript{}, fmt.Errorf("ws usage: ws [pauseN|pause N] [toolpauseN|toolpause N] [afterpauseN|afterpause N] text|bash|validator|publisher|jira|tool ...")
	}

	fields, err := splitWSDemoArgs(args)
	if err != nil {
		return wsDemoScript{}, err
	}
	script := wsDemoScript{}
	for idx := 0; idx < len(fields); idx++ {
		token := fields[idx]
		switch {
		case strings.HasPrefix(token, "toolpause"):
			delay, matched, err := parseWSDemoDelayToken("toolpause", token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if matched {
				script.ToolDelay = delay
				continue
			}
		case strings.HasPrefix(token, "afterpause"):
			delay, matched, err := parseWSDemoDelayToken("afterpause", token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if matched {
				script.AfterDelay = delay
				continue
			}
		case strings.HasPrefix(token, "pause"):
			delay, matched, err := parseWSDemoDelayToken("pause", token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if matched {
				script.PreDelay = delay
				continue
			}
		}

		switch token {
		case "pause", "toolpause", "afterpause":
			if idx+1 >= len(fields) {
				return wsDemoScript{}, fmt.Errorf("%s requires a duration", token)
			}
			delay, err := parseWSDemoDelayValue(fields[idx+1])
			if err != nil {
				return wsDemoScript{}, fmt.Errorf("invalid %s value %q", token, fields[idx+1])
			}
			switch token {
			case "pause":
				script.PreDelay = delay
			case "toolpause":
				script.ToolDelay = delay
			case "afterpause":
				script.AfterDelay = delay
			}
			idx++
		case "text", "echo":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if err := script.setVerb("text"); err != nil {
				return wsDemoScript{}, err
			}
			script.Text = value
		case "bash":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if err := script.setVerb("bash"); err != nil {
				return wsDemoScript{}, err
			}
			script.Text = value
		case "validator":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if err := script.setVerb("validator"); err != nil {
				return wsDemoScript{}, err
			}
			script.Text = value
		case "publisher":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if err := script.setVerb("publisher"); err != nil {
				return wsDemoScript{}, err
			}
			script.Text = value
		case "jira":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if err := script.setVerb("jira"); err != nil {
				return wsDemoScript{}, err
			}
			script.Text = value
		case "tool":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			if err := script.setVerb("tool"); err != nil {
				return wsDemoScript{}, err
			}
			script.ToolName = value
		case "action":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			script.ToolAction = value
		case "input":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			input := json.RawMessage(value)
			if !json.Valid(input) {
				return wsDemoScript{}, fmt.Errorf("ws input must be valid JSON")
			}
			script.ToolInput = input
		case "aftertext":
			value, err := consumeWSDemoValue(fields, &idx, token)
			if err != nil {
				return wsDemoScript{}, err
			}
			script.AfterText = value
		default:
			return wsDemoScript{}, fmt.Errorf("unknown ws tag %q", token)
		}
	}

	if script.AfterText == "" {
		script.AfterText = "Done."
	}

	if script.Verb == "" {
		return wsDemoScript{}, fmt.Errorf("ws requires one of: text, bash, validator, publisher, jira, tool")
	}
	if script.Verb == "tool" {
		if script.ToolName == "" || script.ToolAction == "" {
			return wsDemoScript{}, fmt.Errorf("ws tool requires both tool <name> and action <action>")
		}
	}

	return script, nil
}

func (s *wsDemoScript) setVerb(verb string) error {
	if s.Verb != "" && s.Verb != verb {
		return fmt.Errorf("ws only supports one primary action")
	}
	s.Verb = verb
	return nil
}

func consumeWSDemoValue(fields []string, idx *int, tag string) (string, error) {
	if *idx+1 >= len(fields) {
		return "", fmt.Errorf("%s requires a value", tag)
	}
	*idx = *idx + 1
	return fields[*idx], nil
}

func splitWSDemoArgs(input string) ([]string, error) {
	var fields []string
	var current strings.Builder
	var quote rune
	escaped := false

	flush := func() {
		if current.Len() == 0 {
			return
		}
		fields = append(fields, current.String())
		current.Reset()
	}

	for _, r := range input {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				current.WriteRune(r)
			}
		case r == '"' || r == '\'':
			quote = r
		case unicode.IsSpace(r):
			flush()
		default:
			current.WriteRune(r)
		}
	}

	if escaped {
		return nil, fmt.Errorf("unterminated escape in ws command")
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote in ws command")
	}
	flush()
	return fields, nil
}

func parseWSDemoDelayToken(prefix, token string) (time.Duration, bool, error) {
	if !strings.HasPrefix(token, prefix) || token == prefix {
		return 0, false, nil
	}
	delay, err := parseWSDemoDelayValue(strings.TrimPrefix(token, prefix))
	if err != nil {
		return 0, true, fmt.Errorf("invalid %s value %q", prefix, token)
	}
	return delay, true, nil
}

func parseWSDemoDelayValue(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, fmt.Errorf("missing delay")
	}
	if delay, err := time.ParseDuration(raw); err == nil {
		return delay, nil
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds < 0 {
		return 0, fmt.Errorf("invalid delay %q", raw)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func waitPredictableDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *PredictableService) makeWSDemoResponse(ctx context.Context, req *llm.Request, script wsDemoScript, inputTokens uint64) (*llm.Response, error) {
	if err := waitPredictableDelay(ctx, script.PreDelay); err != nil {
		return nil, err
	}

	switch script.Verb {
	case "text", "say", "echo":
		return s.makeResponse(script.Text, inputTokens), nil
	case "bash":
		return s.makeBashToolResponse(wsDemoCommandWithToolPause(script.Text, script.ToolDelay), inputTokens), nil
	case "validator":
		command := "fhir-validator"
		if script.Text != "" {
			command += " " + script.Text
		}
		return s.makeBashToolResponse(wsDemoCommandWithToolPause(command, script.ToolDelay), inputTokens), nil
	case "publisher":
		command := "ig-publisher"
		if script.Text != "" {
			command += " " + script.Text
		}
		return s.makeBashToolResponse(wsDemoCommandWithToolPause(command, script.ToolDelay), inputTokens), nil
	case "jira":
		if !s.requestHasTool(req, "workspace_hl7-jira") {
			return s.makeResponse("workspace tool unavailable", inputTokens), nil
		}
		input, _ := json.Marshal(map[string]string{"query": script.Text})
		return s.makeWorkspaceToolResponse("hl7-jira", "jira.search", json.RawMessage(input), inputTokens), nil
	case "tool":
		if !s.requestHasTool(req, "workspace_"+script.ToolName) {
			return s.makeResponse("workspace tool unavailable", inputTokens), nil
		}
		return s.makeWorkspaceToolResponse(script.ToolName, script.ToolAction, script.ToolInput, inputTokens), nil
	default:
		return s.makeResponse("ws: unknown action", inputTokens), nil
	}
}

func wsDemoCommandWithToolPause(command string, delay time.Duration) string {
	command = strings.TrimSpace(command)
	if delay <= 0 {
		return command
	}
	delaySpec := strconv.FormatFloat(delay.Seconds(), 'f', -1, 64)
	if command == "" {
		return "sleep " + delaySpec
	}
	return "sleep " + delaySpec + "; " + command
}

// makeMaxTokensResponse creates a response that simulates hitting max_tokens limit
func (s *PredictableService) makeMaxTokensResponse(text string, inputTokens uint64) *llm.Response {
	outputTokens := uint64(len(text) / 4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: text},
		},
		StopReason: llm.StopReasonMaxTokens,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.001,
		},
	}
}

// makeResponse creates a simple text response
func (s *PredictableService) makeResponse(text string, inputTokens uint64) *llm.Response {
	outputTokens := uint64(len(text) / 4) // ~4 chars per token
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: text},
		},
		StopReason: llm.StopReasonStopSequence,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.001,
		},
	}
}

// makeBashToolResponse creates a response that calls the bash tool
func (s *PredictableService) makeBashToolResponse(command string, inputTokens uint64) *llm.Response {
	// Properly marshal the command to avoid JSON escaping issues
	toolInputData := map[string]string{"command": command}
	toolInputBytes, _ := json.Marshal(toolInputData)
	toolInput := json.RawMessage(toolInputBytes)
	responseText := fmt.Sprintf("I'll run the command: %s", command)
	outputTokens := uint64(len(responseText)/4 + len(toolInputBytes)/4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-bash-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: responseText},
			{
				ID:        fmt.Sprintf("tool_%d", time.Now().UnixNano()%1000),
				Type:      llm.ContentTypeToolUse,
				ToolName:  "bash",
				ToolInput: toolInput,
			},
		},
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.002,
		},
	}
}

// makeThinkingResponse creates a response with extended thinking content
func (s *PredictableService) makeThinkingResponse(thoughts string, inputTokens uint64) *llm.Response {
	responseText := "I've considered my approach."
	outputTokens := uint64(len(responseText)/4 + len(thoughts)/4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-thinking-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeThinking, Thinking: thoughts},
			{Type: llm.ContentTypeText, Text: responseText},
		},
		StopReason: llm.StopReasonEndTurn,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.002,
		},
	}
}

// makePatchToolResponse creates a response that calls the patch tool
func (s *PredictableService) makePatchToolResponse(filePath string, inputTokens uint64) *llm.Response {
	// Properly marshal the patch data to avoid JSON escaping issues
	toolInputData := map[string]interface{}{
		"path": filePath,
		"patches": []map[string]string{
			{
				"operation": "replace",
				"oldText":   "example",
				"newText":   "updated example",
			},
		},
	}
	toolInputBytes, _ := json.Marshal(toolInputData)
	toolInput := json.RawMessage(toolInputBytes)
	responseText := fmt.Sprintf("I'll patch the file: %s", filePath)
	outputTokens := uint64(len(responseText)/4 + len(toolInputBytes)/4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-patch-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: responseText},
			{
				ID:        fmt.Sprintf("tool_%d", time.Now().UnixNano()%1000),
				Type:      llm.ContentTypeToolUse,
				ToolName:  "patch",
				ToolInput: toolInput,
			},
		},
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.003,
		},
	}
}

// makePatchToolResponseOverwrite creates a response that uses overwrite operation (always succeeds)
func (s *PredictableService) makePatchToolResponseOverwrite(filePath string, inputTokens uint64) *llm.Response {
	toolInputData := map[string]interface{}{
		"path": filePath,
		"patches": []map[string]string{
			{
				"operation": "overwrite",
				"newText":   "This is the new content of the file.\nLine 2\nLine 3\n",
			},
		},
	}
	toolInputBytes, _ := json.Marshal(toolInputData)
	toolInput := json.RawMessage(toolInputBytes)
	responseText := fmt.Sprintf("I'll create/overwrite the file: %s", filePath)
	outputTokens := uint64(len(responseText)/4 + len(toolInputBytes)/4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-patch-overwrite-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: responseText},
			{
				ID:        fmt.Sprintf("tool_%d", time.Now().UnixNano()%1000),
				Type:      llm.ContentTypeToolUse,
				ToolName:  "patch",
				ToolInput: toolInput,
			},
		},
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.0,
		},
	}
}

// makeMalformedPatchToolResponse creates a response with malformed JSON that will fail to parse
// This simulates when Anthropic sends back invalid JSON in the tool input
func (s *PredictableService) makeMalformedPatchToolResponse(inputTokens uint64) *llm.Response {
	// This malformed JSON has a string where an object is expected (patch field)
	// Mimics the error: "cannot unmarshal string into Go struct field PatchInputOneSingular.patch"
	malformedJSON := `{"path":"/home/agent/example.css","patch":"<parameter name=\"operation\">replace","oldText":".example {\n  color: red;\n}","newText":".example {\n  color: blue;\n}"}`
	toolInput := json.RawMessage(malformedJSON)
	return &llm.Response{
		ID:    fmt.Sprintf("pred-patch-malformed-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: "I'll patch the file with the changes."},
			{
				ID:        fmt.Sprintf("tool_%d", time.Now().UnixNano()%1000),
				Type:      llm.ContentTypeToolUse,
				ToolName:  "patch",
				ToolInput: toolInput,
			},
		},
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: 50,
			CostUSD:      0.003,
		},
	}
}

// GetRecentRequests returns the recent requests made to this service
func (s *PredictableService) GetRecentRequests() []*llm.Request {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.recentRequests) == 0 {
		return nil
	}

	requests := make([]*llm.Request, len(s.recentRequests))
	copy(requests, s.recentRequests)
	return requests
}

// GetLastRequest returns the most recent request, or nil if none
func (s *PredictableService) GetLastRequest() *llm.Request {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.recentRequests) == 0 {
		return nil
	}
	return s.recentRequests[len(s.recentRequests)-1]
}

// ClearRequests clears the request history
func (s *PredictableService) ClearRequests() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recentRequests = nil
}

func (s *PredictableService) requestHasTool(req *llm.Request, toolName string) bool {
	if req == nil {
		return false
	}
	for _, tool := range req.Tools {
		if tool.Name == toolName {
			return true
		}
	}
	return false
}

// countRequestTokens estimates token count based on character count.
// Uses a simple ~4 chars per token approximation.
func (s *PredictableService) countRequestTokens(req *llm.Request) uint64 {
	var totalChars int

	// Count system prompt characters
	for _, sys := range req.System {
		totalChars += len(sys.Text)
	}

	// Count message characters
	for _, msg := range req.Messages {
		for _, content := range msg.Content {
			switch content.Type {
			case llm.ContentTypeText:
				totalChars += len(content.Text)
			case llm.ContentTypeToolUse:
				totalChars += len(content.ToolName)
				totalChars += len(content.ToolInput)
			case llm.ContentTypeToolResult:
				for _, result := range content.ToolResult {
					if result.Type == llm.ContentTypeText {
						totalChars += len(result.Text)
					}
				}
			}
		}
	}

	// Count tool definitions
	for _, tool := range req.Tools {
		totalChars += len(tool.Name)
		totalChars += len(tool.Description)
		totalChars += len(tool.InputSchema)
	}

	// ~4 chars per token is a rough approximation
	return uint64(totalChars / 4)
}

// makeScreenshotToolResponse creates a response that calls the screenshot tool
func (s *PredictableService) makeScreenshotToolResponse(selector string, inputTokens uint64) *llm.Response {
	toolInputData := map[string]any{}
	if selector != "" {
		toolInputData["selector"] = selector
	}
	toolInputBytes, _ := json.Marshal(toolInputData)
	toolInput := json.RawMessage(toolInputBytes)
	responseText := "Taking a screenshot..."
	outputTokens := uint64(len(responseText)/4 + len(toolInputBytes)/4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-screenshot-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: responseText},
			{
				ID:        fmt.Sprintf("tool_%d", time.Now().UnixNano()%1000),
				Type:      llm.ContentTypeToolUse,
				ToolName:  "browser",
				ToolInput: toolInput,
			},
		},
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.0,
		},
	}
}

// makeChangeDirToolResponse creates a response that calls the change_dir tool
func (s *PredictableService) makeChangeDirToolResponse(path string, inputTokens uint64) *llm.Response {
	toolInputData := map[string]string{"path": path}
	toolInputBytes, _ := json.Marshal(toolInputData)
	toolInput := json.RawMessage(toolInputBytes)
	responseText := fmt.Sprintf("I'll change to directory: %s", path)
	outputTokens := uint64(len(responseText)/4 + len(toolInputBytes)/4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-change_dir-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: responseText},
			{
				ID:        fmt.Sprintf("tool_%d", time.Now().UnixNano()%1000),
				Type:      llm.ContentTypeToolUse,
				ToolName:  "change_dir",
				ToolInput: toolInput,
			},
		},
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.001,
		},
	}
}

func (s *PredictableService) makeWorkspaceToolResponse(toolName, action string, input json.RawMessage, inputTokens uint64) *llm.Response {
	toolInputData := map[string]any{"action": action}
	if len(input) > 0 {
		toolInputData["input"] = input
	}
	toolInputBytes, _ := json.Marshal(toolInputData)
	toolInput := json.RawMessage(toolInputBytes)
	responseText := fmt.Sprintf("I'll use workspace_%s for %s.", toolName, action)
	outputTokens := uint64(len(responseText)/4 + len(toolInputBytes)/4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-workspace-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: responseText},
			{
				ID:        fmt.Sprintf("tool_%d", time.Now().UnixNano()%1000),
				Type:      llm.ContentTypeToolUse,
				ToolName:  "workspace_" + toolName,
				ToolInput: toolInput,
			},
		},
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.0,
		},
	}
}

func (s *PredictableService) makeSubagentToolResponse(slug, prompt string, inputTokens uint64) *llm.Response {
	toolInputData := map[string]any{
		"slug":   slug,
		"prompt": prompt,
	}
	toolInputBytes, _ := json.Marshal(toolInputData)
	toolInput := json.RawMessage(toolInputBytes)
	responseText := fmt.Sprintf("Delegating to subagent '%s'...", slug)
	outputTokens := uint64(len(responseText)/4 + len(toolInputBytes)/4)
	if outputTokens == 0 {
		outputTokens = 1
	}
	return &llm.Response{
		ID:    fmt.Sprintf("pred-subagent-%d", time.Now().UnixNano()),
		Type:  "message",
		Role:  llm.MessageRoleAssistant,
		Model: "predictable-v1",
		Content: []llm.Content{
			{Type: llm.ContentTypeText, Text: responseText},
			{
				ID:        fmt.Sprintf("tool_%d", time.Now().UnixNano()%1000),
				Type:      llm.ContentTypeToolUse,
				ToolName:  "subagent",
				ToolInput: toolInput,
			},
		},
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			CostUSD:      0.0,
		},
	}
}

// makeToolSmorgasbordResponse creates a response that uses all available tool types
func (s *PredictableService) makeToolSmorgasbordResponse(inputTokens uint64) *llm.Response {
	baseNano := time.Now().UnixNano()
	content := []llm.Content{
		{Type: llm.ContentTypeText, Text: "Here's a sample of all the tools:"},
	}

	// bash tool
	bashInput, _ := json.Marshal(map[string]string{"command": "echo 'hello from bash'"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_bash_%d", baseNano%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "bash",
		ToolInput: json.RawMessage(bashInput),
	})

	// extended thinking content (not a tool)
	content = append(content, llm.Content{
		Type:     llm.ContentTypeThinking,
		Thinking: "I'm thinking about the best approach for this task. Let me consider all the options available.",
	})

	// patch tool
	patchInput, _ := json.Marshal(map[string]interface{}{
		"path": "/tmp/example.txt",
		"patches": []map[string]string{
			{"operation": "replace", "oldText": "foo", "newText": "bar"},
		},
	})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_patch_%d", (baseNano+2)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "patch",
		ToolInput: json.RawMessage(patchInput),
	})

	// browser: screenshot action
	screenshotInput, _ := json.Marshal(map[string]string{"action": "screenshot"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_screenshot_%d", (baseNano+3)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "browser",
		ToolInput: json.RawMessage(screenshotInput),
	})

	// keyword_search tool
	keywordInput, _ := json.Marshal(map[string]interface{}{
		"query":        "find all references",
		"search_terms": []string{"reference", "example"},
	})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_keyword_%d", (baseNano+4)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "keyword_search",
		ToolInput: json.RawMessage(keywordInput),
	})

	// browser: navigate action
	navigateInput, _ := json.Marshal(map[string]string{"action": "navigate", "url": "https://example.com"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_navigate_%d", (baseNano+5)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "browser",
		ToolInput: json.RawMessage(navigateInput),
	})

	// browser: eval action
	evalInput, _ := json.Marshal(map[string]string{"action": "eval", "expression": "document.title"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_eval_%d", (baseNano+6)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "browser",
		ToolInput: json.RawMessage(evalInput),
	})

	// read_image tool (separate from browser)
	readImageInput, _ := json.Marshal(map[string]string{"path": "/tmp/image.png"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_readimg_%d", (baseNano+7)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "read_image",
		ToolInput: json.RawMessage(readImageInput),
	})

	// browser: console_logs action
	consoleInput, _ := json.Marshal(map[string]string{"action": "console_logs"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_console_%d", (baseNano+8)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "browser",
		ToolInput: json.RawMessage(consoleInput),
	})

	// browser_emulate tool
	emulateInput, _ := json.Marshal(map[string]string{"action": "device", "device": "iphone_14"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_emulate_%d", (baseNano+9)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "browser_emulate",
		ToolInput: json.RawMessage(emulateInput),
	})

	// browser_network tool
	networkInput, _ := json.Marshal(map[string]string{"action": "enable"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_network_%d", (baseNano+10)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "browser_network",
		ToolInput: json.RawMessage(networkInput),
	})

	// browser_accessibility tool
	accessibilityInput, _ := json.Marshal(map[string]string{"action": "tree"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_a11y_%d", (baseNano+11)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "browser_accessibility",
		ToolInput: json.RawMessage(accessibilityInput),
	})

	// browser_profile tool
	profileInput, _ := json.Marshal(map[string]string{"action": "metrics"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_profile_%d", (baseNano+12)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "browser_profile",
		ToolInput: json.RawMessage(profileInput),
	})

	// llm_one_shot tool
	llmInput, _ := json.Marshal(map[string]string{"prompt_file": "/tmp/test-prompt.txt"})
	content = append(content, llm.Content{
		ID:        fmt.Sprintf("tool_llm_%d", (baseNano+13)%1000),
		Type:      llm.ContentTypeToolUse,
		ToolName:  "llm_one_shot",
		ToolInput: json.RawMessage(llmInput),
	})

	return &llm.Response{
		ID:         fmt.Sprintf("pred-smorgasbord-%d", baseNano),
		Type:       "message",
		Role:       llm.MessageRoleAssistant,
		Model:      "predictable-v1",
		Content:    content,
		StopReason: llm.StopReasonToolUse,
		Usage: llm.Usage{
			InputTokens:  inputTokens,
			OutputTokens: 200,
			CostUSD:      0.01,
		},
	}
}

const wideTablesMarkdown = `Here are some wide tables to test rendering:

## Narrow Table (should look fine)

| Name | Age | City |
|------|-----|------|
| Alice | 30 | NYC |
| Bob | 25 | LA |

## Wide Table (many columns)

| ID | First Name | Last Name | Email Address | Phone Number | Street Address | City | State | Zip Code | Country | Department | Job Title | Start Date | Salary | Manager |
|----|-----------|-----------|--------------|-------------|---------------|------|-------|----------|---------|-----------|-----------|-----------|--------|--------|
| 1 | Alexander | Montgomery | alexander.montgomery@longcompanyname.com | +1-555-0123 | 1234 Willowbrook Lane | San Francisco | California | 94102 | United States | Engineering | Senior Staff Engineer | 2019-03-15 | $185,000 | Sarah Johnson |
| 2 | Elizabeth | Fitzgerald | elizabeth.fitzgerald@longcompanyname.com | +1-555-0456 | 5678 Meadowridge Drive | New York | New York | 10001 | United States | Product Management | Director of Product | 2018-07-22 | $210,000 | Michael Chen |
| 3 | Christopher | Worthington | christopher.worthington@longcompanyname.com | +1-555-0789 | 9012 Thunderbird Road | Chicago | Illinois | 60601 | United States | Data Science | Principal Data Scientist | 2020-01-10 | $195,000 | Sarah Johnson |

## Table with Code and Long Content

| Function | Signature | Description | Example Usage | Return Type |
|----------|-----------|-------------|---------------|-------------|
| ` + "`processDataPipeline`" + ` | ` + "`func processDataPipeline(ctx context.Context, input []DataRecord, opts ...ProcessOption) (*PipelineResult, error)`" + ` | Processes a batch of data records through the configured pipeline stages | ` + "`result, err := processDataPipeline(ctx, records, WithParallelism(4), WithTimeout(30*time.Second))`" + ` | ` + "`*PipelineResult`" + ` |
| ` + "`validateConfiguration`" + ` | ` + "`func validateConfiguration(cfg *Config, validators ...ConfigValidator) ([]ValidationError, error)`" + ` | Validates the configuration against all registered validators | ` + "`errs, err := validateConfiguration(cfg, RequiredFieldsValidator{}, RangeValidator{})`" + ` | ` + "`[]ValidationError`" + ` |

## Table with Long Headers

| Configuration Parameter Name | Default Value | Minimum Allowed Value | Maximum Allowed Value | Environment Variable Override | Description of Behavior |
|------------------------------|---------------|----------------------|----------------------|------------------------------|-------------------------|
| max_concurrent_connections | 100 | 1 | 10000 | APP_MAX_CONNECTIONS | Limits simultaneous connections |
| request_timeout_seconds | 30 | 1 | 300 | APP_REQUEST_TIMEOUT | Per-request timeout |
| background_worker_pool_size | 4 | 1 | 64 | APP_WORKER_POOL | Number of background workers |

## Numeric Data Table

| Metric | Q1 2024 | Q2 2024 | Q3 2024 | Q4 2024 | YoY Change | Trend |
|--------|---------|---------|---------|---------|------------|-------|
| Revenue ($M) | 12.45 | 13.82 | 15.01 | 16.73 | +34.4% | 📈 |
| Active Users | 1,234,567 | 1,456,789 | 1,678,901 | 1,890,123 | +53.2% | 📈 |
| Churn Rate | 4.2% | 3.8% | 3.5% | 3.1% | -26.2% | 📉 |
| NPS Score | 42 | 45 | 48 | 52 | +23.8% | 📈 |

That's a variety of table widths for testing!`
