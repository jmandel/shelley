package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"shelley.exe.dev/db"
	"shelley.exe.dev/db/generated"
	"shelley.exe.dev/llm"
)

func TestHydrateGeneratesSystemPromptWithSubagentTool(t *testing.T) {
	h := NewTestHarness(t)
	ctx := context.Background()

	// Create a new conversation
	h.NewConversation("Hello", "")
	convID := h.ConversationID()

	// The system prompt should have been created during NewConversation (via handleNewConversation -> getOrCreateConversationManager -> Hydrate)
	// Let's verify it has the subagent tool in its display data.

	var messages []generated.Message
	err := h.db.Queries(ctx, func(q *generated.Queries) error {
		var qerr error
		messages, qerr = q.ListMessages(ctx, convID)
		return qerr
	})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}

	var systemMsg *generated.Message
	for _, msg := range messages {
		if msg.Type == string(db.MessageTypeSystem) {
			systemMsg = &msg
			break
		}
	}

	if systemMsg == nil {
		t.Fatal("System message not found")
	}

	if systemMsg.DisplayData == nil {
		t.Fatal("System message has no display data")
	}

	var displayData struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal([]byte(*systemMsg.DisplayData), &displayData); err != nil {
		t.Fatalf("Failed to unmarshal display data: %v", err)
	}

	hasSubagent := false
	for _, tool := range displayData.Tools {
		if tool.Name == "subagent" {
			hasSubagent = true
			break
		}
	}

	if !hasSubagent {
		t.Errorf("System prompt display data should include 'subagent' tool")
		t.Logf("Found tools: %v", displayData.Tools)
	}
}

func TestAcceptUserMessageRequiresImmediatePersistence(t *testing.T) {
	server, database, predictable := newTestServer(t)
	ctx := context.Background()

	conversation, err := database.CreateConversation(ctx, nil, true, nil, nil)
	if err != nil {
		t.Fatalf("failed to create conversation: %v", err)
	}

	manager, err := server.getOrCreateConversationManager(ctx, conversation.ConversationID, "")
	if err != nil {
		t.Fatalf("failed to get conversation manager: %v", err)
	}

	manager.recordMessage = func(context.Context, llm.Message, llm.Usage) error {
		return errors.New("boom")
	}

	message := llm.Message{
		Role: llm.MessageRoleUser,
		Content: []llm.Content{{
			Type: llm.ContentTypeText,
			Text: "echo: should not run",
		}},
	}

	if _, err := manager.AcceptUserMessage(ctx, predictable, "predictable", message); err == nil {
		t.Fatal("expected immediate persistence failure")
	}

	time.Sleep(200 * time.Millisecond)
	if got := len(predictable.GetRecentRequests()); got != 0 {
		t.Fatalf("expected no LLM requests after persistence failure, got %d", got)
	}

	var messages []generated.Message
	err = database.Queries(ctx, func(q *generated.Queries) error {
		var qerr error
		messages, qerr = q.ListMessages(ctx, conversation.ConversationID)
		return qerr
	})
	if err != nil {
		t.Fatalf("failed to list messages: %v", err)
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
			if content.Type == llm.ContentTypeText && content.Text == "echo: should not run" {
				t.Fatal("unexpected persisted user message after immediate persistence failure")
			}
		}
	}
}
