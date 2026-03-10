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

	dbpkg "shelley.exe.dev/db"
	"shelley.exe.dev/llm"
)

func TestWorkspaceTopicsLifecycle(t *testing.T) {
	server, database, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createBody := bytes.NewBufferString(`{"name":"debug timeout"}`)
	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/topics", createBody)
	if err != nil {
		t.Fatalf("failed to build create request: %v", err)
	}
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from create, got %d", createResp.StatusCode)
	}

	var created workspaceTopicInfo
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode create response: %v", err)
	}
	if created.Name != "debug-timeout" {
		t.Fatalf("expected sanitized topic name, got %q", created.Name)
	}
	if created.SessionID == "" {
		t.Fatal("expected session id in create response")
	}
	if _, err := time.Parse(time.RFC3339, created.CreatedAt); err != nil {
		t.Fatalf("expected RFC3339 createdAt, got %q: %v", created.CreatedAt, err)
	}

	topicResp, err := http.Get(httpServer.URL + "/topics/debug-timeout")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	defer topicResp.Body.Close()
	if topicResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from get topic, got %d", topicResp.StatusCode)
	}

	var fetched workspaceTopicInfo
	if err := json.NewDecoder(topicResp.Body).Decode(&fetched); err != nil {
		t.Fatalf("failed to decode topic response: %v", err)
	}
	if fetched.SessionID != created.SessionID {
		t.Fatalf("expected session id %q, got %q", created.SessionID, fetched.SessionID)
	}
	if fetched.Clients != 0 {
		t.Fatalf("expected 0 clients before websocket connect, got %d", fetched.Clients)
	}

	topicsResp, err := http.Get(httpServer.URL + "/topics")
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}
	defer topicsResp.Body.Close()
	if topicsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from list topics, got %d", topicsResp.StatusCode)
	}

	var topics []workspaceTopicInfo
	if err := json.NewDecoder(topicsResp.Body).Decode(&topics); err != nil {
		t.Fatalf("failed to decode topics response: %v", err)
	}
	if len(topics) != 1 || topics[0].Name != "debug-timeout" {
		t.Fatalf("expected one topic named debug-timeout, got %#v", topics)
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/topics/debug-timeout", nil)
	if err != nil {
		t.Fatalf("failed to build delete request: %v", err)
	}
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("failed to delete topic: %v", err)
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from delete, got %d", deleteResp.StatusCode)
	}

	notFoundResp, err := http.Get(httpServer.URL + "/topics/debug-timeout")
	if err != nil {
		t.Fatalf("failed to get archived topic: %v", err)
	}
	defer notFoundResp.Body.Close()
	if notFoundResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for archived topic, got %d", notFoundResp.StatusCode)
	}

	recreateBody := bytes.NewBufferString(`{"name":"debug-timeout"}`)
	recreateReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/topics", recreateBody)
	if err != nil {
		t.Fatalf("failed to build recreate request: %v", err)
	}
	recreateReq.Header.Set("Content-Type", "application/json")
	recreateResp, err := http.DefaultClient.Do(recreateReq)
	if err != nil {
		t.Fatalf("failed to recreate topic: %v", err)
	}
	defer recreateResp.Body.Close()
	if recreateResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from recreate, got %d", recreateResp.StatusCode)
	}

	var recreated workspaceTopicInfo
	if err := json.NewDecoder(recreateResp.Body).Decode(&recreated); err != nil {
		t.Fatalf("failed to decode recreate response: %v", err)
	}
	if recreated.SessionID != created.SessionID {
		t.Fatalf("expected recreate to unarchive existing topic %q, got %q", created.SessionID, recreated.SessionID)
	}

	conversation, err := database.GetConversationBySlug(context.Background(), "debug-timeout")
	if err != nil {
		t.Fatalf("failed to reload conversation by slug: %v", err)
	}
	if conversation.Archived {
		t.Fatal("expected recreated topic to be active")
	}
}

func TestWorkspaceAliasRoutesAndManagerDiscovery(t *testing.T) {
	server, _, _ := newTestServer(t)
	server.workspaceName = "test-workspace"

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"alias-topic"}`))
	if err != nil {
		t.Fatalf("failed to build alias topic create request: %v", err)
	}
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("failed to create alias topic: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating /ws topic, got %d", createResp.StatusCode)
	}

	healthResp, err := http.Get(httpServer.URL + "/ws/health")
	if err != nil {
		t.Fatalf("failed to call /ws/health: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /ws/health, got %d", healthResp.StatusCode)
	}

	var health struct {
		Status        string   `json:"status"`
		Mode          string   `json:"mode"`
		WorkspaceName string   `json:"workspaceName"`
		Topics        []string `json:"topics"`
	}
	if err := json.NewDecoder(healthResp.Body).Decode(&health); err != nil {
		t.Fatalf("failed to decode /ws/health response: %v", err)
	}
	if health.Mode != "workspace" || health.WorkspaceName != "test-workspace" {
		t.Fatalf("unexpected /ws/health response: %#v", health)
	}
	if len(health.Topics) != 1 || health.Topics[0] != "alias-topic" {
		t.Fatalf("expected alias-topic in /ws/health topics, got %#v", health.Topics)
	}

	listResp, err := http.Get(httpServer.URL + "/workspaces")
	if err != nil {
		t.Fatalf("failed to call /workspaces: %v", err)
	}
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /workspaces, got %d", listResp.StatusCode)
	}

	var workspaces []workspaceManagerInfo
	if err := json.NewDecoder(listResp.Body).Decode(&workspaces); err != nil {
		t.Fatalf("failed to decode /workspaces response: %v", err)
	}
	if len(workspaces) != 1 || workspaces[0].Name != "test-workspace" {
		t.Fatalf("unexpected /workspaces response: %#v", workspaces)
	}
	if !strings.HasSuffix(workspaces[0].ACP, "/acp") {
		t.Fatalf("expected manager acp base URL to end with /acp, got %q", workspaces[0].ACP)
	}

	getResp, err := http.Get(httpServer.URL + "/workspaces/test-workspace")
	if err != nil {
		t.Fatalf("failed to call /workspaces/{name}: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /workspaces/{name}, got %d", getResp.StatusCode)
	}

	var workspace workspaceManagerInfo
	if err := json.NewDecoder(getResp.Body).Decode(&workspace); err != nil {
		t.Fatalf("failed to decode /workspaces/{name} response: %v", err)
	}
	if workspace.Name != "test-workspace" {
		t.Fatalf("unexpected workspace info: %#v", workspace)
	}

	managerCreateReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/workspaces", bytes.NewBufferString(`{"name":"test-workspace","topics":["precreated"]}`))
	if err != nil {
		t.Fatalf("failed to build manager create request: %v", err)
	}
	managerCreateReq.Header.Set("Content-Type", "application/json")
	managerCreateResp, err := http.DefaultClient.Do(managerCreateReq)
	if err != nil {
		t.Fatalf("failed to post /workspaces: %v", err)
	}
	defer managerCreateResp.Body.Close()
	if managerCreateResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from /workspaces POST, got %d", managerCreateResp.StatusCode)
	}

	precreatedResp, err := http.Get(httpServer.URL + "/topics/precreated")
	if err != nil {
		t.Fatalf("failed to get precreated topic: %v", err)
	}
	defer precreatedResp.Body.Close()
	if precreatedResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for precreated topic, got %d", precreatedResp.StatusCode)
	}
}

func TestWorkspaceTopicWSQueuesPrompt(t *testing.T) {
	t.Setenv("PREDICTABLE_DELAY_MS", "250")

	server, _, predictable := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topic/general"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, conn)

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{Type: "prompt", Data: "echo: first"}); err != nil {
		t.Fatalf("failed to send first prompt: %v", err)
	}
	if err := wsjson.Write(ctx, conn, workspacePromptMessage{Type: "prompt", Data: "echo: second"}); err != nil {
		t.Fatalf("failed to send second prompt: %v", err)
	}

	topicInfo := getWorkspaceTopicInfo(t, httpServer.URL+"/topics/general")
	if topicInfo.Clients != 1 {
		t.Fatalf("expected connected topic to report 1 client, got %d", topicInfo.Clients)
	}

	var (
		queuedSeen bool
		texts      []string
		doneSeen   bool
	)

	for !doneSeen {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "system":
			if msg.Data == "queued prompt" {
				queuedSeen = true
			}
		case "text":
			texts = append(texts, msg.Data)
		case "done":
			doneSeen = true
		}
	}

	if !queuedSeen {
		t.Fatal("expected queued prompt system message")
	}
	if len(texts) == 0 {
		t.Fatal("expected at least one text response")
	}

	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})

	lastRequest := predictable.GetLastRequest()
	var userTexts []string
	for _, msg := range lastRequest.Messages {
		if msg.Role != llm.MessageRoleUser {
			continue
		}
		for _, content := range msg.Content {
			if content.Type == llm.ContentTypeText {
				userTexts = append(userTexts, content.Text)
			}
		}
	}

	if len(userTexts) < 2 {
		t.Fatalf("expected both prompts in the LLM request, got %#v", userTexts)
	}
	if userTexts[len(userTexts)-2] != "echo: first" || userTexts[len(userTexts)-1] != "echo: second" {
		t.Fatalf("expected queued prompts to be preserved in order, got %#v", userTexts)
	}
}

func TestEmitWorkspaceWSMessagesTranslatesToolLifecycle(t *testing.T) {
	server, _, _ := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	outCh := make(chan workspaceWSMessage, 8)
	toolTitles := make(map[string]string)

	assistantRaw, err := json.Marshal(llm.Message{
		Role: llm.MessageRoleAssistant,
		Content: []llm.Content{
			{Type: llm.ContentTypeToolUse, ID: "tool-1", ToolName: "bash"},
		},
	})
	if err != nil {
		t.Fatalf("failed to marshal assistant message: %v", err)
	}
	assistantRawStr := string(assistantRaw)

	server.emitWorkspaceWSMessagesForAPIMessage(ctx, outCh, toolTitles, APIMessage{
		Type:    string(dbpkg.MessageTypeAgent),
		LlmData: &assistantRawStr,
	})

	toolCall := <-outCh
	if toolCall.Type != "tool_call" || toolCall.ToolCallID != "tool-1" || toolCall.Title != "bash" {
		t.Fatalf("unexpected tool_call message: %#v", toolCall)
	}

	toolRaw, err := json.Marshal(llm.Message{
		Role: llm.MessageRoleUser,
		Content: []llm.Content{
			{Type: llm.ContentTypeToolResult, ToolUseID: "tool-1"},
		},
	})
	if err != nil {
		t.Fatalf("failed to marshal tool result message: %v", err)
	}
	toolRawStr := string(toolRaw)

	server.emitWorkspaceWSMessagesForAPIMessage(ctx, outCh, toolTitles, APIMessage{
		Type:    string(dbpkg.MessageTypeTool),
		LlmData: &toolRawStr,
	})

	toolUpdate := <-outCh
	if toolUpdate.Type != "tool_update" || toolUpdate.ToolCallID != "tool-1" || toolUpdate.Title != "bash" || toolUpdate.Status != "completed" {
		t.Fatalf("unexpected tool_update message: %#v", toolUpdate)
	}
}

func waitForConnectedMessage(t *testing.T, ctx context.Context, conn *websocket.Conn) {
	t.Helper()
	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "connected" {
			if msg.Topic == "" || msg.SessionID == "" {
				t.Fatalf("expected connected message with topic and session id, got %#v", msg)
			}
			return
		}
	}
}

func readWorkspaceWSMessage(t *testing.T, ctx context.Context, conn *websocket.Conn) workspaceWSMessage {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var msg workspaceWSMessage
	if err := wsjson.Read(readCtx, conn, &msg); err != nil {
		t.Fatalf("failed to read websocket message: %v", err)
	}
	return msg
}

func getWorkspaceTopicInfo(t *testing.T, url string) workspaceTopicInfo {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("failed to fetch topic info: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from topic info, got %d", resp.StatusCode)
	}

	var info workspaceTopicInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("failed to decode topic info: %v", err)
	}
	return info
}
