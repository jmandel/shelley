package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"shelley.exe.dev/claudetool"
	dbpkg "shelley.exe.dev/db"
	"shelley.exe.dev/llm"
	"shelley.exe.dev/loop"
)

func TestWorkspaceTopicsLifecycle(t *testing.T) {
	server, database, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createBody := bytes.NewBufferString(`{"name":"debug timeout"}`)
	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", createBody)
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

	topicResp, err := http.Get(httpServer.URL + "/ws/topics/debug-timeout")
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

	topicsResp, err := http.Get(httpServer.URL + "/ws/topics")
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

	deleteReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/ws/topics/debug-timeout", nil)
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

	notFoundResp, err := http.Get(httpServer.URL + "/ws/topics/debug-timeout")
	if err != nil {
		t.Fatalf("failed to get archived topic: %v", err)
	}
	defer notFoundResp.Body.Close()
	if notFoundResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for archived topic, got %d", notFoundResp.StatusCode)
	}

	recreateBody := bytes.NewBufferString(`{"name":"debug-timeout"}`)
	recreateReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", recreateBody)
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
	if !strings.HasSuffix(workspaces[0].API, "/ws") {
		t.Fatalf("expected manager api base URL to end with /ws, got %q", workspaces[0].API)
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

	precreatedResp, err := http.Get(httpServer.URL + "/ws/topics/precreated")
	if err != nil {
		t.Fatalf("failed to get precreated topic: %v", err)
	}
	defer precreatedResp.Body.Close()
	if precreatedResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for precreated topic, got %d", precreatedResp.StatusCode)
	}

	rootTopicsResp, err := http.Get(httpServer.URL + "/topics")
	if err != nil {
		t.Fatalf("failed to call root /topics compatibility route: %v", err)
	}
	defer rootTopicsResp.Body.Close()
	if rootTopicsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from root /topics compatibility route, got %d", rootTopicsResp.StatusCode)
	}
}

func TestWorkspaceTopicsUseConfiguredWorkspaceRoot(t *testing.T) {
	server, database, _ := newTestServer(t)
	workspaceRoot := t.TempDir()
	if err := server.SetWorkspaceRoot(workspaceRoot); err != nil {
		t.Fatalf("failed to set workspace root: %v", err)
	}

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"rooted-topic"}`))
	if err != nil {
		t.Fatalf("failed to build rooted topic create request: %v", err)
	}
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("failed to create rooted topic: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from rooted topic create, got %d", createResp.StatusCode)
	}

	var created workspaceTopicInfo
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode rooted topic response: %v", err)
	}

	conversation, err := database.GetConversationByID(context.Background(), created.SessionID)
	if err != nil {
		t.Fatalf("failed to load rooted conversation: %v", err)
	}
	if conversation.Cwd == nil || *conversation.Cwd != filepath.Clean(workspaceRoot) {
		t.Fatalf("expected topic conversation cwd %q, got %#v", filepath.Clean(workspaceRoot), conversation.Cwd)
	}
}

func TestWorkspaceTopicsDoNotListLegacyConversation(t *testing.T) {
	server, database, _ := newTestServer(t)
	slug := "legacy-conversation"
	if _, err := database.CreateConversation(context.Background(), &slug, true, nil, nil); err != nil {
		t.Fatalf("failed to create legacy conversation: %v", err)
	}

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	resp, err := http.Get(httpServer.URL + "/ws/topics")
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from list topics, got %d", resp.StatusCode)
	}

	var topics []workspaceTopicInfo
	if err := json.NewDecoder(resp.Body).Decode(&topics); err != nil {
		t.Fatalf("failed to decode topics response: %v", err)
	}
	if len(topics) != 0 {
		t.Fatalf("expected legacy conversation to stay out of workspace topics, got %#v", topics)
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

	topicInfo := getWorkspaceTopicInfo(t, httpServer.URL+"/ws/topics/general")
	if topicInfo.Clients != 1 {
		t.Fatalf("expected connected topic to report 1 client, got %d", topicInfo.Clients)
	}

	var (
		queuedSeen bool
		texts      []string
		doneCount  int
	)

	for doneCount < 2 {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "system":
			if msg.Data == "queued prompt" {
				queuedSeen = true
			}
		case "text":
			texts = append(texts, msg.Data)
		case "done":
			doneCount++
		}
	}

	if !queuedSeen {
		t.Fatal("expected queued prompt system message")
	}
	if len(texts) < 2 {
		t.Fatalf("expected text from both turns, got %#v", texts)
	}

	waitFor(t, 2*time.Second, func() bool {
		return len(predictable.GetRecentRequests()) >= 2
	})

	requests := predictable.GetRecentRequests()
	if len(requests) < 2 {
		t.Fatalf("expected at least two LLM requests, got %d", len(requests))
	}

	firstTurnText := lastUserText(requests[len(requests)-2])
	secondTurnText := lastUserText(requests[len(requests)-1])
	if firstTurnText != "echo: first" || secondTurnText != "echo: second" {
		t.Fatalf("expected prompts to run as separate turns, got first=%q second=%q", firstTurnText, secondTurnText)
	}
}

func TestWorkspaceTopicWSPromptBroadcastsToSSE(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"sse-collab"}`))
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

	sseResp, err := http.Get(httpServer.URL + "/api/conversation/" + created.SessionID + "/stream")
	if err != nil {
		t.Fatalf("failed to open sse stream: %v", err)
	}
	defer sseResp.Body.Close()
	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from sse stream, got %d", sseResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	streamEvents := make(chan StreamResponse, 32)
	go collectSSEStreamResponses(ctx, sseResp.Body, streamEvents)

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topic/sse-collab"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, conn)
	if err := wsjson.Write(ctx, conn, workspacePromptMessage{Type: "prompt", Data: "echo: from ws to sse"}); err != nil {
		t.Fatalf("failed to send prompt: %v", err)
	}

	if !waitForSSEText(ctx, streamEvents, "from ws to sse") {
		t.Fatal("expected websocket prompt output to appear on the sse stream")
	}
}

func TestWorkspaceTopicAPIChatUsesTopicQueue(t *testing.T) {
	t.Setenv("PREDICTABLE_DELAY_MS", "250")

	server, _, predictable := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"shared-api"}`))
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topic/shared-api"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, conn)

	chatReqBody := bytes.NewBufferString(`{"message":"echo: from api","model":"predictable"}`)
	chatReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/conversation/"+created.SessionID+"/chat", chatReqBody)
	if err != nil {
		t.Fatalf("failed to build chat request: %v", err)
	}
	chatReq.Header.Set("Content-Type", "application/json")

	chatResp, err := http.DefaultClient.Do(chatReq)
	if err != nil {
		t.Fatalf("failed to post api chat: %v", err)
	}
	defer chatResp.Body.Close()
	if chatResp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 from api chat, got %d", chatResp.StatusCode)
	}

	var (
		doneSeen bool
		textSeen bool
	)
	for !doneSeen {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "text":
			textSeen = true
		case "done":
			doneSeen = true
		}
	}

	if !textSeen {
		t.Fatal("expected websocket client to receive text output from api chat")
	}

	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})

	lastRequest := predictable.GetLastRequest()
	if lastRequest == nil {
		t.Fatal("expected predictable model request")
	}

	found := false
	for _, msg := range lastRequest.Messages {
		if msg.Role != llm.MessageRoleUser {
			continue
		}
		for _, content := range msg.Content {
			if content.Type == llm.ContentTypeText && content.Text == "echo: from api" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("expected api chat prompt in predictable request, got %#v", lastRequest.Messages)
	}
}

func TestWorkspaceTopicAPIChatRestoresRuntimeFromPersistedTopic(t *testing.T) {
	t.Setenv("PREDICTABLE_DELAY_MS", "250")

	server, database, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"restored-runtime"}`))
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

	predictable := loop.NewPredictableService()
	restarted := NewServer(
		database,
		&testLLMManager{service: predictable},
		claudetool.ToolSetConfig{EnableBrowser: false},
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})),
		true,
		"",
		"predictable",
		"",
		nil,
	)
	if restarted.topicManager.GetTopicByConversationID(created.SessionID) != nil {
		t.Fatal("expected restarted server to begin without an in-memory topic runtime")
	}

	chatReq := ChatRequest{Message: "echo: recovered", Model: "predictable"}
	chatBody, err := json.Marshal(chatReq)
	if err != nil {
		t.Fatalf("failed to marshal chat request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/conversation/"+created.SessionID+"/chat", bytes.NewReader(chatBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	restarted.handleChatConversation(w, req, created.SessionID)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 from restored api chat, got %d: %s", w.Code, w.Body.String())
	}

	waitFor(t, 2*time.Second, func() bool {
		return restarted.topicManager.GetTopicByConversationID(created.SessionID) != nil
	})
	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})

	topic := restarted.topicManager.GetTopicByConversationID(created.SessionID)
	if topic == nil || topic.Name != "restored-runtime" {
		t.Fatalf("expected recovered topic runtime for restored-runtime, got %#v", topic)
	}
	if got := lastUserText(predictable.GetLastRequest()); got != "echo: recovered" {
		t.Fatalf("expected recovered prompt to flow through topic runtime, got %q", got)
	}
}

func TestRenameTopicConversationKeepsWorkspaceTopicRouting(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"rename-me"}`))
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

	renameReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/conversation/"+created.SessionID+"/rename", bytes.NewBufferString(`{"slug":"renamed-topic"}`))
	if err != nil {
		t.Fatalf("failed to build rename request: %v", err)
	}
	renameReq.Header.Set("Content-Type", "application/json")

	renameResp, err := http.DefaultClient.Do(renameReq)
	if err != nil {
		t.Fatalf("failed to rename topic conversation: %v", err)
	}
	defer renameResp.Body.Close()
	if renameResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from rename, got %d", renameResp.StatusCode)
	}

	oldTopicResp, err := http.Get(httpServer.URL + "/ws/topics/rename-me")
	if err != nil {
		t.Fatalf("failed to get old topic name: %v", err)
	}
	defer oldTopicResp.Body.Close()
	if oldTopicResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected old topic name to disappear, got %d", oldTopicResp.StatusCode)
	}

	newTopicResp, err := http.Get(httpServer.URL + "/ws/topics/renamed-topic")
	if err != nil {
		t.Fatalf("failed to get renamed topic: %v", err)
	}
	defer newTopicResp.Body.Close()
	if newTopicResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from renamed topic, got %d", newTopicResp.StatusCode)
	}

	var renamed workspaceTopicInfo
	if err := json.NewDecoder(newTopicResp.Body).Decode(&renamed); err != nil {
		t.Fatalf("failed to decode renamed topic response: %v", err)
	}
	if renamed.SessionID != created.SessionID {
		t.Fatalf("expected renamed topic to keep session %q, got %q", created.SessionID, renamed.SessionID)
	}
}

func TestEmitWorkspaceWSMessagesTranslatesToolLifecycle(t *testing.T) {
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

	messages, turnComplete := translateWorkspaceWSMessagesForAPIMessage(toolTitles, APIMessage{
		Type:    string(dbpkg.MessageTypeAgent),
		LlmData: &assistantRawStr,
	})
	if turnComplete {
		t.Fatal("did not expect tool use message to end the turn")
	}

	if len(messages) != 1 {
		t.Fatalf("expected one translated message, got %#v", messages)
	}
	toolCall := messages[0]
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

	messages, turnComplete = translateWorkspaceWSMessagesForAPIMessage(toolTitles, APIMessage{
		Type:    string(dbpkg.MessageTypeUser),
		LlmData: &toolRawStr,
	})
	if turnComplete {
		t.Fatal("did not expect tool result to end the turn")
	}

	if len(messages) != 1 {
		t.Fatalf("expected one translated tool result message, got %#v", messages)
	}
	toolUpdate := messages[0]
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

func lastUserText(req *llm.Request) string {
	if req == nil {
		return ""
	}
	for i := len(req.Messages) - 1; i >= 0; i-- {
		msg := req.Messages[i]
		if msg.Role != llm.MessageRoleUser {
			continue
		}
		for _, content := range msg.Content {
			if content.Type == llm.ContentTypeText {
				return content.Text
			}
		}
	}
	return ""
}

func collectSSEStreamResponses(ctx context.Context, body io.Reader, out chan<- StreamResponse) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		var event StreamResponse
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			continue
		}

		select {
		case out <- event:
		case <-ctx.Done():
			return
		}
	}
}

func waitForSSEText(ctx context.Context, events <-chan StreamResponse, text string) bool {
	toolTitles := make(map[string]string)

	for {
		select {
		case <-ctx.Done():
			return false
		case event := <-events:
			for _, msg := range event.Messages {
				translated, _ := translateWorkspaceWSMessagesForAPIMessage(toolTitles, msg)
				for _, translatedMsg := range translated {
					if translatedMsg.Type == "text" && translatedMsg.Data == text {
						return true
					}
				}
			}
		}
	}
}
