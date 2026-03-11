package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	if !strings.HasSuffix(created.Events, "/ws/topics/debug-timeout/events") {
		t.Fatalf("expected topic events URL, got %q", created.Events)
	}
	if _, err := time.Parse(time.RFC3339, created.CreatedAt); err != nil {
		t.Fatalf("expected RFC3339 createdAt, got %q: %v", created.CreatedAt, err)
	}
	createdConversationID := conversationIDForTopic(t, database, "debug-timeout")

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
	if fetched.Events != created.Events {
		t.Fatalf("expected events URL %q, got %q", created.Events, fetched.Events)
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
	recreatedConversationID := conversationIDForTopic(t, database, "debug-timeout")
	if recreatedConversationID != createdConversationID {
		t.Fatalf("expected recreate to unarchive existing topic %q, got %q", createdConversationID, recreatedConversationID)
	}

	conversation, err := database.GetConversationBySlug(context.Background(), "debug-timeout")
	if err != nil {
		t.Fatalf("failed to reload conversation by slug: %v", err)
	}
	if conversation.Archived {
		t.Fatal("expected recreated topic to be active")
	}
}

func TestWorkspaceCompatibilityRoutes(t *testing.T) {
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

	rootTopicsResp, err := http.Get(httpServer.URL + "/topics")
	if err != nil {
		t.Fatalf("failed to call root /topics compatibility route: %v", err)
	}
	defer rootTopicsResp.Body.Close()
	if rootTopicsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from root /topics compatibility route, got %d", rootTopicsResp.StatusCode)
	}

	notFoundPaths := []string{
		"/workspaces",
		"/workspaces/test-workspace",
		"/acp",
		"/acp/alias-topic",
		"/ws/acp",
		"/ws/acp/alias-topic",
	}
	for _, path := range notFoundPaths {
		resp, err := http.Get(httpServer.URL + path)
		if err != nil {
			t.Fatalf("failed to call %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404 from %s, got %d", path, resp.StatusCode)
		}
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

	conversation, err := database.GetConversationBySlug(context.Background(), created.Name)
	if err != nil {
		t.Fatalf("failed to load rooted conversation: %v", err)
	}
	if conversation.Cwd == nil || *conversation.Cwd != filepath.Clean(workspaceRoot) {
		t.Fatalf("expected topic conversation cwd %q, got %#v", filepath.Clean(workspaceRoot), conversation.Cwd)
	}
}

func TestWorkspaceTopicQueueSnapshotReconcilesStaleActivePrompt(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	topic, _, err := server.topicManager.GetOrCreateTopic(context.Background(), "stale-queue")
	if err != nil {
		t.Fatalf("failed to create stale queue topic: %v", err)
	}
	forceStaleActivePrompt(t, topic, "p_stale_queue", "echo: stale queue", "cli-a")

	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/ws/topics/stale-queue/queue", nil)
	if err != nil {
		t.Fatalf("failed to build queue snapshot request: %v", err)
	}
	setWorkspaceAuth(t, req, "cli-a")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to get queue snapshot: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 from queue snapshot, got %d: %s", resp.StatusCode, string(body))
	}

	var snapshot workspaceQueueSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatalf("failed to decode reconciled queue snapshot: %v", err)
	}
	if snapshot.ActivePromptID != "" {
		t.Fatalf("expected stale active prompt to be cleared, got %#v", snapshot)
	}
	if topic.PromptQueue.ActivePromptID() != "" {
		t.Fatalf("expected in-memory prompt queue to be reconciled, got active %q", topic.PromptQueue.ActivePromptID())
	}
	if topic.IsBusy() {
		t.Fatal("expected reconciled topic to report not busy")
	}
}

func TestWorkspaceTopicsListReconcilesStaleBusyState(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	topic, _, err := server.topicManager.GetOrCreateTopic(context.Background(), "stale-busy")
	if err != nil {
		t.Fatalf("failed to create stale busy topic: %v", err)
	}
	forceStaleActivePrompt(t, topic, "p_stale_busy", "echo: stale busy", "cli-a")

	resp, err := http.Get(httpServer.URL + "/ws/topics")
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 from topic list, got %d: %s", resp.StatusCode, string(body))
	}

	var topics []workspaceTopicInfo
	if err := json.NewDecoder(resp.Body).Decode(&topics); err != nil {
		t.Fatalf("failed to decode topic list: %v", err)
	}
	if len(topics) != 1 {
		t.Fatalf("expected one topic, got %#v", topics)
	}
	if topics[0].Busy {
		t.Fatalf("expected stale busy topic to be reconciled, got %#v", topics[0])
	}
	if topic.PromptQueue.ActivePromptID() != "" {
		t.Fatalf("expected busy reconciliation to clear active prompt, got %q", topic.PromptQueue.ActivePromptID())
	}
}

func TestWorkspaceTopicInterruptReconcilesStaleActivePrompt(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	topic, _, err := server.topicManager.GetOrCreateTopic(context.Background(), "stale-interrupt")
	if err != nil {
		t.Fatalf("failed to create stale interrupt topic: %v", err)
	}
	forceStaleActivePrompt(t, topic, "p_stale_interrupt", "echo: stale interrupt", "cli-a")

	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics/stale-interrupt/interrupt", bytes.NewBufferString(`{"reason":"Wrong approach."}`))
	if err != nil {
		t.Fatalf("failed to build interrupt request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setWorkspaceAuth(t, req, "cli-a")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to post interrupt request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 409 from interrupt against stale active prompt, got %d: %s", resp.StatusCode, string(body))
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "no active turn") {
		t.Fatalf("expected stale interrupt response to say no active turn, got %q", string(body))
	}
	if topic.PromptQueue.ActivePromptID() != "" {
		t.Fatalf("expected stale interrupt reconciliation to clear active prompt, got %q", topic.PromptQueue.ActivePromptID())
	}
	if topic.IsBusy() {
		t.Fatal("expected stale interrupt reconciliation to leave topic idle")
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

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topics/general/events"
	conn, _, err := websocket.Dial(ctx, wsURL, workspaceAuthDialOptions(t, "viewer"))
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
		firstPromptID  string
		secondPromptID string
		queuedSeen     bool
		startedSeen    bool
		texts          []string
		doneCount      int
	)

	for doneCount < 2 || firstPromptID == "" || secondPromptID == "" {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "prompt_status":
			if msg.Status == string(PromptStatusAccepted) && msg.Data == "echo: first" {
				firstPromptID = msg.PromptID
			}
			if msg.Status == string(PromptStatusAccepted) && msg.Data == "echo: second" {
				secondPromptID = msg.PromptID
			}
			if msg.PromptID == secondPromptID && msg.Status == string(PromptStatusQueued) {
				queuedSeen = true
			}
			if msg.PromptID == firstPromptID && msg.Status == string(PromptStatusStarted) {
				startedSeen = true
			}
		case "text":
			texts = append(texts, msg.Data)
		case "done":
			doneCount++
		}
	}

	if !queuedSeen {
		t.Fatal("expected second prompt to emit queued status")
	}
	if !startedSeen {
		t.Fatal("expected first prompt to emit started status")
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

func TestWorkspaceTopicQueueRESTAndCancellation(t *testing.T) {
	t.Setenv("PREDICTABLE_DELAY_MS", "250")

	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/ws/topics/queue-rest/events", workspaceAuthDialOptions(t, "cli-a"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")
	waitForConnectedMessage(t, ctx, conn)

	firstPromptID := sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: first", nil)
	secondPromptID := sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: second", nil)

	waitFor(t, 2*time.Second, func() bool {
		req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/ws/topics/queue-rest/queue", nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		defer resp.Body.Close()
		var snapshot workspaceQueueSnapshot
		if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
			return false
		}
		return snapshot.ActivePromptID == firstPromptID && len(snapshot.Entries) == 1 && snapshot.Entries[0].PromptID == secondPromptID
	})

	cancelReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/ws/topics/queue-rest/queue/"+secondPromptID, nil)
	if err != nil {
		t.Fatalf("failed to build queue delete request: %v", err)
	}
	setWorkspaceAuth(t, cancelReq, "cli-a")
	cancelResp, err := http.DefaultClient.Do(cancelReq)
	if err != nil {
		t.Fatalf("failed to delete queued prompt: %v", err)
	}
	cancelResp.Body.Close()
	if cancelResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 from queue delete, got %d", cancelResp.StatusCode)
	}

	var cancelledSeen bool
	for !cancelledSeen {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "prompt_status" && msg.PromptID == secondPromptID && msg.Status == string(PromptStatusCancelled) {
			cancelledSeen = true
		}
	}
}

func TestWorkspaceTopicQueueRESTUpdateAndMove(t *testing.T) {
	t.Setenv("PREDICTABLE_DELAY_MS", "250")

	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/ws/topics/queue-edit/events", workspaceAuthDialOptions(t, "cli-a"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")
	waitForConnectedMessage(t, ctx, conn)

	firstPromptID := sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: first", nil)
	secondPromptID := sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: second", nil)
	thirdPromptID := sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: third", nil)

	waitFor(t, 2*time.Second, func() bool {
		req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/ws/topics/queue-edit/queue", nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		defer resp.Body.Close()
		var snapshot workspaceQueueSnapshot
		if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
			return false
		}
		return snapshot.ActivePromptID == firstPromptID &&
			len(snapshot.Entries) == 2 &&
			snapshot.Entries[0].PromptID == secondPromptID &&
			snapshot.Entries[1].PromptID == thirdPromptID
	})

	updateReq, err := http.NewRequest(http.MethodPatch, httpServer.URL+"/ws/topics/queue-edit/queue/"+thirdPromptID, bytes.NewBufferString(`{"data":"echo: revised third"}`))
	if err != nil {
		t.Fatalf("failed to build queue patch request: %v", err)
	}
	updateReq.Header.Set("Content-Type", "application/json")
	setWorkspaceAuth(t, updateReq, "cli-b")
	updateResp, err := http.DefaultClient.Do(updateReq)
	if err != nil {
		t.Fatalf("failed to patch queued prompt: %v", err)
	}
	if updateResp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(updateResp.Body)
		updateResp.Body.Close()
		t.Fatalf("expected 403 from queue patch for non-owner, got %d: %s", updateResp.StatusCode, string(body))
	}
	updateResp.Body.Close()

	updateReq, err = http.NewRequest(http.MethodPatch, httpServer.URL+"/ws/topics/queue-edit/queue/"+thirdPromptID, bytes.NewBufferString(`{"data":"echo: revised third"}`))
	if err != nil {
		t.Fatalf("failed to build owner queue patch request: %v", err)
	}
	updateReq.Header.Set("Content-Type", "application/json")
	setWorkspaceAuth(t, updateReq, "cli-a")
	updateResp, err = http.DefaultClient.Do(updateReq)
	if err != nil {
		t.Fatalf("failed to patch queued prompt as owner: %v", err)
	}
	if updateResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(updateResp.Body)
		updateResp.Body.Close()
		t.Fatalf("expected 200 from queue patch, got %d: %s", updateResp.StatusCode, string(body))
	}
	var updatedSnapshot workspaceQueueSnapshot
	if err := json.NewDecoder(updateResp.Body).Decode(&updatedSnapshot); err != nil {
		updateResp.Body.Close()
		t.Fatalf("failed to decode queue patch response: %v", err)
	}
	updateResp.Body.Close()
	if got := updatedSnapshot.Entries[1].Text; got != "echo: revised third" {
		t.Fatalf("expected updated queue text, got %q", got)
	}

	var updatedSeen bool
	for !updatedSeen {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "queue_entry_updated" && msg.PromptID == thirdPromptID && msg.Data == "echo: revised third" {
			updatedSeen = true
		}
	}

	moveReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics/queue-edit/queue/"+thirdPromptID+"/move", bytes.NewBufferString(`{"direction":"top"}`))
	if err != nil {
		t.Fatalf("failed to build queue move request: %v", err)
	}
	moveReq.Header.Set("Content-Type", "application/json")
	setWorkspaceAuth(t, moveReq, "cli-b")
	moveResp, err := http.DefaultClient.Do(moveReq)
	if err != nil {
		t.Fatalf("failed to move queued prompt: %v", err)
	}
	if moveResp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(moveResp.Body)
		moveResp.Body.Close()
		t.Fatalf("expected 403 from queue move for non-owner, got %d: %s", moveResp.StatusCode, string(body))
	}
	moveResp.Body.Close()

	moveReq, err = http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics/queue-edit/queue/"+thirdPromptID+"/move", bytes.NewBufferString(`{"direction":"top"}`))
	if err != nil {
		t.Fatalf("failed to build owner queue move request: %v", err)
	}
	moveReq.Header.Set("Content-Type", "application/json")
	setWorkspaceAuth(t, moveReq, "cli-a")
	moveResp, err = http.DefaultClient.Do(moveReq)
	if err != nil {
		t.Fatalf("failed to move queued prompt as owner: %v", err)
	}
	if moveResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(moveResp.Body)
		moveResp.Body.Close()
		t.Fatalf("expected 200 from queue move, got %d: %s", moveResp.StatusCode, string(body))
	}
	var movedSnapshot workspaceQueueSnapshot
	if err := json.NewDecoder(moveResp.Body).Decode(&movedSnapshot); err != nil {
		moveResp.Body.Close()
		t.Fatalf("failed to decode queue move response: %v", err)
	}
	moveResp.Body.Close()
	if got := movedSnapshot.Entries[0].PromptID; got != thirdPromptID {
		t.Fatalf("expected moved prompt to be first in queue, got %q", got)
	}

	var movedSeen bool
	for !movedSeen {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "queue_entry_moved" && msg.PromptID == thirdPromptID && msg.Direction == "top" && msg.Position == 1 {
			movedSeen = true
		}
	}

	moveBottomReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics/queue-edit/queue/"+thirdPromptID+"/move", bytes.NewBufferString(`{"direction":"bottom"}`))
	if err != nil {
		t.Fatalf("failed to build queue bottom move request: %v", err)
	}
	moveBottomReq.Header.Set("Content-Type", "application/json")
	setWorkspaceAuth(t, moveBottomReq, "cli-a")
	moveBottomResp, err := http.DefaultClient.Do(moveBottomReq)
	if err != nil {
		t.Fatalf("failed to move queued prompt to bottom: %v", err)
	}
	if moveBottomResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(moveBottomResp.Body)
		moveBottomResp.Body.Close()
		t.Fatalf("expected 200 from queue bottom move, got %d: %s", moveBottomResp.StatusCode, string(body))
	}
	var bottomSnapshot workspaceQueueSnapshot
	if err := json.NewDecoder(moveBottomResp.Body).Decode(&bottomSnapshot); err != nil {
		moveBottomResp.Body.Close()
		t.Fatalf("failed to decode queue bottom move response: %v", err)
	}
	moveBottomResp.Body.Close()
	if got := bottomSnapshot.Entries[len(bottomSnapshot.Entries)-1].PromptID; got != thirdPromptID {
		t.Fatalf("expected moved prompt to be last in queue after bottom move, got %q", got)
	}

	var bottomSeen bool
	for !bottomSeen {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "queue_entry_moved" && msg.PromptID == thirdPromptID && msg.Direction == "bottom" && msg.Position == 2 {
			bottomSeen = true
		}
	}
}

func TestWorkspaceTopicPromptPositionFront(t *testing.T) {
	t.Setenv("PREDICTABLE_DELAY_MS", "250")

	server, _, predictable := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/ws/topics/queue-front/events", workspaceAuthDialOptions(t, "cli-a"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")
	waitForConnectedMessage(t, ctx, conn)

	front := 0
	_ = sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: first", nil)
	_ = sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: second", nil)
	thirdPromptID := sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: third", &front)

	var (
		thirdQueued bool
		donePrompts []string
	)

	for len(donePrompts) < 3 {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "prompt_status":
			if msg.PromptID == thirdPromptID && msg.Status == string(PromptStatusQueued) && msg.Position == 1 {
				thirdQueued = true
			}
		case "done":
			donePrompts = append(donePrompts, msg.Status)
		}
	}

	if !thirdQueued {
		t.Fatal("expected p-3 to be queued at the front")
	}
	if len(donePrompts) != 3 {
		t.Fatalf("expected 3 done events, got %d", len(donePrompts))
	}

	waitFor(t, 2*time.Second, func() bool {
		return len(predictable.GetRecentRequests()) >= 3
	})

	requests := predictable.GetRecentRequests()
	if len(requests) < 3 {
		t.Fatalf("expected at least three LLM requests, got %d", len(requests))
	}
	if got := lastUserText(requests[len(requests)-3]); got != "echo: first" {
		t.Fatalf("expected first prompt to run first, got %q", got)
	}
	if got := lastUserText(requests[len(requests)-2]); got != "echo: third" {
		t.Fatalf("expected front-inserted prompt to run second, got %q", got)
	}
	if got := lastUserText(requests[len(requests)-1]); got != "echo: second" {
		t.Fatalf("expected originally queued prompt to run last, got %q", got)
	}
}

func TestWorkspaceTopicInjectDuringActiveTurn(t *testing.T) {
	server, _, predictable := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/ws/topics/inject-live/events", workspaceAuthDialOptions(t, "cli-a"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")
	waitForConnectedMessage(t, ctx, conn)

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{
		Type:     "prompt",
		PromptID: "p-1",
		Data:     `ws bash "printf primary" toolpause0.2 aftertext "Primary turn complete."`,
	}); err != nil {
		t.Fatalf("failed to send primary prompt: %v", err)
	}

	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "tool_call" {
			break
		}
	}

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{
		Type: "inject",
		Data: `ws text "Injected guidance acknowledged."`,
	}); err != nil {
		t.Fatalf("failed to inject prompt: %v", err)
	}

	var (
		acceptedSeen  bool
		deliveredSeen bool
		injectedSeen  bool
		textSeen      bool
		doneSeen      bool
		injectID      string
	)

	for !(acceptedSeen && deliveredSeen && injectedSeen && textSeen && doneSeen) {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "inject_status":
			if msg.Status == "accepted" {
				if msg.InjectID == "" {
					t.Fatal("expected server-assigned injectId on accepted inject_status")
				}
				injectID = msg.InjectID
				acceptedSeen = true
			}
			if injectID != "" && msg.InjectID == injectID && msg.Status == "delivered" {
				deliveredSeen = true
			}
		case "user":
			if msg.Data == `ws text "Injected guidance acknowledged."` {
				injectedSeen = true
			}
		case "text":
			if msg.Data == "Injected guidance acknowledged." {
				textSeen = true
			}
		case "done":
			if msg.Status == "completed" {
				doneSeen = true
			}
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		return len(predictable.GetRecentRequests()) >= 2
	})

	requests := predictable.GetRecentRequests()
	if len(requests) < 2 {
		t.Fatalf("expected at least two LLM requests, got %d", len(requests))
	}
	if got := lastUserText(requests[len(requests)-1]); got != `ws text "Injected guidance acknowledged."` {
		t.Fatalf("expected injected prompt to become the latest user message, got %q", got)
	}
}

func TestWorkspaceTopicInjectRESTConflictWhenIdle(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"inject-idle"}`))
	if err != nil {
		t.Fatalf("failed to build topic create request: %v", err)
	}
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from create topic, got %d", createResp.StatusCode)
	}

	injectReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics/inject-idle/inject", bytes.NewBufferString(`{"data":"hello"}`))
	if err != nil {
		t.Fatalf("failed to build inject request: %v", err)
	}
	injectReq.Header.Set("Content-Type", "application/json")
	setWorkspaceAuth(t, injectReq, "cli-a")
	injectResp, err := http.DefaultClient.Do(injectReq)
	if err != nil {
		t.Fatalf("failed to post inject request: %v", err)
	}
	defer injectResp.Body.Close()
	if injectResp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(injectResp.Body)
		t.Fatalf("expected 409 from inject when idle, got %d: %s", injectResp.StatusCode, string(body))
	}

	var rejected workspaceWSMessage
	if err := json.NewDecoder(injectResp.Body).Decode(&rejected); err != nil {
		t.Fatalf("failed to decode inject rejection: %v", err)
	}
	if rejected.Status != "rejected" || rejected.Reason != "no_active_turn" {
		t.Fatalf("unexpected inject rejection %#v", rejected)
	}
}

func TestWorkspaceTopicInjectDoesNotAckBeforePersistence(t *testing.T) {
	server, _, predictable := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"inject-persist-fail"}`))
	if err != nil {
		t.Fatalf("failed to build topic create request: %v", err)
	}
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from create topic, got %d", createResp.StatusCode)
	}

	topic, err := server.resolveWorkspaceTopicRuntime(context.Background(), "inject-persist-fail")
	if err != nil {
		t.Fatalf("failed to resolve topic runtime: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/ws/topics/inject-persist-fail/events", workspaceAuthDialOptions(t, "cli-a"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")
	waitForConnectedMessage(t, ctx, conn)

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{
		Type:     "prompt",
		PromptID: "p-1",
		Data:     `ws bash "printf primary" toolpause0.2 aftertext "Primary turn complete."`,
	}); err != nil {
		t.Fatalf("failed to send primary prompt: %v", err)
	}

	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "tool_call" {
			break
		}
	}

	topic.Manager.recordMessageWithUserData = func(context.Context, llm.Message, llm.Usage, ...interface{}) error {
		return errors.New("persist failed")
	}

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{
		Type: "inject",
		Data: `ws text "should never be delivered"`,
	}); err != nil {
		t.Fatalf("failed to inject prompt: %v", err)
	}

	var (
		acceptedSeen bool
		rejectedSeen bool
	)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "inject_status" && msg.Status == "accepted" {
			acceptedSeen = true
		}
		if msg.Type == "inject_status" && msg.Status == "rejected" && msg.Reason == "inject_failed" {
			if msg.InjectID == "" {
				t.Fatal("expected server-assigned injectId on rejected inject_status")
			}
			rejectedSeen = true
			break
		}
	}

	if acceptedSeen {
		t.Fatal("unexpected inject accepted event before persistence succeeded")
	}
	if !rejectedSeen {
		t.Fatal("expected inject rejection when persistence fails")
	}

	waitFor(t, time.Second, func() bool {
		return len(predictable.GetRecentRequests()) >= 1
	})
	if got := len(predictable.GetRecentRequests()); got != 1 {
		t.Fatalf("expected inject failure to avoid a second LLM request, got %d", got)
	}
}

func TestWorkspaceTopicInterruptRESTAndQueueDrain(t *testing.T) {
	server, _, predictable := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/ws/topics/interrupt-rest/events", workspaceAuthDialOptions(t, "cli-a"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")
	waitForConnectedMessage(t, ctx, conn)

	firstPromptID := sendWorkspacePromptAndWaitAccepted(t, ctx, conn, `ws bash "printf slow" toolpause0.2 aftertext "Primary turn complete."`, nil)
	secondPromptID := sendWorkspacePromptAndWaitAccepted(t, ctx, conn, "echo: second", nil)

	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "tool_call" {
			break
		}
	}

	interruptReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics/interrupt-rest/interrupt", bytes.NewBufferString(`{"reason":"Wrong approach."}`))
	if err != nil {
		t.Fatalf("failed to build interrupt request: %v", err)
	}
	interruptReq.Header.Set("Content-Type", "application/json")
	setWorkspaceAuth(t, interruptReq, "cli-a")
	interruptResp, err := http.DefaultClient.Do(interruptReq)
	if err != nil {
		t.Fatalf("failed to post interrupt request: %v", err)
	}
	defer interruptResp.Body.Close()
	if interruptResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(interruptResp.Body)
		t.Fatalf("expected 200 from interrupt, got %d: %s", interruptResp.StatusCode, string(body))
	}

	var doneEvent workspaceWSMessage
	if err := json.NewDecoder(interruptResp.Body).Decode(&doneEvent); err != nil {
		t.Fatalf("failed to decode interrupt response: %v", err)
	}
	if doneEvent.PromptID != firstPromptID || doneEvent.Status != "interrupted" || doneEvent.Reason != "Wrong approach." {
		t.Fatalf("unexpected interrupt response %#v", doneEvent)
	}

	var (
		cancelledSeen   bool
		interruptedSeen bool
		secondStarted   bool
		secondDone      bool
	)

	for !secondDone {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "prompt_status":
			if msg.PromptID == firstPromptID && msg.Status == string(PromptStatusCancelled) {
				cancelledSeen = true
			}
			if msg.PromptID == secondPromptID && msg.Status == string(PromptStatusStarted) {
				secondStarted = true
			}
		case "done":
			if msg.Status == "interrupted" {
				interruptedSeen = true
			}
			if msg.Status == "completed" {
				secondDone = true
			}
		}
	}

	if !cancelledSeen {
		t.Fatal("expected interrupted prompt to emit cancelled prompt_status")
	}
	if !interruptedSeen {
		t.Fatal("expected interrupted done event on websocket")
	}
	if !secondStarted {
		t.Fatal("expected next queued prompt to start after interrupt")
	}

	waitFor(t, 2*time.Second, func() bool {
		return len(predictable.GetRecentRequests()) >= 2
	})
	requests := predictable.GetRecentRequests()
	if len(requests) < 2 {
		t.Fatalf("expected at least two requests, got %d", len(requests))
	}
	if got := lastUserText(requests[len(requests)-1]); got != "echo: second" {
		t.Fatalf("expected queued prompt to run after interrupt, got %q", got)
	}
}

func TestWorkspaceTopicWSReplaysRecentMessagesOnConnect(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topics/general/events"
	conn, _, err := websocket.Dial(ctx, wsURL, workspaceAuthDialOptions(t, "viewer"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, conn)
	if err := wsjson.Write(ctx, conn, workspacePromptMessage{Type: "prompt", Data: "echo: replay-me"}); err != nil {
		t.Fatalf("failed to send prompt: %v", err)
	}

	var doneSeen bool
	for !doneSeen {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "done" {
			doneSeen = true
		}
	}

	replayConn, _, err := websocket.Dial(ctx, wsURL, workspaceAuthDialOptions(t, "replay-viewer"))
	if err != nil {
		t.Fatalf("failed to dial replay websocket: %v", err)
	}
	defer replayConn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, replayConn)

	var (
		replayedText bool
		replayedDone bool
	)
	for !(replayedText && replayedDone) {
		msg := readWorkspaceWSMessage(t, ctx, replayConn)
		switch msg.Type {
		case "text":
			if msg.Data == "replay-me" {
				replayedText = true
			}
		case "done":
			replayedDone = true
		}
	}
}

func TestWorkspaceTopicWSPromptBroadcastsToSSE(t *testing.T) {
	server, database, _ := newTestServer(t)
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
	conversationID := conversationIDForTopic(t, database, created.Name)

	sseResp, err := http.Get(httpServer.URL + "/api/conversation/" + conversationID + "/stream")
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

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topics/sse-collab/events"
	conn, _, err := websocket.Dial(ctx, wsURL, workspaceAuthDialOptions(t, "sse-viewer"))
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

	server, database, predictable := newTestServer(t)
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
	conversationID := conversationIDForTopic(t, database, created.Name)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topics/shared-api/events"
	conn, _, err := websocket.Dial(ctx, wsURL, workspaceAuthDialOptions(t, "api-viewer"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, conn)

	chatReqBody := bytes.NewBufferString(`{"message":"echo: from api","model":"predictable"}`)
	chatReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/conversation/"+conversationID+"/chat", chatReqBody)
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
		userSeen bool
	)
	for !doneSeen {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "user":
			if msg.Data == "echo: from api" {
				userSeen = true
			}
		case "text":
			textSeen = true
		case "done":
			doneSeen = true
		}
	}

	if !userSeen {
		t.Fatal("expected websocket client to receive user prompt from api chat")
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

func TestWorkspaceTopicToolOutputStaysOnToolUpdate(t *testing.T) {
	server, _, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/ws/topics/tool-output/events", workspaceAuthDialOptions(t, "cli-a"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")
	waitForConnectedMessage(t, ctx, conn)

	if err := wsjson.Write(ctx, conn, workspacePromptMessage{
		Type:     "prompt",
		PromptID: "p-tool-1",
		Data:     `ws bash "printf validator-output" toolpause0.1 aftertext "Validator finished."`,
	}); err != nil {
		t.Fatalf("failed to send prompt: %v", err)
	}

	var (
		toolCallSeen     bool
		toolUpdateSeen   bool
		afterTextSeen    bool
		doneSeen         bool
		toolOutputLeaked bool
	)

	for !(toolCallSeen && toolUpdateSeen && afterTextSeen && doneSeen) {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		switch msg.Type {
		case "tool_call":
			if msg.Title == "bash" {
				toolCallSeen = true
			}
		case "tool_update":
			if msg.Title == "bash" {
				if msg.Data != "validator-output" {
					t.Fatalf("expected tool_update data validator-output, got %#v", msg)
				}
				toolUpdateSeen = true
			}
		case "text":
			if msg.Data == "validator-output" {
				toolOutputLeaked = true
			}
			if msg.Data == "Validator finished." {
				afterTextSeen = true
			}
		case "done":
			if msg.Status == "completed" {
				doneSeen = true
			}
		}
	}

	if toolOutputLeaked {
		t.Fatal("expected tool output to stay on tool_update.data, not leak as assistant text")
	}
}

func TestWorkspaceTopicWSReplaysUserMessagesOnConnect(t *testing.T) {
	server, database, _ := newTestServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	createReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/ws/topics", bytes.NewBufferString(`{"name":"user-replay"}`))
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
	conversationID := conversationIDForTopic(t, database, created.Name)

	chatReqBody := bytes.NewBufferString(`{"message":"echo: replay me","model":"predictable"}`)
	chatReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/conversation/"+conversationID+"/chat", chatReqBody)
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/topics/user-replay/events"
	conn, _, err := websocket.Dial(ctx, wsURL, workspaceAuthDialOptions(t, "replay-viewer"))
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "test complete")

	waitForConnectedMessage(t, ctx, conn)

	var replayedUser bool
	for !replayedUser {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "user" && msg.Data == "echo: replay me" {
			replayedUser = true
		}
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
	conversationID := conversationIDForTopic(t, database, created.Name)

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
	if restarted.topicManager.GetTopicByConversationID(conversationID) != nil {
		t.Fatal("expected restarted server to begin without an in-memory topic runtime")
	}

	chatReq := ChatRequest{Message: "echo: recovered", Model: "predictable"}
	chatBody, err := json.Marshal(chatReq)
	if err != nil {
		t.Fatalf("failed to marshal chat request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/conversation/"+conversationID+"/chat", bytes.NewReader(chatBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	restarted.handleChatConversation(w, req, conversationID)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 from restored api chat, got %d: %s", w.Code, w.Body.String())
	}

	waitFor(t, 2*time.Second, func() bool {
		return restarted.topicManager.GetTopicByConversationID(conversationID) != nil
	})
	waitFor(t, 2*time.Second, func() bool {
		return predictable.GetLastRequest() != nil
	})

	topic := restarted.topicManager.GetTopicByConversationID(conversationID)
	if topic == nil || topic.Name != "restored-runtime" {
		t.Fatalf("expected recovered topic runtime for restored-runtime, got %#v", topic)
	}
	if got := lastUserText(predictable.GetLastRequest()); got != "echo: recovered" {
		t.Fatalf("expected recovered prompt to flow through topic runtime, got %q", got)
	}
}

func TestRenameTopicConversationKeepsWorkspaceTopicRouting(t *testing.T) {
	server, database, _ := newTestServer(t)
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
	conversationID := conversationIDForTopic(t, database, created.Name)

	renameReq, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/conversation/"+conversationID+"/rename", bytes.NewBufferString(`{"slug":"renamed-topic"}`))
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
	if renamed.Name != "renamed-topic" {
		t.Fatalf("expected renamed topic name, got %#v", renamed)
	}
}

func TestEmitWorkspaceWSMessagesTranslatesToolLifecycle(t *testing.T) {
	translator := newWorkspaceTranslatorState()

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

	messages, turnComplete := translateWorkspaceWSMessagesForAPIMessage(translator, APIMessage{
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
			{
				Type:      llm.ContentTypeToolResult,
				ToolUseID: "tool-1",
				ToolResult: []llm.Content{
					{Type: llm.ContentTypeText, Text: "validator output"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to marshal tool result message: %v", err)
	}
	toolRawStr := string(toolRaw)

	messages, turnComplete = translateWorkspaceWSMessagesForAPIMessage(translator, APIMessage{
		Type:    string(dbpkg.MessageTypeUser),
		LlmData: &toolRawStr,
	})
	if turnComplete {
		t.Fatal("did not expect tool result to end the turn")
	}

	if len(messages) != 1 {
		t.Fatalf("expected one tool update message, got %#v", messages)
	}
	toolUpdate := messages[0]
	if toolUpdate.Type != "tool_update" || toolUpdate.ToolCallID != "tool-1" || toolUpdate.Title != "bash" || toolUpdate.Status != "completed" {
		t.Fatalf("unexpected tool_update message: %#v", toolUpdate)
	}
	if toolUpdate.Data != "validator output" {
		t.Fatalf("expected translated tool result text on tool_update, got %#v", toolUpdate)
	}
}

func waitForConnectedMessage(t *testing.T, ctx context.Context, conn *websocket.Conn) {
	t.Helper()
	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type == "connected" {
			if msg.Topic == "" {
				t.Fatalf("expected connected message with topic, got %#v", msg)
			}
			if msg.ProtocolVersion != workspaceProtocolVersion {
				t.Fatalf("expected connected protocolVersion %q, got %#v", workspaceProtocolVersion, msg)
			}
			if !msg.Replay {
				t.Fatalf("expected connected replay=true, got %#v", msg)
			}
			return
		}
	}
}

func sendWorkspacePromptAndWaitAccepted(t *testing.T, ctx context.Context, conn *websocket.Conn, data string, position *int) string {
	t.Helper()
	if err := wsjson.Write(ctx, conn, workspacePromptMessage{Type: "prompt", Data: data, Position: position}); err != nil {
		t.Fatalf("failed to send prompt %q: %v", data, err)
	}
	for {
		msg := readWorkspaceWSMessage(t, ctx, conn)
		if msg.Type != "prompt_status" || msg.Status != string(PromptStatusAccepted) || msg.Data != data {
			continue
		}
		if msg.PromptID == "" {
			t.Fatalf("expected accepted prompt %q to include promptId", data)
		}
		return msg.PromptID
	}
}

func forceStaleActivePrompt(t *testing.T, topic *Topic, promptID, text, subject string) {
	t.Helper()

	topic.PromptQueue.mu.Lock()
	topic.PromptQueue.active = &QueuedPrompt{
		PromptID: promptID,
		Text:     text,
		SubmittedBy: workspaceSubjectRef{
			ID:          subject,
			DisplayName: subject,
		},
		QueuedAt: time.Now().UTC(),
		Status:   PromptStatusStarted,
	}
	topic.PromptQueue.mu.Unlock()

	topic.turnMu.Lock()
	topic.turnDone = make(chan struct{})
	topic.turnMu.Unlock()

	topic.turnStatusMu.Lock()
	topic.pendingTurnStatus[promptID] = "completed"
	topic.turnStatusMu.Unlock()

	topic.Manager.SetAgentWorking(false)
}

func workspaceAuthDialOptions(t *testing.T, subject string) *websocket.DialOptions {
	t.Helper()
	return &websocket.DialOptions{
		HTTPHeader: http.Header{
			workspaceHeaderSubject:     []string{subject},
			workspaceHeaderDisplayName: []string{subject},
		},
	}
}

func setWorkspaceAuth(t *testing.T, req *http.Request, subject string) {
	t.Helper()
	req.Header.Set(workspaceHeaderSubject, subject)
	req.Header.Set(workspaceHeaderDisplayName, subject)
}

func conversationIDForTopic(t *testing.T, database *dbpkg.DB, topicName string) string {
	t.Helper()
	conversation, err := database.GetConversationBySlug(context.Background(), topicName)
	if err != nil {
		t.Fatalf("failed to load topic conversation %q: %v", topicName, err)
	}
	return conversation.ConversationID
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
	translator := newWorkspaceTranslatorState()

	for {
		select {
		case <-ctx.Done():
			return false
		case event := <-events:
			for _, msg := range event.Messages {
				translated, _ := translateWorkspaceWSMessagesForAPIMessage(translator, msg)
				for _, translatedMsg := range translated {
					if translatedMsg.Type == "text" && translatedMsg.Data == text {
						return true
					}
				}
			}
		}
	}
}
