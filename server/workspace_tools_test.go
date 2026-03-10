package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
