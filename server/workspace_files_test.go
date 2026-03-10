package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceFilesLifecycle(t *testing.T) {
	server, _, _ := newTestServer(t)
	workspaceRoot := t.TempDir()
	server.workspaceRoot = workspaceRoot

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	putReq, err := http.NewRequest(http.MethodPut, httpServer.URL+"/ws/files/docs/note.txt", bytes.NewBufferString("hello workspace"))
	if err != nil {
		t.Fatalf("failed to build put request: %v", err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("failed to put workspace file: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from put, got %d", putResp.StatusCode)
	}

	content, err := os.ReadFile(filepath.Join(workspaceRoot, "docs", "note.txt"))
	if err != nil {
		t.Fatalf("failed to read written file: %v", err)
	}
	if string(content) != "hello workspace" {
		t.Fatalf("expected written content, got %q", string(content))
	}

	getResp, err := http.Get(httpServer.URL + "/ws/files/docs/note.txt")
	if err != nil {
		t.Fatalf("failed to get workspace file: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from get, got %d", getResp.StatusCode)
	}

	gotBody, err := ioReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("failed to read get response: %v", err)
	}
	if string(gotBody) != "hello workspace" {
		t.Fatalf("expected file body, got %q", string(gotBody))
	}

	listResp, err := http.Get(httpServer.URL + "/ws/files/docs")
	if err != nil {
		t.Fatalf("failed to list workspace directory: %v", err)
	}
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from directory list, got %d", listResp.StatusCode)
	}

	var listing workspaceFileListResponse
	if err := json.NewDecoder(listResp.Body).Decode(&listing); err != nil {
		t.Fatalf("failed to decode directory listing: %v", err)
	}
	if listing.Path != "/docs" {
		t.Fatalf("expected /docs listing path, got %q", listing.Path)
	}
	if len(listing.Entries) != 1 || listing.Entries[0].Path != "docs/note.txt" || listing.Entries[0].IsDir {
		t.Fatalf("unexpected directory entries: %#v", listing.Entries)
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/ws/files/docs/note.txt", nil)
	if err != nil {
		t.Fatalf("failed to build delete request: %v", err)
	}
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("failed to delete workspace file: %v", err)
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from delete, got %d", deleteResp.StatusCode)
	}

	if _, err := os.Stat(filepath.Join(workspaceRoot, "docs", "note.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected file to be removed, stat err=%v", err)
	}
}

func TestWorkspaceFilesRejectTraversal(t *testing.T) {
	server, _, _ := newTestServer(t)
	server.workspaceRoot = t.TempDir()

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	putReq, err := http.NewRequest(http.MethodPut, httpServer.URL+"/ws/files/%2e%2e/%2e%2e/escape.txt", bytes.NewBufferString("nope"))
	if err != nil {
		t.Fatalf("failed to build put request: %v", err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("failed to execute put request: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 from traversal put, got %d", putResp.StatusCode)
	}

	getResp, err := http.Get(httpServer.URL + "/ws/files/%2e%2e/%2e%2e/escape.txt")
	if err != nil {
		t.Fatalf("failed to execute get request: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 from traversal get, got %d", getResp.StatusCode)
	}
}

func TestWorkspaceFilesRootListing(t *testing.T) {
	server, _, _ := newTestServer(t)
	workspaceRoot := t.TempDir()
	server.workspaceRoot = workspaceRoot

	if err := os.Mkdir(filepath.Join(workspaceRoot, "alpha"), 0o755); err != nil {
		t.Fatalf("failed to create alpha dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspaceRoot, "beta.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatalf("failed to create beta file: %v", err)
	}

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	resp, err := http.Get(httpServer.URL + "/ws/files")
	if err != nil {
		t.Fatalf("failed to list workspace root: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from root listing, got %d", resp.StatusCode)
	}

	var listing workspaceFileListResponse
	if err := json.NewDecoder(resp.Body).Decode(&listing); err != nil {
		t.Fatalf("failed to decode root listing: %v", err)
	}
	if listing.Path != "/" {
		t.Fatalf("expected root listing path, got %q", listing.Path)
	}
	if len(listing.Entries) != 2 {
		t.Fatalf("expected two root entries, got %#v", listing.Entries)
	}
	if listing.Entries[0].Name != "alpha" || !listing.Entries[0].IsDir {
		t.Fatalf("expected directories sorted first, got %#v", listing.Entries)
	}
}

func ioReadAll(body io.Reader) ([]byte, error) {
	return io.ReadAll(body)
}
