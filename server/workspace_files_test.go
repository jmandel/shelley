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

	createDirReq, err := newWorkspaceRequest(http.MethodPost, httpServer.URL+"/ws/files/directories?path=docs", nil)
	if err != nil {
		t.Fatalf("failed to build create directory request: %v", err)
	}
	createDirResp, err := http.DefaultClient.Do(createDirReq)
	if err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}
	defer createDirResp.Body.Close()
	if createDirResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from directory create, got %d", createDirResp.StatusCode)
	}

	putReq, err := newWorkspaceRequest(http.MethodPut, httpServer.URL+"/ws/files/content?path=docs/note.txt", bytes.NewBufferString("hello workspace"))
	if err != nil {
		t.Fatalf("failed to build put request: %v", err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("failed to put workspace file: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from put, got %d", putResp.StatusCode)
	}

	content, err := os.ReadFile(filepath.Join(workspaceRoot, "docs", "note.txt"))
	if err != nil {
		t.Fatalf("failed to read written file: %v", err)
	}
	if string(content) != "hello workspace" {
		t.Fatalf("expected written content, got %q", string(content))
	}

	metaReq, err := newWorkspaceRequest(http.MethodGet, httpServer.URL+"/ws/files?path=docs", nil)
	if err != nil {
		t.Fatalf("failed to build metadata request: %v", err)
	}
	metaResp, err := http.DefaultClient.Do(metaReq)
	if err != nil {
		t.Fatalf("failed to get workspace metadata: %v", err)
	}
	defer metaResp.Body.Close()
	if metaResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from metadata get, got %d", metaResp.StatusCode)
	}

	var listing workspaceFileResponse
	if err := json.NewDecoder(metaResp.Body).Decode(&listing); err != nil {
		t.Fatalf("failed to decode metadata response: %v", err)
	}
	if listing.Node.Path != "docs" || listing.Node.Kind != "directory" {
		t.Fatalf("unexpected directory node: %#v", listing.Node)
	}
	if len(listing.Entries) != 1 || listing.Entries[0].Path != "docs/note.txt" || listing.Entries[0].Kind != "file" {
		t.Fatalf("unexpected directory entries: %#v", listing.Entries)
	}

	getReq, err := newWorkspaceRequest(http.MethodGet, httpServer.URL+"/ws/files/content?path=docs/note.txt", nil)
	if err != nil {
		t.Fatalf("failed to build content request: %v", err)
	}
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("failed to get workspace file content: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from content get, got %d", getResp.StatusCode)
	}

	gotBody, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("failed to read content response: %v", err)
	}
	if string(gotBody) != "hello workspace" {
		t.Fatalf("expected file body, got %q", string(gotBody))
	}

	moveReq, err := newWorkspaceRequest(http.MethodPost, httpServer.URL+"/ws/files/move", bytes.NewBufferString(`{"from":"docs/note.txt","to":"docs/renamed.txt"}`))
	if err != nil {
		t.Fatalf("failed to build move request: %v", err)
	}
	moveReq.Header.Set("Content-Type", "application/json")
	moveResp, err := http.DefaultClient.Do(moveReq)
	if err != nil {
		t.Fatalf("failed to move workspace path: %v", err)
	}
	defer moveResp.Body.Close()
	if moveResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from move, got %d", moveResp.StatusCode)
	}

	if _, err := os.Stat(filepath.Join(workspaceRoot, "docs", "note.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected source file to be gone after move, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, "docs", "renamed.txt")); err != nil {
		t.Fatalf("expected moved file to exist: %v", err)
	}

	deleteFileReq, err := newWorkspaceRequest(http.MethodDelete, httpServer.URL+"/ws/files?path=docs/renamed.txt", nil)
	if err != nil {
		t.Fatalf("failed to build file delete request: %v", err)
	}
	deleteFileResp, err := http.DefaultClient.Do(deleteFileReq)
	if err != nil {
		t.Fatalf("failed to delete workspace file: %v", err)
	}
	defer deleteFileResp.Body.Close()
	if deleteFileResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from file delete, got %d", deleteFileResp.StatusCode)
	}

	deleteDirReq, err := newWorkspaceRequest(http.MethodDelete, httpServer.URL+"/ws/files?path=docs", nil)
	if err != nil {
		t.Fatalf("failed to build directory delete request: %v", err)
	}
	deleteDirResp, err := http.DefaultClient.Do(deleteDirReq)
	if err != nil {
		t.Fatalf("failed to delete workspace directory: %v", err)
	}
	defer deleteDirResp.Body.Close()
	if deleteDirResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from directory delete, got %d", deleteDirResp.StatusCode)
	}

	if _, err := os.Stat(filepath.Join(workspaceRoot, "docs")); !os.IsNotExist(err) {
		t.Fatalf("expected directory to be removed, stat err=%v", err)
	}
}

func TestWorkspaceFilesRejectAbsoluteAndTraversal(t *testing.T) {
	server, _, _ := newTestServer(t)
	server.workspaceRoot = t.TempDir()

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	cases := []struct {
		name       string
		method     string
		target     string
		body       string
		statusCode int
	}{
		{
			name:       "absolute unix path",
			method:     http.MethodGet,
			target:     "/ws/files?path=/etc/passwd",
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "windows absolute path",
			method:     http.MethodGet,
			target:     "/ws/files?path=C:/temp/secret.txt",
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "traversal metadata",
			method:     http.MethodGet,
			target:     "/ws/files?path=../../escape.txt",
			statusCode: http.StatusForbidden,
		},
		{
			name:       "traversal content write",
			method:     http.MethodPut,
			target:     "/ws/files/content?path=../../escape.txt",
			body:       "nope",
			statusCode: http.StatusForbidden,
		},
		{
			name:       "backslash path",
			method:     http.MethodGet,
			target:     "/ws/files?path=..\\\\escape.txt",
			statusCode: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := newWorkspaceRequest(tc.method, httpServer.URL+tc.target, bytes.NewBufferString(tc.body))
			if err != nil {
				t.Fatalf("failed to build request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("failed to execute request: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.statusCode {
				t.Fatalf("expected %d, got %d", tc.statusCode, resp.StatusCode)
			}
		})
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

	req, err := newWorkspaceRequest(http.MethodGet, httpServer.URL+"/ws/files", nil)
	if err != nil {
		t.Fatalf("failed to build root listing request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to list workspace root: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from root listing, got %d", resp.StatusCode)
	}

	var listing workspaceFileResponse
	if err := json.NewDecoder(resp.Body).Decode(&listing); err != nil {
		t.Fatalf("failed to decode root listing: %v", err)
	}
	if listing.Node.Path != "" || listing.Node.Kind != "directory" || listing.Node.Name != "." {
		t.Fatalf("unexpected root node: %#v", listing.Node)
	}
	if len(listing.Entries) != 2 {
		t.Fatalf("expected two root entries, got %#v", listing.Entries)
	}
	if listing.Entries[0].Name != "alpha" || listing.Entries[0].Kind != "directory" {
		t.Fatalf("expected directories sorted first, got %#v", listing.Entries)
	}
	if listing.Entries[1].Path != "beta.txt" {
		t.Fatalf("expected relative file path, got %#v", listing.Entries[1])
	}
}

func TestWorkspaceFilesRecursiveDelete(t *testing.T) {
	server, _, _ := newTestServer(t)
	workspaceRoot := t.TempDir()
	server.workspaceRoot = workspaceRoot

	if err := os.MkdirAll(filepath.Join(workspaceRoot, "docs", "nested"), 0o755); err != nil {
		t.Fatalf("failed to create nested directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspaceRoot, "docs", "nested", "note.txt"), []byte("note"), 0o644); err != nil {
		t.Fatalf("failed to create nested file: %v", err)
	}

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	deleteReq, err := newWorkspaceRequest(http.MethodDelete, httpServer.URL+"/ws/files?path=docs&recursive=true", nil)
	if err != nil {
		t.Fatalf("failed to build recursive delete request: %v", err)
	}
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("failed to execute recursive delete: %v", err)
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from recursive delete, got %d", deleteResp.StatusCode)
	}

	if _, err := os.Stat(filepath.Join(workspaceRoot, "docs")); !os.IsNotExist(err) {
		t.Fatalf("expected docs to be removed recursively, stat err=%v", err)
	}
}

func TestWorkspaceFileWriteRequiresExistingParent(t *testing.T) {
	server, _, _ := newTestServer(t)
	server.workspaceRoot = t.TempDir()

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	putReq, err := newWorkspaceRequest(http.MethodPut, httpServer.URL+"/ws/files/content?path=docs/note.txt", bytes.NewBufferString("hello"))
	if err != nil {
		t.Fatalf("failed to build put request: %v", err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("failed to execute put request: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 from put without parent directory, got %d", putResp.StatusCode)
	}
}

func TestWorkspaceFilesRequireWorkspacePrincipal(t *testing.T) {
	server, _, _ := newTestServer(t)
	server.workspaceRoot = t.TempDir()

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/ws/files", nil)
	if err != nil {
		t.Fatalf("failed to build unauthenticated request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to execute unauthenticated request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 from unauthenticated request, got %d", resp.StatusCode)
	}
}

func newWorkspaceRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(workspaceHeaderSubject, "alice@example.com")
	req.Header.Set(workspaceHeaderDisplayName, "Alice")
	return req, nil
}
