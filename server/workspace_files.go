package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type workspaceFileNode struct {
	Path       string `json:"path"`
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Size       int64  `json:"size"`
	ModifiedAt string `json:"modifiedAt"`
	MIMEType   string `json:"mimeType,omitempty"`
}

type workspaceFileResponse struct {
	Node    workspaceFileNode   `json:"node"`
	Entries []workspaceFileNode `json:"entries,omitempty"`
}

type workspaceFileMoveRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (s *Server) handleWorkspaceFiles(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireWorkspacePrincipal(w, r); !ok {
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleWorkspaceFileMetadata(w, r)
	case http.MethodDelete:
		s.handleWorkspaceDeleteFile(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceFileContent(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireWorkspacePrincipal(w, r); !ok {
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleWorkspaceReadFileContent(w, r)
	case http.MethodPut:
		s.handleWorkspaceWriteFileContent(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceDirectories(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireWorkspacePrincipal(w, r); !ok {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	relPath, absPath, err := s.resolveWorkspaceQueryPath(r, "path", false)
	if err != nil {
		writeWorkspacePathError(w, err)
		return
	}

	if info, err := os.Stat(absPath); err == nil {
		if info.IsDir() {
			http.Error(w, "path already exists", http.StatusConflict)
			return
		}
		http.Error(w, "path already exists", http.StatusConflict)
		return
	} else if !os.IsNotExist(err) {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if err := os.MkdirAll(absPath, 0o755); err != nil {
		if os.IsPermission(err) {
			http.Error(w, "Permission denied", http.StatusForbidden)
			return
		}
		http.Error(w, "failed to create directory", http.StatusConflict)
		return
	}

	info, err := os.Stat(absPath)
	if err != nil {
		http.Error(w, "failed to stat created directory", http.StatusInternalServerError)
		return
	}

	writeWorkspaceJSON(w, http.StatusCreated, workspaceFileResponse{
		Node: workspaceFileNodeFromInfo(relPath, info),
	})
}

func (s *Server) handleWorkspaceMove(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireWorkspacePrincipal(w, r); !ok {
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req workspaceFileMoveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	fromRel, fromAbs, err := s.resolveWorkspacePath(req.From, false)
	if err != nil {
		writeWorkspacePathError(w, err)
		return
	}
	toRel, toAbs, err := s.resolveWorkspacePath(req.To, false)
	if err != nil {
		writeWorkspacePathError(w, err)
		return
	}
	if fromRel == toRel {
		http.Error(w, "from and to must differ", http.StatusBadRequest)
		return
	}

	if _, err := os.Stat(fromAbs); err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if _, err := os.Stat(toAbs); err == nil {
		http.Error(w, "destination already exists", http.StatusConflict)
		return
	} else if !os.IsNotExist(err) {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if err := ensureWorkspaceParentDir(toAbs); err != nil {
		writeWorkspaceMutationError(w, err)
		return
	}

	if err := os.Rename(fromAbs, toAbs); err != nil {
		if os.IsPermission(err) {
			http.Error(w, "Permission denied", http.StatusForbidden)
			return
		}
		http.Error(w, "failed to move path", http.StatusConflict)
		return
	}

	info, err := os.Stat(toAbs)
	if err != nil {
		http.Error(w, "failed to stat moved path", http.StatusInternalServerError)
		return
	}

	writeWorkspaceJSON(w, http.StatusOK, workspaceFileResponse{
		Node: workspaceFileNodeFromInfo(toRel, info),
	})
}

func (s *Server) handleWorkspaceFileMetadata(w http.ResponseWriter, r *http.Request) {
	relPath, absPath, err := s.resolveWorkspaceQueryPath(r, "path", true)
	if err != nil {
		writeWorkspacePathError(w, err)
		return
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	resp := workspaceFileResponse{
		Node: workspaceFileNodeFromInfo(relPath, info),
	}
	if info.IsDir() {
		entries, err := workspaceDirectoryEntries(relPath, absPath)
		if err != nil {
			if os.IsPermission(err) {
				http.Error(w, "Permission denied", http.StatusForbidden)
				return
			}
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		resp.Entries = entries
	}

	writeWorkspaceJSON(w, http.StatusOK, resp)
}

func (s *Server) handleWorkspaceReadFileContent(w http.ResponseWriter, r *http.Request) {
	relPath, absPath, err := s.resolveWorkspaceQueryPath(r, "path", false)
	if err != nil {
		writeWorkspacePathError(w, err)
		return
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if info.IsDir() {
		http.Error(w, "path must refer to a file", http.StatusBadRequest)
		return
	}

	content, err := os.ReadFile(absPath)
	if err != nil {
		if os.IsPermission(err) {
			http.Error(w, "Permission denied", http.StatusForbidden)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	contentType := mime.TypeByExtension(path.Ext(relPath))
	if contentType == "" {
		contentType = http.DetectContentType(content)
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(content)
}

func (s *Server) handleWorkspaceWriteFileContent(w http.ResponseWriter, r *http.Request) {
	relPath, absPath, err := s.resolveWorkspaceQueryPath(r, "path", false)
	if err != nil {
		writeWorkspacePathError(w, err)
		return
	}

	statusCode := http.StatusOK
	info, err := os.Stat(absPath)
	switch {
	case err == nil:
		if info.IsDir() {
			http.Error(w, "path must refer to a file", http.StatusConflict)
			return
		}
	case os.IsNotExist(err):
		statusCode = http.StatusCreated
	default:
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if err := ensureWorkspaceParentDir(absPath); err != nil {
		writeWorkspaceMutationError(w, err)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	if err := os.WriteFile(absPath, body, 0o644); err != nil {
		if os.IsPermission(err) {
			http.Error(w, "Permission denied", http.StatusForbidden)
			return
		}
		http.Error(w, "failed to write file", http.StatusInternalServerError)
		return
	}

	info, err = os.Stat(absPath)
	if err != nil {
		http.Error(w, "failed to stat written file", http.StatusInternalServerError)
		return
	}

	writeWorkspaceJSON(w, statusCode, workspaceFileResponse{
		Node: workspaceFileNodeFromInfo(relPath, info),
	})
}

func (s *Server) handleWorkspaceDeleteFile(w http.ResponseWriter, r *http.Request) {
	relPath, absPath, err := s.resolveWorkspaceQueryPath(r, "path", false)
	if err != nil {
		writeWorkspacePathError(w, err)
		return
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	recursive := r.URL.Query().Get("recursive") == "true"
	if info.IsDir() && recursive {
		if err := os.RemoveAll(absPath); err != nil {
			if os.IsPermission(err) {
				http.Error(w, "Permission denied", http.StatusForbidden)
				return
			}
			http.Error(w, "failed to delete path", http.StatusInternalServerError)
			return
		}
	} else {
		if err := os.Remove(absPath); err != nil {
			switch {
			case errors.Is(err, syscall.ENOTEMPTY):
				http.Error(w, "directory not empty", http.StatusConflict)
			case os.IsPermission(err):
				http.Error(w, "Permission denied", http.StatusForbidden)
			default:
				http.Error(w, "failed to delete path", http.StatusInternalServerError)
			}
			return
		}
	}

	writeWorkspaceJSON(w, http.StatusOK, map[string]string{
		"path":   relPath,
		"status": "deleted",
	})
}

func workspaceDirectoryEntries(relPath, absPath string) ([]workspaceFileNode, error) {
	dirEntries, err := os.ReadDir(absPath)
	if err != nil {
		return nil, err
	}

	entries := make([]workspaceFileNode, 0, len(dirEntries))
	for _, entry := range dirEntries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		entryPath := entry.Name()
		if relPath != "" {
			entryPath = path.Join(relPath, entry.Name())
		}
		entries = append(entries, workspaceFileNodeFromInfo(entryPath, info))
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Kind != entries[j].Kind {
			return entries[i].Kind == "directory"
		}
		return entries[i].Name < entries[j].Name
	})
	return entries, nil
}

func workspaceFileNodeFromInfo(relPath string, info os.FileInfo) workspaceFileNode {
	kind := "file"
	if info.IsDir() {
		kind = "directory"
	}

	name := path.Base(relPath)
	if relPath == "" {
		name = "."
	}

	node := workspaceFileNode{
		Path:       relPath,
		Name:       name,
		Kind:       kind,
		Size:       info.Size(),
		ModifiedAt: info.ModTime().UTC().Format(time.RFC3339),
	}
	if kind == "file" {
		node.MIMEType = mime.TypeByExtension(path.Ext(relPath))
	}
	return node
}

func ensureWorkspaceParentDir(absPath string) error {
	parent := filepath.Dir(absPath)
	info, err := os.Stat(parent)
	switch {
	case err == nil:
		if !info.IsDir() {
			return fmt.Errorf("parent path is not a directory")
		}
		return nil
	case os.IsNotExist(err):
		return fmt.Errorf("parent directory does not exist")
	default:
		return err
	}
}

func (s *Server) resolveWorkspaceQueryPath(r *http.Request, key string, allowRoot bool) (string, string, error) {
	return s.resolveWorkspacePath(r.URL.Query().Get(key), allowRoot)
}

func (s *Server) resolveWorkspacePath(rawPath string, allowRoot bool) (string, string, error) {
	cleaned, err := normalizeWorkspacePath(rawPath, allowRoot)
	if err != nil {
		return "", "", err
	}

	root, err := filepath.Abs(s.workspaceRoot)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve workspace root")
	}

	target := root
	if cleaned != "" {
		target = filepath.Join(root, filepath.FromSlash(cleaned))
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve path")
	}
	if !isWithinWorkspaceRoot(root, target) {
		return "", "", fmt.Errorf("path escapes workspace root")
	}

	return cleaned, target, nil
}

func normalizeWorkspacePath(rawPath string, allowRoot bool) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		if allowRoot {
			return "", nil
		}
		return "", fmt.Errorf("path required")
	}
	if strings.Contains(rawPath, "\\") {
		return "", fmt.Errorf("path must be workspace-relative")
	}
	if strings.HasPrefix(rawPath, "/") || filepath.IsAbs(rawPath) || looksLikeWindowsAbsolutePath(rawPath) {
		return "", fmt.Errorf("path must be workspace-relative")
	}
	if hasWorkspaceTraversal(rawPath) {
		return "", fmt.Errorf("path escapes workspace root")
	}

	cleaned := path.Clean(rawPath)
	if cleaned == "." {
		if allowRoot {
			return "", nil
		}
		return "", fmt.Errorf("path required")
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path escapes workspace root")
	}
	return cleaned, nil
}

func looksLikeWindowsAbsolutePath(rawPath string) bool {
	if len(rawPath) < 2 {
		return false
	}
	drive := rawPath[0]
	return ((drive >= 'a' && drive <= 'z') || (drive >= 'A' && drive <= 'Z')) && rawPath[1] == ':'
}

func isWithinWorkspaceRoot(root, target string) bool {
	if target == root {
		return true
	}
	return strings.HasPrefix(target, root+string(os.PathSeparator))
}

func hasWorkspaceTraversal(rawPath string) bool {
	for _, segment := range strings.Split(rawPath, "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

func writeWorkspaceJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(value)
}

func writeWorkspacePathError(w http.ResponseWriter, err error) {
	switch err.Error() {
	case "path required", "path must be workspace-relative":
		http.Error(w, err.Error(), http.StatusBadRequest)
	case "path escapes workspace root":
		http.Error(w, err.Error(), http.StatusForbidden)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func writeWorkspaceMutationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, os.ErrPermission):
		http.Error(w, "Permission denied", http.StatusForbidden)
	case err != nil && strings.Contains(err.Error(), "parent"):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func defaultWorkspaceRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "/"
	}
	return wd
}
