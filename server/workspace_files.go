package server

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type workspaceFileEntry struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	IsDir      bool   `json:"isDir"`
	Size       int64  `json:"size"`
	ModifiedAt string `json:"modifiedAt"`
}

type workspaceFileListResponse struct {
	Path    string               `json:"path"`
	Entries []workspaceFileEntry `json:"entries"`
}

func (s *Server) handleWorkspaceFile(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleWorkspaceReadFile(w, r)
	case http.MethodPut:
		s.handleWorkspaceWriteFile(w, r)
	case http.MethodDelete:
		s.handleWorkspaceDeleteFile(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWorkspaceReadFile(w http.ResponseWriter, r *http.Request) {
	relPath, absPath, err := s.resolveWorkspacePath(r.PathValue("path"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
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
		s.writeWorkspaceDirectoryListing(w, relPath, absPath)
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

	contentType := mime.TypeByExtension(filepath.Ext(absPath))
	if contentType == "" {
		contentType = http.DetectContentType(content)
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(content)
}

func (s *Server) handleWorkspaceWriteFile(w http.ResponseWriter, r *http.Request) {
	relPath, absPath, err := s.resolveWorkspacePath(r.PathValue("path"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	if relPath == "" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		http.Error(w, "failed to create parent directory", http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(absPath, body, 0o644); err != nil {
		http.Error(w, "failed to write file", http.StatusInternalServerError)
		return
	}

	info, err := os.Stat(absPath)
	if err != nil {
		http.Error(w, "failed to stat written file", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"path":   relPath,
		"size":   info.Size(),
		"status": "ok",
	})
}

func (s *Server) handleWorkspaceDeleteFile(w http.ResponseWriter, r *http.Request) {
	relPath, absPath, err := s.resolveWorkspacePath(r.PathValue("path"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	if relPath == "" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}

	if err := os.Remove(absPath); err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to delete path", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"path":   relPath,
		"status": "deleted",
	})
}

func (s *Server) writeWorkspaceDirectoryListing(w http.ResponseWriter, relPath, absPath string) {
	dirEntries, err := os.ReadDir(absPath)
	if err != nil {
		if os.IsPermission(err) {
			http.Error(w, "Permission denied", http.StatusForbidden)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	entries := make([]workspaceFileEntry, 0, len(dirEntries))
	for _, entry := range dirEntries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		entryPath := entry.Name()
		if relPath != "" {
			entryPath = path.Join(relPath, entry.Name())
		}
		entries = append(entries, workspaceFileEntry{
			Name:       entry.Name(),
			Path:       entryPath,
			IsDir:      entry.IsDir(),
			Size:       info.Size(),
			ModifiedAt: info.ModTime().UTC().Format(time.RFC3339),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return entries[i].Name < entries[j].Name
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workspaceFileListResponse{
		Path:    displayWorkspacePath(relPath),
		Entries: entries,
	})
}

func (s *Server) resolveWorkspacePath(rawPath string) (string, string, error) {
	if hasWorkspaceTraversal(rawPath) {
		return "", "", fmt.Errorf("path escapes workspace root")
	}

	cleaned := strings.TrimPrefix(path.Clean("/"+rawPath), "/")
	if cleaned == "." {
		cleaned = ""
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

func isWithinWorkspaceRoot(root, target string) bool {
	if target == root {
		return true
	}
	return strings.HasPrefix(target, root+string(os.PathSeparator))
}

func displayWorkspacePath(relPath string) string {
	if relPath == "" {
		return "/"
	}
	return "/" + relPath
}

func hasWorkspaceTraversal(rawPath string) bool {
	for _, segment := range strings.Split(rawPath, "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

func defaultWorkspaceRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "/"
	}
	return wd
}
