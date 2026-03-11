package server

import (
	"net/http"
	"strings"
)

const (
	workspaceHeaderSubject     = "X-Workspace-Subject"
	workspaceHeaderDisplayName = "X-Workspace-Display-Name"
)

type workspacePrincipal struct {
	Subject     string
	DisplayName string
	Email       string
}

func (s *Server) workspacePrincipalFromRequest(r *http.Request) (workspacePrincipal, bool, error) {
	if principal, ok := workspacePrincipalFromTrustedHeaders(r); ok {
		return principal, true, nil
	}
	return workspacePrincipal{}, false, nil
}

func (s *Server) requireWorkspacePrincipal(w http.ResponseWriter, r *http.Request) (workspacePrincipal, bool) {
	principal, ok, err := s.workspacePrincipalFromRequest(r)
	if err != nil {
		http.Error(w, "invalid authorization token", http.StatusUnauthorized)
		return workspacePrincipal{}, false
	}
	if !ok {
		http.Error(w, "authorization required", http.StatusUnauthorized)
		return workspacePrincipal{}, false
	}
	return principal, true
}

func workspaceSubjectFromPrincipal(principal workspacePrincipal) workspaceSubjectRef {
	return workspaceSubjectRef{
		ID:          principal.Subject,
		DisplayName: principal.DisplayName,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func workspacePrincipalFromTrustedHeaders(r *http.Request) (workspacePrincipal, bool) {
	subject := strings.TrimSpace(r.Header.Get(workspaceHeaderSubject))
	if subject == "" {
		return workspacePrincipal{}, false
	}
	displayName := firstNonEmpty(
		strings.TrimSpace(r.Header.Get(workspaceHeaderDisplayName)),
		subject,
	)
	return workspacePrincipal{
		Subject:     subject,
		DisplayName: displayName,
	}, true
}
