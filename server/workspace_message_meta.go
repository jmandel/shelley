package server

import "encoding/json"

const (
	workspaceProtocolVersion = "workspace-topic-v1"
)

type workspacePromptUserData struct {
	PromptID    string               `json:"promptId,omitempty"`
	SubmittedBy *workspaceSubjectRef `json:"submittedBy,omitempty"`
}

type workspaceDoneUserData struct {
	Status        string               `json:"status,omitempty"`
	Reason        string               `json:"reason,omitempty"`
	InterruptedBy *workspaceSubjectRef `json:"interruptedBy,omitempty"`
}

func parseWorkspacePromptUserData(raw *string) (workspacePromptUserData, bool) {
	var meta workspacePromptUserData
	if !decodeWorkspaceUserData(raw, &meta) {
		return workspacePromptUserData{}, false
	}
	if meta.SubmittedBy == nil {
		return workspacePromptUserData{}, false
	}
	return meta, true
}

func parseWorkspaceDoneUserData(raw *string) (workspaceDoneUserData, bool) {
	var meta workspaceDoneUserData
	if !decodeWorkspaceUserData(raw, &meta) {
		return workspaceDoneUserData{}, false
	}
	if meta.Status == "" && meta.Reason == "" && meta.InterruptedBy == nil {
		return workspaceDoneUserData{}, false
	}
	return meta, true
}

func decodeWorkspaceUserData(raw *string, dest any) bool {
	if raw == nil || *raw == "" {
		return false
	}
	return json.Unmarshal([]byte(*raw), dest) == nil
}

func workspaceParticipantRef(subject workspaceSubjectRef) *workspaceSubjectRef {
	if subject.ID == "" && subject.DisplayName == "" {
		return nil
	}
	ref := subject
	return &ref
}

type workspaceTranslatorState struct {
	toolTitles map[string]string
}

func newWorkspaceTranslatorState() *workspaceTranslatorState {
	return &workspaceTranslatorState{
		toolTitles: make(map[string]string),
	}
}

func (s *workspaceTranslatorState) NoteToolCall(toolCallID, title string) {
	if toolCallID != "" && title != "" {
		s.toolTitles[toolCallID] = title
	}
}

func (s *workspaceTranslatorState) ToolTitle(toolCallID string) string {
	if title := s.toolTitles[toolCallID]; title != "" {
		return title
	}
	return toolCallID
}
