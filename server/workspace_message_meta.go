package server

import "encoding/json"

const (
	workspaceProtocolVersion     = "demo-v1"
)

type workspacePromptUserData struct {
	PromptID    string               `json:"promptId,omitempty"`
	Injected    bool                 `json:"injected,omitempty"`
	InjectID    string               `json:"injectId,omitempty"`
	SubmittedBy *workspaceSubjectRef `json:"submittedBy,omitempty"`
}

type workspaceDoneUserData struct {
	PromptID      string               `json:"promptId,omitempty"`
	Status        string               `json:"status,omitempty"`
	Reason        string               `json:"reason,omitempty"`
	InterruptedBy *workspaceSubjectRef `json:"interruptedBy,omitempty"`
}

func parseWorkspacePromptUserData(raw *string) (workspacePromptUserData, bool) {
	var meta workspacePromptUserData
	if !decodeWorkspaceUserData(raw, &meta) {
		return workspacePromptUserData{}, false
	}
	if meta.PromptID == "" && !meta.Injected && meta.InjectID == "" && meta.SubmittedBy == nil {
		return workspacePromptUserData{}, false
	}
	return meta, true
}

func parseWorkspaceDoneUserData(raw *string) (workspaceDoneUserData, bool) {
	var meta workspaceDoneUserData
	if !decodeWorkspaceUserData(raw, &meta) {
		return workspaceDoneUserData{}, false
	}
	if meta.PromptID == "" && meta.Status == "" && meta.Reason == "" && meta.InterruptedBy == nil {
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

func workspaceParticipantRef(id string) *workspaceSubjectRef {
	return &workspaceSubjectRef{
		Kind: "participant",
		ID:   id,
	}
}

type workspaceTranslatorState struct {
	currentPromptID string
	toolTitles      map[string]string
	toolPromptIDs   map[string]string
}

func newWorkspaceTranslatorState() *workspaceTranslatorState {
	return &workspaceTranslatorState{
		toolTitles:    make(map[string]string),
		toolPromptIDs: make(map[string]string),
	}
}

func (s *workspaceTranslatorState) SetCurrentPromptID(promptID string) {
	s.currentPromptID = promptID
}

func (s *workspaceTranslatorState) CurrentPromptID() string {
	return s.currentPromptID
}

func (s *workspaceTranslatorState) NoteToolCall(toolCallID, title string) {
	if toolCallID == "" {
		return
	}
	if title != "" {
		s.toolTitles[toolCallID] = title
	}
	if s.currentPromptID != "" {
		s.toolPromptIDs[toolCallID] = s.currentPromptID
	}
}

func (s *workspaceTranslatorState) ToolTitle(toolCallID string) string {
	if title := s.toolTitles[toolCallID]; title != "" {
		return title
	}
	return toolCallID
}

func (s *workspaceTranslatorState) PromptIDForToolCall(toolCallID string) string {
	if promptID := s.toolPromptIDs[toolCallID]; promptID != "" {
		return promptID
	}
	return s.currentPromptID
}

func (s *workspaceTranslatorState) FinishPrompt() string {
	promptID := s.currentPromptID
	s.currentPromptID = ""
	return promptID
}
