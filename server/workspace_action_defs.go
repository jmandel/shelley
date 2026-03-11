package server

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type workspaceActionDef struct {
	Name         string          `json:"name"`
	Title        string          `json:"title,omitempty"`
	Description  string          `json:"description,omitempty"`
	InputSchema  json.RawMessage `json:"inputSchema,omitempty"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
	Annotations  json.RawMessage `json:"annotations,omitempty"`
}

type workspaceActionInfo struct {
	Name         string          `json:"name"`
	Title        string          `json:"title,omitempty"`
	Description  string          `json:"description,omitempty"`
	InputSchema  json.RawMessage `json:"inputSchema,omitempty"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
	Annotations  json.RawMessage `json:"annotations,omitempty"`
}

func normalizeWorkspaceActionDefs(raw json.RawMessage) ([]workspaceActionDef, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	// Check for empty array "[]"
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "[]" || trimmed == "null" {
		return nil, nil
	}

	var names []string
	if err := json.Unmarshal(raw, &names); err == nil {
		defs := make([]workspaceActionDef, 0, len(names))
		for _, name := range names {
			defs = append(defs, workspaceActionDef{Name: name})
		}
		return validateWorkspaceActionDefs(defs)
	}

	var defs []workspaceActionDef
	if err := json.Unmarshal(raw, &defs); err != nil {
		return nil, fmt.Errorf("invalid actions: %w", err)
	}
	return validateWorkspaceActionDefs(defs)
}

func decodeWorkspaceActionDefs(raw string) ([]workspaceActionDef, error) {
	if raw == "" {
		return nil, nil
	}
	return normalizeWorkspaceActionDefs(json.RawMessage(raw))
}

func validateWorkspaceActionDefs(defs []workspaceActionDef) ([]workspaceActionDef, error) {
	if len(defs) == 0 {
		return nil, fmt.Errorf("actions required")
	}

	seen := make(map[string]struct{}, len(defs))
	validated := make([]workspaceActionDef, 0, len(defs))
	for _, def := range defs {
		def.Name = strings.TrimSpace(def.Name)
		def.Title = strings.TrimSpace(def.Title)
		def.Description = strings.TrimSpace(def.Description)
		if def.Name == "" {
			return nil, fmt.Errorf("action name required")
		}
		if _, ok := seen[def.Name]; ok {
			return nil, fmt.Errorf("duplicate action: %s", def.Name)
		}
		seen[def.Name] = struct{}{}
		normalizedSchema, err := validateWorkspaceActionInputSchema(def.InputSchema, def.Name)
		if err != nil {
			return nil, err
		}
		def.InputSchema = normalizedSchema
		normalizedOutputSchema, err := validateWorkspaceOptionalSchema(def.OutputSchema, def.Name, "outputSchema")
		if err != nil {
			return nil, err
		}
		def.OutputSchema = normalizedOutputSchema
		normalizedAnnotations, err := validateWorkspaceOptionalObject(def.Annotations, def.Name, "annotations")
		if err != nil {
			return nil, err
		}
		def.Annotations = normalizedAnnotations
		validated = append(validated, def)
	}

	sort.Slice(validated, func(i, j int) bool {
		return validated[i].Name < validated[j].Name
	})
	return validated, nil
}

func validateWorkspaceActionInputSchema(raw json.RawMessage, actionName string) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, fmt.Errorf("invalid inputSchema for action %s: %w", actionName, err)
	}
	if len(schema) == 0 {
		return nil, fmt.Errorf("inputSchema for action %s must be a JSON object", actionName)
	}

	typeValue, ok := schema["type"]
	if ok && typeValue != "object" {
		return nil, fmt.Errorf("inputSchema for action %s must have type object", actionName)
	}
	if !ok {
		schema["type"] = "object"
	}

	normalized, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("marshal inputSchema for action %s: %w", actionName, err)
	}
	return normalized, nil
}

func workspaceActionNames(defs []workspaceActionDef) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	return names
}

func workspaceActionInfos(defs []workspaceActionDef) []workspaceActionInfo {
	infos := make([]workspaceActionInfo, 0, len(defs))
	for _, def := range defs {
		infos = append(infos, workspaceActionInfo{
			Name:         def.Name,
			Title:        def.Title,
			Description:  def.Description,
			InputSchema:  append(json.RawMessage(nil), def.InputSchema...),
			OutputSchema: append(json.RawMessage(nil), def.OutputSchema...),
			Annotations:  append(json.RawMessage(nil), def.Annotations...),
		})
	}
	return infos
}

func visibleWorkspaceActionDefs(defs []workspaceActionDef, visibleActions []string) []workspaceActionDef {
	if len(defs) == 0 || len(visibleActions) == 0 {
		return nil
	}

	visibleSet := make(map[string]struct{}, len(visibleActions))
	for _, action := range visibleActions {
		visibleSet[action] = struct{}{}
	}

	visibleDefs := make([]workspaceActionDef, 0, len(visibleActions))
	for _, def := range defs {
		if _, ok := visibleSet[def.Name]; ok {
			visibleDefs = append(visibleDefs, def)
		}
	}

	sort.Slice(visibleDefs, func(i, j int) bool {
		return visibleDefs[i].Name < visibleDefs[j].Name
	})
	return visibleDefs
}

func workspaceActionSchemaAny(def workspaceActionDef) (any, error) {
	if len(def.InputSchema) == 0 {
		return map[string]any{
			"type":                 "object",
			"description":          "Tool-specific input payload.",
			"additionalProperties": true,
		}, nil
	}

	var schema any
	if err := json.Unmarshal(def.InputSchema, &schema); err != nil {
		return nil, err
	}
	return schema, nil
}

func validateWorkspaceOptionalSchema(raw json.RawMessage, actionName, fieldName string) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, fmt.Errorf("invalid %s for action %s: %w", fieldName, actionName, err)
	}
	if len(schema) == 0 {
		return nil, fmt.Errorf("%s for action %s must be a JSON object", fieldName, actionName)
	}
	normalized, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("marshal %s for action %s: %w", fieldName, actionName, err)
	}
	return normalized, nil
}

func validateWorkspaceOptionalObject(raw json.RawMessage, actionName, fieldName string) (json.RawMessage, error) {
	return validateWorkspaceOptionalSchema(raw, actionName, fieldName)
}
