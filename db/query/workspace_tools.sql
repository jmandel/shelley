-- name: CreateWorkspaceTool :one
INSERT INTO workspace_tools (
    tool_id, name, description, protocol, actions, provider, credential_ref, config
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ListWorkspaceTools :many
SELECT * FROM workspace_tools
ORDER BY created_at ASC;

-- name: GetWorkspaceToolByName :one
SELECT * FROM workspace_tools
WHERE name = ?;

-- name: DeleteWorkspaceToolByName :exec
DELETE FROM workspace_tools
WHERE name = ?;

-- name: CreateWorkspaceGrant :one
INSERT INTO workspace_grants (
    grant_id, tool_id, subject, actions, access, approvers, scope
)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ListWorkspaceGrantsByToolID :many
SELECT * FROM workspace_grants
WHERE tool_id = ?
ORDER BY created_at ASC;

-- name: DeleteWorkspaceGrant :exec
DELETE FROM workspace_grants
WHERE grant_id = ?;
