CREATE TABLE IF NOT EXISTS workspace_tools (
    tool_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    protocol TEXT NOT NULL DEFAULT 'mcp',
    actions TEXT NOT NULL,
    provider TEXT,
    credential_ref TEXT,
    config TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workspace_grants (
    grant_id TEXT PRIMARY KEY,
    tool_id TEXT NOT NULL REFERENCES workspace_tools(tool_id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    actions TEXT NOT NULL,
    access TEXT NOT NULL DEFAULT 'allowed',
    approvers TEXT,
    scope TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_workspace_grants_tool_id ON workspace_grants(tool_id);
