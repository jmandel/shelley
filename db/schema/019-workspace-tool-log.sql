CREATE TABLE IF NOT EXISTS workspace_tool_log (
    log_id TEXT PRIMARY KEY,
    tool_id TEXT NOT NULL REFERENCES workspace_tools(tool_id) ON DELETE CASCADE,
    topic_name TEXT,
    action TEXT NOT NULL,
    subject TEXT NOT NULL,
    access_decision TEXT NOT NULL,
    approved_by TEXT,
    input_summary TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
