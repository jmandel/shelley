CREATE TABLE IF NOT EXISTS topics (
    topic_name TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL UNIQUE REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_topics_conversation_id ON topics(conversation_id);
