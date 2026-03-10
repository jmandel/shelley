-- name: CreateTopic :one
INSERT INTO topics (topic_name, conversation_id)
VALUES (?, ?)
RETURNING *;

-- name: GetTopic :one
SELECT * FROM topics
WHERE topic_name = ?;

-- name: GetTopicByConversationID :one
SELECT * FROM topics
WHERE conversation_id = ?;

-- name: ListTopics :many
SELECT * FROM topics
ORDER BY created_at ASC;

-- name: ListActiveTopics :many
SELECT t.*
FROM topics t
JOIN conversations c ON c.conversation_id = t.conversation_id
WHERE c.archived = FALSE
ORDER BY c.updated_at DESC;

-- name: UpdateTopicName :one
UPDATE topics
SET topic_name = ?
WHERE conversation_id = ?
RETURNING *;
