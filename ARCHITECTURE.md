Shelley is an agentic loop with tool use. See
https://sketch.dev/blog/agent-loop for an example of the idea.

When Shelley is started with "go run ./cmd/shelley" it starts a web server and
opens a sqlite database, and users interact with the ui built in ui/. (The
server itself is implemented in server/; cmd/shelley is a very thing shim.)

## Components

### ui/

TODO: A mobile-first UI.
Infrastructure:
  * pnpm
  * Typescript
  * esbuild
  * ESLint and eslint-typescript
  * VueJS
  * Jest

### db/

conversation(conversation_id, slug, user_initiated, parent_conversation_id):

  Represents a single conversation. Top-level chats are user_initiated=true with
  parent_conversation_id NULL. Subagent/tool conversations set user_initiated=false
  and reference their parent conversation.

message(conversation_id, message_id, type (agent/user/tool), llm_data (json), user_data (json), usage (json))

  Messages are visible in the UI and sent to the LLM as part of the 
  conversation. There may be both user-visible and llm-visible representations
  of messages.

The database is sqlite. We use sqlc to define queries and schema.

Subagent/tool conversations are done with user_initiated=false and use
parent_conversation_id to connect to their parent chat.

### server/

The server serves the agent HTTP API and maintains active
conversations. The HTTP API is:

/conversations?limit=5000&offset=0
/conversations?q=search_term

  Returns conversations, either matching a query, or matching
  the paging requirements.

/conversation/<id>

  Returns all the messages within a conversation.

/conversation/<id>/stream

  Returns all the messages within a conversation and
  uses SSE to wait for updates.

/conversation/<id>/chat (POST)

  Injects a user message into the conversation


When a conversation is active (because it's had a message sent to it, or there
are stream subscribers), a Conversation struct is instantiated from the data,
and the server keeps a map of these. Each of these has a Loop struct to keep
track of the interaction with the llm.

### Subagents

Subagents are child conversations spawned via the `subagent` tool. Each subagent
gets a slug that is unique within the parent conversation, shares the parent
working directory, and persists as its own conversation thread with a minimal
system prompt. The server stores the relationship with
parent_conversation_id and keeps these out of the top-level list by setting
user_initiated=false.

Subagent execution is managed by the server's SubagentRunner. It injects a user
message into the subagent conversation, then either waits for completion or
returns immediately when `wait=false` is set. While waiting, it polls the
subagent's working state with a timeout; when the timeout is reached it produces
a short LLM-generated progress summary. Nested subagents are disabled by the
subagent toolset configuration.

The UI renders subagent tool output with a dedicated component and provides a
"View subagent conversation" link. The conversation drawer groups subagent
threads under their parent using `/conversation/<id>/subagents`.

#### Recent evolution

- Added parent conversation tracking (parent_conversation_id) and API endpoints
  to enumerate a conversation's subagents.
- Introduced the SubagentRunner flow that handles waiting, cancellation, and
  timeout-based progress summaries.
- Expanded the tool schema to support `timeout_seconds` and fire-and-forget
  execution via `wait=false`.
- Added UI rendering and navigation for subagent threads, plus grouping in the
  conversation drawer.
- Locked down nested subagents by omitting SubagentRunner from subagent toolsets.

## loop/

The core agentic loop.

## claudetool/

Various tools for the LLM.


## Other

Shelley talks to the LLMs using the llm/ library.

Logging happens with slog and the tint library.
