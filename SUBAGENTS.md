# Sub-agents in Shelley

## Overview

Sub-agents are a powerful feature in Shelley that enable parallel and delegated task execution. A sub-agent is an independent conversation that can be spawned from a parent conversation to work on subtasks. This document explains what sub-agents are, how they work, and how their implementation has evolved.

## What are Sub-agents?

Sub-agents are separate, isolated conversations that can be created and managed by a parent agent. Think of them as independent worker threads that:

- Have their own conversation context and message history
- Can use the full suite of Shelley tools (bash, patch, browser, etc.)
- Work independently without cluttering the parent conversation
- Return only their final results back to the parent
- Can be interacted with asynchronously or synchronously

## When to Use Sub-agents

Sub-agents are ideal for:

1. **Long-running tasks**: Delegate time-consuming operations that don't need to block the parent conversation
2. **Token-intensive exploration**: Tasks that generate lots of output but only need a summary returned
3. **Parallel work**: Multiple independent subtasks that can run simultaneously
4. **Complex problem decomposition**: Breaking down large problems into smaller, isolated pieces
5. **Context isolation**: Keeping detailed work separate from the main conversation flow

## Architecture

### Database Schema

Sub-agents are implemented through a parent-child relationship in the conversations table:

```sql
-- conversations table has a parent_conversation_id column
ALTER TABLE conversations ADD COLUMN parent_conversation_id TEXT 
    REFERENCES conversations(conversation_id);

-- Efficient parent-child lookups
CREATE INDEX idx_conversations_parent_id ON conversations(parent_conversation_id) 
    WHERE parent_conversation_id IS NOT NULL;
```

Each sub-agent conversation:
- Has `parent_conversation_id` set to its parent's ID
- Has `user_initiated = FALSE` (not a top-level conversation)
- Has its own unique `slug` identifier within the parent's namespace
- Inherits the working directory (`cwd`) from creation time

### Key Components

#### 1. SubagentTool (`claudetool/subagent.go`)

The tool interface that parent agents use to spawn and interact with sub-agents:

```go
type SubagentTool struct {
    DB                   SubagentDB           // Database operations
    ParentConversationID string               // ID of the parent conversation
    WorkingDir           *MutableWorkingDir   // Shared working directory
    Runner               SubagentRunner       // Executes the subagent
}
```

**Tool Schema:**
```json
{
  "slug": "research-api",           // Unique identifier
  "prompt": "Research the API...",  // Message to send
  "timeout_seconds": 60,            // Max wait time (default: 60, max: 300)
  "wait": true                      // Whether to wait for completion
}
```

#### 2. SubagentRunner (`server/subagent.go`)

The server-side implementation that manages sub-agent execution:

```go
type SubagentRunner struct {
    server *Server
}

func (r *SubagentRunner) RunSubagent(
    ctx context.Context, 
    conversationID, prompt string, 
    wait bool, 
    timeout time.Duration
) (string, error)
```

**Key responsibilities:**
- Gets or creates the conversation manager for the sub-agent
- Sends the user message to the sub-agent
- If `wait=true`: Polls until completion or timeout
- If `wait=false`: Returns immediately with confirmation
- On timeout: Generates an AI-powered progress summary

#### 3. SubagentDB Interface (`db/db.go`)

Database operations for sub-agent management:

```go
func (a *SubagentDBAdapter) GetOrCreateSubagentConversation(
    ctx context.Context, 
    slug, parentID, cwd string
) (conversationID, actualSlug string, err error)
```

**Slug uniqueness handling:**
- First tries to find existing conversation with exact slug
- If creating new, handles unique constraint violations automatically
- Appends numeric suffixes (e.g., `research-1`, `research-2`) if needed
- Returns the actual slug used (may differ from requested)

### Tool Isolation

Sub-agents intentionally **cannot** spawn nested sub-agents to avoid complexity:

```go
// createSubagentToolSetConfig creates a ToolSetConfig for subagent conversations.
// Subagent conversations don't have nested subagents to avoid complexity.
func (s *Server) createSubagentToolSetConfig(conversationID string) claudetool.ToolSetConfig {
    return claudetool.ToolSetConfig{
        LLMProvider:      s.llmManager,
        EnableJITInstall: true,
        EnableBrowser:    true,
        // No SubagentRunner/DB - subagents can't spawn nested subagents
    }
}
```

Sub-agents **can** use:
- ✅ bash tool (execute commands)
- ✅ patch tool (edit files)
- ✅ browser tools (web automation)
- ✅ think tool (reasoning)
- ✅ changedir tool (navigate filesystem)
- ✅ keyword tool (semantic search)
- ❌ subagent tool (no nesting)

### System Prompt

Sub-agents receive a specialized system prompt (`server/subagent_system_prompt.txt`):

```
You are a subagent of Shelley, a coding agent. You have been delegated a specific task by the parent agent.

Key constraints:
- Complete your assigned task thoroughly
- Your final message will be returned to the parent agent as the result
- Write important findings to files if the parent may need them later
- Be concise in your final response - summarize what you did and the outcome
- If you encounter blocking issues, explain them clearly so the parent can help

Working directory: {{.WorkingDirectory}}
{{if .GitInfo}}
Git repository root: {{.GitInfo.Root}}
{{end}}
```

This prompt emphasizes:
- Focused completion of the delegated task
- Concise final reporting
- File-based persistence for artifacts
- Clear communication of blockers

## Execution Flow

### Synchronous Mode (wait=true, default)

1. Parent agent calls `subagent` tool with a prompt
2. `SubagentTool.Run()` is invoked
3. Database creates or retrieves sub-agent conversation
4. `SubagentRunner.RunSubagent()` starts execution
5. If sub-agent is already working, it's cancelled first
6. User message is sent to the sub-agent
7. Parent polls every 500ms checking `IsAgentWorking()`
8. On completion: Returns last assistant message text
9. On timeout: Generates AI summary of progress so far

### Asynchronous Mode (wait=false)

1. Parent agent calls `subagent` tool with `wait=false`
2. Sub-agent starts processing in background
3. Tool returns immediately with: `"Subagent started processing. Conversation ID: {id}"`
4. Parent continues with other work
5. Parent can later query the same slug to get results

### Timeout Handling

When a timeout is reached while the sub-agent is still working:

```go
func (r *SubagentRunner) generateProgressSummary(
    ctx context.Context, 
    conversationID, modelID string, 
    llmService llm.Service
) (string, error)
```

This function:
1. Retrieves the conversation message history
2. Builds a text summary of what's been done
3. Makes a **non-conversation** LLM call to generate a progress summary
4. Returns formatted summary: `[Subagent is still working (timeout reached). Progress summary:] {summary}`

The summary prompt asks the LLM to explain:
- What has been accomplished
- What appears to be currently in progress
- Whether progress is being made or if it's stuck

## UI Integration

Sub-agents are visualized in the UI (`ui/src/components/SubagentTool.tsx`):

### Tool Display

```tsx
<div className="tool">
  <div className="tool-header">
    <span className="tool-emoji">⚡</span>
    <span className="tool-name">subagent</span>
    <span className="tool-command">
      Subagent '{slug}' running...
    </span>
  </div>
  
  {/* Expandable details */}
  <div className="tool-details">
    <div className="tool-section">
      <div className="tool-label">Prompt to '{slug}':</div>
      <div className="tool-code">{prompt}</div>
    </div>
    
    <div className="tool-section">
      <div className="tool-label">Response:</div>
      <div className="tool-code">{response}</div>
    </div>
    
    {/* Link to view full conversation */}
    <a href="/c/{slug}">View subagent conversation →</a>
  </div>
</div>
```

### Navigation

Users can:
- Click the expand/collapse arrow to see sub-agent details
- View the prompt sent to the sub-agent
- See the response received
- Click a link to navigate to the full sub-agent conversation
- See badges for fire-and-forget mode or custom timeouts

## Implementation Evolution

### Initial Implementation (January 23, 2026)

The sub-agent feature was introduced in commit `bd4bc1b` as a complete, production-ready implementation. This was not an incremental feature but a holistic addition spanning:

**Files added:**
- `claudetool/subagent.go` (179 lines) - Tool interface
- `claudetool/subagent_test.go` (154 lines) - Unit tests
- `server/subagent.go` (350 lines) - Execution logic
- `server/subagent_system_prompt.txt` (13 lines) - System prompt
- `server/subagent_test.go` (235 lines) - Server tests
- `ui/src/components/SubagentTool.tsx` (144 lines) - UI component

**Database changes:**
- Schema migration `009-add-parent-conversation.sql`
- New queries: `CreateSubagentConversation`, `GetSubagents`, `GetConversationBySlugAndParent`

**Design decisions in initial implementation:**

1. **No nested sub-agents**: Explicitly prevented to avoid complexity
2. **Slug-based addressing**: Human-readable identifiers within parent namespace
3. **Automatic slug uniqueness**: Database handles collisions with numeric suffixes
4. **Timeout with AI summary**: Graceful degradation using LLM to summarize progress
5. **Fire-and-forget mode**: Optional async execution with `wait=false`
6. **Full tool access**: Sub-agents get bash, browser, and all tools except sub-agents
7. **UI integration**: First-class visualization with links to full conversations

### Key Design Insights

**Why no nested sub-agents?**
- Prevents unbounded recursion and complexity
- Encourages flat, manageable task decomposition
- Simplifies reasoning about conversation hierarchies

**Why slug-based addressing?**
- Human-readable: `research-api` vs `c7x9q2`
- Semantic organization: Groups related sub-agent invocations
- Natural reuse: Same slug reuses conversation

**Why AI-powered timeout summaries?**
- Provides useful context even when sub-agent doesn't finish
- Better than "still working" or truncated logs
- Helps parent decide whether to wait longer or adjust approach

**Why fire-and-forget mode?**
- Enables true parallelism for independent tasks
- Parent doesn't block on long-running operations
- Can check back later by reusing the slug

## Usage Examples

### Example 1: Research and Implement

```javascript
// Parent agent delegates research
{
  "slug": "research-api",
  "prompt": "Research the GitHub API authentication methods and write up the options in research.md",
  "timeout_seconds": 120
}

// Later, implements based on research
{
  "slug": "implement-auth",
  "prompt": "Read research.md and implement OAuth authentication following the findings",
  "timeout_seconds": 180
}
```

### Example 2: Parallel Testing

```javascript
// Fire off multiple test suites in parallel
{
  "slug": "test-unit",
  "prompt": "Run all unit tests and save results to test-results/unit.txt",
  "wait": false
}

{
  "slug": "test-integration",
  "prompt": "Run integration tests and save results to test-results/integration.txt",
  "wait": false
}

{
  "slug": "test-e2e",
  "prompt": "Run e2e tests and save results to test-results/e2e.txt",
  "wait": false
}

// Later, check results by querying each slug
```

### Example 3: Timeout and Retry

```javascript
// First attempt with short timeout
{
  "slug": "complex-migration",
  "prompt": "Migrate the database schema to version 5",
  "timeout_seconds": 60
}

// Response: "[Subagent is still working (timeout reached). Progress summary:]
// The subagent has successfully backed up the database and started the migration.
// It's currently executing migration script 003-add-indexes.sql which appears to be
// taking longer than expected due to the large table size. Progress is being made."

// Parent decides to wait longer
{
  "slug": "complex-migration",  // Same slug continues the conversation
  "prompt": "Continue the migration work",
  "timeout_seconds": 300
}
```

## Best Practices

### For Parent Agents

1. **Use descriptive slugs**: `test-runner` not `task1`
2. **Specify appropriate timeouts**: 60s for quick tasks, 300s for complex ones
3. **Delegate complete subtasks**: Not just individual commands
4. **Review results**: Don't blindly trust sub-agent output
5. **Use file artifacts**: Have sub-agents write important data to files
6. **Handle timeouts gracefully**: Use progress summaries to decide next steps

### For System Design

1. **Monitor sub-agent proliferation**: Track parent/child relationships
2. **Clean up old conversations**: Implement retention policies
3. **Set resource limits**: Consider max concurrent sub-agents per parent
4. **Log sub-agent creation**: Helps debug complex delegation patterns
5. **Test timeout scenarios**: Ensure graceful degradation

## Future Considerations

Potential enhancements to sub-agents:

1. **Sub-agent pooling**: Reuse sub-agents across multiple parent conversations
2. **Streaming results**: Return incremental results before completion
3. **Explicit cancellation**: Parent-initiated sub-agent cancellation
4. **Resource monitoring**: Track CPU, memory, token usage per sub-agent
5. **Priority queues**: High-priority sub-agents get processed first
6. **Result caching**: Memoize sub-agent results for identical prompts
7. **Nested sub-agents**: Controlled depth-limited nesting if complexity is manageable

## Technical Details

### Conversation State Management

Sub-agents use the same `ConversationManager` infrastructure as parent conversations:

```go
type ConversationManager struct {
    conversationID string
    db             *db.DB
    logger         *slog.Logger
    toolSetConfig  claudetool.ToolSetConfig
    toolSet        *claudetool.ToolSet
    // ... other fields
}
```

Key behaviors:
- Each sub-agent gets its own `ConversationManager` instance
- State is tracked independently from parent
- Cancellation affects only the specific sub-agent
- Touch() keeps the manager alive during polling

### Message Flow

```
Parent Conversation
    │
    ├─ User: "Research the API"
    │
    ├─ Assistant: [thinking] I'll use a sub-agent
    │
    ├─ Tool Use: subagent(slug="research", prompt="Research GitHub API...")
    │    │
    │    └──> Sub-agent Conversation (slug="research")
    │            │
    │            ├─ System: "You are a subagent..."
    │            ├─ User: "Research GitHub API..."
    │            ├─ Assistant: [thinking] I'll check the docs
    │            ├─ Tool Use: bash(command="curl https://docs.github.com/...")
    │            ├─ Tool Result: [API documentation]
    │            ├─ Assistant: "I found three authentication methods..."
    │            │
    │    <──── Returns final assistant message
    │
    ├─ Tool Result: "I found three authentication methods..."
    │
    └─ Assistant: "Based on the research, I recommend..."
```

## API Reference

### SubagentTool

```go
type SubagentTool struct {
    DB                   SubagentDB
    ParentConversationID string
    WorkingDir           *MutableWorkingDir
    Runner               SubagentRunner
}

func (s *SubagentTool) Run(ctx context.Context, input json.RawMessage) llm.ToolOut
```

### SubagentRunner

```go
type SubagentRunner interface {
    RunSubagent(
        ctx context.Context,
        conversationID string,
        prompt string,
        wait bool,
        timeout time.Duration,
    ) (string, error)
}
```

### SubagentDB

```go
type SubagentDB interface {
    GetOrCreateSubagentConversation(
        ctx context.Context,
        slug string,
        parentID string,
        cwd string,
    ) (conversationID, actualSlug string, err error)
}
```

### HTTP API

**Get sub-agents for a conversation:**
```
GET /api/conversation/{id}/subagents
```

Returns:
```json
[
  {
    "conversation_id": "c8x7a2",
    "slug": "research-api",
    "created_at": "2026-01-23T10:30:00Z",
    "updated_at": "2026-01-23T10:32:15Z",
    "cwd": "/home/user/project"
  }
]
```

## Troubleshooting

### Sub-agent not responding

1. Check if sub-agent conversation exists: `GET /api/conversation/{slug}`
2. Verify parent conversation ID is correct
3. Check server logs for errors during sub-agent creation
4. Ensure timeout is sufficient for the task

### Slug conflicts

- System automatically appends numbers: `task`, `task-1`, `task-2`
- Use more specific slugs to avoid collisions
- Check returned `actualSlug` in tool result

### Timeout summaries not helpful

- Increase timeout for complex tasks
- Break down task into smaller steps
- Use fire-and-forget mode and check back later
- Review sub-agent conversation directly in UI

### Performance issues

- Limit concurrent sub-agents (no enforced limit currently)
- Monitor token usage across all active sub-agents
- Consider shorter timeouts with progress checks
- Clean up completed sub-agent conversations

## Conclusion

Sub-agents are a powerful abstraction for task delegation and parallel execution in Shelley. They provide:

- Clean separation of concerns
- Token efficiency through context isolation
- Parallel execution capabilities
- Graceful timeout handling
- First-class UI integration

The implementation is complete, tested, and production-ready as of January 23, 2026. The design emphasizes simplicity (no nesting), usability (slug-based addressing), and robustness (AI-powered summaries, automatic slug uniqueness).

As Shelley evolves, sub-agents provide a foundation for sophisticated multi-agent workflows while maintaining the system's core simplicity and reliability.
