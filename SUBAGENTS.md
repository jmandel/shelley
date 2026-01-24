# Subagents: Origin, Design, and Evolution

This document summarizes how subagents first landed in Shelley, what the initial
design supported, and how the feature set has expanded since.

## Initial Landing

Subagents were introduced to let a parent conversation delegate focused tasks to
child conversations without blocking its own flow. The initial design centered
on the following elements:

- **Subagent tool interface**: a `subagent` tool accepts a `slug` and `prompt` to
  start or continue a child conversation within a parent thread.
- **Parent/child storage**: conversations gained a `parent_conversation_id` and
  use `user_initiated=false` so subagents do not appear in the top-level list.
- **Dedicated system prompt**: subagent conversations load a minimal system
  prompt so they stay focused on delegated work.
- **Shared working directory**: subagents inherit the parent working directory.
- **No recursion**: subagent toolsets omit the subagent runner to prevent nested
  subagents.

These pieces gave Shelley the core ability to spawn a subagent, run a delegated
task, and return the final response to the parent.

## Initial Capabilities

At launch, the subagent feature provided:

- **Unique slugs per parent** so multiple subagents could run in parallel.
- **Message injection** into the subagent conversation to start work.
- **Blocking completion**: the parent agent could wait for the subagent's last
  response and surface it as tool output.
- **Persistence**: subagent conversations were stored and retrievable like any
  other conversation.

## Capabilities Added After Landing

Subagent functionality has expanded beyond the initial design:

- **Timeout control and fire-and-forget**: a `timeout_seconds` parameter caps how
  long the parent waits, and `wait=false` returns immediately while the subagent
  continues in the background.
- **Progress summaries on timeout**: when a subagent is still working at the
  timeout threshold, Shelley generates a short LLM summary of progress instead
  of returning nothing.
- **UI support**: a dedicated tool renderer shows subagent status, displays
  prompts/results, and links directly to the subagent conversation.
- **Sidebar grouping**: the conversation drawer queries
  `/conversation/<id>/subagents` and nests subagent threads under their parent.
- **Slug sanitation and feedback**: non-alphanumeric slug characters are cleaned
  up and the tool response reports the final slug used.

Together these changes turned subagents from a basic delegation primitive into a
fully surfaced workflow for parallel tasks, richer status reporting, and clearer
navigation across parent and child conversations.
