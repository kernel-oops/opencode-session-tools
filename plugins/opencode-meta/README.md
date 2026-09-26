# opencode-meta (OpenCode v2 plugin)

Typed, read-only `opencode_meta` tool for bounded questions about current OpenCode session metadata. This is the v2 port: it no longer opens the SQLite database directly (as the v1 version did) and instead uses the v2 plugin API plus a live event index shared with `../opencode-recall-lite/session-index.ts`.

## Operations

| Operation | Source | Notes |
|---|---|---|
| `session` | `ctx.session.get` | Defaults to the calling session. Id, title, parent, project, directory, agent, model, outcome, cost, tokens, times. |
| `session_tree` | `get` + index | Parent via API; children from sessions created since indexing started. |
| `session_stats` | `ctx.session.context` | Message counts by type, assistant part types, per-tool call/error counts, cost and tokens for messages in model context. |
| `recent_sessions` | index | `directory`, `days` (default 7), `limit`. |
| `tool_usage` | `context` over one session or up to 25 recent indexed sessions | `session_id`, `directory`, `tool_name`, `days` (default 30), `limit`. |
| `capabilities` | app/location/index | Replaces v1 `schema` and `database_info`; reports index coverage and limits. |

## `send_message`

Cross-session messaging after Claude Code's: `send_message {to, text, delivery?}` sends plain text (up to 8,000 characters) into another session, found by session ID or by title (exact, or a fragment matching exactly one indexed session; an ambiguous title returns the candidates). By default the target receives it at its next step boundary (`delivery: "steer"`; `"queue"` waits for the target's next turn, which can be hours for a session running a long chain of subagents), framed as coming from the sending session (ID and title), with a note that it is peer information rather than user approval and cannot answer a permission prompt, plus the reply address. It never messages the calling session. A subagent's message to its own parent is held, because the subagent's final reply also reaches that parent: if the subagent calls another tool (it is still working) the message is delivered then; if its turn ends first, a foreground run's messages are merged into the parent's `subagent` tool result ahead of the final reply, and a background run's are delivered as it finishes, beside the completion notice. Held messages are never dropped (`relay.ts`). Delivery uses the public `ctx.session.prompt` and records `metadata.crossSession.from`.

Retired from v1: `schema`, `todo_packets`, `database_info` — v2 exposes no database or todo table to plugins.

The tool is registered with `codemode: false` so it appears directly in the model's tool list. The `[recall-nudge]` system text now lives in `opencode-recall-lite`; install both plugins together (this one imports the shared index module by relative path).

Options: `"debug": true` logs registration to the server log. Restart or let the plugin watcher reload after editing.

Tests: `bun test` in this directory (mocked v2 context).
