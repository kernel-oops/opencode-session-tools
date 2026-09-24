# opencode-recall-lite (OpenCode v2 plugin)

A small replacement for [opencode-session-recall](https://github.com/rmk40/opencode-session-recall) by maelos (MIT), which has no OpenCode v2 release. The idea, the `recall` / `recall_get` tool names and the `[recall-nudge]` reminder come from that plugin; this is an independent, much smaller reimplementation on the v2 plugin API and shares no code with it. It keeps the two behaviours the workflow relies on: a `recall` tool that searches previously seen sessions across projects, and a `[recall-nudge]` system-prompt reminder to search history before answering questions about it.

## Tools

- `recall` — `query` (required), optional `directory`, `project`, `days`, `limit` (≤ 20). Ranks indexed sessions by phrase/term matches in titles (weighted) and transcripts, returning bounded snippets.
- `recall_get` — `session_id`, optional `max_chars` (≤ 49 152). Refreshes that session from the live API and returns its metadata plus the transcript tail.

Both tools are registered with `codemode: false` so the model sees them directly rather than only through the `execute` codemode tool.

## How it indexes

The v2 plugin context exposes no session list or cross-session message API, so this plugin keeps its own index in the plugin's scoped storage (`ctx.storage`, backed by the server KV table):

- `session.created` / `session.renamed` / `session.deleted` events maintain a session record (`s:<id>`: title, parent, project, directory, agent, timestamps).
- `session.execution.succeeded|failed|interrupted` events trigger a refresh: `ctx.session.get` for metadata and `ctx.session.context` for the messages, which are flattened into a role-labelled text document (`t:<id>`, newest 32 KiB kept, 2 KiB per message).
- Calling `recall` also refreshes the calling session; `recall_get` refreshes its target.

Storage is global per plugin id, so sessions from every project directory on the server land in one index even though OpenCode instantiates the plugin once per location.

## Limits (read before relying on it)

- **Only sessions seen since the plugin first ran are searchable.** Plugin event subscriptions are live-only; there is no replay API for plugins, so v1 history and sessions created before activation are not indexed. `recall_get`/`opencode_meta` can still address any session directly by ID.
- Transcript text comes from `ctx.session.context`, i.e. the messages currently in model context; compacted-away history is not indexed.
- Ranking is substring/term counting, not BM25 or embeddings.
- The nudge is added through `ctx.session.hook("context")`; disable it with `"options": { "nudge": false }`. `"debug": true` prints registration/refresh lines to the server log.

## Files

`index.ts` (plain `{ id, setup }` export; local plugins outside the OpenCode repository cannot import `@opencode/plugin` at runtime), `lib.ts`, `session-index.ts` (shared with `../opencode-meta`), `mock-ctx.ts` and `index.test.ts` (Bun tests).
