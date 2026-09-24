import {
  boundedJson,
  getSession,
  listSessions,
  markStarted,
  messageText,
  readMeta,
  sessionFromInfo,
  startIndexing,
  upsertSession,
  type SessionRecord,
  type StorageLike,
} from "./session-index.ts"

export const TEXT_PREFIX = "t:"
export const MAX_SESSION_TEXT_BYTES = 32 * 1024
export const MAX_MESSAGE_TEXT_BYTES = 2 * 1024
export const MAX_OUTPUT_BYTES = 24 * 1024
export const DEFAULT_LIMIT = 8
export const MAX_LIMIT = 20
export const MAX_GET_BYTES = 48 * 1024

export const NUDGE =
  "[recall-nudge] For finding, recovering, summarising or verifying historical conversation content, decisions, commands, prior fixes or session history, call `recall` first and drill into a hit with `recall_get`; do not guess from memory. Use `opencode_meta` for exact current session metadata, relations and statistics. The recall index only covers sessions seen since the plugin started indexing."

export interface TextDoc {
  updated: number
  text: string
}

export interface SessionApiLike {
  get(input: { sessionID: string }): Promise<unknown>
  context(input: { sessionID: string }): Promise<unknown>
}

function clipBytes(value: string, maxBytes: number): string {
  if (Buffer.byteLength(value) <= maxBytes) return value
  return `${Buffer.from(value).subarray(0, maxBytes).toString("utf8")}…`
}

/** Build the bounded text document for a session from its context messages (newest kept). */
export function buildTextDoc(messages: unknown[], updated: number): TextDoc {
  const lines: string[] = []
  for (const message of messages) {
    const entry = messageText(message)
    if (!entry) continue
    lines.push(`[${entry.role}] ${clipBytes(entry.text.replace(/\s+/g, " ").trim(), MAX_MESSAGE_TEXT_BYTES)}`)
  }
  let text = lines.join("\n")
  while (Buffer.byteLength(text) > MAX_SESSION_TEXT_BYTES && lines.length > 1) {
    lines.shift()
    text = lines.join("\n")
  }
  return { updated, text: clipBytes(text, MAX_SESSION_TEXT_BYTES) }
}

/** Refresh one session's record and text document from the live session API. */
export async function refreshSession(
  sessions: SessionApiLike,
  storage: StorageLike,
  sessionID: string,
  now = Date.now(),
): Promise<SessionRecord | undefined> {
  const info = await sessions.get({ sessionID }).catch(() => undefined)
  const record = sessionFromInfo(info, now)
  if (!record) return undefined
  const stored = await upsertSession(storage, record)
  const messages = await sessions.context({ sessionID }).catch(() => [])
  const doc = buildTextDoc(Array.isArray(messages) ? messages : [], stored.updated)
  await storage.set(TEXT_PREFIX + sessionID, doc)
  return stored
}

export async function getTextDoc(storage: StorageLike, sessionID: string): Promise<TextDoc | undefined> {
  const value = await storage.get(TEXT_PREFIX + sessionID)
  return value && typeof value === "object" && typeof (value as TextDoc).text === "string" ? (value as TextDoc) : undefined
}

export interface RecallArgs {
  query: string
  directory?: string
  project?: string
  days?: number
  limit?: number
}

export interface RecallHit {
  session_id: string
  title?: string
  directory?: string
  project_id?: string
  parent_id?: string
  agent?: string
  updated: string
  score: number
  snippets: string[]
}

export function terms(query: string): string[] {
  return Array.from(new Set(query.toLowerCase().split(/\s+/).filter((term) => term.length > 1)))
}

function countOccurrences(haystack: string, needle: string, cap: number): number {
  let count = 0
  let index = 0
  while (count < cap) {
    index = haystack.indexOf(needle, index)
    if (index < 0) break
    count += 1
    index += needle.length
  }
  return count
}

export function score(record: SessionRecord, doc: TextDoc | undefined, query: string): { score: number; snippets: string[] } {
  const words = terms(query)
  if (words.length === 0) return { score: 0, snippets: [] }
  const title = (record.title ?? "").toLowerCase()
  const body = (doc?.text ?? "").toLowerCase()
  let total = 0
  const phrase = query.trim().toLowerCase()
  if (phrase.length > 1) {
    if (title.includes(phrase)) total += 8
    if (body.includes(phrase)) total += 5
  }
  for (const word of words) {
    if (title.includes(word)) total += 3
    total += Math.min(countOccurrences(body, word, 20), 20)
  }
  const snippets: string[] = []
  if (doc?.text && total > 0) {
    const original = doc.text
    const lower = body
    const seen = new Set<number>()
    for (const word of [phrase, ...words]) {
      if (snippets.length >= 3 || word.length < 2) continue
      let from = 0
      while (snippets.length < 3) {
        const at = lower.indexOf(word, from)
        if (at < 0) break
        const bucket = Math.floor(at / 160)
        if (!seen.has(bucket)) {
          seen.add(bucket)
          const start = Math.max(0, at - 80)
          const end = Math.min(original.length, at + word.length + 80)
          snippets.push(`${start > 0 ? "…" : ""}${original.slice(start, end).replace(/\n/g, " ")}${end < original.length ? "…" : ""}`)
        }
        from = at + word.length
      }
    }
  }
  return { score: total, snippets }
}

export function validateRecallArgs(args: unknown): RecallArgs {
  const input = (args ?? {}) as Record<string, unknown>
  if (typeof input.query !== "string" || input.query.trim().length === 0) throw new Error("query must be a non-empty string")
  for (const key of ["directory", "project"] as const) {
    if (input[key] !== undefined && (typeof input[key] !== "string" || (input[key] as string).length === 0)) {
      throw new Error(`${key} must be a non-empty string`)
    }
  }
  if (input.limit !== undefined && (!Number.isInteger(input.limit) || (input.limit as number) < 1 || (input.limit as number) > MAX_LIMIT)) {
    throw new Error(`limit must be an integer from 1 to ${MAX_LIMIT}`)
  }
  if (input.days !== undefined && (!Number.isInteger(input.days) || (input.days as number) < 1 || (input.days as number) > 3650)) {
    throw new Error("days must be an integer from 1 to 3650")
  }
  return {
    query: input.query.trim(),
    directory: input.directory as string | undefined,
    project: input.project as string | undefined,
    days: input.days as number | undefined,
    limit: (input.limit as number | undefined) ?? DEFAULT_LIMIT,
  }
}

export async function recall(storage: StorageLike, rawArgs: unknown, now = Date.now()): Promise<string> {
  const args = validateRecallArgs(rawArgs)
  const cutoff = args.days ? now - args.days * 86_400_000 : undefined
  const records = (await listSessions(storage)).filter(
    (record) =>
      (!args.directory || record.directory === args.directory) &&
      (!args.project || record.projectID === args.project) &&
      (cutoff === undefined || record.updated >= cutoff),
  )
  const hits: RecallHit[] = []
  for (const record of records) {
    const doc = await getTextDoc(storage, record.id)
    const result = score(record, doc, args.query)
    if (result.score <= 0) continue
    hits.push({
      session_id: record.id,
      title: record.title,
      directory: record.directory,
      project_id: record.projectID,
      parent_id: record.parentID,
      agent: record.agent,
      updated: new Date(record.updated).toISOString(),
      score: result.score,
      snippets: result.snippets,
    })
  }
  hits.sort((a, b) => b.score - a.score || b.updated.localeCompare(a.updated))
  const meta = await readMeta(storage)
  return boundedJson(
    {
      query: args.query,
      indexed_sessions: records.length,
      index_since: meta ? new Date(meta.started).toISOString() : null,
      hits: hits.slice(0, args.limit),
      truncated: hits.length > args.limit,
      note: "Only sessions seen since indexing started are searchable; drill into a hit with recall_get.",
    },
    MAX_OUTPUT_BYTES,
  )
}

export interface RecallGetArgs {
  session_id: string
  max_chars?: number
}

export function validateGetArgs(args: unknown): RecallGetArgs {
  const input = (args ?? {}) as Record<string, unknown>
  if (typeof input.session_id !== "string" || input.session_id.length === 0) throw new Error("session_id must be a non-empty string")
  if (
    input.max_chars !== undefined &&
    (!Number.isInteger(input.max_chars) || (input.max_chars as number) < 200 || (input.max_chars as number) > MAX_GET_BYTES)
  ) {
    throw new Error(`max_chars must be an integer from 200 to ${MAX_GET_BYTES}`)
  }
  return { session_id: input.session_id, max_chars: (input.max_chars as number | undefined) ?? 12_000 }
}

export async function recallGet(sessions: SessionApiLike, storage: StorageLike, rawArgs: unknown): Promise<string> {
  const args = validateGetArgs(rawArgs)
  const record = (await refreshSession(sessions, storage, args.session_id)) ?? (await getSession(storage, args.session_id))
  if (!record) throw new Error(`session not found: ${args.session_id}`)
  const doc = await getTextDoc(storage, args.session_id)
  const text = doc?.text ?? ""
  const clipped = text.length > args.max_chars! ? `…${text.slice(text.length - args.max_chars!)}` : text
  return boundedJson(
    {
      session: {
        id: record.id,
        title: record.title,
        directory: record.directory,
        project_id: record.projectID,
        parent_id: record.parentID,
        agent: record.agent,
        created: new Date(record.created).toISOString(),
        updated: new Date(record.updated).toISOString(),
      },
      transcript: clipped,
      transcript_truncated: clipped.length < text.length,
    },
    MAX_GET_BYTES + 2048,
  )
}

export const RECALL_INPUT = {
  type: "object",
  properties: {
    query: { type: "string", description: "Words or a phrase to search for in session titles and transcripts." },
    directory: { type: "string", description: "Only sessions whose location directory equals this absolute path." },
    project: { type: "string", description: "Only sessions with this project ID." },
    days: { type: "integer", minimum: 1, maximum: 3650, description: "Only sessions updated within this many days." },
    limit: { type: "integer", minimum: 1, maximum: MAX_LIMIT, description: `Maximum hits (default ${DEFAULT_LIMIT}).` },
  },
  required: ["query"],
  additionalProperties: false,
} as const

export const RECALL_GET_INPUT = {
  type: "object",
  properties: {
    session_id: { type: "string", description: "Session ID from a recall hit." },
    max_chars: { type: "integer", minimum: 200, maximum: MAX_GET_BYTES, description: "Transcript tail size (default 12000)." },
  },
  required: ["session_id"],
  additionalProperties: false,
} as const

export interface RecallPluginOptions {
  nudge?: boolean
  debug?: boolean
}

/** Wire the index, tools and nudge onto a v2 plugin context. Returns the cleanup function. */
export async function setupRecall(ctx: any): Promise<() => void> {
  const options = (ctx.options ?? {}) as RecallPluginOptions
  const storage = ctx.storage as StorageLike
  const sessions = ctx.session as SessionApiLike
  const log = (message: string, extra?: unknown) => {
    if (options.debug) console.error(`[opencode-recall-lite] ${message}`, extra ?? "")
  }
  await markStarted(storage)
  const stop = startIndexing(ctx.event, storage, {
    onSettled: async (sessionID) => {
      await refreshSession(sessions, storage, sessionID)
      log("refreshed", sessionID)
    },
    onError: (error) => log("index error", error),
  })
  await ctx.tool.transform((editor: any) => {
    editor.add({
      name: "recall",
      description:
        "Search previously seen OpenCode sessions (titles and transcripts, across projects on this server) for words or a phrase. Returns ranked hits with snippets; drill into one with recall_get.",
      input: RECALL_INPUT,
      options: { codemode: false },
      execute: async (input: unknown, context: any) => {
        if (context?.sessionID) await refreshSession(sessions, storage, context.sessionID).catch(() => undefined)
        return { content: await recall(storage, input) }
      },
    })
    editor.add({
      name: "recall_get",
      description: "Return the bounded transcript tail and metadata of one previously seen session, by session ID.",
      input: RECALL_GET_INPUT,
      options: { codemode: false },
      execute: async (input: unknown) => ({ content: await recallGet(sessions, storage, input) }),
    })
    log("tools registered", editor.list?.().map((tool: any) => tool.id))
  })
  if (options.nudge !== false) {
    await ctx.session.hook("context", (event: any) => {
      const system = event.system as Array<{ type: string; text?: string }>
      if (system.some((part) => typeof part.text === "string" && part.text.includes("[recall-nudge]"))) return
      system.push({ type: "text", text: NUDGE })
    })
  }
  return () => stop()
}
