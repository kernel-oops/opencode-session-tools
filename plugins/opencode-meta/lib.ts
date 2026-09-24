import {
  boundedJson,
  getSession,
  listSessions,
  markStarted,
  readMeta,
  sessionFromInfo,
  startIndexing,
  upsertSession,
  type SessionRecord,
  type StorageLike,
} from "../opencode-recall-lite/session-index.ts"
import { SEND_DESCRIPTION, SEND_INPUT, sendMessage, type SendSessionApi } from "./send.ts"

const DEFAULT_LIMIT = 20
const MAX_LIMIT = 100
const MAX_OUTPUT_BYTES = 64 * 1024
const MAX_INSPECTED_SESSIONS = 25

export const OPERATIONS = ["session", "session_tree", "session_stats", "recent_sessions", "tool_usage", "capabilities"] as const
export type Operation = (typeof OPERATIONS)[number]

export interface MetaArgs {
  operation: Operation
  session_id?: string
  directory?: string
  tool_name?: string
  days?: number
  limit?: number
}

export interface SessionApiLike {
  get(input: { sessionID: string }): Promise<unknown>
  context(input: { sessionID: string }): Promise<unknown>
}

export interface ToolContextLike {
  sessionID?: string
  agent?: string
  messageID?: string
}

export const META_INPUT = {
  type: "object",
  properties: {
    operation: { type: "string", enum: [...OPERATIONS], description: "Which bounded metadata query to run." },
    session_id: { type: "string", description: "Target session (defaults to the current session)." },
    directory: { type: "string", description: "Filter by absolute location directory (recent_sessions, tool_usage)." },
    tool_name: { type: "string", description: "Filter tool_usage to one tool name." },
    days: { type: "integer", minimum: 1, maximum: 3650, description: "Look-back window in days." },
    limit: { type: "integer", minimum: 1, maximum: MAX_LIMIT, description: `Maximum rows (default ${DEFAULT_LIMIT}).` },
  },
  required: ["operation"],
  additionalProperties: false,
} as const

export function validateArgs(args: unknown, context: ToolContextLike = {}): Required<Pick<MetaArgs, "operation" | "limit">> & MetaArgs {
  const input = (args ?? {}) as Record<string, unknown>
  const operation = input.operation as Operation
  if (!OPERATIONS.includes(operation)) throw new Error(`operation must be one of: ${OPERATIONS.join(", ")}`)
  for (const key of ["session_id", "directory", "tool_name"] as const) {
    const value = input[key]
    if (value !== undefined && (typeof value !== "string" || value.length === 0)) throw new Error(`${key} must be a non-empty string`)
  }
  const limit = input.limit
  if (limit !== undefined && (!Number.isInteger(limit) || (limit as number) < 1 || (limit as number) > MAX_LIMIT)) {
    throw new Error(`limit must be an integer from 1 to ${MAX_LIMIT}`)
  }
  const days = input.days
  if (days !== undefined && (!Number.isInteger(days) || (days as number) < 1 || (days as number) > 3650)) {
    throw new Error("days must be an integer from 1 to 3650")
  }
  const allowed: Record<Operation, ReadonlySet<string>> = {
    session: new Set(["operation", "session_id"]),
    session_tree: new Set(["operation", "session_id", "limit"]),
    session_stats: new Set(["operation", "session_id"]),
    recent_sessions: new Set(["operation", "directory", "days", "limit"]),
    tool_usage: new Set(["operation", "session_id", "directory", "tool_name", "days", "limit"]),
    capabilities: new Set(["operation"]),
  }
  for (const [key, value] of Object.entries(input)) {
    if (value !== undefined && !allowed[operation].has(key)) throw new Error(`argument ${key} is not valid for operation ${operation}`)
  }
  const needsSession = operation === "session" || operation === "session_tree" || operation === "session_stats"
  const sessionID = (input.session_id as string | undefined) ?? (needsSession ? context.sessionID : undefined)
  if (needsSession && !sessionID) throw new Error(`session_id is required for operation ${operation} outside a session`)
  return {
    operation,
    session_id: sessionID,
    directory: input.directory as string | undefined,
    tool_name: input.tool_name as string | undefined,
    days: input.days as number | undefined,
    limit: (input.limit as number | undefined) ?? DEFAULT_LIMIT,
  }
}

function iso(value: number | undefined): string | undefined {
  return value === undefined ? undefined : new Date(value).toISOString()
}

function publicSession(record: SessionRecord, info?: any) {
  return {
    id: record.id,
    title: record.title ?? null,
    parent_id: record.parentID ?? null,
    project_id: record.projectID ?? null,
    directory: record.directory ?? null,
    workspace_id: record.workspaceID ?? null,
    agent: record.agent ?? null,
    model: info?.model ?? null,
    outcome: info?.outcome ?? null,
    cost_usd: typeof info?.cost === "number" ? info.cost : undefined,
    tokens: info?.tokens ?? undefined,
    subpath: info?.subpath ?? undefined,
    metadata_keys: info?.metadata && typeof info.metadata === "object" ? Object.keys(info.metadata) : undefined,
    time: {
      created: iso(record.created),
      updated: iso(record.updated),
      idle: info?.time?.idle ? iso(msOf(info.time.idle)) : undefined,
      archived: info?.time?.archived ? iso(msOf(info.time.archived)) : undefined,
    },
  }
}

function msOf(value: unknown): number | undefined {
  if (typeof value === "number") return value
  if (typeof value === "string") return Date.parse(value)
  if (value && typeof value === "object" && typeof (value as any).epochMillis === "number") return (value as any).epochMillis
  return undefined
}

async function loadSession(sessions: SessionApiLike, storage: StorageLike, sessionID: string) {
  const info = await sessions.get({ sessionID }).catch(() => undefined)
  const fresh = sessionFromInfo(info)
  const record = fresh ? await upsertSession(storage, fresh) : await getSession(storage, sessionID)
  if (!record) throw new Error(`session not found: ${sessionID}`)
  return { record, info }
}

interface ToolCall {
  name: string
  status?: string
  created?: number
}

function toolCalls(messages: unknown[]): ToolCall[] {
  const calls: ToolCall[] = []
  for (const message of messages as any[]) {
    if (message?.type !== "assistant" || !Array.isArray(message.content)) continue
    for (const part of message.content) {
      if (part?.type === "tool" && typeof part.name === "string") {
        calls.push({ name: part.name, status: part.state?.status, created: msOf(part.time?.created) })
      }
    }
  }
  return calls
}

export async function executeOperation(
  args: ReturnType<typeof validateArgs>,
  sessions: SessionApiLike,
  storage: StorageLike,
  app: { name?: string; version?: string; channel?: string } | undefined,
  location: { directory?: string; project?: unknown } | undefined,
  now = Date.now(),
): Promise<unknown> {
  switch (args.operation) {
    case "session": {
      const { record, info } = await loadSession(sessions, storage, args.session_id!)
      return { session: publicSession(record, info) }
    }
    case "session_tree": {
      const { record, info } = await loadSession(sessions, storage, args.session_id!)
      const parent = record.parentID ? await loadSession(sessions, storage, record.parentID).catch(() => undefined) : undefined
      const children = (await listSessions(storage))
        .filter((item) => item.parentID === record.id)
        .sort((a, b) => b.updated - a.updated)
        .slice(0, args.limit)
        .map((item) => publicSession(item))
      return {
        session: publicSession(record, info),
        parent: parent ? publicSession(parent.record, parent.info) : null,
        children,
        children_truncated: children.length === args.limit,
        note: "Children come from the event index, so only sessions created since indexing started are listed.",
      }
    }
    case "session_stats": {
      const { record, info } = await loadSession(sessions, storage, args.session_id!)
      const messages = ((await sessions.context({ sessionID: record.id }).catch(() => [])) ?? []) as any[]
      const byType: Record<string, number> = {}
      const parts: Record<string, number> = {}
      for (const message of messages) {
        byType[message?.type ?? "unknown"] = (byType[message?.type ?? "unknown"] ?? 0) + 1
        if (message?.type === "assistant" && Array.isArray(message.content)) {
          for (const part of message.content) parts[part?.type ?? "unknown"] = (parts[part?.type ?? "unknown"] ?? 0) + 1
        }
      }
      const calls = toolCalls(messages)
      const tools: Record<string, { calls: number; errors: number }> = {}
      for (const call of calls) {
        const entry = (tools[call.name] ??= { calls: 0, errors: 0 })
        entry.calls += 1
        if (call.status === "error") entry.errors += 1
      }
      return {
        session_id: record.id,
        totals: { messages: messages.length, tool_calls: calls.length },
        message_types: byType,
        assistant_part_types: parts,
        tools: Object.entries(tools)
          .sort((a, b) => b[1].calls - a[1].calls)
          .slice(0, 50)
          .map(([name, value]) => ({ name, ...value })),
        cost_usd: typeof info?.cost === "number" ? info.cost : undefined,
        tokens: info?.tokens ?? undefined,
        note: "Counts cover the messages currently in model context for this session (compacted history is excluded).",
      }
    }
    case "recent_sessions": {
      const cutoff = now - (args.days ?? 7) * 86_400_000
      const rows = (await listSessions(storage))
        .filter((item) => item.updated >= cutoff && (!args.directory || item.directory === args.directory))
        .sort((a, b) => b.updated - a.updated)
        .slice(0, args.limit)
        .map((item) => publicSession(item))
      const meta = await readMeta(storage)
      return { days: args.days ?? 7, directory: args.directory ?? null, index_since: iso(meta?.started), sessions: rows, truncated: rows.length === args.limit }
    }
    case "tool_usage": {
      const cutoff = now - (args.days ?? 30) * 86_400_000
      let targets: SessionRecord[]
      if (args.session_id) {
        targets = [(await loadSession(sessions, storage, args.session_id)).record]
      } else {
        targets = (await listSessions(storage))
          .filter((item) => item.updated >= cutoff && (!args.directory || item.directory === args.directory))
          .sort((a, b) => b.updated - a.updated)
          .slice(0, MAX_INSPECTED_SESSIONS)
      }
      const usage: Record<string, { calls: number; sessions: Set<string>; last_used: number }> = {}
      for (const target of targets) {
        const messages = ((await sessions.context({ sessionID: target.id }).catch(() => [])) ?? []) as any[]
        for (const call of toolCalls(messages)) {
          if (args.tool_name && call.name !== args.tool_name) continue
          if (call.created !== undefined && call.created < cutoff) continue
          const entry = (usage[call.name] ??= { calls: 0, sessions: new Set(), last_used: 0 })
          entry.calls += 1
          entry.sessions.add(target.id)
          entry.last_used = Math.max(entry.last_used, call.created ?? target.updated)
        }
      }
      const rows = Object.entries(usage)
        .map(([tool_name, value]) => ({ tool_name, calls: value.calls, sessions: value.sessions.size, last_used: iso(value.last_used) }))
        .sort((a, b) => b.calls - a.calls || a.tool_name.localeCompare(b.tool_name))
        .slice(0, args.limit)
      return {
        days: args.days ?? 30,
        inspected_sessions: targets.length,
        scope: args.session_id ? "one session" : `up to ${MAX_INSPECTED_SESSIONS} most recently updated indexed sessions`,
        usage: rows,
        truncated: rows.length === args.limit,
      }
    }
    case "capabilities": {
      const records = await listSessions(storage)
      const meta = await readMeta(storage)
      const updated = records.map((item) => item.updated)
      return {
        app: { name: app?.name ?? null, version: app?.version ?? null, channel: app?.channel ?? null },
        location: { directory: location?.directory ?? null },
        operations: OPERATIONS,
        index: {
          sessions_indexed: records.length,
          since: iso(meta?.started),
          oldest_updated: updated.length ? iso(Math.min(...updated)) : null,
          newest_updated: updated.length ? iso(Math.max(...updated)) : null,
        },
        limits: { max_limit: MAX_LIMIT, max_output_bytes: MAX_OUTPUT_BYTES, max_inspected_sessions: MAX_INSPECTED_SESSIONS },
        notes: [
          "Data comes from the v2 plugin API (session get/context) plus a live event index; no database access.",
          "Sessions created before indexing started are only visible when addressed directly by session_id.",
          "Schema, todo and database_info operations from v1 are retired: v2 exposes no database to plugins.",
        ],
      }
    }
  }
}

export interface MetaPluginOptions {
  debug?: boolean
}

/**
 * `indexing: false` when another plugin instance sharing this storage already maintains the session index
 * (the combined npm entry runs recall-lite's indexer and this one's tools over one index).
 */
export async function setupMeta(ctx: any, setup: { indexing?: boolean } = {}): Promise<() => void> {
  const options = (ctx.options ?? {}) as MetaPluginOptions
  const storage = ctx.storage as StorageLike
  const sessions = ctx.session as SessionApiLike
  const log = (message: string, extra?: unknown) => {
    if (options.debug) console.error(`[opencode-meta] ${message}`, extra ?? "")
  }
  let stop = () => {}
  if (setup.indexing !== false) {
    await markStarted(storage)
    stop = startIndexing(ctx.event, storage, { onError: (error) => log("index error", error) })
  }
  await ctx.tool.transform((editor: any) => {
    editor.add({
      name: "opencode_meta",
      description:
        "Read bounded, current OpenCode session metadata, relations, statistics and tool usage through the v2 API. Use recall rather than this tool to search historical conversation content.",
      input: META_INPUT,
      options: { codemode: false },
      execute: async (input: unknown, context: ToolContextLike) => {
        const validated = validateArgs(input, context)
        try {
          const result = await executeOperation(validated, sessions, storage, ctx.app, ctx.location)
          return { content: boundedJson(result, MAX_OUTPUT_BYTES) }
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error)
          throw new Error(`opencode_meta ${validated.operation} failed: ${message}`)
        }
      },
    })
    editor.add({
      name: "send_message",
      description: SEND_DESCRIPTION,
      input: SEND_INPUT,
      options: { codemode: false },
      execute: async (input: unknown, context: ToolContextLike) => {
        const result = await sendMessage(storage, ctx.session as SendSessionApi, input as { to: string; text: string }, context)
        return { content: boundedJson(result, MAX_OUTPUT_BYTES) }
      },
    })
    log("tool registered", editor.list?.().map((tool: any) => tool.id))
  })
  return () => stop()
}
