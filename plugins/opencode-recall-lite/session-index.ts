// Shared, dependency-free session index kept in the plugin's scoped storage.
// Both opencode-meta and opencode-recall-lite import this file by relative path.

export interface SessionRecord {
  id: string
  title?: string
  parentID?: string
  projectID?: string
  directory?: string
  workspaceID?: string
  agent?: string
  created: number
  updated: number
}

export interface StorageLike {
  get(key: string): Promise<unknown>
  set(key: string, value: unknown): Promise<void>
  remove(key: string): Promise<void>
  scan(options: { prefix: string; after?: string; limit?: number }): Promise<{
    entries: readonly { key: string; value: unknown }[]
    next?: string
  }>
}

export const SESSION_PREFIX = "s:"
export const META_KEY = "meta"
export const DEFAULT_SESSION_LIMIT = 2000
export const MAX_STRING_BYTES = 8 * 1024

export function millis(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value
  if (typeof value === "string") {
    const parsed = Date.parse(value)
    return Number.isFinite(parsed) ? parsed : undefined
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>
    if (typeof record.epochMillis === "number") return record.epochMillis
    if (typeof record.epochMillis === "bigint") return Number(record.epochMillis)
    if (typeof record.toDate === "function") {
      const date = (record.toDate as () => Date)()
      return date instanceof Date ? date.getTime() : undefined
    }
    if (typeof record.getTime === "function") return (record.getTime as () => number)()
  }
  return undefined
}

function text(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined
}

/** Build a record from a v2 `Session.Info` value (decoded or JSON-encoded). */
export function sessionFromInfo(info: any, now = Date.now()): SessionRecord | undefined {
  const id = text(info?.id)
  if (!id) return undefined
  const time = info.time ?? {}
  const model = info.model
  return {
    id,
    title: text(info.title),
    parentID: text(info.parentID),
    projectID: text(info.projectID),
    directory: text(info.location?.directory),
    workspaceID: text(info.location?.workspaceID),
    agent: text(info.agent),
    created: millis(time.created) ?? now,
    updated: millis(time.updated) ?? millis(time.created) ?? now,
    ...(model && typeof model === "object" ? {} : {}),
  }
}

/** Build a record from a `session.created` event payload. */
export function sessionFromCreated(event: any): SessionRecord | undefined {
  const data = event?.data
  const id = text(data?.sessionID)
  if (!id) return undefined
  const created = millis(event.created) ?? Date.now()
  return {
    id,
    title: text(data.title),
    parentID: text(data.parentID),
    projectID: text(data.projectID),
    directory: text(data.location?.directory),
    workspaceID: text(data.location?.workspaceID),
    agent: text(data.agent),
    created,
    updated: created,
  }
}

export async function getSession(storage: StorageLike, id: string): Promise<SessionRecord | undefined> {
  const value = await storage.get(SESSION_PREFIX + id)
  return value && typeof value === "object" ? (value as SessionRecord) : undefined
}

export async function upsertSession(
  storage: StorageLike,
  record: Partial<SessionRecord> & { id: string },
): Promise<SessionRecord> {
  const existing = await getSession(storage, record.id)
  const merged: SessionRecord = {
    ...(existing ?? { created: record.created ?? Date.now(), updated: 0 }),
    ...Object.fromEntries(Object.entries(record).filter(([, value]) => value !== undefined)),
    id: record.id,
  } as SessionRecord
  merged.updated = Math.max(existing?.updated ?? 0, record.updated ?? 0, merged.updated ?? 0)
  await storage.set(SESSION_PREFIX + record.id, merged)
  return merged
}

export async function removeSession(storage: StorageLike, id: string): Promise<void> {
  await storage.remove(SESSION_PREFIX + id)
}

export async function listSessions(storage: StorageLike, limit = DEFAULT_SESSION_LIMIT): Promise<SessionRecord[]> {
  const out: SessionRecord[] = []
  let after: string | undefined
  while (out.length < limit) {
    const page = await storage.scan({ prefix: SESSION_PREFIX, after, limit: Math.min(200, limit - out.length) })
    for (const entry of page.entries) {
      if (entry.value && typeof entry.value === "object") out.push(entry.value as SessionRecord)
    }
    if (!page.next || page.entries.length === 0) break
    after = page.next
  }
  return out
}

export interface IndexMeta {
  started: number
  events: number
}

export async function markStarted(storage: StorageLike, now = Date.now()): Promise<IndexMeta> {
  const existing = (await storage.get(META_KEY)) as IndexMeta | undefined
  const meta: IndexMeta = { started: existing?.started ?? now, events: existing?.events ?? 0 }
  await storage.set(META_KEY, meta)
  return meta
}

export async function readMeta(storage: StorageLike): Promise<IndexMeta | undefined> {
  const value = await storage.get(META_KEY)
  return value && typeof value === "object" ? (value as IndexMeta) : undefined
}

export type SessionEventKind = "created" | "renamed" | "deleted" | "settled" | "other"

export function classifyEvent(event: any): { kind: SessionEventKind; sessionID?: string } {
  const type = typeof event?.type === "string" ? event.type : ""
  const sessionID = text(event?.data?.sessionID)
  if (type === "session.created") return { kind: "created", sessionID }
  if (type === "session.renamed") return { kind: "renamed", sessionID }
  if (type === "session.deleted") return { kind: "deleted", sessionID }
  if (
    type === "session.execution.succeeded" ||
    type === "session.execution.failed" ||
    type === "session.execution.interrupted"
  )
    return { kind: "settled", sessionID }
  return { kind: "other", sessionID }
}

export interface ApplyOptions {
  /** Called after a session settles so the caller can refresh derived data. */
  onSettled?: (sessionID: string) => Promise<void>
  now?: () => number
}

/** Apply one server event to the index. Idempotent; safe when several plugin instances see the same event. */
export async function applyEvent(storage: StorageLike, event: any, options: ApplyOptions = {}): Promise<boolean> {
  const now = options.now ?? Date.now
  const { kind, sessionID } = classifyEvent(event)
  if (!sessionID || kind === "other") return false
  if (kind === "created") {
    const record = sessionFromCreated(event)
    if (record) await upsertSession(storage, record)
    return true
  }
  if (kind === "renamed") {
    await upsertSession(storage, { id: sessionID, title: text(event.data.title), updated: millis(event.created) ?? now() })
    return true
  }
  if (kind === "deleted") {
    await removeSession(storage, sessionID)
    return true
  }
  await upsertSession(storage, { id: sessionID, updated: millis(event.created) ?? now() })
  if (options.onSettled) await options.onSettled(sessionID)
  return true
}

export interface EventSource {
  subscribe(options?: { signal?: AbortSignal }): AsyncIterable<unknown>
}

/** Subscribe to server events and keep the index current until the returned cleanup runs. */
export function startIndexing(
  events: EventSource,
  storage: StorageLike,
  options: ApplyOptions & { onError?: (error: unknown) => void } = {},
): () => void {
  const controller = new AbortController()
  void (async () => {
    try {
      for await (const event of events.subscribe({ signal: controller.signal })) {
        if (controller.signal.aborted) break
        try {
          await applyEvent(storage, event, options)
        } catch (error) {
          options.onError?.(error)
        }
      }
    } catch (error) {
      if (!controller.signal.aborted) options.onError?.(error)
    }
  })()
  return () => controller.abort()
}

export function sanitise(value: unknown, maxStringBytes = MAX_STRING_BYTES): any {
  if (typeof value === "bigint") {
    return value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= BigInt(Number.MIN_SAFE_INTEGER)
      ? Number(value)
      : value.toString()
  }
  if (typeof value === "string" && Buffer.byteLength(value) > maxStringBytes) {
    return `${Buffer.from(value).subarray(0, maxStringBytes).toString("utf8")}…[truncated]`
  }
  if (Array.isArray(value)) return value.map((item) => sanitise(item, maxStringBytes))
  if (value instanceof Date) return value.toISOString()
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>
    if (typeof record.epochMillis === "number") return new Date(record.epochMillis).toISOString()
    return Object.fromEntries(Object.entries(record).map(([key, item]) => [key, sanitise(item, maxStringBytes)]))
  }
  return value
}

function findArrays(value: unknown): unknown[][] {
  if (Array.isArray(value)) return [value, ...value.flatMap(findArrays)]
  if (value && typeof value === "object") return Object.values(value).flatMap(findArrays)
  return []
}

/** Serialise to JSON, trimming the largest arrays until the output fits. */
export function boundedJson(value: unknown, maxBytes: number): string {
  const clean = sanitise(value)
  let output = JSON.stringify(clean, null, 2)
  while (Buffer.byteLength(output) > maxBytes) {
    const arrays = findArrays(clean)
      .filter((array) => array.length > 0)
      .sort((a, b) => b.length - a.length)
    if (arrays.length === 0) return JSON.stringify({ error: `serialised output exceeded ${maxBytes} bytes` })
    arrays[0].pop()
    if (clean && typeof clean === "object" && !Array.isArray(clean)) (clean as Record<string, unknown>).output_truncated = true
    output = JSON.stringify(clean, null, 2)
  }
  return output
}

/** Extract role-labelled plain text from a v2 session message; undefined when there is nothing worth indexing. */
export function messageText(message: any): { role: string; text: string; created?: number } | undefined {
  if (!message || typeof message !== "object") return undefined
  const created = millis(message.time?.created)
  const type = message.type
  if (type === "user" || type === "synthetic" || type === "system" || type === "skill") {
    const value = text(message.text)
    return value ? { role: type, text: value, created } : undefined
  }
  if (type === "shell") {
    const value = text(message.command)
    return value ? { role: "shell", text: `$ ${value}`, created } : undefined
  }
  if (type === "assistant") {
    const parts: string[] = []
    for (const part of Array.isArray(message.content) ? message.content : []) {
      if (part?.type === "text" && text(part.text)) parts.push(part.text)
      else if (part?.type === "tool" && text(part.name)) parts.push(`[tool:${part.name}]`)
    }
    return parts.length ? { role: "assistant", text: parts.join("\n"), created } : undefined
  }
  return undefined
}
