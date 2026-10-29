import { createHash } from "node:crypto"

// Durable peer inbox for send_message. Originals are immutable plugin-storage records; delivery state lives in batch
// manifests and recipient dispositions are separate records, so reading, acknowledging, superseding or delivering
// never rewrites or deletes an original.
//
// Ownership: one registry per process (inside the digest registry on globalThis, shared by every plugin copy) holds the
// per-recipient locks. Plugin storage has no compare-and-set, so this relies on the single central server owning the
// store; a second runtime writing the same store is unsupported, not made safe by these locks.

export const EVENT_PREFIX = "peer:event:"
export const STATE_PREFIX = "peer:state:"
export const BATCH_PREFIX = "peer:batch:"
export const MAX_EVENT_BYTES = 40 * 1024
export const MAX_BATCH_BYTES = 48 * 1024
export const MAX_BATCH_EVENTS = 20
export const MAX_PAGE = 20
// Session titles have no length limit; headers repeat them per row, so list and ack results show only this much.
export const MAX_HEADER_TITLE = 200

export const RECEIPT_KINDS = ["fyi", "actionable"] as const
/** Receipt states an FYI may carry and stay passive. */
export const SAFE_STATES = ["progress", "completed"] as const
/** Recognised states that always need the recipient's attention. */
export const ATTENTION_STATES = ["blocked", "failed", "cancelled", "unknown"] as const

export interface Evidence {
  path: string
  sha256?: string
  section?: string
}

export interface Receipt {
  version: 1
  id?: string
  kind: string
  work_item?: string
  revision?: string
  job_id?: string
  state?: string
  supersedes?: string[]
  restrictions?: string[]
  unresolved?: string[]
  evidence?: Evidence[]
}

const ID_PATTERN = "^[A-Za-z0-9._-]{1,128}$"
const ID_RE = new RegExp(ID_PATTERN)
const SHA_RE = /^[0-9a-f]{64}$/
const strings = (maxItems: number, maxLength: number) => ({ type: "array", maxItems, items: { type: "string", minLength: 1, maxLength } })

export const RECEIPT_INPUT = {
  type: "object",
  description:
    'Optional typed receipt; untyped messages keep their existing delivery. kind "fyi" at normal priority with no state other than "progress" or "completed", no restrictions and no unresolved items is stored in the recipient\'s peer inbox without waking it or any model: the result says recorded and passive, and the recipient has not read it until it checks peer_inbox. Any other receipt (kind "actionable", an FYI with state "blocked", "failed", "cancelled" or "unknown", restrictions or unresolved items, or an unrecognised kind or state) is delivered as usual, with its fields shown. An FYI may name earlier FYI receipt IDs of its own for the same work_item, revision and job_id in `supersedes`; those then show as archived, never deleted. Free text labelled FYI is not checked: label honestly.',
  properties: {
    version: { type: "integer", enum: [1] },
    id: { type: "string", pattern: ID_PATTERN, description: "Your idempotency key, scoped to you and the recipient: resending the same receipt is not duplicated; reusing it for different content is refused." },
    kind: { type: "string", minLength: 1, maxLength: 64, description: '"fyi" or "actionable"; anything else is delivered as an untyped message.' },
    work_item: { type: "string", minLength: 1, maxLength: 512 },
    revision: { type: "string", minLength: 1, maxLength: 512, description: "Exact candidate or content revision (for example a commit hash)." },
    job_id: { type: "string", minLength: 1, maxLength: 512 },
    state: { type: "string", minLength: 1, maxLength: 64, description: '"progress", "completed", "blocked", "failed", "cancelled" or "unknown".' },
    supersedes: { ...strings(50, 128), description: "Exact earlier receipt IDs of yours that this FYI replaces." },
    restrictions: { ...strings(50, 4000), description: "Exact restrictions, never summarised; any makes the receipt actionable." },
    unresolved: { ...strings(50, 4000), description: "Open decisions, blockers or unknown outcomes; any makes the receipt actionable." },
    evidence: {
      type: "array",
      maxItems: 20,
      items: {
        type: "object",
        properties: { path: { type: "string", minLength: 1, maxLength: 4096 }, sha256: { type: "string", pattern: "^[0-9a-f]{64}$" }, section: { type: "string", minLength: 1, maxLength: 512 } },
        required: ["path"],
        additionalProperties: false,
      },
    },
  },
  required: ["version", "kind"],
  additionalProperties: false,
} as const

export const bytes = (value: unknown) => Buffer.byteLength(typeof value === "string" ? value : JSON.stringify(value))

function reject(reason: string): never {
  throw new Error(`receipt rejected: ${reason}. Nothing was recorded or sent.`)
}

function text(value: unknown, name: string, max: number): string | undefined {
  if (value === undefined) return undefined
  if (typeof value !== "string" || !value.trim() || value.length > max) reject(`${name} must be a non-empty string of at most ${max} characters`)
  return value
}

function list<T>(value: unknown, name: string, max: number, item: (value: unknown, name: string) => T): T[] | undefined {
  if (value === undefined) return undefined
  if (!Array.isArray(value) || value.length > max) reject(`${name} must be an array of at most ${max} items`)
  return value.map((entry, index) => item(entry, `${name}[${index}]`))
}

const RECEIPT_KEYS = new Set(Object.keys(RECEIPT_INPUT.properties))

/** Runtime validation of a receipt (the tool schema alone is not trusted). Exact strings are kept, never trimmed. */
export function readReceipt(value: unknown): Receipt {
  if (!value || typeof value !== "object" || Array.isArray(value)) reject("receipt must be an object")
  const input = value as Record<string, unknown>
  for (const key of Object.keys(input)) if (!RECEIPT_KEYS.has(key)) reject(`unknown field ${key}`)
  if (input.version !== 1) reject("version must be 1")
  const id = text(input.id, "id", 128)
  if (id !== undefined && !ID_RE.test(id)) reject("id may use only letters, digits, '.', '_' and '-'")
  const ids = list(input.supersedes, "supersedes", 50, (entry, name) => {
    const ref = text(entry, name, 128)!
    if (!ID_RE.test(ref)) reject(`${name} is not a receipt ID`)
    return ref
  })
  const receipt: Receipt = {
    version: 1,
    ...(id !== undefined ? { id } : {}),
    kind: text(input.kind, "kind", 64) ?? reject("kind is required"),
  }
  for (const key of ["work_item", "revision", "job_id"] as const) {
    const field = text(input[key], key, 512)
    if (field !== undefined) receipt[key] = field
  }
  const state = text(input.state, "state", 64)
  if (state !== undefined) receipt.state = state
  if (ids) receipt.supersedes = ids
  for (const key of ["restrictions", "unresolved"] as const) {
    const field = list(input[key], key, 50, (entry, name) => text(entry, name, 4000)!)
    if (field) receipt[key] = field
  }
  const evidence = list(input.evidence, "evidence", 20, (entry, name) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) reject(`${name} must be an object`)
    const item = entry as Record<string, unknown>
    for (const key of Object.keys(item)) if (key !== "path" && key !== "sha256" && key !== "section") reject(`unknown field ${name}.${key}`)
    const sha256 = text(item.sha256, `${name}.sha256`, 64)
    if (sha256 !== undefined && !SHA_RE.test(sha256)) reject(`${name}.sha256 must be 64 lowercase hex characters`)
    const section = text(item.section, `${name}.section`, 512)
    return { path: text(item.path, `${name}.path`, 4096) ?? reject(`${name}.path is required`), ...(sha256 ? { sha256 } : {}), ...(section ? { section } : {}) }
  })
  if (evidence) receipt.evidence = evidence
  if (bytes(receipt) > MAX_EVENT_BYTES) reject(`it exceeds ${MAX_EVENT_BYTES} bytes`)
  return receipt
}

/**
 * Where a receipt goes. Only an explicit, safe, normal-priority FYI is passive; everything else, including unknown
 * kinds or states, keeps the existing delivery. This trusts the producer's labels: free text marked FYI is not checked.
 */
export function classify(receipt: Receipt, askedUrgent: boolean): { passive: boolean; routing: string } {
  if (!(RECEIPT_KINDS as readonly string[]).includes(receipt.kind)) return { passive: false, routing: `unrecognised kind "${receipt.kind}": delivered as an untyped message` }
  const state = receipt.state
  if (state !== undefined && ![...SAFE_STATES, ...ATTENTION_STATES].includes(state as never)) return { passive: false, routing: `unrecognised state "${state}": delivered as an untyped message` }
  if (receipt.kind === "actionable") return { passive: false, routing: "actionable" }
  const markers = [
    askedUrgent && 'priority "urgent"',
    (ATTENTION_STATES as readonly string[]).includes(state ?? "") && `state "${state}"`,
    receipt.restrictions?.length && "restrictions",
    receipt.unresolved?.length && "unresolved items",
  ].filter(Boolean)
  return markers.length ? { passive: false, routing: `fyi with ${markers.join(", ")}: delivered as actionable` } : { passive: true, routing: "fyi" }
}

/** The receipt's fields as plain lines for the recipient, restrictions and unresolved items exactly as given. */
export function receiptLines(receipt: Receipt): string[] {
  const head = [
    `kind ${receipt.kind}`,
    receipt.state && `state ${receipt.state}`,
    receipt.work_item && `work item ${receipt.work_item}`,
    receipt.revision && `revision ${receipt.revision}`,
    receipt.job_id && `job ${receipt.job_id}`,
    receipt.id && `receipt ${receipt.id}`,
  ].filter(Boolean)
  const section = (title: string, items: string[] | undefined) => (items?.length ? [title, ...items.map((item) => `- ${item}`)] : [])
  return [
    `[receipt: ${head.join(" · ")}]`,
    ...section("Restrictions (exact):", receipt.restrictions),
    ...section("Unresolved:", receipt.unresolved),
    ...section("Evidence:", receipt.evidence?.map((item) => [item.path, item.sha256 && `sha256 ${item.sha256}`, item.section && `section ${item.section}`].filter(Boolean).join(", "))),
    ...section("Supersedes:", receipt.supersedes),
  ]
}

export const withReceipt = (text: string, receipt?: Receipt) => (receipt ? `${text}\n\n${receiptLines(receipt).join("\n")}` : text)

export interface PeerEvent {
  v: 1
  /** Recipient-facing ID: `<sender session>:<receipt or generated ID>`. */
  ref: string
  id: string
  to: string
  from: string
  fromTitle?: string
  text: string
  at: number
  /** Stored without any automatic delivery. */
  passive: boolean
  receipt?: Receipt
  recipientIsClaudeCode?: boolean
  /** Imported from the old `digest:held:` store. */
  legacy?: boolean
  /** Delivered on its own (immediately or through the parent relay), never by a digest. */
  direct?: boolean
  /**
   * Supersession decided when this original was stored: one immutable record, so it applies to all named or none.
   * `refs` are the predecessors this decision archived (those no earlier superseder had), so restore derives the same view.
   */
  supersession?: { applied: boolean; reason?: string; refs?: string[] }
  /** SHA-256 of the canonical payload, for idempotent retries. */
  hash: string
}

export type Disposition = {
  version: number
  status?: "processed" | "archived"
  reason?: string
  superseded_by?: string
  read_at?: number
  at?: number
  /** A parent-relay original released into the recipient's digest (not a recipient change, so no new version). */
  released_at?: number
}

/** `withdrawn`: known never attempted, because the delivery policy changed before admission; its originals wait again. */
export type BatchState = "prepared" | "admission_unknown" | "admitted" | "cancelled" | "failed_before_admission" | "withdrawn"

/**
 * The caller's delivery policy at the admission boundary: `gate` serialises it, `frame` is the policy a new batch is
 * framed under (stored in its manifest), and `allowed` is checked before reusing a stored batch and right before the prompt.
 */
export interface AdmissionPolicy {
  readonly gate: <T>(run: () => Promise<T>) => Promise<T>
  readonly frame: string
  readonly allowed: (batch: Batch) => boolean
}

export interface Batch {
  id: string
  to: string
  /** Stable native message ID given to session.prompt; a retry reuses it. */
  prompt_id: string
  refs: string[]
  /** Too large for any digest: named in this batch for explicit retrieval, not delivered. */
  oversize: string[]
  text: string
  sha256: string
  metadata: Record<string, unknown>
  delivery?: "steer" | "queue"
  /** The delivery policy its text was framed under; a manifest without one (older records) never matches a current policy. */
  frame?: string
  state: BatchState
  created: number
  updated: number
  delivered_at?: number
  error?: string
}

interface Entry {
  event: PeerEvent
  disposition?: Disposition
  batch?: string
  /** Derived from a later original's applied supersession; an explicit disposition takes precedence. */
  supersededBy?: string
}

export interface InboxRegistry {
  entries: Map<string, Map<string, Entry>>
  batches: Map<string, Map<string, Batch>>
  locks: Map<string, Promise<unknown>>
  /** True once every original, disposition and manifest has been read back. Until then nothing is delivered automatically and recipient views are refused. */
  loaded: boolean
  /** Restore progress kept between bounded chunks. */
  scan?: { phase: number; after?: string; raw: Array<Array<{ key: string; value: unknown }>> }
}

/** A standalone inbox (nothing to restore) is loaded from the start; the digest's shared one starts unloaded. */
export const freshInbox = (loaded = true): InboxRegistry => ({ entries: new Map(), batches: new Map(), locks: new Map(), loaded })

export interface InboxStore {
  get(key: string): Promise<unknown>
  set(key: string, value: unknown): Promise<unknown>
}

export const eventKey = (to: string, ref: string) => `${EVENT_PREFIX}${to}:${ref}`
export const stateKey = (to: string, ref: string) => `${STATE_PREFIX}${to}:${ref}`
export const batchKey = (to: string, id: string) => `${BATCH_PREFIX}${to}:${id}`

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex")
const message = (error: unknown) => (error instanceof Error ? error.message : String(error))

let counter = 0
export const newID = (now: number) => `${now.toString(36).padStart(9, "0")}-${(counter++ % 1_679_616).toString(36).padStart(4, "0")}${Math.random().toString(36).slice(2, 6)}`

const CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
/** A native message ID in core's own `msg_` format (12 hex time digits, 14 random characters). */
export function promptID(now: number): string {
  const time = (BigInt(now) * 0x1000n + BigInt(counter++ % 0x1000)).toString(16).padStart(12, "0").slice(-12)
  return `msg_${time}${Array.from(crypto.getRandomValues(new Uint8Array(14)), (byte) => CHARS[byte % 62]).join("")}`
}

const byAge = (a: Entry, b: Entry) => a.event.at - b.event.at || (a.event.ref < b.event.ref ? -1 : a.event.ref > b.event.ref ? 1 : 0)

export interface PromptApi {
  get(input: { sessionID: string }): Promise<unknown>
  prompt(input: { sessionID: string; id?: string; text: string; delivery?: "queue" | "steer"; metadata?: Record<string, unknown> }): Promise<unknown>
}

export class PeerInbox {
  constructor(
    readonly registry: InboxRegistry,
    private readonly store: InboxStore,
    private readonly ready: () => Promise<void>,
    private readonly log: (message: string) => void = () => {},
    private readonly now: () => number = Date.now,
  ) {}

  private entries(to: string) {
    let map = this.registry.entries.get(to)
    if (!map) this.registry.entries.set(to, (map = new Map()))
    return map
  }

  private batches(to: string) {
    let map = this.registry.batches.get(to)
    if (!map) this.registry.batches.set(to, (map = new Map()))
    return map
  }

  /** Serialises this recipient's mutations within the process (not across processes). */
  private async locked<T>(to: string, run: () => Promise<T>): Promise<T> {
    const previous = this.registry.locks.get(to) ?? Promise.resolve()
    const result = previous.then(run, run)
    const tail = result.catch(() => {})
    this.registry.locks.set(to, tail)
    try {
      return await result
    } finally {
      if (this.registry.locks.get(to) === tail) this.registry.locks.delete(to)
    }
  }

  /** Records an original durably. Throws (nothing recorded) when it cannot be stored; never reports a swallowed failure. */
  async append(input: {
    to: string
    from: string
    fromTitle?: string
    text: string
    passive: boolean
    receipt?: Receipt
    recipientIsClaudeCode?: boolean
    direct?: boolean
  }): Promise<{ event: PeerEvent; duplicate: boolean; supersession?: { applied: boolean; reason?: string; archived: string[] } }> {
    try {
      await this.ready()
    } catch (error) {
      throw new Error(`peer inbox storage is unavailable, so the message was not recorded: ${message(error)}`)
    }
    const at = this.now()
    return this.write({ ...input, id: input.receipt?.id ?? newID(at), at })
  }

  /** Imports an old `digest:held:` peer message under its own ID (idempotent). Used during restore. */
  importLegacy(item: { to: string; from: string; fromTitle?: string; text: string; at: number; id: string; recipientIsClaudeCode?: boolean }) {
    return this.write({ ...item, passive: false, legacy: true })
  }

  private async write(input: {
    to: string
    from: string
    fromTitle?: string
    text: string
    passive: boolean
    receipt?: Receipt
    recipientIsClaudeCode?: boolean
    direct?: boolean
    id: string
    at: number
    legacy?: boolean
  }) {
    const { to, from, id } = input
    const ref = `${from}:${id}`
    const hash = sha256(JSON.stringify([to, from, input.text, input.passive, input.receipt ?? null]))
    const key = eventKey(to, ref)
    return this.locked(to, async () => {
      let stored = this.entries(to).get(ref)?.event as PeerEvent | undefined
      if (!stored) {
        try {
          stored = (await this.store.get(key)) as PeerEvent | undefined
        } catch (error) {
          throw new Error(`peer inbox storage could not be read, so the message was not recorded: ${message(error)}`)
        }
      }
      if (stored) {
        if (stored.hash !== hash) throw new Error(`receipt id "${id}" was already used for different content to this recipient; nothing was recorded or sent.`)
        if (!this.entries(to).has(ref)) this.entries(to).set(ref, { event: stored })
        return { event: stored, duplicate: true }
      }
      const receipt = input.receipt
      const supersession = input.passive && receipt?.supersedes?.length ? this.supersession(to, from, ref, receipt) : undefined
      const event: PeerEvent = {
        v: 1,
        ref,
        id,
        to,
        from,
        ...(input.fromTitle ? { fromTitle: input.fromTitle } : {}),
        text: input.text,
        at: input.at,
        passive: input.passive,
        ...(receipt ? { receipt } : {}),
        ...(input.recipientIsClaudeCode ? { recipientIsClaudeCode: true } : {}),
        ...(input.legacy ? { legacy: true } : {}),
        ...(input.direct ? { direct: true } : {}),
        ...(supersession ? { supersession } : {}),
        hash,
      }
      if (!input.legacy && bytes(event) > MAX_EVENT_BYTES) throw new Error(`message and receipt exceed ${MAX_EVENT_BYTES} bytes; nothing was recorded or sent.`)
      try {
        await this.store.set(key, event)
      } catch (error) {
        // The write may have landed: reconcile by reading the same key back, never by assuming either way.
        const after = (await this.store.get(key).catch(() => undefined)) as PeerEvent | undefined
        if (after?.hash !== hash) throw new Error(`peer inbox storage write failed, so the message was not recorded: ${message(error)}`)
      }
      this.entries(to).set(ref, { event })
      const archived = supersession?.applied ? this.applySupersession(event) : []
      return { event, duplicate: false, ...(supersession ? { supersession: { applied: supersession.applied, ...(supersession.reason ? { reason: supersession.reason } : {}), archived } } : {}) }
    })
  }

  /** Whether every explicitly named earlier safe FYI of the same sender/work/revision/job may be superseded. Called locked; writes nothing. */
  private supersession(to: string, from: string, ref: string, receipt: Receipt): { applied: boolean; reason?: string; refs?: string[] } {
    if (!receipt.work_item || !receipt.revision) return { applied: false, reason: "supersession needs work_item and revision" }
    for (const id of receipt.supersedes!) {
      const target = this.entries(to).get(`${from}:${id}`)
      const other = target?.event.receipt
      const why = `${from}:${id}` === ref
        ? `${id} is this receipt`
        : !target
          ? `${id} is not an earlier receipt of yours to this recipient`
          : !target.event.passive || !other
            ? `${id} is not a passive FYI`
            : other.kind !== receipt.kind || other.work_item !== receipt.work_item || other.revision !== receipt.revision || other.job_id !== receipt.job_id
              ? `${id} has a different kind, work item, revision or job`
              : undefined
      if (why) return { applied: false, reason: why }
    }
    // First superseder wins: only predecessors not already superseded are recorded as this decision's.
    return { applied: true, refs: [...new Set(receipt.supersedes!.map((id) => `${from}:${id}`))].filter((target) => !this.entries(to).get(target)!.supersededBy) }
  }

  /** Derives the archived view from an applied decision's recorded predecessors (independent of restore order). Returns those newly archived. */
  private applySupersession(event: PeerEvent): string[] {
    const archived: string[] = []
    for (const ref of event.supersession?.refs ?? []) {
      const target = this.registry.entries.get(event.to)?.get(ref)
      if (!target || target.supersededBy) continue
      target.supersededBy = event.ref
      if (!target.disposition?.status) archived.push(target.event.ref)
    }
    return archived
  }

  /** What the recipient sees: pending until a native admission is known, then offered; processed/archived only explicitly or by supersession. */
  private view(entry: Entry) {
    const batch = entry.batch ? this.batches(entry.event.to).get(entry.batch) : undefined
    const status: "pending" | "offered" | "processed" | "archived" = entry.disposition?.status ?? (entry.supersededBy ? "archived" : batch?.state === "admitted" && !batch.oversize.includes(entry.event.ref) ? "offered" : "pending")
    const delivery = batch ? (batch.oversize.includes(entry.event.ref) ? "retrieval_required" : batch.state) : undefined
    return { status, delivery, batch }
  }

  /** Held, actionable entries an automatic digest may still deliver: never passive or direct ones, and nothing before recovery completes. */
  waiting(to: string): Entry[] {
    const out: Entry[] = []
    if (!this.registry.loaded) return out
    for (const entry of this.registry.entries.get(to)?.values() ?? []) {
      if (entry.event.passive || (entry.event.direct && !entry.disposition?.released_at) || entry.disposition?.status) continue
      const batch = entry.batch ? this.batches(to).get(entry.batch) : undefined
      if (!batch || batch.state === "failed_before_admission" || batch.state === "withdrawn") out.push(entry)
    }
    return out.sort(byAge)
  }

  /** Recipients with something an automatic digest may deliver. */
  recipients(): string[] {
    return [...this.registry.entries.keys()].filter((to) => this.waiting(to).length > 0)
  }

  lastBatchAt(to: string): number | undefined {
    let last: number | undefined
    for (const batch of this.registry.batches.get(to)?.values() ?? []) last = Math.max(last ?? 0, batch.created)
    return last
  }

  /**
   * Delivers at most one bounded batch: whole events only, oldest first, at most MAX_BATCH_EVENTS and MAX_BATCH_BYTES
   * for the whole prompt (text, metadata and framing). The manifest is stored before native admission with a stable
   * prompt ID, and a retry is durably fenced again first. A failure that may have been admitted is recorded as unknown
   * and never retried automatically. The caller claims the recipient synchronously first.
   */
  async deliver(
    to: string,
    api: PromptApi,
    render: (events: PeerEvent[], remaining: number, oversize: PeerEvent[]) => { text: string; metadata: Record<string, unknown> },
    policy?: AdmissionPolicy,
  ): Promise<{ batch?: Batch; delivered: number }> {
    const batch = await this.locked(to, async () => {
      let waiting = this.waiting(to)
      for (;;) {
        const retry = waiting.map((entry) => (entry.batch ? this.batches(to).get(entry.batch) : undefined)).find((batch) => batch?.state === "failed_before_admission")
        if (!retry) break
        if (!policy || policy.allowed(retry)) return this.fence(retry)
        // Framed under another policy: durably withdrawn before its originals are framed anew, else they stay fenced.
        if (!(await this.withdraw(retry, "the delivery policy that framed it changed before admission; not sent"))) return undefined
        waiting = this.waiting(to)
      }
      if (!waiting.length) return undefined
      // Measures the prompt as admitted: its stable ID has a fixed length.
      const size = (rendered: { text: string; metadata: Record<string, unknown> }) =>
        bytes({ sessionID: to, id: promptID(0), text: rendered.text, delivery: "steer", metadata: rendered.metadata })
      const chosen: PeerEvent[] = []
      let oversize: PeerEvent[] = []
      for (const entry of waiting) {
        if (chosen.length >= MAX_BATCH_EVENTS) break
        if (size(render([...chosen, entry.event], waiting.length - chosen.length - 1, [])) > MAX_BATCH_BYTES) break
        chosen.push(entry.event)
      }
      if (!chosen.length) oversize = [waiting[0]!.event]
      const { text, metadata } = render(chosen, waiting.length - chosen.length - oversize.length, oversize)
      return this.reserve(to, [...chosen, ...oversize], oversize, text, metadata, "steer", policy?.frame)
    })
    if (!batch) return { delivered: 0 }
    const state = await this.admit(batch, api, policy)
    return { batch, delivered: state === "admitted" ? batch.refs.length - batch.oversize.length : 0 }
  }

  /**
   * Delivers one stored original on its own, outside any digest (immediate or parent-relay delivery), under a durable
   * manifest with a stable native ID. Once an original has a manifest it is never attempted again: `duplicate`.
   */
  async direct(event: PeerEvent, api: PromptApi, text: string, metadata: Record<string, unknown>, delivery: "steer" | "queue"): Promise<{ batch?: Batch; state?: BatchState; duplicate?: boolean }> {
    // Its manifest may not be read back yet: never risk a second attempt under a new ID.
    if (!this.registry.loaded) await this.ready()
    if (!this.registry.loaded) throw new Error(`peer inbox recovery is still in progress, so ${event.ref} was recorded but not sent; resend the same receipt shortly.`)
    const reserved = await this.locked(event.to, async () => {
      const entry = this.entries(event.to).get(event.ref)
      if (!entry) throw new Error(`${event.ref} is not recorded; nothing was sent.`)
      if (entry.batch) return { batch: this.batches(event.to).get(entry.batch), duplicate: true }
      return { batch: await this.reserve(event.to, [event], [], text, metadata, delivery) }
    })
    if (reserved.duplicate || !reserved.batch) return reserved
    return { batch: reserved.batch, state: await this.admit(reserved.batch, api) }
  }

  /**
   * The parent relay chose digest delivery for a held original: durably marks it eligible for the recipient's digest.
   * Until then (or after a crash before it) a relay original is never delivered automatically. Throws when not recorded.
   */
  async release(event: PeerEvent): Promise<void> {
    await this.locked(event.to, async () => {
      const entry = this.entries(event.to).get(event.ref)
      if (!entry) throw new Error(`${event.ref} is not recorded`)
      if (entry.batch || entry.disposition?.released_at) return
      const next: Disposition = { ...entry.disposition, version: entry.disposition?.version ?? 0, released_at: this.now() }
      await this.store.set(stateKey(event.to, event.ref), next)
      entry.disposition = next
    })
  }

  /** The recipient's current view of one stored original, without its text (for a sender's duplicate receipt); undefined while unknown. */
  current(to: string, ref: string) {
    const entry = this.registry.loaded ? this.registry.entries.get(to)?.get(ref) : undefined
    if (!entry) return undefined
    const { status, version, read_at, delivery } = this.header(entry)
    return { status, version, ...(read_at ? { read_at } : {}), ...(delivery ? { delivery } : {}) }
  }

  /** Stores a new manifest durably before anything can be sent; undefined (nothing sent) when it cannot be stored. Called locked. */
  private async reserve(to: string, events: PeerEvent[], oversize: PeerEvent[], text: string, metadata: Record<string, unknown>, delivery: "steer" | "queue", frame?: string) {
    const at = this.now()
    const id = newID(at)
    const next: Batch = {
      id,
      to,
      prompt_id: promptID(at),
      refs: events.map((event) => event.ref),
      oversize: oversize.map((event) => event.ref),
      text,
      sha256: sha256(text),
      metadata,
      delivery,
      ...(frame === undefined ? {} : { frame }),
      state: "prepared",
      created: at,
      updated: at,
    }
    try {
      await this.store.set(batchKey(to, id), next)
    } catch (error) {
      this.log(`${to}: could not store a delivery manifest, nothing sent: ${message(error)}`)
      return undefined
    }
    this.batches(to).set(id, next)
    for (const ref of next.refs) this.entries(to).get(ref)!.batch = id
    return next
  }

  /** A retry of a known failure before admission: durably marked prepared again first, so a crash or lost response reads as unknown, never as retryable. Called locked. */
  private async fence(batch: Batch) {
    const fenced: Batch = { ...batch, state: "prepared", updated: this.now() }
    try {
      await this.store.set(batchKey(batch.to, batch.id), fenced)
    } catch (error) {
      this.log(`${batch.to}: digest ${batch.id} retry not fenced, so not attempted: ${message(error)}`)
      return undefined
    }
    Object.assign(batch, fenced)
    return batch
  }

  /**
   * One native admission attempt for a prepared manifest, with its stable ID. Under a policy, the lookup and admission run
   * inside its gate and the policy is checked after the last await, immediately before the prompt: a batch it no longer
   * allows is withdrawn, never sent, and its originals wait for a new batch framed under the current policy.
   */
  private async admit(batch: Batch, api: PromptApi, policy?: AdmissionPolicy): Promise<BatchState> {
    const to = batch.to
    const attempt = async (): Promise<{ state: BatchState; error?: string }> => {
      const target = await api.get({ sessionID: to }).catch(() => undefined)
      if (!target) return { state: "failed_before_admission", error: "recipient session not found; not sent" }
      if (policy && !policy.allowed(batch)) return { state: "withdrawn", error: "the delivery policy changed before admission; not sent" }
      try {
        await api.prompt({ sessionID: to, id: batch.prompt_id, text: batch.text, delivery: batch.delivery ?? "steer", metadata: batch.metadata })
        return { state: "admitted" }
      } catch (failure) {
        return { state: "admission_unknown", error: message(failure) }
      }
    }
    const { state, error } = policy ? await policy.gate(attempt) : await attempt()
    // An unconfirmed withdrawal leaves the batch prepared and fenced; `withdraw` logged why.
    if (state === "withdrawn" && !(await this.locked(to, () => this.withdraw(batch, error!)))) return "prepared"
    if (state !== "withdrawn") await this.update(batch, (current) => (current === "prepared" ? state : current), error)
    const after = state === "withdrawn" ? "; its originals wait for a new batch" : `; no automatic resend${state === "failed_before_admission" ? " until the next interval, with the same ID" : ""}`
    if (state !== "admitted") this.log(`${to}: delivery ${batch.id} ${state}${error ? `: ${error}` : ""}${after}`)
    return state
  }

  /**
   * Durable evidence that a known-unsent batch was never attempted, required before its originals can be framed again.
   * A write that throws may still have landed, so it is read back exactly; unconfirmed, the batch keeps its old state
   * and binding (fenced: never reused, never relabelled unsent). Called locked.
   */
  private async withdraw(batch: Batch, error: string): Promise<boolean> {
    if (batch.state !== "prepared" && batch.state !== "failed_before_admission") return false
    const staged: Batch = { ...batch, state: "withdrawn", updated: this.now(), error }
    const key = batchKey(batch.to, batch.id)
    const confirmed = await this.store.set(key, staged).then(
      () => true,
      async (failure) => {
        const stored = await this.store.get(key).catch(() => undefined)
        if (JSON.stringify(stored) === JSON.stringify(staged)) return true
        this.log(`${batch.to}: digest ${batch.id} withdrawal not confirmed, so its originals stay fenced: ${message(failure)}`)
        return false
      },
    )
    if (confirmed) Object.assign(batch, staged)
    return confirmed
  }

  private async update(batch: Batch, next: (current: BatchState) => BatchState, error?: string, extra: Partial<Batch> = {}) {
    await this.locked(batch.to, async () => {
      const state = next(batch.state)
      if (state === batch.state && !Object.keys(extra).length) return
      Object.assign(batch, extra, { state, updated: this.now() }, error ? { error } : {})
      // The in-memory state is already the truth for this process; a failed write leaves an older durable state, which
      // restore treats conservatively (prepared reads as unknown).
      await this.store.set(batchKey(batch.to, batch.id), batch).catch((failure) => this.log(`${batch.to}: digest ${batch.id} state not persisted: ${message(failure)}`))
    })
  }

  /** Authoritative native inbox evidence for an exact prompt ID and recipient. Returns whether it matched a batch. */
  async nativeSettled(sessionID: string, inboxID: string, kind: "delivered" | "cancelled"): Promise<boolean> {
    const batch = [...(this.registry.batches.get(sessionID)?.values() ?? [])].find((item) => item.prompt_id === inboxID)
    if (!batch) return false
    if (kind === "delivered") await this.update(batch, () => "admitted", undefined, { delivered_at: batch.delivered_at ?? this.now() })
    else if (!batch.delivered_at) await this.update(batch, () => "cancelled")
    return true
  }

  /**
   * Restores originals, dispositions and manifests, at most `pages` scan pages per call; a later call continues where
   * this one stopped (also after a failed page). Until every record is read, `loaded` stays false: no automatic
   * delivery, and recipient views are refused rather than shown incomplete. Returns whether recovery is complete.
   */
  async load(scan: (prefix: string, after?: string) => Promise<{ entries: Array<{ key: string; value: unknown }>; next?: string }>, pages = 100): Promise<boolean> {
    if (this.registry.loaded) return true
    const prefixes = [EVENT_PREFIX, STATE_PREFIX, BATCH_PREFIX]
    const progress = (this.registry.scan ??= { phase: 0, raw: prefixes.map(() => []) })
    for (let page = 0; page < pages && progress.phase < prefixes.length; page++) {
      const result = await scan(prefixes[progress.phase]!, progress.after)
      // A cursor that does not advance would never finish: fail recovery rather than loop or pass it off as complete.
      if (result.next && progress.after && result.next <= progress.after) throw new Error(`storage scan of ${prefixes[progress.phase]} did not advance`)
      progress.raw[progress.phase]!.push(...result.entries)
      progress.after = result.next || undefined
      if (!progress.after) progress.phase += 1
    }
    if (progress.phase < prefixes.length) return false
    const [events, states, batches] = progress.raw as [Array<{ key: string; value: unknown }>, Array<{ key: string; value: unknown }>, Array<{ key: string; value: unknown }>]
    const restored: PeerEvent[] = []
    for (const { value } of events) {
      const event = value as PeerEvent
      if (!event?.ref || !event.to || typeof event.text !== "string" || typeof event.hash !== "string") continue
      if (!this.entries(event.to).has(event.ref)) this.entries(event.to).set(event.ref, { event })
      if (event.supersession?.applied) restored.push(event)
    }
    for (const event of restored) this.applySupersession(event)
    for (const { key, value } of states) {
      const rest = key.slice(STATE_PREFIX.length)
      const to = rest.slice(0, rest.indexOf(":"))
      const entry = this.registry.entries.get(to)?.get(rest.slice(to.length + 1))
      const disposition = value as Disposition
      if (entry && typeof disposition?.version === "number" && (entry.disposition?.version ?? -1) < disposition.version) entry.disposition = disposition
    }
    for (const { value } of batches) {
      const batch = value as Batch
      if (!batch?.id || !batch.to || !Array.isArray(batch.refs) || this.batches(batch.to).has(batch.id)) continue
      batch.oversize ??= []
      // A crash between storing the manifest and learning the admission outcome leaves it unknown, not retryable.
      if (batch.state === "prepared") batch.state = "admission_unknown"
      this.batches(batch.to).set(batch.id, batch)
      for (const ref of batch.refs) {
        const entry = this.registry.entries.get(batch.to)?.get(ref)
        // A withdrawn batch never claims an original another batch already holds.
        if (entry && !(batch.state === "withdrawn" && entry.batch)) entry.batch = batch.id
      }
    }
    this.registry.scan = undefined
    this.registry.loaded = true
    return true
  }

  // ---- Recipient operations (always the calling session's own inbox) ----

  /** The version an ack must name: an archiving supersession counts as a change, so an ack from an older view conflicts. */
  private version(entry: Entry) {
    return (entry.disposition?.version ?? 0) + (entry.supersededBy && !entry.disposition?.status ? 1 : 0)
  }

  private header(entry: Entry) {
    const { status, delivery, batch } = this.view(entry)
    const receipt = entry.event.receipt
    return {
      id: entry.event.ref,
      from: entry.event.from,
      ...(entry.event.fromTitle ? { from_title: entry.event.fromTitle.slice(0, MAX_HEADER_TITLE) } : {}),
      at: new Date(entry.event.at).toISOString(),
      status,
      version: this.version(entry),
      passive: entry.event.passive,
      ...(receipt ? { kind: receipt.kind, state: receipt.state, work_item: receipt.work_item, revision: receipt.revision, job_id: receipt.job_id } : { kind: "untyped" }),
      ...(receipt?.restrictions?.length ? { restrictions: receipt.restrictions.length } : {}),
      ...(receipt?.unresolved?.length ? { unresolved: receipt.unresolved.length } : {}),
      ...(delivery ? { delivery, batch: batch!.id } : {}),
      ...(entry.disposition?.reason ? { reason: entry.disposition.reason } : !entry.disposition?.status && entry.supersededBy ? { reason: "superseded" } : {}),
      ...(!entry.disposition?.status && entry.supersededBy ? { superseded_by: entry.supersededBy } : {}),
      ...(entry.disposition?.read_at ? { read_at: new Date(entry.disposition.read_at).toISOString() } : {}),
      bytes: bytes(entry.event.text),
    }
  }

  /** Completes (or continues) recovery for a recipient operation; returns why the inbox is not yet usable, if it is not. */
  async recover(): Promise<string | undefined> {
    try {
      await this.ready()
    } catch (error) {
      return `recovery from storage failed: ${message(error)}`
    }
    return this.registry.loaded ? undefined : "recovery from storage is still in progress"
  }

  private assertLoaded() {
    if (!this.registry.loaded) throw new Error("peer inbox recovery from storage is incomplete, so nothing was read or changed; retry shortly.")
  }

  status(to: string, problem?: string) {
    // Never shown as an empty, complete inbox when its records have not all been read back.
    if (!this.registry.loaded) return { session_id: to, complete: false, note: `Counts unknown: ${problem ?? "recovery from storage is incomplete"}. Retry shortly.` }
    const counts = { pending: 0, offered: 0, processed: 0, archived: 0 }
    let passivePending = 0
    let actionablePending = 0
    let unread = 0
    let stored = 0
    for (const entry of this.registry.entries.get(to)?.values() ?? []) {
      const { status } = this.view(entry)
      counts[status] += 1
      if (status === "pending") entry.event.passive ? (passivePending += 1) : (actionablePending += 1)
      if (!entry.disposition?.read_at && (status === "pending" || status === "offered")) unread += 1
      stored += bytes(entry.event)
    }
    const attempts: Record<string, number> = {}
    for (const batch of this.registry.batches.get(to)?.values() ?? []) if (batch.state !== "admitted") attempts[batch.state] = (attempts[batch.state] ?? 0) + 1
    return {
      session_id: to,
      ...counts,
      passive_pending: passivePending,
      actionable_pending: actionablePending,
      unread,
      ...(Object.keys(attempts).length ? { delivery_attempts_needing_attention: attempts } : {}),
      stored_bytes: stored,
      complete: true,
    }
  }

  list(to: string, input: { cursor?: string; limit?: number; status?: string }) {
    this.assertLoaded()
    const all = [...(this.registry.entries.get(to)?.values() ?? [])].sort(byAge).filter((entry) => !input.status || this.view(entry).status === input.status)
    const start = input.cursor ? all.findIndex((entry) => entry.event.ref === input.cursor) + 1 : 0
    if (input.cursor && start === 0) throw new Error(`unknown cursor ${input.cursor}`)
    const page = all.slice(start, start + Math.min(input.limit ?? MAX_PAGE, MAX_PAGE))
    const more = start + page.length < all.length
    return {
      items: page.map((entry) => this.header(entry)),
      ...(more ? { next_cursor: page.at(-1)!.event.ref, remaining: all.length - start - page.length } : {}),
      complete: true,
    }
  }

  /** Exact originals, whole entries only; the whole result, with its descriptors, is at most MAX_BATCH_BYTES. Reading records read_at; it is not processing. */
  async read(to: string, ids: string[]) {
    this.assertLoaded()
    const chosen: Array<{ id: string; entry: Entry; item: unknown }> = []
    const missing: string[] = []
    const remaining: string[] = []
    const result = () => {
      const items = chosen.map((choice) => choice.item)
      return { items, ...(missing.length ? { missing } : {}), ...(remaining.length ? { remaining, note: "Read the remaining IDs in another call." } : {}) }
    }
    for (const id of ids) {
      const entry = this.registry.entries.get(to)?.get(id)
      if (!entry) {
        missing.push(id)
        continue
      }
      if (remaining.length) {
        remaining.push(id)
        continue
      }
      chosen.push({ id, entry, item: { ...this.header(entry), text: entry.event.text, ...(entry.event.receipt ? { receipt: entry.event.receipt } : {}) } })
      if (bytes(result()) > MAX_BATCH_BYTES) remaining.push(chosen.pop()!.id)
    }
    // Later descriptors count too: give back whole entries, last first, until the complete result fits.
    while (chosen.length && bytes(result()) > MAX_BATCH_BYTES) remaining.unshift(chosen.pop()!.id)
    for (const { id, entry } of chosen) {
      if (entry.disposition?.read_at) continue
      await this.locked(to, async () => {
        const next = { ...entry.disposition, version: entry.disposition?.version ?? 0, read_at: this.now() }
        await this.store.set(stateKey(to, id), next).then(
          () => (entry.disposition = next),
          (error) => this.log(`${to}: read time for ${id} not recorded: ${message(error)}`),
        )
      })
    }
    return result()
  }

  /** Explicit, versioned dispositions, applied per item (no multi-record transaction). Never sends or wakes anything. */
  async ack(to: string, items: Array<{ id: string; expected_version: number }>, disposition: "processed" | "archive") {
    this.assertLoaded()
    const status = disposition === "archive" ? "archived" : "processed"
    const results: unknown[] = []
    for (const item of items) {
      results.push(
        await this.locked(to, async () => {
          const entry = this.registry.entries.get(to)?.get(item.id)
          if (!entry) return { id: item.id, ok: false, error: "not found" }
          const version = this.version(entry)
          if (entry.disposition?.status === status) return { id: item.id, ok: true, status, version, unchanged: true }
          if (version !== item.expected_version) return { ok: false, conflict: true, ...this.header(entry) }
          const next: Disposition = { ...entry.disposition, version: version + 1, status, reason: disposition === "archive" ? "archived by recipient" : "processed by recipient", at: this.now() }
          try {
            await this.store.set(stateKey(to, item.id), next)
          } catch (error) {
            return { id: item.id, ok: false, error: `not recorded: ${message(error)}` }
          }
          entry.disposition = next
          return { id: item.id, ok: true, status, version: next.version }
        }),
      )
    }
    return { results }
  }
}
