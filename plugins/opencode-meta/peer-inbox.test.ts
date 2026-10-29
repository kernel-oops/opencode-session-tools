import { afterEach, describe, expect, test } from "bun:test"
import plugin from "./index.ts"
import { Digest, freshRegistry, REGISTRY_KEY, sharedRegistry } from "./digest.ts"
import { digestDivert, peerInboxOperation, PEER_INBOX_INPUT } from "./lib.ts"
import { classify, EVENT_PREFIX, MAX_BATCH_BYTES, MAX_EVENT_BYTES, PeerInbox, readReceipt, STATE_PREFIX, freshInbox, type Receipt } from "./peer-inbox.ts"
import { ParentRelay } from "./relay.ts"
import { sendMessage, SEND_INPUT } from "./send.ts"
import { makeMockCtx, sessionInfo } from "../opencode-recall-lite/mock-ctx.ts"

const fyi = (extra: Partial<Receipt> = {}): Receipt => ({ version: 1, kind: "fyi", work_item: "PROJ-1", revision: "abc123", state: "progress", ...extra })

function memoryStorage() {
  const store = new Map<string, unknown>()
  return {
    store,
    get: async (key: string) => store.get(key),
    set: async (key: string, value: unknown) => void store.set(key, JSON.parse(JSON.stringify(value))),
    remove: async (key: string) => void store.delete(key),
    scan: async ({ prefix, after, limit }: { prefix: string; after?: string; limit: number }) => {
      const keys = [...store.keys()].filter((key) => key.startsWith(prefix) && (!after || key > after)).sort()
      const page = keys.slice(0, limit)
      return { entries: page.map((key) => ({ key, value: store.get(key) })), next: keys.length > limit ? page.at(-1) : undefined }
    },
  }
}

/** Direct fixture: controller ses_ctl, workers, a child of the controller; counts every session API call but get. */
function world(options: { storage?: ReturnType<typeof memoryStorage>; idle?: boolean } = {}) {
  let now = 1_000_000_000
  const calls: string[] = []
  const prompts: any[] = []
  const known: Record<string, any> = {
    ses_ctl: { id: "ses_ctl", title: "Controller" },
    ses_w1: { id: "ses_w1", title: "Worker one" },
    ses_w2: { id: "ses_w2", title: "Worker two" },
    ses_kid: { id: "ses_kid", title: "Child", parentID: "ses_ctl" },
  }
  const idle = options.idle ?? true
  const sessions = {
    get: async ({ sessionID }: { sessionID: string }) =>
      known[sessionID] && { ...known[sessionID], time: idle ? { idle: 5, updated: 5 } : { idle: 5, updated: 9 } },
    prompt: async (input: any) => {
      calls.push("prompt")
      prompts.push(input)
    },
  }
  const storage = options.storage ?? memoryStorage()
  const registry = freshRegistry()
  const digest = new Digest(registry, sessions, storage, () => {}, () => now)
  const relay = new ParentRelay(sessions, { graceMs: 1 })
  const wake = { digest, sleep: async () => {}, intervalMs: 1, timeoutMs: 1 }
  const send = (from: string, text: string, extra: Record<string, unknown> = {}, to = "ses_ctl") =>
    sendMessage(storage, sessions, { to, text, ...extra } as any, { sessionID: from }, relay, wake) as Promise<any>
  const op = (input: Record<string, unknown>, sessionID = "ses_ctl") => peerInboxOperation(digest.inbox, input, { sessionID }) as Promise<any>
  return { calls, prompts, sessions, storage, registry, digest, relay, send, op, known, advance: (ms: number) => (now += ms) }
}

const eventKeys = (storage: ReturnType<typeof memoryStorage>) => [...storage.store.keys()].filter((key) => key.startsWith(EVENT_PREFIX))

describe("receipt contract", () => {
  test("send_message exposes the optional receipt in its schema; old callers need nothing new", () => {
    expect(SEND_INPUT.required).toEqual(["to", "text"])
    expect(SEND_INPUT.properties.receipt.required).toEqual(["version", "kind"])
    expect(SEND_INPUT.properties.receipt.description).toContain("without waking it")
  })

  test("runtime validation rejects malformed or oversize receipts, saying nothing was recorded or sent", async () => {
    const w = world()
    for (const bad of [
      "fyi",
      { version: 2, kind: "fyi" },
      { version: 1 },
      { version: 1, kind: "fyi", extra: true },
      { version: 1, kind: "fyi", id: "has space" },
      { version: 1, kind: "fyi", restrictions: [""] },
      { version: 1, kind: "fyi", evidence: [{ path: "/x", sha256: "nothex" }] },
      { version: 1, kind: "fyi", supersedes: ["a:b"] },
      { version: 1, kind: "fyi", unresolved: Array.from({ length: 50 }, () => "é".repeat(4000)) },
    ]) {
      await expect(w.send("ses_w1", "hello", { receipt: bad })).rejects.toThrow("Nothing was recorded or sent")
    }
    expect(w.prompts).toHaveLength(0)
    expect(eventKeys(w.storage)).toHaveLength(0)
  })

  test("a whole event over 40 KiB is refused, not clipped", async () => {
    const w = world()
    // Valid text and a valid receipt that together exceed the event bound.
    const receipt = fyi({ evidence: Array.from({ length: 20 }, (_, i) => ({ path: `/p/${i}/${"x".repeat(900)}` })) })
    expect(() => readReceipt(receipt)).not.toThrow()
    await expect(w.send("ses_w1", "€".repeat(7_999), { receipt })).rejects.toThrow(`exceed ${MAX_EVENT_BYTES} bytes`)
    expect(eventKeys(w.storage)).toHaveLength(0)
  })

  test("only an explicit safe normal FYI is passive; every marker or unknown keeps delivery", () => {
    expect(classify(fyi(), false)).toEqual({ passive: true, routing: "fyi" })
    expect(classify(fyi({ state: "completed" }), false).passive).toBe(true)
    expect(classify(fyi({ state: undefined }), false).passive).toBe(true)
    for (const receipt of [
      fyi({ state: "blocked" }),
      fyi({ state: "failed" }),
      fyi({ state: "cancelled" }),
      fyi({ state: "unknown" }),
      fyi({ state: "succeeded-ish" }),
      fyi({ restrictions: ["do not deploy"] }),
      fyi({ unresolved: ["waiting for the user"] }),
      fyi({ kind: "actionable" }),
      fyi({ kind: "note" }),
    ]) {
      expect(classify(receipt, false).passive).toBe(false)
    }
    expect(classify(fyi(), true)).toMatchObject({ passive: false, routing: 'fyi with priority "urgent": delivered as actionable' })
  })
})

describe("passive FYIs", () => {
  test("are stored durably before anything else and return a truthful receipt", async () => {
    const w = world()
    const result = await w.send("ses_w1", "batch 3 of 7 done", { receipt: fyi({ id: "r1" }) })
    expect(result).toMatchObject({ recorded: true, passive: true, delivered: false, processed: false, receipt_id: "r1", to: "ses_ctl", status: "pending" })
    expect(result.note).toContain("has not read this")
    expect(result.due_at).toBeUndefined()
    expect(w.calls).toEqual([])
    expect(eventKeys(w.storage)).toEqual([`${EVENT_PREFIX}ses_ctl:ses_w1:r1`])
  })

  test("unsafe, unknown and urgent receipts follow existing delivery, with their fields shown exactly", async () => {
    const w = world()
    const restricted = await w.send("ses_w1", "done", { receipt: fyi({ restrictions: ["Do not deploy before 18:00 UTC"] }) })
    expect(restricted).toMatchObject({ delivered: true, passive: false, routing: "fyi with restrictions: delivered as actionable" })
    expect(w.prompts[0].text).toStartWith('[cross-session message from ses_w1 "Worker one"]')
    expect(w.prompts[0].text).toContain("- Do not deploy before 18:00 UTC")
    expect(w.prompts[0].text).toContain("not from your user")
    expect(w.prompts[0].metadata.crossSession.receipt.restrictions).toEqual(["Do not deploy before 18:00 UTC"])
    const unknown = await w.send("ses_w1", "?", { receipt: { version: 1, kind: "mystery" } })
    expect(unknown).toMatchObject({ delivered: true, passive: false })
    const urgent = await w.send("ses_w1", "prod down", { priority: "urgent", urgent_reason: "incident: production is failing right now", receipt: fyi() })
    expect(urgent).toMatchObject({ delivered: true, passive: false, routing: "urgent" })
    const downgraded = await w.send("ses_w1", "meh", { priority: "urgent", receipt: fyi() })
    expect(downgraded).toMatchObject({ delivered: true, downgraded: true, passive: false })
    expect(w.prompts).toHaveLength(4)
    // Each typed receipt is an immutable original first, delivered on its own under a manifest with its native ID.
    expect(eventKeys(w.storage)).toHaveLength(4)
    expect(w.prompts.every((prompt) => /^msg_/.test(prompt.id))).toBe(true)
    expect(await w.digest.pending("ses_ctl")).toBe(0)
  })

  test("legacy untyped messages are unchanged", async () => {
    const w = world()
    const result = await w.send("ses_w1", "Tell me when it is merged.")
    expect(result).toEqual({ delivered: true, to: "ses_ctl", title: "Controller", delivery: "steer" })
    expect(w.prompts[0].metadata).toEqual({ crossSession: { from: "ses_w1", priority: "normal" } })
  })

  test("a child's explicit FYI to its parent bypasses the relay; its final report merge is unchanged", async () => {
    const w = world()
    await w.send("ses_kid", "progress: 4 of 9", { receipt: fyi() })
    await w.send("ses_kid", "plain progress")
    expect(w.relay.pending("ses_kid")).toBe(1)
    const event: any = {
      tool: "subagent",
      status: "completed",
      result: { output: { sessionID: "ses_kid", status: "completed", output: "final" }, content: '<subagent sessionID="ses_kid">\nfinal\n</subagent>' },
    }
    w.relay.subagentFinished(event)
    expect(event.result.content).toBe('<subagent sessionID="ses_kid">\n[Sent to you with send_message during this run:]\n\nplain progress\n\n[Final reply:]\n\nfinal\n</subagent>')
    await w.send("ses_kid", "another FYI", { receipt: fyi() })
    await w.relay.turnEnded("ses_kid")
    await w.relay.dispose()
    expect(w.calls).toEqual([])
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ pending: 2, passive_pending: 2 })
  })

  test("idempotent retries, refused reuse, failed and uncertain storage writes", async () => {
    const storage = memoryStorage()
    const w = world({ storage })
    const first = await w.send("ses_w1", "same", { receipt: fyi({ id: "dup" }) })
    const again = await w.send("ses_w1", "same", { receipt: fyi({ id: "dup" }) })
    expect(again).toMatchObject({ recorded: true, duplicate: true, receipt_id: first.receipt_id })
    await expect(w.send("ses_w1", "different", { receipt: fyi({ id: "dup" }) })).rejects.toThrow("already used for different content")
    // The same ID from another sender is a different record.
    expect(await w.send("ses_w2", "same", { receipt: fyi({ id: "dup" }) })).toMatchObject({ recorded: true, receipt_id: "dup" })
    expect(eventKeys(storage)).toHaveLength(2)

    const set = storage.set
    storage.set = async () => {
      throw new Error("disk full")
    }
    await expect(w.send("ses_w1", "lost?", { receipt: fyi({ id: "f1" }) })).rejects.toThrow("not recorded: disk full")
    // A write that landed but reported failure is reconciled by reading the same key back.
    storage.set = async (key, value) => {
      await set(key, value)
      throw new Error("timeout")
    }
    expect(await w.send("ses_w1", "landed", { receipt: fyi({ id: "f2" }) })).toMatchObject({ recorded: true, receipt_id: "f2" })
    storage.set = set
    const get = storage.get
    storage.get = async () => {
      throw new Error("io")
    }
    await expect(w.send("ses_w1", "unreadable", { receipt: fyi({ id: "f3" }) })).rejects.toThrow("could not be read")
    storage.get = get
    expect(eventKeys(storage).sort()).toEqual([`${EVENT_PREFIX}ses_ctl:ses_w1:dup`, `${EVENT_PREFIX}ses_ctl:ses_w1:f2`, `${EVENT_PREFIX}ses_ctl:ses_w2:dup`])
    expect(w.calls).toEqual([])
  })

  test("an unavailable restore refuses to record, then recovers", async () => {
    const storage = memoryStorage()
    const scan = storage.scan
    storage.scan = async () => {
      throw new Error("store offline")
    }
    const w = world({ storage })
    await expect(w.send("ses_w1", "x", { receipt: fyi() })).rejects.toThrow("peer inbox storage is unavailable")
    storage.scan = scan
    expect(await w.send("ses_w1", "x", { receipt: fyi() })).toMatchObject({ recorded: true })
  })

  test("concurrent sends with distinct and identical IDs keep one original each", async () => {
    const w = world()
    const results = await Promise.all([
      ...Array.from({ length: 30 }, (_, i) => w.send("ses_w1", `m${i}`, { receipt: fyi({ id: `c${i}` }) })),
      ...Array.from({ length: 5 }, () => w.send("ses_w2", "same", { receipt: fyi({ id: "one" }) })),
    ])
    expect(eventKeys(w.storage)).toHaveLength(31)
    expect(results.filter((result) => result.duplicate)).toHaveLength(4)
  })
})

describe("explicit safe supersession is a view", () => {
  async function seeded() {
    const w = world()
    await w.send("ses_w1", "1 of 3", { receipt: fyi({ id: "p1" }) })
    await w.send("ses_w1", "2 of 3", { receipt: fyi({ id: "p2" }) })
    return w
  }

  test("same sender, work item, revision and job with exact earlier IDs archives them; originals stay byte-identical", async () => {
    const w = await seeded()
    const before = JSON.stringify(w.storage.store.get(`${EVENT_PREFIX}ses_ctl:ses_w1:p1`))
    const result = await w.send("ses_w1", "3 of 3", { receipt: fyi({ id: "p3", state: "completed", supersedes: ["p1", "p2"] }) })
    expect(result).toMatchObject({ supersession_applied: true, archived: ["ses_w1:p1", "ses_w1:p2"] })
    const listed = await w.op({ operation: "list" })
    expect(listed.items.map((item: any) => [item.id, item.status, item.superseded_by])).toEqual([
      ["ses_w1:p1", "archived", "ses_w1:p3"],
      ["ses_w1:p2", "archived", "ses_w1:p3"],
      ["ses_w1:p3", "pending", undefined],
    ])
    const read = await w.op({ operation: "read", ids: ["ses_w1:p1"] })
    expect(read.items[0].text).toBe("1 of 3")
    expect(JSON.stringify(w.storage.store.get(`${EVENT_PREFIX}ses_ctl:ses_w1:p1`))).toBe(before)
  })

  test("every mismatch stores the new report uncoalesced and archives nothing", async () => {
    const cases: Array<[Partial<Receipt>, string]> = [
      [{ revision: "def456" }, "different kind, work item, revision or job"],
      [{ job_id: "job-2" }, "different kind, work item, revision or job"],
      [{ work_item: "PROJ-2" }, "different kind, work item, revision or job"],
      [{ supersedes: ["p1", "missing"] }, "missing is not an earlier receipt"],
      [{ supersedes: ["future"], id: "future" }, "future is this receipt"],
      [{ revision: undefined }, "needs work_item and revision"],
    ]
    for (const [extra, reason] of cases) {
      const w = await seeded()
      const result = await w.send("ses_w1", "next", { receipt: fyi({ id: "n1", supersedes: ["p1"], ...extra }) })
      expect(result).toMatchObject({ recorded: true, supersession_applied: false })
      expect(result.reason).toContain(reason)
      expect((await w.op({ operation: "status" })).archived).toBe(0)
    }
    // Another sender cannot archive ses_w1's receipts; an actionable report never supersedes.
    const w = await seeded()
    expect(await w.send("ses_w2", "x", { receipt: fyi({ id: "x", supersedes: ["p1"] }) })).toMatchObject({ supersession_applied: false })
    await w.send("ses_w1", "blocked", { receipt: fyi({ id: "b", state: "blocked", supersedes: ["p1"] }) })
    expect((await w.op({ operation: "status" })).archived).toBe(0)
  })
})

describe("peer_inbox: self-bound status, list, read, ack", () => {
  test("has no target parameter and rejects foreign arguments", async () => {
    expect(Object.keys(PEER_INBOX_INPUT.properties)).not.toContain("session_id")
    const w = world()
    await expect(w.op({ operation: "status", session_id: "ses_other" } as any)).rejects.toThrow("argument session_id is not valid")
    await expect(peerInboxOperation(w.digest.inbox, { operation: "status" }, {})).rejects.toThrow("needs the calling session")
    await w.send("ses_w1", "for ctl", { receipt: fyi() })
    expect((await w.op({ operation: "status" }, "ses_w2")).pending).toBe(0)
    expect((await w.op({ operation: "read", ids: [(await w.op({ operation: "list" })).items[0].id] }, "ses_w2")).missing).toHaveLength(1)
  })

  test("bounded pagination over 100+ entries with a stable cursor", async () => {
    const w = world()
    for (let i = 0; i < 105; i++) {
      await w.send(i % 2 ? "ses_w1" : "ses_w2", `report ${i}`, { receipt: fyi({ id: `r${String(i).padStart(3, "0")}` }) })
      w.advance(1)
    }
    const seen: string[] = []
    let cursor: string | undefined
    let pages = 0
    do {
      const page = await w.op({ operation: "list", ...(cursor ? { cursor } : {}), limit: 20 })
      expect(page.items.length).toBeLessThanOrEqual(20)
      seen.push(...page.items.map((item: any) => item.id))
      cursor = page.next_cursor
      pages++
    } while (cursor)
    expect(pages).toBe(6)
    expect(new Set(seen).size).toBe(105)
    await expect(w.op({ operation: "list", limit: 21 })).rejects.toThrow("limit must be an integer from 1 to 20")
    expect(w.calls).toEqual([])
  })

  test("list and conflicting ack stay small when a sender's title is huge", async () => {
    const w = world()
    w.known.ses_w1.title = "€".repeat(12_000) // 36 KB, within the 40 KiB original cap
    for (let i = 0; i < 20; i++) await w.send("ses_w1", "Routine", { receipt: fyi({ id: `t${i}` }) })
    const list = await w.op({ operation: "list" })
    expect(list.items).toHaveLength(20)
    expect(list.items[0].from_title).toBe("€".repeat(200))
    expect(Buffer.byteLength(JSON.stringify(list))).toBeLessThanOrEqual(MAX_BATCH_BYTES)
    const ack = await w.op({ operation: "ack", disposition: "processed", items: list.items.map((item: any) => ({ id: item.id, expected_version: 9 })) })
    expect(ack.results.every((r: any) => r.conflict)).toBe(true)
    expect(Buffer.byteLength(JSON.stringify(ack))).toBeLessThanOrEqual(MAX_BATCH_BYTES)
  })

  test("read returns whole originals up to 48 KiB and names the rest; read is not processed", async () => {
    const w = world()
    const ids: string[] = []
    for (let i = 0; i < 4; i++) ids.push((await w.send("ses_w1", "€".repeat(7_000), { receipt: fyi({ id: `big${i}` }) })).receipt_id)
    const read = await w.op({ operation: "read", ids: ids.map((id) => `ses_w1:${id}`) })
    expect(read.items).toHaveLength(2)
    expect(read.items[0].text).toBe("€".repeat(7_000))
    expect(read.remaining).toEqual(["ses_w1:big2", "ses_w1:big3"])
    expect(Buffer.byteLength(JSON.stringify(read.items))).toBeLessThanOrEqual(MAX_BATCH_BYTES)
    const status = await w.op({ operation: "status" })
    expect(status).toMatchObject({ pending: 4, processed: 0, unread: 2 })
  })

  test("ack needs the current version, is idempotent and never sends", async () => {
    const w = world()
    await w.send("ses_w1", "one", { receipt: fyi({ id: "a" }) })
    await w.send("ses_w1", "two", { receipt: fyi({ id: "b", supersedes: ["a"] }) })
    const stale = await w.op({ operation: "ack", disposition: "processed", items: [{ id: "ses_w1:a", expected_version: 0 }] })
    expect(stale.results[0]).toMatchObject({ ok: false, conflict: true, status: "archived", version: 1 })
    const done = await w.op({ operation: "ack", disposition: "processed", items: [{ id: "ses_w1:b", expected_version: 0 }, { id: "nope", expected_version: 0 }] })
    expect(done.results).toEqual([
      { id: "ses_w1:b", ok: true, status: "processed", version: 1 },
      { id: "nope", ok: false, error: "not found" },
    ])
    const repeat = await w.op({ operation: "ack", disposition: "processed", items: [{ id: "ses_w1:b", expected_version: 0 }] })
    expect(repeat.results[0]).toMatchObject({ ok: true, unchanged: true, version: 1 })
    expect(w.storage.store.get(`${STATE_PREFIX}ses_ctl:ses_w1:b`)).toMatchObject({ status: "processed", version: 1 })
    expect(w.calls).toEqual([])
  })

  test("a capped restore scan reports an incomplete index instead of passing for complete", async () => {
    const storage = memoryStorage()
    const scan = storage.scan
    storage.scan = async (input) => ({ ...(await scan({ ...input, limit: 1 })), next: input.prefix === EVENT_PREFIX ? "zzz" : undefined })
    const w = world({ storage })
    await expect(w.digest.restoreFromStorage()).rejects.toThrow("did not advance")
    expect((await w.op({ operation: "status" })).complete).toBe(false)
    await expect(w.op({ operation: "list" })).rejects.toThrow("not available yet")
  })
})

describe("plugin: 100 explicit FYIs cost no prompt or model work", () => {
  afterEach(() => {
    delete (globalThis as any)[REGISTRY_KEY]
  })

  function plugged(idle: boolean, storage?: Map<string, unknown>) {
    const ctx: any = makeMockCtx({
      sessions: {
        ses_ctl: sessionInfo("ses_ctl", { title: "Controller", time: idle ? { idle: 5, updated: 5 } : { idle: 5, updated: 9 } }),
        ses_w1: sessionInfo("ses_w1", { title: "Worker" }),
        ses_kid: sessionInfo("ses_kid", { title: "Child", parentID: "ses_ctl" }),
      },
    })
    if (storage) for (const [key, value] of storage) ctx._store.set(key, value)
    const calls: string[] = []
    const real = ctx.session
    // Every session API use except reads is counted: prompt, synthetic, generate, compaction, title, hooks...
    ctx.session = new Proxy(
      { ...real, prompt: async () => {}, synthetic: async () => {} },
      {
        get(target, name) {
          const value = (target as any)[name]
          if (typeof value !== "function" || name === "get" || name === "context") return value
          return (...args: unknown[]) => {
            calls.push(String(name))
            return value(...args)
          }
        },
      },
    )
    const tool = (name: string) => ctx._tools.find((item: any) => item.name === name)
    return { ctx, calls, tool }
  }

  for (const idle of [true, false]) {
    test(`${idle ? "idle" : "busy"} recipient, digest on and off, ticks, turn ends and reload`, async () => {
      let p = plugged(idle)
      let cleanup = await plugin.setup(p.ctx)
      const send = (from: string, i: number) =>
        p.tool("send_message").execute({ to: "ses_ctl", text: `report ${i}`, receipt: fyi({ id: `r${i}`, job_id: `job-${i % 3}` }) }, { sessionID: from })
      for (let i = 0; i < 50; i++) await send(i % 5 ? "ses_w1" : "ses_kid", i)
      await p.tool("opencode_meta").execute({ operation: "message_digest", enabled: true, minutes: 5 }, { sessionID: "ses_ctl" })
      for (let i = 50; i < 100; i++) await send(i % 5 ? "ses_w1" : "ses_kid", i)
      let now = Date.now()
      const clock = new Digest(sharedRegistry(), p.ctx.session, p.ctx.storage, () => {}, () => now)
      for (let tick = 0; tick < 20; tick++) {
        now += 10 * 60_000
        await clock.tick(0)
      }
      p.ctx.emit({ type: "session.execution.succeeded", created: 1, data: { sessionID: "ses_kid" } })
      await p.ctx.settle()
      await p.tool("opencode_meta").execute({ operation: "message_digest", enabled: false }, { sessionID: "ses_ctl" })
      await cleanup()
      // Reload: a fresh process registry restores everything from storage, still without prompting anyone.
      const stored = p.ctx._store as Map<string, unknown>
      const before = p.calls
      delete (globalThis as any)[REGISTRY_KEY]
      p = plugged(idle, stored)
      cleanup = await plugin.setup(p.ctx)
      const status = JSON.parse((await p.tool("peer_inbox").execute({ operation: "status" }, { sessionID: "ses_ctl" })).content)
      expect(status).toMatchObject({ pending: 100, passive_pending: 100, offered: 0, processed: 0, complete: true })
      const page = JSON.parse((await p.tool("peer_inbox").execute({ operation: "list" }, { sessionID: "ses_ctl" })).content)
      const read = JSON.parse((await p.tool("peer_inbox").execute({ operation: "read", ids: page.items.map((item: any) => item.id) }, { sessionID: "ses_ctl" })).content)
      expect(read.items).toHaveLength(20)
      const ack = JSON.parse(
        (await p.tool("peer_inbox").execute({ operation: "ack", disposition: "processed", items: read.items.map((item: any) => ({ id: item.id, expected_version: item.version })) }, { sessionID: "ses_ctl" })).content,
      )
      expect(ack.results.every((result: any) => result.ok)).toBe(true)
      await cleanup()
      expect([...before, ...p.calls]).toEqual([])
      expect([...stored.keys()].filter((key) => key.startsWith(EVENT_PREFIX))).toHaveLength(100)
    })
  }

  test("an unrelated untyped message still prompts exactly as before alongside a passive backlog", async () => {
    // Busy, so send_message does not spend seconds watching an idle wake.
    const p = plugged(false)
    const cleanup = await plugin.setup(p.ctx)
    for (let i = 0; i < 10; i++) await p.tool("send_message").execute({ to: "ses_ctl", text: `fyi ${i}`, receipt: fyi() }, { sessionID: "ses_w1" })
    await p.tool("send_message").execute({ to: "ses_ctl", text: "please review" }, { sessionID: "ses_w1" })
    await cleanup()
    expect(p.calls).toEqual(["prompt"])
  })
})

test("a standalone PeerInbox works without the digest", async () => {
  const storage = memoryStorage()
  const inbox = new PeerInbox(freshInbox(), storage, async () => {})
  const stored = await inbox.append({ to: "ses_a", from: "ses_b", text: "x", passive: true, receipt: fyi() })
  expect(stored.duplicate).toBe(false)
  expect(inbox.status("ses_a")).toMatchObject({ pending: 1, passive_pending: 1 })
})

describe("bounded digest batches from the peer inbox", () => {
  const MIN = 60_000

  test("whole events only: at most 20 and 48 KiB per batch, one batch per interval, oldest first, attention shown first", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 5)
    for (let i = 0; i < 45; i++) {
      await w.send(i % 2 ? "ses_w1" : "ses_w2", `update ${i}`, i === 3 ? { receipt: fyi({ id: "x3", kind: "actionable", state: "failed", restrictions: ["Exact: do not retry before 18:00 UTC"] }) } : {})
      w.advance(1)
    }
    // Passive FYIs never join a digest.
    for (let i = 0; i < 10; i++) await w.send("ses_w1", `fyi ${i}`, { receipt: fyi() })
    w.advance(5 * MIN)
    await w.digest.tick(0)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    const first = w.prompts[0]
    expect(first.id).toMatch(/^msg_[0-9a-f]{12}[0-9A-Za-z]{14}$/)
    expect(first.metadata.crossSession.digest).toMatchObject({ count: 20, remaining: 25 })
    expect(first.text).toStartWith("[cross-session digest: 20 routine messages from other sessions, held since")
    expect(first.text).toContain("25 more held messages wait for the next digest")
    expect(first.text).toContain("not from your user")
    expect(first.text).not.toContain("fyi ")
    // The failed, restricted one is shown first, its restriction exactly; the batch holds the 20 oldest.
    expect(first.text.indexOf("update 3")).toBeLessThan(first.text.indexOf("update 0"))
    expect(first.text).toContain("- Exact: do not retry before 18:00 UTC")
    expect(first.text).toContain("update 19")
    expect(first.text).not.toContain("update 20")
    w.advance(5 * MIN)
    await w.digest.tick(0)
    w.advance(5 * MIN)
    await w.digest.tick(0)
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.prompts.map((prompt) => prompt.metadata.crossSession.digest.count)).toEqual([20, 20, 5])
    expect(new Set(w.prompts.map((prompt) => prompt.id)).size).toBe(3)
    expect(w.prompts.every((prompt) => Buffer.byteLength(prompt.text) <= MAX_BATCH_BYTES)).toBe(true)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ offered: 45, pending: 10, passive_pending: 10, actionable_pending: 0 })
    expect(eventKeys(w.storage)).toHaveLength(55)
  })

  test("worst-case UTF-8 messages are never split: each batch stays under 48 KiB", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 5)
    // 8,000 UTF-16 units of a three-byte character is the most a send_message text can occupy.
    for (let i = 0; i < 5; i++) await w.send("ses_w1", `${i}${"€".repeat(7_999)}`)
    for (let round = 0; round < 5; round++) {
      w.advance(5 * MIN)
      await w.digest.tick(0)
    }
    // Two such texts fit 48 KiB on their own, but not with a "more held" line, metadata and framing.
    expect(w.prompts.map((prompt) => prompt.metadata.crossSession.digest.count)).toEqual([1, 1, 1, 2])
    for (const prompt of w.prompts) expect(Buffer.byteLength(JSON.stringify(prompt))).toBeLessThanOrEqual(MAX_BATCH_BYTES)
    for (let i = 0; i < 5; i++) expect(w.prompts.filter((prompt) => prompt.text.includes(`${i}${"€".repeat(7_999)}`))).toHaveLength(1)
  })

  test("turning digest mode off delivers one bounded batch of held messages and never passive FYIs", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 15)
    for (let i = 0; i < 5; i++) await w.send("ses_w1", `fyi ${i}`, { receipt: fyi() })
    expect((await w.digest.disable("ses_ctl")).flushed).toBe(0)
    expect(w.prompts).toHaveLength(0)
    await w.digest.enable("ses_ctl", 15)
    for (let i = 0; i < 25; i++) await w.send("ses_w1", `held ${i}`)
    expect((await w.digest.disable("ses_ctl")).flushed).toBe(20)
    // The rest goes one batch later, not as an immediate chain of prompts.
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    w.advance(2 * MIN)
    await w.digest.tick(0)
    expect(w.prompts.map((prompt) => prompt.metadata.crossSession.digest.count)).toEqual([20, 5])
    expect(w.prompts.every((prompt) => !prompt.text.includes("fyi "))).toBe(true)
  })

  test("a known failure before admission retries the same manifest and native ID; nothing is duplicated", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 5)
    await w.send("ses_w1", "held")
    const present = w.known.ses_ctl
    delete w.known.ses_ctl
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(0)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ delivery_attempts_needing_attention: { failed_before_admission: 1 } })
    const [batch] = [...w.registry.inbox.batches.get("ses_ctl")!.values()]
    w.known.ses_ctl = present
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    expect(w.prompts[0].id).toBe(batch!.prompt_id)
    expect(w.registry.inbox.batches.get("ses_ctl")!.size).toBe(1)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ offered: 1, pending: 0 })
  })

  test("unknown admission, cancellation and late native evidence are reconciled only by exact prompt ID", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 5)
    await w.send("ses_w1", "one")
    const original = w.sessions.prompt
    w.sessions.prompt = async () => {
      throw new Error("connection reset")
    }
    w.advance(5 * MIN)
    await w.digest.tick(0)
    const [unknown] = [...w.registry.inbox.batches.get("ses_ctl")!.values()]
    expect(unknown!.state).toBe("admission_unknown")
    w.sessions.prompt = original
    // A delivered event for some other inbox item changes nothing; the exact ID proves admission.
    expect(await w.digest.inbox.nativeSettled("ses_ctl", "msg_other", "delivered")).toBe(false)
    expect(await w.digest.inbox.nativeSettled("ses_w1", unknown!.prompt_id, "delivered")).toBe(false)
    expect(await w.digest.inbox.nativeSettled("ses_ctl", unknown!.prompt_id, "delivered")).toBe(true)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ offered: 1, pending: 0 })
    // A cancelled admission is terminal: its messages stay visible and are never resent automatically.
    await w.send("ses_w1", "two")
    w.advance(5 * MIN)
    await w.digest.tick(0)
    const cancelled = w.prompts.at(-1)
    expect(await w.digest.inbox.nativeSettled("ses_ctl", cancelled.id, "cancelled")).toBe(true)
    w.advance(60 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    const listed = await w.op({ operation: "list" })
    expect(listed.items.map((item: any) => [item.status, item.delivery])).toEqual([
      ["offered", "admitted"],
      ["pending", "cancelled"],
    ])
    // Delivered evidence that arrives before the prompt call returns is not overwritten by it.
    await w.send("ses_w2", "three")
    w.sessions.prompt = async (input: any) => {
      await w.digest.inbox.nativeSettled("ses_ctl", input.id, "delivered")
      throw new Error("lost response")
    }
    w.advance(60 * MIN)
    await w.digest.tick(0)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ offered: 2 })
  })

  test("crash points: manifest store failure, stored-but-unknown, admitted-before-status; restore never resends", async () => {
    const storage = memoryStorage()
    const w = world({ storage })
    await w.digest.enable("ses_ctl", 5)
    await w.send("ses_w1", "a")
    const set = storage.set
    storage.set = async (key, value) => {
      if (key.startsWith("peer:batch:")) throw new Error("disk full")
      return set(key, value)
    }
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(0)
    expect(w.digest.pending("ses_ctl")).toBe(1)
    storage.set = set
    // Crash after the manifest is stored but before the outcome is known.
    w.sessions.prompt = () => new Promise(() => {})
    w.advance(5 * MIN)
    void w.digest.tick(0)
    await Bun.sleep(5)
    const stored = [...storage.store].find(([key]) => key.startsWith("peer:batch:"))![1] as any
    expect(stored.state).toBe("prepared")
    const restarted = world({ storage })
    await restarted.digest.restoreFromStorage()
    expect(restarted.digest.inbox.status("ses_ctl")).toMatchObject({ pending: 1, delivery_attempts_needing_attention: { admission_unknown: 1 } })
    restarted.advance(60 * MIN)
    await restarted.digest.tick(0)
    expect(restarted.prompts).toHaveLength(0)
    // An admitted manifest whose item status was never written still restores as offered.
    await restarted.send("ses_w2", "b")
    restarted.advance(60 * MIN)
    await restarted.digest.tick(0)
    const again = world({ storage })
    await again.digest.restoreFromStorage()
    expect(again.digest.inbox.status("ses_ctl")).toMatchObject({ offered: 1, pending: 1 })
    expect([...storage.store.keys()].filter((key) => key.startsWith(STATE_PREFIX))).toEqual([])
  })

  test("messages arriving while a batch is being sent belong to a later manifest", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 5)
    await w.send("ses_w1", "before")
    const original = w.sessions.prompt
    w.sessions.prompt = async (input: any) => {
      await w.send("ses_w2", "during")
      return original(input)
    }
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.prompts[0].metadata.crossSession.digest.count).toBe(1)
    expect(w.prompts[0].text).not.toContain("during")
    expect(w.digest.pending("ses_ctl")).toBe(1)
  })

  test("legacy held peers are imported once under their own IDs, even when removing the old key fails", async () => {
    const storage = memoryStorage()
    await storage.set("digest:cfg:ses_ctl", { minutes: 5, last: 0 })
    await storage.set("digest:held:ses_ctl:old1", { id: "old1", to: "ses_ctl", from: "ses_w1", text: "legacy text", at: 5 })
    storage.remove = async () => {
      throw new Error("read-only")
    }
    const a = world({ storage })
    await a.digest.restoreFromStorage()
    const b = world({ storage })
    await b.digest.restoreFromStorage()
    expect(storage.store.has("digest:held:ses_ctl:old1")).toBe(true)
    expect(eventKeys(storage)).toEqual([`${EVENT_PREFIX}ses_ctl:ses_w1:old1`])
    expect(b.digest.pending("ses_ctl")).toBe(1)
    await b.digest.tick(0)
    expect(b.prompts[0].text).toContain("legacy text")
  })

  test("a relay hold that cannot be stored falls back to ordinary relay delivery", async () => {
    const storage = memoryStorage()
    const w = world({ storage })
    await w.digest.enable("ses_ctl", 5)
    const relay = new ParentRelay(w.sessions, { divert: digestDivert(w.digest) })
    storage.set = async () => {
      throw new Error("disk full")
    }
    relay.hold("ses_kid", { parentID: "ses_ctl", text: "progress", framed: "[framed] progress", metadata: {}, delivery: "steer", priority: "normal" })
    await relay.flush("ses_kid")
    expect(w.prompts.map((prompt) => prompt.text)).toEqual(["[framed] progress"])
  })

  test("peer frames keep the prefixes the permission reviewer treats as untrusted peer content", async () => {
    // A permission reviewer can match these prefixes to treat the text as peer content, never user approval.
    const prefixes = ["[cross-session message from ", "[cross-session digest: "]
    const w = world()
    await w.send("ses_w1", "x", { receipt: fyi({ kind: "actionable" }) })
    await w.digest.enable("ses_ctl", 5)
    await w.send("ses_w1", "y", { receipt: fyi({ kind: "actionable", restrictions: ["r"] }) })
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(2)
    for (const prompt of w.prompts) expect(prefixes.some((prefix) => prompt.text.startsWith(prefix))).toBe(true)
  })
})
