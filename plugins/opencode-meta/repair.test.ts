// Regressions for the independent review of the peer inbox: retry fencing, recovery, typed IDs on every route,
// all-or-none supersession and whole-envelope bounds.
import { afterEach, expect, test } from "bun:test"
import plugin from "./index.ts"
import { Digest, decodeSetting, freshRegistry, batchText, REGISTRY_KEY } from "./digest.ts"
import { BATCH_PREFIX, EVENT_PREFIX, MAX_BATCH_BYTES, STATE_PREFIX, bytes, type Receipt } from "./peer-inbox.ts"
import { ParentRelay } from "./relay.ts"
import { digestDivert, peerInboxOperation } from "./lib.ts"
import { sendMessage } from "./send.ts"
import { makeMockCtx, sessionInfo } from "../opencode-recall-lite/mock-ctx.ts"

function storage() {
  const data = new Map<string, any>()
  return {
    data,
    get: async (key: string) => structuredClone(data.get(key)),
    set: async (key: string, value: any) => void data.set(key, structuredClone(value)),
    remove: async (key: string) => void data.delete(key),
    scan: async ({ prefix, after, limit }: { prefix: string; after?: string; limit: number }) => {
      const keys = [...data.keys()].filter((key) => key.startsWith(prefix) && (!after || key > after)).sort()
      const page = keys.slice(0, limit)
      return { entries: page.map((key) => ({ key, value: structuredClone(data.get(key)) })), ...(keys.length > limit ? { next: page.at(-1) } : {}) }
    },
  }
}
// The plugin keeps one registry per process; each plugin test starts from a fresh one.
afterEach(() => void delete (globalThis as any)[REGISTRY_KEY])

const fyi = (id: string, extra: Partial<Receipt> = {}): Receipt => ({ version: 1, kind: "fyi", id, work_item: "work", revision: "rev", ...extra })
const known: Record<string, any> = { ses_target: { id: "ses_target" }, ses_sender: { id: "ses_sender" }, ses_kid: { id: "ses_kid", parentID: "ses_target" } }
function world(store = storage()) {
  const prompts: any[] = []
  const api = {
    get: async ({ sessionID }: any) => known[sessionID] && { ...known[sessionID], time: { idle: 0, updated: 1 } },
    prompt: async (input: any) => void prompts.push(input),
  }
  const digest = new Digest(freshRegistry(), api, store)
  const relay = new ParentRelay(api, { graceMs: 1 })
  const send = (receipt: Receipt | undefined, text = "hello", from = "ses_sender") =>
    sendMessage(store, api, { to: "ses_target", text, ...(receipt ? { receipt } : {}) }, { sessionID: from }, relay, { digest, sleep: async () => {}, intervalMs: 1, timeoutMs: 1 }) as Promise<any>
  return { store, api, digest, relay, prompts, send }
}
const batchStates = (store: ReturnType<typeof storage>) => [...store.data].filter(([key]) => key.startsWith(BATCH_PREFIX)).map(([, value]) => value.state)

test("1: a retry is durably fenced before its native call; unfenced it is not attempted, fenced it restores as unknown", async () => {
  const w = world()
  await w.digest.hold({ to: "ses_target", from: "ses_sender", text: "retry must not wake twice" })
  const get = w.api.get
  w.api.get = async () => undefined
  await w.digest.flush("ses_target")
  w.api.get = get
  expect(batchStates(w.store)).toEqual(["failed_before_admission"])
  const set = w.store.set
  let calls = 0
  w.api.prompt = async () => {
    calls++
    throw new Error("native admission may have succeeded; response lost")
  }
  // The fence cannot be stored: no native call at all, and the known failure stays retryable.
  w.store.set = async (key, value) => (key.startsWith(BATCH_PREFIX) ? Promise.reject(new Error("disk full")) : set(key, value))
  await w.digest.flush("ses_target")
  expect(calls).toBe(0)
  expect(batchStates(w.store)).toEqual(["failed_before_admission"])
  // The fence lands, the call's outcome is lost and the later state write fails: durable evidence says prepared.
  let writes = 0
  w.store.set = async (key, value) => (key.startsWith(BATCH_PREFIX) && writes++ > 0 ? Promise.reject(new Error("disk full")) : set(key, value))
  await w.digest.flush("ses_target")
  expect(calls).toBe(1)
  expect(batchStates(w.store)).toEqual(["prepared"])
  w.store.set = set
  const restarted = world(w.store)
  await restarted.digest.restoreFromStorage()
  await restarted.digest.flush("ses_target")
  expect(restarted.prompts).toHaveLength(0)
  expect(restarted.digest.inbox.status("ses_target")).toMatchObject({ pending: 1, delivery_attempts_needing_attention: { admission_unknown: 1 } })
})

test("2: recovery past one bounded chunk delivers nothing until it completes, then never resends an admitted original", async () => {
  const w = world()
  await w.digest.hold({ to: "ses_target", from: "ses_sender", text: "must not duplicate" })
  await w.digest.flush("ses_target")
  expect(w.prompts).toHaveLength(1)
  const own = [...w.store.data].find(([key]) => key.startsWith(BATCH_PREFIX))!
  w.store.data.delete(own[0])
  for (let i = 0; i < 20_000; i++) {
    const id = `b${String(i).padStart(5, "0")}`
    w.store.data.set(`${BATCH_PREFIX}ses_aaa:${id}`, { id, to: "ses_aaa", refs: [], oversize: [], state: "admitted", created: 0, updated: 0 })
  }
  w.store.data.set(own[0], own[1])
  const restarted = world(w.store)
  await restarted.digest.restoreFromStorage()
  expect(restarted.digest.inbox.status("ses_target").complete).toBe(false)
  expect(restarted.digest.pending("ses_target")).toBe(0)
  await restarted.digest.flush("ses_target")
  await restarted.digest.tick(0)
  expect(restarted.prompts).toHaveLength(0)
  // The tick continued recovery from where it stopped; the original's own manifest is now known.
  expect(restarted.digest.inbox.status("ses_target")).toMatchObject({ complete: true, offered: 1, pending: 0 })
  expect(restarted.digest.inbox.list("ses_target", {}).items).toHaveLength(1)
})

test("3: a failed startup recovery is never shown as an empty complete inbox, and nothing is delivered from it", async () => {
  const ctx = makeMockCtx({ sessions: { ses_target: sessionInfo("ses_target") } }) as any
  const scan = ctx.storage.scan
  ctx.storage.scan = async () => {
    throw new Error("injected storage read failure")
  }
  const dispose = await plugin.setup(ctx)
  try {
    const tool = ctx._tools.find((tool: any) => tool.name === "peer_inbox")
    const status = JSON.parse((await tool.execute({ operation: "status" }, { sessionID: "ses_target" })).content)
    expect(status).toMatchObject({ complete: false })
    expect(status.pending).toBeUndefined()
    expect(status.note).toContain("injected storage read failure")
    await expect(tool.execute({ operation: "list" }, { sessionID: "ses_target" })).rejects.toThrow("not available yet")
    await expect(tool.execute({ operation: "read", ids: ["x:y"] }, { sessionID: "ses_target" })).rejects.toThrow("not available yet")
    // Once storage reads work again, the next operation completes recovery.
    ctx.storage.scan = scan
    expect(JSON.parse((await tool.execute({ operation: "status" }, { sessionID: "ses_target" })).content)).toMatchObject({ complete: true, pending: 0 })
  } finally {
    await dispose()
  }
})

test("4: typed immediate and relayed receipts keep the immutable original and ID contract", async () => {
  const w = world()
  await w.send(fyi("same", { kind: "actionable" }), "first exact original")
  await expect(w.send(fyi("same", { kind: "actionable" }), "DIFFERENT original")).rejects.toThrow("already used")
  const again = await w.send(fyi("same", { kind: "actionable" }), "first exact original")
  expect(again).toMatchObject({ recorded: true, duplicate: true, delivered: false })
  expect(w.prompts).toHaveLength(1)
  expect(w.prompts[0].id).toMatch(/^msg_/)
  expect(w.store.data.get(`${EVENT_PREFIX}ses_target:ses_sender:same`)).toMatchObject({ text: "first exact original", direct: true })
  expect(batchStates(w.store)).toEqual(["admitted"])
  // Immediate originals never become digest backlog.
  await w.digest.enable("ses_target", 5)
  expect(w.digest.pending("ses_target")).toBe(0)
  // Relay: stored at send time, a duplicate is not held twice, and the flush goes through its own manifest.
  const relayed = await w.send(fyi("kid", { kind: "actionable" }), "child report", "ses_kid")
  expect(relayed).toMatchObject({ held: true, recorded: true, receipt_id: "kid" })
  await expect(w.send(fyi("kid", { kind: "actionable" }), "changed child report", "ses_kid")).rejects.toThrow("already used")
  expect(await w.send(fyi("kid", { kind: "actionable" }), "child report", "ses_kid")).toMatchObject({ duplicate: true })
  expect(w.relay.pending("ses_kid")).toBe(1)
  await w.relay.toolStarted("ses_kid", "read")
  expect(w.prompts).toHaveLength(2)
  expect(w.prompts[1].id).toMatch(/^msg_/)
  expect(w.prompts[1].text).toContain("child report")
  expect(w.digest.pending("ses_target")).toBe(0)
})

test("5: supersession is one immutable decision: all named or none, also when the write fails or after restart", async () => {
  const w = world()
  await w.send(fyi("a"))
  await w.send(fyi("b"))
  const set = w.store.set
  w.store.set = async (key, value) => (key === `${EVENT_PREFIX}ses_target:ses_sender:c` ? Promise.reject(new Error("disk full")) : set(key, value))
  await expect(w.send(fyi("c", { supersedes: ["a", "b"] }))).rejects.toThrow("not recorded")
  expect(w.digest.inbox.list("ses_target", {}).items.map((item: any) => item.status)).toEqual(["pending", "pending"])
  w.store.set = set
  const result = await w.send(fyi("c", { supersedes: ["a", "b"] }))
  expect(result).toMatchObject({ supersession_applied: true, archived: ["ses_sender:a", "ses_sender:b"] })
  expect([...w.store.data.keys()].filter((key) => key.startsWith(STATE_PREFIX))).toEqual([])
  const restarted = world(w.store)
  await restarted.digest.restoreFromStorage()
  expect(restarted.digest.inbox.list("ses_target", {}).items.map((item: any) => [item.id, item.status, item.superseded_by, item.version])).toEqual([
    ["ses_sender:a", "archived", "ses_sender:c", 1],
    ["ses_sender:b", "archived", "ses_sender:c", 1],
    ["ses_sender:c", "pending", undefined, 0],
  ])
})

test("6: the whole typed event is bounded before any route, immediate and relayed included", async () => {
  const w = world()
  const receipt = fyi("large", { kind: "actionable", evidence: Array.from({ length: 20 }, (_, i) => ({ path: `/p/${i}/${"x".repeat(900)}` })) })
  await expect(w.send(receipt, "€".repeat(7999))).rejects.toThrow("exceed")
  await expect(w.send(receipt, "€".repeat(7999), "ses_kid")).rejects.toThrow("exceed")
  expect(w.prompts).toHaveLength(0)
  expect(w.relay.pending("ses_kid")).toBe(0)
})

test("7: read results and digest prompts are bounded as whole envelopes", async () => {
  const evidence = Array.from({ length: 4 }, () => ({ path: "x".repeat(4000) }))
  const probe = world()
  await probe.send(fyi("a", { evidence }), "x".repeat(8000))
  await probe.send(fyi("b", { evidence }), "x".repeat(8000))
  const first = await probe.digest.inbox.read("ses_target", ["ses_sender:a", "ses_sender:b"])
  const left = MAX_BATCH_BYTES - (first.items as any[]).reduce((sum, item) => sum + bytes(item), 0)
  const w = world()
  await w.send(fyi("a", { evidence: [...evidence, { path: "x".repeat(left - 12) }] }), "x".repeat(8000))
  await w.send(fyi("b", { evidence }), "x".repeat(8000))
  const read = await w.digest.inbox.read("ses_target", ["ses_sender:a", "ses_sender:b", "ses_sender:missing"])
  expect(bytes(read)).toBeLessThanOrEqual(MAX_BATCH_BYTES)
  expect(read.items.length + (read.remaining?.length ?? 0)).toBe(2)
  expect(read.missing).toEqual(["ses_sender:missing"])

  const receipt = fyi("a", { kind: "actionable", restrictions: Array.from({ length: 4 }, () => "x".repeat(4000)) })
  const sized = world()
  await sized.digest.hold({ to: "ses_target", from: "ses_sender", text: "x".repeat(8000), receipt })
  await sized.digest.hold({ to: "ses_target", from: "ses_sender", text: "x".repeat(8000), receipt: { ...receipt, id: "b" } })
  const events = [...sized.digest.registry.inbox.entries.get("ses_target")!.values()].map((entry) => entry.event)
  const room = MAX_BATCH_BYTES - bytes(batchText(events, { remaining: 0, oversize: [] }))
  const d = world()
  await d.digest.hold({ to: "ses_target", from: "ses_sender", text: "x".repeat(8000), receipt: { ...receipt, restrictions: [...receipt.restrictions!, "x".repeat(room - 3)] } })
  await d.digest.hold({ to: "ses_target", from: "ses_sender", text: "x".repeat(8000), receipt: { ...receipt, id: "b" } })
  await d.digest.flush("ses_target")
  await d.digest.flush("ses_target")
  expect(d.prompts.map((prompt) => prompt.metadata.crossSession.digest.count)).toEqual([1, 1])
  for (const prompt of d.prompts) expect(bytes(prompt)).toBeLessThanOrEqual(MAX_BATCH_BYTES)
})

test("7: entry IDs are bounded at the peer_inbox boundary", async () => {
  const ctx = makeMockCtx({ sessions: { ses_target: sessionInfo("ses_target") } }) as any
  const dispose = await plugin.setup(ctx)
  try {
    const tool = ctx._tools.find((tool: any) => tool.name === "peer_inbox")
    await expect(tool.execute({ operation: "read", ids: ["x".repeat(257)] }, { sessionID: "ses_target" })).rejects.toThrow("at most 256")
  } finally {
    await dispose()
  }
})

// ---- Second review (R2) ----

test("R2-1: settings and legacy held records recover in bounded resumable chunks; nothing reads as complete before they finish", async () => {
  const store = storage()
  for (let i = 0; i < 20_000; i++) {
    const id = String(i).padStart(5, "0")
    store.data.set(`digest:cfg:ses_a${id}`, { off: true })
    store.data.set(`digest:held:ses_a${id}:old`, { id: `old${id}`, to: `ses_a${id}`, from: "ses_worker", text: "quarantined completion", at: 1, completion: { source: "shell", state: "completed" } })
  }
  store.data.set("digest:cfg:ses_zzz", { off: true })
  store.data.set("digest:held:ses_zzz:peer", { id: "peer", to: "ses_zzz", from: "ses_worker", text: "legacy safety report", at: 1 })
  const w = world(store)
  // A plugin-option default applied before the stored "off" was read must not outlive it.
  await w.digest.enable("ses_zzz", 5, false)
  await w.digest.restoreFromStorage()
  expect(w.digest.registry.inbox.loaded).toBe(false)
  expect(await peerInboxOperation(w.digest.inbox, { operation: "status" }, { sessionID: "ses_zzz" })).toMatchObject({ complete: false })
  let calls = 0
  while (!w.digest.registry.inbox.loaded && calls++ < 10) await w.digest.restoreFromStorage()
  // The status operation above continued recovery too; at least one more bounded chunk was still needed.
  expect(calls).toBeGreaterThanOrEqual(1)
  expect(w.digest.registry.off.has("ses_zzz")).toBe(true)
  expect(w.digest.enabled("ses_zzz")).toBe(false)
  expect(store.data.has("digest:held:ses_zzz:peer")).toBe(false)
  expect(await peerInboxOperation(w.digest.inbox, { operation: "status" }, { sessionID: "ses_zzz" })).toMatchObject({ complete: true, pending: 1 })
  expect(w.digest.describe("ses_a00000")).toMatchObject({ legacy_completion_records: 1 })
  expect([...w.digest.registry.held.values()].flat()).toHaveLength(20_000)
  expect(w.prompts).toHaveLength(0)
})

test("R2-1: a failed prelude page is read again from its cursor without duplicating quarantine or imports", async () => {
  const store = storage()
  store.data.set("digest:held:ses_target:old", { id: "old", to: "ses_target", from: "ses_worker", text: "exact legacy body", at: 1 })
  store.data.set("digest:held:ses_target:done", { id: "done", to: "ses_target", from: "ses_worker", text: "completion", at: 1, completion: { source: "shell", state: "completed" } })
  const w = world(store)
  const remove = store.remove
  store.remove = async () => {
    throw new Error("old-key removal failed")
  }
  const scan = store.scan
  let fail = true
  store.scan = async (input) => {
    if (input.prefix === EVENT_PREFIX && fail) {
      fail = false
      throw new Error("scan failed")
    }
    return scan(input)
  }
  await expect(w.digest.restoreFromStorage()).rejects.toThrow("scan failed")
  await w.digest.restoreFromStorage()
  store.remove = remove
  expect(w.digest.registry.inbox.loaded).toBe(true)
  expect([...w.digest.registry.held.values()].flat().map((item) => item.id)).toEqual(["done"])
  expect(w.digest.inbox.list("ses_target", {}).items.map((item) => item.id)).toEqual(["ses_worker:old"])
})

test("R2-2: a normal typed report to a digest-mode parent joins its digest; urgent and digest-off still go now", async () => {
  const w = world()
  const relay = new ParentRelay(w.api, { graceMs: 1, divert: digestDivert(w.digest) })
  const send = (receipt: Receipt, priority?: "urgent") =>
    sendMessage(w.store, w.api, { to: "ses_target", text: `report ${receipt.id}`, receipt, ...(priority ? { priority, urgent_reason: "blocker: the parent must decide now" } : {}) }, { sessionID: "ses_kid" }, relay, { digest: w.digest }) as Promise<any>
  await w.digest.restoreFromStorage()
  await w.digest.enable("ses_target", 5)
  expect(await send(fyi("normal", { kind: "actionable" }))).toMatchObject({ held: true, recorded: true })
  // Held by the relay, the original is not yet a digest item: a foreground merge must stay exclusive.
  expect(w.digest.pending("ses_target")).toBe(0)
  await relay.toolStarted("ses_kid", "read")
  expect(w.prompts).toHaveLength(0)
  expect(w.digest.pending("ses_target")).toBe(1)
  expect(await send(fyi("urgent", { kind: "actionable" }), "urgent")).toMatchObject({ held: true })
  await relay.toolStarted("ses_kid", "read")
  expect(w.prompts.map((prompt) => prompt.text)).toEqual([expect.stringContaining("report urgent")])
  // The release is durable: a restarted process delivers it in the digest, once.
  const restarted = world(w.store)
  await restarted.digest.restoreFromStorage()
  expect(restarted.digest.pending("ses_target")).toBe(1)
  await restarted.digest.flush("ses_target")
  expect(restarted.prompts).toHaveLength(1)
  expect(restarted.prompts[0].text).toContain("report normal")
  await restarted.digest.disable("ses_target")
  expect(restarted.prompts).toHaveLength(1)
  // Digest off: delivered on its own now.
  const other = world()
  const plain = new ParentRelay(other.api, { graceMs: 1, divert: digestDivert(other.digest) })
  await other.digest.restoreFromStorage()
  await sendMessage(other.store, other.api, { to: "ses_target", text: "report off", receipt: fyi("off", { kind: "actionable" }) }, { sessionID: "ses_kid" }, plain, { digest: other.digest })
  await plain.toolStarted("ses_kid", "read")
  expect(other.prompts).toHaveLength(1)
})

test("R2-3: the first superseder survives a same-millisecond restart, at the same version", async () => {
  const store = storage()
  const at = () => 1_000_000
  const make = () => {
    const api = { get: async ({ sessionID }: any) => ({ id: sessionID, time: { idle: 0, updated: 1 } }), prompt: async () => {} }
    const digest = new Digest(freshRegistry(), api, store, () => {}, at)
    return { digest, send: (receipt: Receipt) => sendMessage(store, api, { to: "ses_target", text: "x", receipt }, { sessionID: "ses_sender" }, undefined, { digest }) as Promise<any> }
  }
  const first = make()
  await first.digest.restoreFromStorage()
  await first.send(fyi("p"))
  await first.send(fyi("z", { supersedes: ["p"] }))
  expect(await first.send(fyi("a", { supersedes: ["p", "z"] }))).toMatchObject({ supersession_applied: true, archived: ["ses_sender:z"] })
  const view = (digest: Digest) => digest.inbox.list("ses_target", {}).items.map((item) => [item.id, item.status, item.version, item.superseded_by])
  const before = view(first.digest)
  expect(before).toEqual([
    ["ses_sender:a", "pending", 0, undefined],
    ["ses_sender:p", "archived", 1, "ses_sender:z"],
    ["ses_sender:z", "archived", 1, "ses_sender:a"],
  ])
  const second = make()
  await second.digest.restoreFromStorage()
  expect(view(second.digest)).toEqual(before)
})

test("R2-4: a duplicate passive FYI reports the recipient's actual view, not an assumed unread one", async () => {
  const w = world()
  await w.digest.restoreFromStorage()
  expect(await w.send(fyi("seen"))).toMatchObject({ status: "pending", processed: false })
  await w.digest.inbox.read("ses_target", ["ses_sender:seen"])
  await w.digest.inbox.ack("ses_target", [{ id: "ses_sender:seen", expected_version: 0 }], "processed")
  const again = await w.send(fyi("seen"))
  expect(again).toMatchObject({ duplicate: true, status: "processed", version: 1, read_at: expect.any(String) })
  expect(again).not.toHaveProperty("processed")
  expect(again.note).not.toContain("has not read")
  expect(w.prompts).toHaveLength(0)
})

test("R3-1/R5: explicit choices during unfinished recovery are refused at once and change nothing; saved records outrank option defaults", async () => {
  const store = storage()
  // Enough settings for four bounded recovery chunks: each refused call below advances one.
  for (let i = 0; i < 65_000; i++) store.data.set(`digest:cfg:ses_a${String(i).padStart(5, "0")}`, { off: true })
  store.data.set("digest:cfg:ses_on", { off: true })
  store.data.set("digest:cfg:ses_off", { minutes: 7, last: 1 })
  const w = world(store)
  await w.digest.restoreFromStorage()
  // A plugin-option default, unpersisted, before its saved record is read.
  await w.digest.enable("ses_off", 5, false)
  await expect(w.digest.enable("ses_on", 5)).rejects.toThrow("unavailable until storage recovery completes")
  expect(w.digest.registry.inbox.loaded).toBe(false)
  await expect(w.digest.disable("ses_off")).rejects.toThrow("nothing was changed")
  expect(store.data.get("digest:cfg:ses_on")).toEqual({ off: true })
  expect(store.data.get("digest:cfg:ses_off")).toEqual({ minutes: 7, last: 1 })
  while (!w.digest.registry.inbox.loaded) await w.digest.restoreFromStorage()
  expect(w.digest.describe("ses_on").digest).toBe("off")
  expect(w.digest.describe("ses_off")).toMatchObject({ digest: "on", minutes: 7 })
  // Once recovery is complete, choices apply.
  await w.digest.enable("ses_on", 5)
  expect(w.digest.describe("ses_on")).toMatchObject({ digest: "on", minutes: 5 })
  expect(w.prompts).toHaveLength(0)
})

function deferred() {
  let resolve!: () => void
  let reject!: (error: unknown) => void
  const promise = new Promise<void>((yes, no) => void ((resolve = yes), (reject = no)))
  return { promise, resolve, reject }
}

test("R4-2: a write that lands and then throws leaves the saved choice live; an unreadable outcome is reported as unknown", async () => {
  const store = storage()
  store.data.set("digest:cfg:ses_zzz", { off: true })
  const w = world(store)
  while (!w.digest.registry.inbox.loaded) await w.digest.restoreFromStorage()
  const set = store.set
  store.set = async (key, value) => {
    await set(key, value)
    throw new Error("landed; result lost")
  }
  await expect(w.digest.enable("ses_zzz", 5)).rejects.toThrow("landed; result lost")
  expect(w.digest.describe("ses_zzz")).toMatchObject({ digest: "on", minutes: 5 })

  // Neither the write nor its readback is known: no mode is claimed until a later choice is saved.
  const get = store.get
  store.set = () => Promise.reject(new Error("no answer"))
  store.get = () => Promise.reject(new Error("unreadable"))
  await expect(w.digest.disable("ses_zzz")).rejects.toThrow("no answer")
  expect(w.digest.describe("ses_zzz").digest).toBe("unknown")
  store.set = set
  store.get = get
  await w.digest.disable("ses_zzz")
  expect(w.digest.describe("ses_zzz").digest).toBe("off")
  expect(store.data.get("digest:cfg:ses_zzz")).toEqual({ off: true })
})

test("R4/R5: an earlier choice, failed or successful, never lands over or clears a later one", async () => {
  const store = storage()
  store.data.set("digest:cfg:ses_zzz", { minutes: 7, last: 1 })
  const w = world(store)
  while (!w.digest.registry.inbox.loaded) await w.digest.restoreFromStorage()
  const entered = deferred()
  const pending = deferred()
  const set = store.set
  let first = true
  store.set = async (key, value) => {
    if (!first) return set(key, value)
    first = false
    entered.resolve()
    await pending.promise
  }
  const early = w.digest.enable("ses_zzz", 5).then(() => undefined, (error: Error) => error)
  await entered.promise
  // The later choice queues behind the earlier write instead of racing its response.
  const later = w.digest.disable("ses_zzz")
  pending.reject(new Error("earlier choice failed"))
  expect((await early)?.message).toBe("earlier choice failed")
  await later
  expect(w.digest.describe("ses_zzz").digest).toBe("off")
  expect(store.data.get("digest:cfg:ses_zzz")).toEqual({ off: true })

  // R5-2: an earlier successful write whose response is late cannot clear a later unknown outcome.
  const slow = deferred()
  const reached = deferred()
  let call = 0
  store.set = async (key, value) => {
    call++
    if (call === 1) {
      await set(key, value)
      reached.resolve()
      await slow.promise
      return
    }
    throw new Error("later write lost")
  }
  const get = store.get
  const success = w.digest.enable("ses_zzz", 5)
  await reached.promise
  const failing = w.digest.disable("ses_zzz").then(() => undefined, (error: Error) => error)
  store.get = async (key) => (call >= 2 ? Promise.reject(new Error("unreadable")) : get(key))
  slow.resolve()
  await success
  expect((await failing)?.message).toBe("later write lost")
  expect(w.digest.describe("ses_zzz").digest).toBe("unknown")
  store.get = get
  store.set = set
  expect(w.prompts).toHaveLength(0)
})

const MIN = 60_000
/** A recovered world with a clock, one normal report held under a 15-minute digest, and storage fault switches. */
async function held(now = { t: 10_000_000 }) {
  const store = storage()
  const prompts: any[] = []
  const api = { get: async ({ sessionID }: any) => ({ id: sessionID, time: { idle: 0, updated: 1 } }), prompt: async (input: any) => void prompts.push(input) }
  const digest = new Digest(freshRegistry(), api, store, () => {}, () => now.t)
  while (!digest.registry.inbox.loaded) await digest.restoreFromStorage()
  await digest.enable("ses_target", 15)
  await digest.hold({ to: "ses_target", from: "ses_sender", text: "routine report" })
  const faults = { set: undefined as undefined | "reject" | "land-then-throw", get: undefined as undefined | "reject" | unknown }
  const set = store.set
  const get = store.get
  store.set = async (key, value) => {
    if (faults.set === "reject") throw new Error("write failed")
    await set(key, value)
    if (faults.set === "land-then-throw") throw new Error("landed; result lost")
  }
  store.get = async (key) => (faults.get === "reject" ? Promise.reject(new Error("unreadable")) : faults.get !== undefined ? faults.get : get(key))
  const tick = async () => {
    now.t += 1 * MIN
    await digest.tick(0)
  }
  return { store, prompts, digest, faults, now, tick }
}

test("R5: settings transitions — known on/off/unknown × successful, failed and landed-then-thrown writes × readback", async () => {
  type Row = { name: string; choice: "enable" | "disable"; set?: "reject" | "land-then-throw"; get?: "reject" | unknown; mode: "on" | "off" | "unknown"; minutes?: number }
  const rows: Row[] = [
    { name: "on → off saved", choice: "disable", mode: "off" },
    { name: "on → off, write failed, old on read back", choice: "disable", set: "reject", mode: "on", minutes: 15 },
    { name: "on → off, landed then threw", choice: "disable", set: "land-then-throw", mode: "off" },
    { name: "on → off, write and readback failed", choice: "disable", set: "reject", get: "reject", mode: "unknown" },
    { name: "on → off, malformed readback minutes 0", choice: "disable", set: "reject", get: { minutes: 0, last: 1 }, mode: "unknown" },
    { name: "on → off, malformed readback off:false", choice: "disable", set: "reject", get: { off: false }, mode: "unknown" },
    { name: "on → off, malformed readback NaN-ish string", choice: "disable", set: "reject", get: { minutes: "15", last: 1 }, mode: "unknown" },
    { name: "on → off, readback absent keeps the live state", choice: "disable", set: "reject", get: null, mode: "on", minutes: 15 },
    { name: "on → 30, write failed, old read back", choice: "enable", set: "reject", mode: "on", minutes: 15 },
    { name: "on → 30, landed then threw", choice: "enable", set: "land-then-throw", mode: "on", minutes: 30 },
    { name: "on → 30, readback 61 minutes", choice: "enable", set: "reject", get: { minutes: 61, last: 1 }, mode: "unknown" },
  ]
  for (const row of rows) {
    const w = await held()
    w.faults.set = row.set
    w.faults.get = row.get
    const run = row.choice === "enable" ? w.digest.enable("ses_target", 30) : w.digest.disable("ses_target")
    const failed = await run.then(() => false, () => true)
    expect([row.name, failed]).toEqual([row.name, row.set !== undefined])
    expect([row.name, w.digest.describe("ses_target").digest]).toEqual([row.name, row.mode])
    if (row.minutes) expect([row.name, w.digest.describe("ses_target").minutes]).toEqual([row.name, row.minutes])
    // Ticks before the 15-minute interval never deliver; under unknown, nothing is delivered at all, ever.
    w.faults.set = undefined
    w.faults.get = undefined
    for (let i = 0; i < 5; i++) await w.tick()
    const expected = row.mode === "off" ? 1 : 0
    expect([row.name, w.prompts.length]).toEqual([row.name, expected])
    if (row.mode === "unknown") {
      expect([row.name, w.digest.pending("ses_target")]).toEqual([row.name, 1])
      w.now.t += 60 * MIN
      await w.digest.tick(0)
      expect([row.name, w.prompts.length]).toEqual([row.name, 0])
      expect(w.prompts.some((prompt) => prompt.text.includes("Digest mode is off"))).toBe(false)
    }
  }
})

test("R5-1: unknown never drains the backlog as off; urgent still goes alone; a later saved choice re-establishes delivery", async () => {
  const w = await held()
  w.faults.set = "reject"
  w.faults.get = "reject"
  await expect(w.digest.disable("ses_target")).rejects.toThrow("write failed")
  expect(w.digest.describe("ses_target")).toMatchObject({ digest: "unknown" })
  expect(w.digest.describe("ses_target")).not.toHaveProperty("minutes")
  w.faults.set = undefined
  w.faults.get = undefined
  for (let i = 0; i < 30; i++) await w.tick()
  expect(await w.digest.flush("ses_target")).toBe(0)
  expect(await w.digest.flush("ses_target", "off")).toBe(0)
  expect(w.prompts).toHaveLength(0)
  // A normal message to an unknown-mode recipient is held, with no claimed due time.
  const relayed = await sendMessage(w.store, { get: async ({ sessionID }: any) => ({ id: sessionID, time: { idle: 0, updated: 1 } }), prompt: async (input: any) => void w.prompts.push(input) }, { to: "ses_target", text: "second" }, { sessionID: "ses_sender" }, undefined, { digest: w.digest }) as any
  expect(relayed).toMatchObject({ held: true, due: "unknown" })
  expect(w.prompts).toHaveLength(0)
  // Urgent keeps its own delivery and never carries the backlog.
  const urgent = await sendMessage(w.store, { get: async ({ sessionID }: any) => ({ id: sessionID, time: { idle: 0, updated: 1 } }), prompt: async (input: any) => void w.prompts.push(input) }, { to: "ses_target", text: "act now", priority: "urgent", urgent_reason: "blocker: the target must decide now" }, { sessionID: "ses_sender" }, undefined, { digest: w.digest, sleep: async () => {}, intervalMs: 1, timeoutMs: 1 }) as any
  expect(urgent).toMatchObject({ delivered: true })
  expect(w.prompts).toHaveLength(1)
  expect(w.prompts[0].text).not.toContain("routine report")
  // A later saved off re-establishes a known policy: one bounded turn-off batch.
  await w.digest.disable("ses_target")
  expect(w.digest.describe("ses_target").digest).toBe("off")
  expect(w.prompts).toHaveLength(2)
  expect(w.prompts[1].text).toContain("routine report")
})

test("R5-3: malformed saved settings recovered at startup are unknown, not on or off, and hold their backlog", async () => {
  const store = storage()
  store.data.set("digest:cfg:ses_zero", { minutes: 0, last: 1 })
  store.data.set("digest:cfg:ses_extra", { off: true, minutes: 15 })
  store.data.set("digest:cfg:ses_valid", { minutes: 10 })
  const digest = new Digest(freshRegistry(), { get: async () => undefined, prompt: async () => {} }, store)
  while (!digest.registry.inbox.loaded) await digest.restoreFromStorage()
  expect(digest.describe("ses_zero").digest).toBe("unknown")
  expect(digest.describe("ses_extra").digest).toBe("unknown")
  expect(digest.describe("ses_valid")).toMatchObject({ digest: "on", minutes: 10 })
  // An unknown session is not given a plugin-option default either: enabled() reports it as holding.
  expect(digest.enabled("ses_zero")).toBe(true)
  expect(decodeSetting({ minutes: 15, last: Number.POSITIVE_INFINITY }, 0)).toEqual({ kind: "malformed" })
  expect(decodeSetting({ minutes: 15.5, last: 1 }, 0)).toEqual({ kind: "malformed" })
  expect(decodeSetting({ off: true }, 0)).toEqual({ kind: "off" })
  expect(decodeSetting(undefined, 0)).toEqual({ kind: "absent" })
})

test("R5: the digest timestamp write and an old turn-off batch never overwrite or bypass a newer choice", async () => {
  const w = await held()
  w.now.t += 16 * MIN
  // The due digest's timestamp write is held; a newer off choice queues behind it and wins.
  const gate = deferred()
  const reached = deferred()
  const set = w.store.set
  let held1 = true
  w.store.set = async (key, value) => {
    if (held1 && key === "digest:cfg:ses_target") {
      held1 = false
      reached.resolve()
      await gate.promise
    }
    return set(key, value)
  }
  const flushing = w.digest.flush("ses_target")
  await reached.promise
  const off = w.digest.disable("ses_target")
  gate.resolve()
  expect(await flushing).toBe(1)
  await off
  expect(w.store.data.get("digest:cfg:ses_target")).toEqual({ off: true })
  // An old turn-off batch does not run once a newer choice turned digest mode back on.
  await w.digest.enable("ses_target", 15)
  await w.digest.hold({ to: "ses_target", from: "ses_sender", text: "another" })
  expect(await w.digest.flush("ses_target", "off")).toBe(0)
  expect(w.prompts).toHaveLength(1)
})

test("R5-2 entrypoint: across plugin locations a later choice queues behind an earlier slow success, which cannot clear its unknown outcome", async () => {
  const key = "digest:cfg:ses_target"
  const open = async (share?: any) => {
    const ctx: any = makeMockCtx({ sessions: { ses_target: sessionInfo("ses_target", { time: { idle: 0, updated: 1 } }) } })
    if (share) {
      ctx.storage = share.storage
      ctx._store = share._store
      ctx.location = { directory: "/offline/second-location" }
    }
    const prompts: any[] = []
    ctx.session.prompt = async (input: any) => void prompts.push(input)
    const dispose = await plugin.setup(ctx)
    const run = async (input: any) => JSON.parse((await ctx._tools.find((tool: any) => tool.name === "opencode_meta").execute({ operation: "message_digest", ...input }, { sessionID: "ses_target" })).content)
    return { ctx, prompts, dispose, run }
  }
  const first = await open()
  const second = await open(first.ctx)
  try {
    const set = first.ctx.storage.set
    const get = first.ctx.storage.get
    const reached = deferred()
    const slow = deferred()
    let early = true
    first.ctx.storage.set = async (k: string, value: any) => {
      if (k !== key) return set(k, value)
      if (early) {
        early = false
        await set(k, value)
        reached.resolve()
        await slow.promise
        return
      }
      throw new Error("later disable outcome unavailable")
    }
    const earlier = first.run({ enabled: true, minutes: 5 }).then(() => undefined, (error: Error) => error)
    await reached.promise
    const later = second.run({ enabled: false }).then(() => undefined, (error: Error) => error)
    first.ctx.storage.get = async (k: string) => (k === key ? Promise.reject(new Error("later readback unavailable")) : get(k))
    slow.resolve()
    expect(await earlier).toBeUndefined()
    expect((await later)?.message).toContain("later disable")
    expect((await first.run({})).digest).toBe("unknown")
    expect((await second.run({})).digest).toBe("unknown")
    expect([...first.prompts, ...second.prompts]).toHaveLength(0)
  } finally {
    await second.dispose()
    await first.dispose()
  }
})

// ---- R6: the delivery policy holds at the native admission boundary, through the registered tools ----

async function registered() {
  delete (globalThis as any)[REGISTRY_KEY]
  const live = { time: { idle: 0, updated: 1 } }
  const ctx: any = makeMockCtx({ sessions: { ses_target: sessionInfo("ses_target", live), ses_sender: sessionInfo("ses_sender", live) } })
  const prompts: any[] = []
  ctx.session.prompt = async (input: any) => void prompts.push(structuredClone(input))
  const dispose = await plugin.setup(ctx)
  const run = async (name: string, input: any, sessionID = "ses_target") =>
    JSON.parse((await ctx._tools.find((tool: any) => tool.name === name).execute(input, { sessionID })).content)
  const choose = (enabled: boolean, minutes = 5) => run("opencode_meta", { operation: "message_digest", enabled, ...(enabled ? { minutes } : {}) })
  const mode = async () => (await run("opencode_meta", { operation: "message_digest" })).digest
  const digest = () => (globalThis as any)[REGISTRY_KEY] as { inbox: { batches: Map<string, Map<string, any>> } }
  const digestBatches = () => digest().inbox.batches.get("ses_target")?.values() ?? []
  const batches = () => [...digestBatches()].map((batch) => batch.state)
  return { ctx, prompts, run, choose, mode, batches, digestBatches, dispose }
}
const unknownChoice = (ctx: any) => {
  const set = ctx.storage.set, get = ctx.storage.get
  ctx.storage.set = async (key: string, value: any) => (key === "digest:cfg:ses_target" ? Promise.reject(new Error("new choice unknown")) : set(key, value))
  ctx.storage.get = async (key: string) => (key === "digest:cfg:ses_target" ? Promise.reject(new Error("new readback unknown")) : get(key))
}
const settledYet = async (promise: Promise<unknown>) => {
  let settled = false
  void promise.then(() => (settled = true), () => (settled = true))
  for (let i = 0; i < 20; i++) await Promise.resolve()
  return settled
}

for (const newer of ["on", "unknown"] as const) {
  test(`R6-1 (${newer}): a newer choice queues behind an admission already deciding, so it cannot complete before it`, async () => {
    const f = await registered()
    const entered = deferred()
    const finish = deferred()
    try {
      await f.choose(true, 15)
      await f.run("send_message", { to: "ses_target", text: "held report" }, "ses_sender")
      const getSession = f.ctx.session.get
      let lookups = 0
      f.ctx.session.get = async (input: any) => {
        const value = await getSession(input)
        if (input.sessionID === "ses_target" && ++lookups === 2) {
          entered.resolve()
          await finish.promise
        }
        return value
      }
      const off = f.choose(false)
      await entered.promise
      if (newer === "unknown") unknownChoice(f.ctx)
      const later = f.choose(true, 30).then(
        (result) => ({ result }),
        (error) => ({ error }),
      )
      // The later choice is queued behind the admission decision: it has not completed, and nothing was sent yet.
      expect(await settledYet(later)).toBe(false)
      expect(f.prompts).toHaveLength(0)
      finish.resolve()
      await off
      const outcome: any = await later
      // The turn-off admission was decided, under the policy then current, before the newer choice completed.
      expect(f.prompts).toHaveLength(1)
      expect(f.prompts[0].text).toContain("off")
      if (newer === "on") expect(outcome.result.digest).toBe("on")
      if (newer === "unknown") expect(String(outcome.error)).toContain("new choice unknown")
      expect(await f.mode()).toBe(newer)
      expect(f.batches()).toEqual(["admitted"])
    } finally {
      finish.resolve()
      await f.dispose?.()
      delete (globalThis as any)[REGISTRY_KEY]
    }
  })

  test(`R6-1 (${newer}): a policy changed before the admission gate withdraws the unsent batch; its original waits again`, async () => {
    const f = await registered()
    const reserving = deferred()
    const finish = deferred()
    try {
      await f.choose(true, 15)
      await f.run("send_message", { to: "ses_target", text: "held report" }, "ses_sender")
      const set = f.ctx.storage.set
      const get = f.ctx.storage.get
      f.ctx.storage.set = async (key: string, value: any) => {
        if (key.startsWith(BATCH_PREFIX) && value.state === "prepared") {
          reserving.resolve()
          await finish.promise
        }
        return set(key, value)
      }
      const off = f.choose(false)
      await reserving.promise
      if (newer === "unknown") unknownChoice(f.ctx)
      const later = f.choose(true, 30).catch((error: unknown) => error)
      await later
      finish.resolve()
      await off
      // Never sent under the superseded off policy; known unsent, so not unknown and not retried as it was framed.
      expect(f.prompts).toHaveLength(0)
      expect(f.batches()).toEqual(["withdrawn"])
      expect(await f.mode()).toBe(newer)
      expect(JSON.stringify(await f.run("peer_inbox", { operation: "status" }))).toContain('"actionable_pending":1')
      const withdrawn = [...f.digestBatches()][0]
      // A later explicit turn-off sends the original in a new batch, framed now, under a new native ID.
      f.ctx.storage.set = set
      f.ctx.storage.get = get
      await f.choose(false)
      expect(f.prompts).toHaveLength(1)
      expect(f.prompts[0].id).not.toBe(withdrawn.prompt_id)
      expect(f.prompts[0].text).toContain("held report")
      expect(f.batches().sort()).toEqual(["admitted", "withdrawn"])
    } finally {
      finish.resolve()
      await f.dispose?.()
      delete (globalThis as any)[REGISTRY_KEY]
    }
  })
}

test("R6-1: a turn-off batch that failed before admission is withdrawn under a newer policy, also after restore, never resent as framed", async () => {
  const w = world()
  while (!w.digest.registry.inbox.loaded) await w.digest.restoreFromStorage()
  await w.digest.enable("ses_target", 15)
  await w.digest.hold({ to: "ses_target", from: "ses_sender", text: "held across a policy change" })
  const get = w.api.get
  w.api.get = async () => undefined
  await w.digest.disable("ses_target")
  w.api.get = get
  expect(batchStates(w.store)).toEqual(["failed_before_admission"])
  await w.digest.enable("ses_target", 30)
  // A fresh process reads the known failure back and the newer saved choice.
  delete (globalThis as any)[REGISTRY_KEY]
  const again = world(w.store)
  while (!again.digest.registry.inbox.loaded) await again.digest.restoreFromStorage()
  const old = [...w.store.data.values()].find((value: any) => value?.state === "failed_before_admission")
  // The stale frame is durably withdrawn first; only then is the original framed anew, under a new native ID.
  await again.digest.flush("ses_target")
  expect(again.prompts).toHaveLength(1)
  expect(again.prompts[0].id).not.toBe(old.prompt_id)
  expect(again.prompts[0].text).toContain("held across a policy change")
  expect(again.prompts[0].text).not.toContain("is off")
  expect(batchStates(w.store).sort()).toEqual(["admitted", "withdrawn"])
})

// ---- R7: a stored batch is reused only under the policy that framed it; withdrawal must be durable first ----

/** A due/on-15 batch whose recipient lookup failed before any prompt, read back by a fresh process. */
async function knownUnsent(legacy = false) {
  const w = world()
  while (!w.digest.registry.inbox.loaded) await w.digest.restoreFromStorage()
  await w.digest.enable("ses_target", 15)
  await w.digest.hold({ to: "ses_target", from: "ses_sender", text: "held under on-15" })
  const get = w.api.get
  w.api.get = async () => undefined
  await w.digest.flush("ses_target")
  w.api.get = get
  const [key, old] = [...w.store.data].find(([key]) => key.startsWith(BATCH_PREFIX))!
  expect(old.state).toBe("failed_before_admission")
  expect(old.text).toContain("every 15 min")
  // A manifest stored before frames were recorded has none: never treated as matching.
  if (legacy) {
    delete old.frame
    w.store.data.set(key, old)
  }
  delete (globalThis as any)[REGISTRY_KEY]
  const again = world(w.store)
  while (!again.digest.registry.inbox.loaded) await again.digest.restoreFromStorage()
  return { again, key, old, originals: JSON.stringify([...w.store.data].filter(([key]) => key.startsWith(EVENT_PREFIX))) }
}

test("R7-2: an unchanged policy retries the known-unsent batch with its own ID and frame", async () => {
  const { again, old } = await knownUnsent()
  await again.digest.flush("ses_target")
  expect(again.prompts.map((prompt) => prompt.id)).toEqual([old.prompt_id])
  expect(batchStates(again.store)).toEqual(["admitted"])
})

for (const legacy of [false, true])
  for (const newer of ["off", "on30"] as const)
    test(`R7-2: a ${legacy ? "legacy " : ""}due/on-15 batch is durably withdrawn and reframed after a newer ${newer} choice`, async () => {
      const { again, key, old, originals } = await knownUnsent(legacy)
      if (newer === "off") await again.digest.disable("ses_target")
      else {
        await again.digest.enable("ses_target", 30)
        await again.digest.flush("ses_target")
      }
      expect(again.store.data.get(key).state).toBe("withdrawn")
      expect(again.prompts).toHaveLength(1)
      expect(again.prompts[0].id).not.toBe(old.prompt_id)
      expect(again.prompts[0].text).toContain("held under on-15")
      expect(again.prompts[0].text).not.toContain("every 15 min")
      expect(again.prompts[0].text).toContain(newer === "off" ? "Digest mode is off" : "every 30 min")
      expect(JSON.stringify([...again.store.data].filter(([key]) => key.startsWith(EVENT_PREFIX)))).toBe(originals)
    })

/** Storage faults on the withdrawal write only. */
const faults = {
  "rejected before the write": (data: Map<string, any>, key: string, value: any) => Promise.reject(new Error("not written")),
  "landed, then threw": (data: Map<string, any>, key: string, value: any) => (data.set(key, structuredClone(value)), Promise.reject(new Error("lost response"))),
  "landed differently, then threw": (data: Map<string, any>, key: string, value: any) => (data.set(key, { ...structuredClone(value), state: "torn" }), Promise.reject(new Error("torn write"))),
} as const

for (const [name, fault] of Object.entries(faults))
  for (const unreadable of [false, true])
    test(`R7-1: a withdrawal ${name}${unreadable ? " with unreadable readback" : ""} is confirmed only by an exact readback`, async () => {
      const { again, key, old } = await knownUnsent()
      const set = again.store.set, get = again.store.get
      again.store.set = async (target: string, value: any) => (target === key && value.state === "withdrawn" ? fault(again.store.data, target, value) : set(target, value))
      if (unreadable) again.store.get = async (target: string) => (target === key ? Promise.reject(new Error("unreadable")) : get(target))
      await again.digest.enable("ses_target", 30)
      await again.digest.flush("ses_target")
      const confirmed = name === "landed, then threw" && !unreadable
      expect(again.prompts).toHaveLength(confirmed ? 1 : 0)
      if (confirmed) {
        expect(again.prompts[0].id).not.toBe(old.prompt_id)
        expect(batchStates(again.store).sort()).toEqual(["admitted", "withdrawn"])
        return
      }
      // Unconfirmed: the old binding stays fenced; a second pass under the same fault opens nothing new either.
      await again.digest.flush("ses_target")
      expect(again.prompts).toHaveLength(0)
      expect([...again.store.data].filter(([target]) => target.startsWith(BATCH_PREFIX))).toHaveLength(1)
      // Once storage recovers, the withdrawal lands first and only then is the original framed anew.
      again.store.set = set
      again.store.get = get
      if (name === "landed differently, then threw") again.store.data.set(key, old)
      await again.digest.flush("ses_target")
      expect(again.prompts).toHaveLength(1)
      expect(again.prompts[0].id).not.toBe(old.prompt_id)
      expect(again.store.data.get(key).state).toBe("withdrawn")
    })

test("R7-1: an unconfirmed withdrawal at the admission guard stays fenced across a reload and is never relabelled unsent", async () => {
  const f = await registered()
  const reserving = deferred()
  const finish = deferred()
  try {
    await f.choose(true, 15)
    await f.run("send_message", { to: "ses_target", text: "held report" }, "ses_sender")
    const set = f.ctx.storage.set
    f.ctx.storage.set = async (key: string, value: any) => {
      if (key.startsWith(BATCH_PREFIX) && value.state === "withdrawn") throw new Error("withdrawal was not written")
      if (key.startsWith(BATCH_PREFIX) && value.state === "prepared") {
        reserving.resolve()
        await finish.promise
      }
      return set(key, value)
    }
    const off = f.choose(false)
    await reserving.promise
    await f.choose(true, 30)
    finish.resolve()
    await off
    expect(f.prompts).toHaveLength(0)
    expect(f.batches()).toEqual(["prepared"])
    // A later due flush opens no new attempt for the fenced original.
    await new Digest((globalThis as any)[REGISTRY_KEY], f.ctx.session, f.ctx.storage).flush("ses_target")
    expect(f.prompts).toHaveLength(0)
    // After a reload the unconfirmed attempt reads as unknown, never as unsent, and is not resent.
    delete (globalThis as any)[REGISTRY_KEY]
    const restored = new Digest(freshRegistry(), f.ctx.session, f.ctx.storage)
    while (!restored.registry.inbox.loaded) await restored.restoreFromStorage()
    await restored.flush("ses_target")
    expect(f.prompts).toHaveLength(0)
    expect([...restored.registry.inbox.batches.get("ses_target")!.values()].map((batch) => batch.state)).toEqual(["admission_unknown"])
  } finally {
    finish.resolve()
    await f.dispose?.()
  }
})

test("R7-1: after a confirmed withdrawal, a wall-clock rollback still restores the new admission as offered", async () => {
  const { again, key, old } = await knownUnsent()
  await again.digest.enable("ses_target", 30)
  const rolledBack = new Digest(again.digest.registry, again.api, again.store, undefined, () => old.created - 1000)
  await rolledBack.flush("ses_target")
  expect(again.prompts).toHaveLength(1)
  expect(again.store.data.get(key).state).toBe("withdrawn")
  delete (globalThis as any)[REGISTRY_KEY]
  const restored = new Digest(freshRegistry(), again.api, again.store)
  while (!restored.registry.inbox.loaded) await restored.restoreFromStorage()
  expect(restored.inbox.status("ses_target")).toMatchObject({ offered: 1, pending: 0 })
})
