// An immediate message (urgent, or any steer actually delivered now) carries the recipient's waiting digest originals
// after it in the same prompt: one manifest, one native ID, whole originals, bounded, fenced like any delivery.
import { expect, test } from "bun:test"
import { Digest, freshRegistry } from "./digest.ts"
import { BATCH_PREFIX, MAX_BATCH_BYTES, MAX_BATCH_EVENTS, bytes, type Receipt } from "./peer-inbox.ts"
import { ParentRelay } from "./relay.ts"
import { digestDivert } from "./lib.ts"
import { sendMessage } from "./send.ts"

const MIN = 60_000
const URGENT = { priority: "urgent", urgent_reason: "blocker: the controller must decide now" }
const act = (id: string, extra: Partial<Receipt> = {}): Receipt => ({ version: 1, kind: "actionable", id, ...extra })

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
type Store = ReturnType<typeof storage>

const known: Record<string, any> = {
  ses_ctl: { id: "ses_ctl", title: "Controller" },
  ses_w1: { id: "ses_w1", title: "Worker one" },
  ses_w2: { id: "ses_w2", title: "Worker two" },
  ses_kid: { id: "ses_kid", title: "Kid", parentID: "ses_ctl" },
}

async function world(store: Store = storage()) {
  let now = 1_000_000_000
  const prompts: any[] = []
  const api: any = {
    // Busy (updated ahead of idle): carrying never depends on the recipient looking idle.
    get: async ({ sessionID }: any) => known[sessionID] && { ...known[sessionID], time: { idle: 5, updated: 9 } },
    prompt: async (input: any) => void prompts.push(structuredClone(input)),
  }
  const registry = freshRegistry()
  const digest = new Digest(registry, api, store, () => {}, () => now)
  while (!registry.inbox.loaded) await digest.restoreFromStorage()
  const relay = new ParentRelay(api, { graceMs: 1, divert: digestDivert(digest) })
  const send = (from: string, text: string, extra: Record<string, unknown> = {}) =>
    sendMessage(store, api, { to: "ses_ctl", text, ...extra } as any, { sessionID: from }, relay, { digest, sleep: async () => {}, intervalMs: 1, timeoutMs: 1 }) as Promise<any>
  return { store, api, prompts, registry, digest, relay, send, advance: (ms: number) => (now += ms), now: () => now }
}

const batches = (store: Store) => [...store.data].filter(([key]) => key.startsWith(BATCH_PREFIX)).map(([, value]) => value)
const states = (store: Store) => batches(store).map((batch) => batch.state).sort()
const promptBytes = (p: any) => bytes({ sessionID: p.sessionID, id: p.id, text: p.text, delivery: p.delivery, metadata: p.metadata })
const count = (text: string, part: string) => text.split(part).length - 1
const settle = () => Bun.sleep(1)
function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((done) => (resolve = done))
  return { promise, resolve }
}

test("urgent first, then the held digest, in one prompt and one manifest; exactly once; passive FYIs untouched", async () => {
  const w = await world()
  await w.digest.enable("ses_ctl", 15)
  expect(await w.send("ses_w1", "passive note", { receipt: { version: 1, kind: "fyi", id: "p1", state: "progress" } })).toMatchObject({ passive: true })
  await w.send("ses_w2", "routine typed", { receipt: act("r1", { restrictions: ["exactly \"this\" — ünchanged"] }) })
  w.advance(MIN)
  await w.send("ses_w2", "routine untyped")
  w.advance(MIN)
  const last = w.registry.settings.get("ses_ctl")!.last
  const result = await w.send("ses_w1", "decide now", { ...URGENT, receipt: act("u1") })
  expect(result).toMatchObject({ delivered: true, recorded: true, receipt_id: "u1" })
  expect(w.prompts).toHaveLength(1)
  const [prompt] = w.prompts
  expect(prompt.id).toMatch(/^msg_/)
  expect(prompt.text).toStartWith('[cross-session message from ses_w1 "Worker one"]\n\n[urgent — blocker')
  const footer = prompt.text.indexOf('reply with send_message to "ses_w1"')
  expect(footer).toBeGreaterThan(prompt.text.indexOf("decide now"))
  expect(footer).toBeLessThan(prompt.text.indexOf("[cross-session digest: 2 routine messages"))
  expect(count(prompt.text, "routine typed")).toBe(1)
  expect(count(prompt.text, "routine untyped")).toBe(1)
  expect(prompt.text).toContain('- exactly "this" — ünchanged')
  expect(prompt.text).not.toContain("passive note")
  expect(prompt.text).toEndWith("reply with send_message to the sender's session ID if a reply is needed.")
  expect(prompt.metadata.crossSession).toMatchObject({ from: "ses_w1", priority: "urgent", receipt: { id: "u1" }, carried: { count: 2, remaining: 0, ids: ["ses_w2:r1", expect.stringMatching(/^ses_w2:/)] } })
  const [batch] = batches(w.store)
  expect(batches(w.store)).toHaveLength(1)
  expect(batch).toMatchObject({ state: "admitted", prompt_id: prompt.id, refs: ["ses_w1:u1", "ses_w2:r1", expect.any(String)], oversize: [] })
  expect(batch.frame).toContain('"carry"')
  expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ offered: 3, pending: 1, passive_pending: 1, actionable_pending: 0 })
  // The consumed carry restarts the interval; nothing is delivered again by ticks, native evidence or a resend.
  expect(w.registry.settings.get("ses_ctl")!.last).toBe(w.now())
  expect(w.now()).toBeGreaterThan(last)
  await w.digest.inbox.nativeSettled("ses_ctl", prompt.id, "delivered")
  w.advance(60 * MIN)
  await w.digest.tick(0)
  expect(await w.send("ses_w1", "decide now", { ...URGENT, receipt: act("u1") })).toMatchObject({ duplicate: true, delivered: false })
  await expect(w.send("ses_w1", "decide differently", { ...URGENT, receipt: act("u1") })).rejects.toThrow("already used")
  await w.digest.disable("ses_ctl")
  expect(w.prompts).toHaveLength(1)
})

test("no carry: queue delivery, unknown mode, incomplete recovery, nothing held; digest off still carries leftovers", async () => {
  const w = await world()
  await w.digest.enable("ses_ctl", 15)
  await w.send("ses_w2", "routine held")
  await w.send("ses_w1", "queued urgent", { ...URGENT, delivery: "queue" })
  w.registry.uncertain.add("ses_ctl")
  await w.send("ses_w1", "unknown-mode urgent", { ...URGENT, receipt: act("unk") })
  w.registry.uncertain.delete("ses_ctl")
  w.registry.inbox.loaded = false
  await w.send("ses_w1", "recovering urgent", URGENT)
  w.registry.inbox.loaded = true
  expect(w.prompts.map((p) => p.text.includes("routine held"))).toEqual([false, false, false])
  expect(w.prompts.every((p) => !p.metadata.crossSession.carried)).toBe(true)
  expect(w.digest.pending("ses_ctl")).toBe(1)
  // Turned off with more than one digest held: the remainder rides with the next immediate message, bounded.
  for (let i = 0; i < 24; i++) await w.send(i % 2 ? "ses_w1" : "ses_w2", `backlog ${i}`)
  await w.digest.disable("ses_ctl")
  expect(w.prompts.at(-1).metadata.crossSession.digest).toMatchObject({ count: 20, remaining: 5 })
  await w.send("ses_w1", "plain normal message now that digest is off")
  const last = w.prompts.at(-1)
  expect(last.text).toStartWith("[cross-session message from ses_w1")
  expect(last.text).toContain("Digest mode is off for this session now")
  expect(last.metadata.crossSession).toMatchObject({ priority: "normal", carried: { count: 5, remaining: 0 } })
  expect(w.digest.pending("ses_ctl")).toBe(0)
  // Nothing held: exactly the old untyped prompt, with no manifest of its own.
  const before = batches(w.store).length
  await w.send("ses_w1", "nothing to carry", URGENT)
  expect(w.prompts.at(-1).text).not.toContain("cross-session digest")
  expect(w.prompts.at(-1).id).toBeUndefined()
  expect(batches(w.store)).toHaveLength(before)
})

test("bounds: at most 20 originals with the typed immediate; the whole UTF-8/escaped envelope fits 48 KiB; the rest stay pending", async () => {
  const w = await world()
  await w.digest.enable("ses_ctl", 5)
  for (let i = 0; i < 25; i++) await w.send("ses_w2", `small ${i}`)
  await w.send("ses_w1", "typed urgent", { ...URGENT, receipt: act("cap") })
  expect(w.prompts[0].metadata.crossSession.carried).toMatchObject({ count: MAX_BATCH_EVENTS - 1, remaining: 6 })
  expect(batches(w.store)[0].refs).toHaveLength(MAX_BATCH_EVENTS)
  expect(w.digest.pending("ses_ctl")).toBe(6)

  const v = await world()
  await v.digest.enable("ses_ctl", 5)
  // Every character escaped or multibyte in JSON: about 15 KB of prompt per message.
  const heavy = (i: number) => `#${i}#` + '"\\😀é\n'.repeat(1300)
  for (let i = 0; i < 5; i++) await v.send("ses_w2", heavy(i))
  await v.send("ses_w1", "typed urgent", { ...URGENT, receipt: act("big", { unresolved: ['"quoted" ✓ '.repeat(300)] }) })
  const first = v.prompts[0]
  const carried = first.metadata.crossSession.carried.count
  expect(carried).toBeGreaterThan(0)
  expect(carried).toBeLessThan(5)
  expect(promptBytes(first)).toBeLessThanOrEqual(MAX_BATCH_BYTES)
  expect(first.metadata.crossSession.carried.remaining).toBe(5 - carried)
  // Messages that did not fit beside the urgent one stay pending (not retrieval_required) and go by the next digest.
  const left = v.digest.inbox.list("ses_ctl", { status: "pending" }).items
  expect(left).toHaveLength(5 - carried)
  expect(left.every((item: any) => item.delivery === undefined)).toBe(true)
  for (let i = 0; i < 6; i++) {
    v.advance(6 * MIN)
    await v.digest.tick(0)
  }
  expect(v.digest.pending("ses_ctl")).toBe(0)
  const all = v.prompts.map((p) => p.text).join("\n")
  for (let i = 0; i < 5; i++) expect(count(all, `#${i}#`)).toBe(1)
  expect(v.prompts.every((p) => promptBytes(p) <= MAX_BATCH_BYTES)).toBe(true)

  // An immediate original too large to leave room still goes now, alone; the held one stays pending.
  const u = await world()
  await u.digest.enable("ses_ctl", 5)
  await u.send("ses_w2", "small routine")
  await u.send("ses_w1", "huge", { ...URGENT, receipt: act("huge", { restrictions: Array.from({ length: 9 }, () => "r".repeat(4000)) }) })
  expect(u.prompts).toHaveLength(1)
  expect(u.prompts[0].text).not.toContain("small routine")
  expect(u.prompts[0].metadata.crossSession).not.toHaveProperty("carried")
  expect(u.digest.inbox.list("ses_ctl", { status: "pending" }).items).toMatchObject([{ status: "pending" }])
  expect(u.digest.inbox.list("ses_ctl", { status: "pending" }).items[0].delivery).toBeUndefined()
})

test("races: a digest already delivering is neither awaited nor taken over; a carry in flight blocks timer flushes", async () => {
  const w = await world()
  await w.digest.enable("ses_ctl", 5)
  await w.send("ses_w2", "routine A")
  const gate = deferred()
  const get = w.api.get
  let block = true
  w.api.get = async (input: any) => {
    if (block && input.sessionID === "ses_ctl") {
      block = false
      await gate.promise
    }
    return get(input)
  }
  const flushing = w.digest.flush("ses_ctl")
  await settle()
  expect(w.registry.flushing.has("ses_ctl")).toBe(true)
  await w.send("ses_w1", "urgent while flushing", URGENT)
  expect(w.prompts).toHaveLength(1)
  expect(w.prompts[0].text).not.toContain("routine A")
  gate.resolve()
  expect(await flushing).toBe(1)
  expect(w.prompts[1].text).toContain("routine A")
  expect(w.prompts[1].text).not.toContain("urgent while flushing")

  // A carry in flight holds the claim: flushes and ticks deliver nothing, a second urgent goes alone at once.
  await w.send("ses_w2", "routine B")
  const release = deferred()
  const prompt = w.api.prompt
  let first = true
  w.api.prompt = async (input: any) => {
    if (first) {
      first = false
      await release.promise
    }
    return prompt(input)
  }
  const carrying = w.send("ses_w1", "urgent one", URGENT)
  await settle()
  expect(first).toBe(false)
  expect(await w.digest.flush("ses_ctl")).toBe(0)
  w.advance(60 * MIN)
  await w.digest.tick(0)
  await w.send("ses_w1", "urgent two", URGENT)
  expect(w.prompts).toHaveLength(3)
  expect(w.prompts[2].text).not.toContain("routine B")
  release.resolve()
  await carrying
  expect(w.prompts).toHaveLength(4)
  expect(w.prompts[3].text.indexOf("urgent one")).toBeLessThan(w.prompts[3].text.indexOf("routine B"))
  await w.digest.tick(0)
  expect(w.prompts).toHaveLength(4)
})

for (const confirmed of [true, false]) {
  test(`settings change before admission: ${confirmed ? "durably withdrawn, then the urgent one goes alone" : "unconfirmed withdrawal stays fenced and unsent"}`, async () => {
    const w = await world()
    await w.digest.enable("ses_ctl", 15)
    await w.send("ses_w2", "routine A")
    await w.send("ses_w2", "routine B")
    const set = w.store.set
    const get = w.store.get
    const reserving = deferred()
    const finish = deferred()
    w.store.set = async (key: string, value: any) => {
      if (key.startsWith(BATCH_PREFIX) && value.state === "prepared" && value.frame?.includes("carry")) {
        reserving.resolve()
        await finish.promise
      }
      if (!confirmed && value?.state === "withdrawn") throw new Error("withdrawal write lost")
      return set(key, value)
    }
    const sending = w.send("ses_w1", "urgent", { ...URGENT, receipt: act("u") }).catch((error: unknown) => error)
    await reserving.promise
    await w.digest.enable("ses_ctl", 30)
    finish.resolve()
    const result: any = await sending
    if (confirmed) {
      expect(result).toMatchObject({ delivered: true })
      expect(w.prompts).toHaveLength(1)
      expect(w.prompts[0].text).not.toContain("routine")
      expect(w.prompts[0].metadata.crossSession).not.toHaveProperty("carried")
      expect(states(w.store)).toEqual(["admitted", "withdrawn"])
      expect(w.digest.pending("ses_ctl")).toBe(2)
      // The withdrawn frame is never replayed: the next digest is framed anew and never embeds the urgent message.
      await w.digest.disable("ses_ctl")
      expect(w.prompts).toHaveLength(2)
      expect(w.prompts[1].text).toContain("routine A")
      expect(w.prompts[1].text).not.toContain("[urgent")
    } else {
      expect(String(result)).toContain("not retried automatically")
      expect(w.prompts).toHaveLength(0)
      expect(w.digest.pending("ses_ctl")).toBe(0)
      w.store.set = set
      w.store.get = get
      // Restored, the fenced manifest reads as unknown: nothing is replayed, resent or re-carried.
      const again = await world(w.store)
      expect(again.digest.inbox.status("ses_ctl")).toMatchObject({ delivery_attempts_needing_attention: { admission_unknown: 1 } })
      expect(again.digest.pending("ses_ctl")).toBe(0)
      expect(await again.send("ses_w1", "urgent", { ...URGENT, receipt: act("u") })).toMatchObject({ duplicate: true })
      await again.digest.disable("ses_ctl")
      expect(again.prompts).toHaveLength(0)
    }
  })
}

test("storage: a manifest that cannot be stored sends nothing typed, and untyped text alone; the held stay pending", async () => {
  const w = await world()
  await w.digest.enable("ses_ctl", 15)
  await w.send("ses_w2", "routine A")
  const set = w.store.set
  w.store.set = async (key: string, value: any) => {
    if (key.startsWith(BATCH_PREFIX)) throw new Error("disk full")
    return set(key, value)
  }
  await expect(w.send("ses_w1", "typed urgent", { ...URGENT, receipt: act("t") })).rejects.toThrow("delivery record could not be stored")
  expect(w.prompts).toHaveLength(0)
  expect(await w.send("ses_w1", "untyped urgent", URGENT)).toMatchObject({ delivered: true })
  expect(w.prompts).toHaveLength(1)
  expect(w.prompts[0].text).not.toContain("routine A")
  expect(w.digest.pending("ses_ctl")).toBe(1)
  // Once storage works, resending the same typed receipt carries the held one.
  w.store.set = set
  await w.send("ses_w1", "typed urgent", { ...URGENT, receipt: act("t") })
  expect(w.prompts[1].text).toContain("routine A")
})

for (const typed of [true, false]) {
  test(`${typed ? "a typed" : "an untyped"} combined prompt that throws is admission_unknown as a whole: no fallback, resend or replay`, async () => {
    const w = await world()
    await w.digest.enable("ses_ctl", 15)
    await w.send("ses_w2", "routine A")
    await w.send("ses_w2", "routine B")
    const last = w.registry.settings.get("ses_ctl")!.last
    const prompt = w.api.prompt
    let attempts = 0
    w.api.prompt = async () => {
      attempts++
      throw new Error("reloading")
    }
    await expect(w.send("ses_w1", "urgent", { ...URGENT, ...(typed ? { receipt: act("u") } : {}) })).rejects.toThrow(typed ? "Receipt u was recorded, but whether ses_ctl received it is unknown" : "Whether ses_ctl received it is unknown")
    expect(attempts).toBe(1)
    w.api.prompt = prompt
    expect(w.digest.pending("ses_ctl")).toBe(0)
    expect(w.registry.settings.get("ses_ctl")!.last).toBe(last)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ pending: typed ? 3 : 2, delivery_attempts_needing_attention: { admission_unknown: 1 } })
    w.advance(60 * MIN)
    await w.digest.tick(0)
    await w.digest.disable("ses_ctl")
    expect(w.prompts).toHaveLength(0)
    // Exact native evidence settles the whole combination as admitted; still nothing is sent again.
    const [batch] = batches(w.store)
    expect(await w.digest.inbox.nativeSettled("ses_ctl", batch.prompt_id, "delivered")).toBe(true)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ offered: typed ? 3 : 2, pending: 0 })
    expect(w.prompts).toHaveLength(0)
  })
}

test("crash after the combined manifest is stored: restore reads it unknown and never replays or re-carries", async () => {
  const w = await world()
  await w.digest.enable("ses_ctl", 15)
  await w.send("ses_w2", "routine A")
  let snapshot: Store | undefined
  w.api.prompt = async () => {
    snapshot = storage()
    for (const [key, value] of w.store.data) snapshot.data.set(key, structuredClone(value))
    return new Promise(() => {})
  }
  void w.send("ses_w1", "urgent", { ...URGENT, receipt: act("u") })
  await settle()
  const again = await world(snapshot!)
  expect(again.digest.inbox.status("ses_ctl")).toMatchObject({ pending: 2, delivery_attempts_needing_attention: { admission_unknown: 1 } })
  expect(again.digest.pending("ses_ctl")).toBe(0)
  expect(await again.send("ses_w1", "urgent", { ...URGENT, receipt: act("u") })).toMatchObject({ duplicate: true })
  await again.send("ses_w2", "later urgent", URGENT)
  expect(again.prompts[0].text).not.toContain("routine A")
  await again.digest.disable("ses_ctl")
  expect(again.prompts).toHaveLength(1)
  const [batch] = batches(again.store)
  await again.digest.inbox.nativeSettled("ses_ctl", batch.prompt_id, "delivered")
  expect(again.digest.inbox.status("ses_ctl")).toMatchObject({ offered: 2, pending: 0 })
})

test("a combined manifest that failed before admission is withdrawn by the next digest, never replaying the urgent text", async () => {
  const w = await world()
  await w.digest.enable("ses_ctl", 15)
  await w.send("ses_w2", "routine A")
  const set = w.store.set
  const get = w.api.get
  let gone = false
  w.store.set = async (key: string, value: any) => {
    if (key.startsWith(BATCH_PREFIX) && value.state === "prepared" && value.frame?.includes("carry")) gone = true
    return set(key, value)
  }
  w.api.get = async (input: any) => (gone && input.sessionID === "ses_ctl" ? undefined : get(input))
  await expect(w.send("ses_w1", "urgent text", { ...URGENT, receipt: act("u") })).rejects.toThrow("Receipt u was recorded but not sent: ses_ctl was not found")
  gone = false
  w.store.set = set
  expect(w.digest.pending("ses_ctl")).toBe(1)
  expect(await w.digest.flush("ses_ctl")).toBe(1)
  expect(w.prompts).toHaveLength(1)
  expect(w.prompts[0].text).toContain("routine A")
  expect(w.prompts[0].text).not.toContain("urgent text")
  expect(states(w.store)).toEqual(["admitted", "withdrawn"])
  // Durably withdrawn, the typed original is known unsent: resending the same receipt now delivers it, once.
  await w.send("ses_w1", "urgent text", { ...URGENT, receipt: act("u") })
  expect(w.prompts).toHaveLength(2)
  expect(w.prompts[1].text).toContain("urgent text")
  expect(await w.send("ses_w1", "urgent text", { ...URGENT, receipt: act("u") })).toMatchObject({ duplicate: true })
})

test("parent relay: an urgent typed or untyped report carries the parent's digest when delivered, never at hold time; diversion and merge unchanged", async () => {
  const w = await world()
  await w.digest.enable("ses_ctl", 15)
  await w.send("ses_w1", "routine one")
  expect(await w.send("ses_kid", "kid untyped urgent", URGENT)).toMatchObject({ held: true })
  expect(w.prompts).toHaveLength(0)
  expect(w.digest.pending("ses_ctl")).toBe(1)
  await w.relay.toolStarted("ses_kid", "bash")
  expect(w.prompts).toHaveLength(1)
  expect(w.prompts[0].text).toStartWith("[cross-session message from ses_kid")
  expect(w.prompts[0].text.indexOf("kid untyped urgent")).toBeLessThan(w.prompts[0].text.indexOf("routine one"))
  expect(w.prompts[0].metadata.crossSession.carried).toMatchObject({ count: 1 })

  await w.send("ses_w2", "routine two")
  await w.send("ses_kid", "kid typed urgent", { ...URGENT, receipt: act("k1") })
  await w.relay.toolStarted("ses_kid", "bash")
  expect(w.prompts).toHaveLength(2)
  expect(w.prompts[1].text.indexOf("kid typed urgent")).toBeLessThan(w.prompts[1].text.indexOf("routine two"))
  expect(batches(w.store).find((batch) => batch.prompt_id === w.prompts[1].id).refs[0]).toBe("ses_kid:k1")

  // A normal report is still diverted into the digest; an urgent final report is still merged, carrying nothing.
  await w.send("ses_kid", "kid progress")
  await w.relay.toolStarted("ses_kid", "bash")
  expect(w.prompts).toHaveLength(2)
  expect(w.digest.pending("ses_ctl")).toBe(1)
  await w.send("ses_kid", "kid final urgent", URGENT)
  await w.relay.turnEnded("ses_kid")
  const event: any = {
    tool: "subagent",
    status: "completed",
    result: { output: { sessionID: "ses_kid", status: "completed", output: "Summary." }, content: "Summary.", metadata: { sessionID: "ses_kid" } },
  }
  w.relay.subagentFinished(event)
  expect(event.result.output.output).toContain("kid final urgent")
  expect(event.result.output.output).not.toContain("kid progress")
  await Bun.sleep(5)
  expect(w.prompts).toHaveLength(2)
  expect(w.digest.pending("ses_ctl")).toBe(1)
})
