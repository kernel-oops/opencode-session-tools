import { afterEach, describe, expect, test } from "bun:test"
import plugin from "./index.ts"
import {
  cfgKey,
  completionHook,
  completionItem,
  Digest,
  digestText,
  excerpt,
  freshRegistry,
  GATHER_MS,
  HELD_PREFIX,
  readCompletionMode,
  readDigestOption,
  REGISTRY_KEY,
  type Registry,
  type SyntheticEvent,
} from "./digest.ts"
import { digestDivert } from "./lib.ts"
import { EVENT_PREFIX } from "./peer-inbox.ts"
import { ParentRelay } from "./relay.ts"
import { readPriority, readUrgentReason, sendMessage, URGENT_CATEGORIES } from "./send.ts"
import { makeMockCtx, sessionInfo } from "../opencode-recall-lite/mock-ctx.ts"

const MIN = 60_000


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

/** A controller (ses_ctl) and two workers; `idle` says whether the controller looks idle to `get`. */
function world(options: { start?: number; registry?: Registry; storage?: ReturnType<typeof memoryStorage> } = {}) {
  let now = options.start ?? 1_000_000_000
  let idle = true
  const prompts: any[] = []
  const known: Record<string, any> = {
    ses_ctl: { id: "ses_ctl", title: "Release coordinator" },
    ses_w1: { id: "ses_w1", title: "Worker one" },
    ses_w2: { id: "ses_w2", title: "Worker two" },
    ses_kid: { id: "ses_kid", title: "Controller's subagent", parentID: "ses_ctl" },
  }
  const sessions = {
    get: async ({ sessionID }: { sessionID: string }) => {
      const base = known[sessionID]
      if (!base) return undefined
      // Mid-run: updated ahead of idle (send.ts's own test for this). Idle sessions are not polled by send here,
      // because the wake observation is only for steer to an idle target and tests pass a no-wait sleep.
      return sessionID === "ses_ctl" ? { ...base, time: idle ? { idle: 5, updated: 5 } : { idle: 5, updated: 9 } } : base
    },
    prompt: async (input: any) => void prompts.push(input),
  }
  const storage = options.storage ?? memoryStorage()
  const registry = options.registry ?? freshRegistry()
  const digest = new Digest(registry, sessions, storage, () => {}, () => now)
  const wake = { digest, sleep: async () => {}, intervalMs: 1, timeoutMs: 1 }
  const send = (from: string, text: string, extra: Record<string, unknown> = {}) =>
    sendMessage(storage, sessions, { to: "ses_ctl", text, ...extra } as any, { sessionID: from }, undefined, wake) as Promise<any>
  return {
    prompts,
    sessions,
    storage,
    registry,
    digest,
    send,
    known,
    advance: (ms: number) => (now += ms),
    now: () => now,
    setIdle: (value: boolean) => (idle = value),
  }
}

/** Held originals live in the peer inbox and survive delivery; nothing is written under the old held prefix. */
const heldKeys = (storage: ReturnType<typeof memoryStorage>) => [...storage.store.keys()].filter((key) => key.startsWith(EVENT_PREFIX) || key.startsWith(HELD_PREFIX))

describe("priority", () => {
  test("defaults to normal, and is recorded in metadata for every recipient", async () => {
    expect(readPriority(undefined)).toBe("normal")
    expect(readPriority("urgent")).toBe("urgent")
    expect(() => readPriority("high")).toThrow('priority must be "urgent" or "normal"')
    const w = world()
    await w.send("ses_w1", "progress")
    expect(w.prompts[0].metadata).toEqual({ crossSession: { from: "ses_w1", priority: "normal" } })
  })

  test("a recipient not in digest mode is unchanged: both priorities are delivered at once, with the same result as before", async () => {
    const w = world()
    w.setIdle(false)
    const normal = await w.send("ses_w1", "progress")
    const urgent = await w.send("ses_w2", "blocked", { priority: "urgent", urgent_reason: "blocker: test needs the recipient to act now" })
    expect(normal).toEqual({ delivered: true, to: "ses_ctl", title: "Release coordinator", delivery: "steer" })
    expect(urgent).toEqual({ delivered: true, to: "ses_ctl", title: "Release coordinator", delivery: "steer" })
    expect(w.prompts.map((prompt) => prompt.metadata.crossSession.priority)).toEqual(["normal", "urgent"])
    expect(heldKeys(w.storage)).toEqual([])
  })
})

describe("urgent reason", () => {
  test("accepts only a known category, a colon and a real explanation", () => {
    expect(readUrgentReason("blocker: the deploy cannot proceed without your decision")).toEqual({
      category: "blocker",
      reason: "blocker: the deploy cannot proceed without your decision",
    })
    expect(readUrgentReason("Awaited-Reply : the hash you asked for at 21:30")?.category).toBe("awaited-reply")
    expect(readUrgentReason(undefined)).toBeUndefined()
    expect(readUrgentReason("")).toBeUndefined()
    expect(readUrgentReason("blocker")).toBeUndefined()
    expect(readUrgentReason("blocker: short")).toBeUndefined()
    expect(readUrgentReason("fyi: review started on the S7 branch")).toBeUndefined()
    expect(readUrgentReason("the deploy cannot proceed without your decision")).toBeUndefined()
    expect([...URGENT_CATEGORIES]).toEqual(["blocker", "failure", "decision", "permission", "incident", "awaited-reply"])
  })

  test("urgent without a valid reason is held as normal for a digest recipient, and the sender is told why", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 15)
    const result = await w.send("ses_w1", "review started on S7", { priority: "urgent" })
    expect(w.prompts).toHaveLength(0)
    expect(result).toMatchObject({ delivered: false, held: true, digest: true, priority: "normal", downgraded: true })
    expect(result.downgrade_note).toContain("urgent_reason")
    const vague = await w.send("ses_w2", "sent the helper path directly", { priority: "urgent", urgent_reason: "fyi: sent it" })
    expect(vague).toMatchObject({ held: true, priority: "normal", downgraded: true })
    expect(w.digest.pending("ses_ctl")).toBe(2)
  })

  test("urgent with a valid reason is delivered at once, shows the reason and records it", async () => {
    const w = world()
    w.setIdle(false)
    await w.digest.enable("ses_ctl", 15)
    const result = await w.send("ses_w1", "PROJ-551 accepted at d4f86f76", {
      priority: "urgent",
      urgent_reason: "awaited-reply: the accepted hash you asked for, needed for the host re-integration",
    })
    expect(result).toMatchObject({ delivered: true, priority: "urgent" })
    expect(result.downgraded).toBeUndefined()
    expect(w.prompts).toHaveLength(1)
    expect(w.prompts[0].text).toContain("[urgent — awaited-reply: the accepted hash you asked for")
    expect(w.prompts[0].metadata.crossSession).toEqual({
      from: "ses_w1",
      priority: "urgent",
      urgent: "awaited-reply: the accepted hash you asked for, needed for the host re-integration",
    })
  })

  test("a downgraded urgent message to a recipient not in digest mode is still delivered at once, marked as asked", async () => {
    const w = world()
    w.setIdle(false)
    const result = await w.send("ses_w1", "progress", { priority: "urgent" })
    expect(result).toMatchObject({ delivered: true, downgraded: true })
    expect(w.prompts[0].metadata.crossSession).toEqual({ from: "ses_w1", priority: "normal", asked: "urgent" })
  })
})

describe("digest mode", () => {
  test("a normal message is held, persisted, and the sender is told when the digest is due", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 15)
    const result = await w.send("ses_w1", "batch 3 of 7 done")
    expect(w.prompts).toHaveLength(0)
    expect(result).toMatchObject({ delivered: false, held: true, digest: true, priority: "normal", to: "ses_ctl" })
    expect(result.note).toContain("next message digest")
    expect(result.note).toContain('priority "urgent"')
    expect(typeof result.due_in_minutes).toBe("number")
    expect(Date.parse(result.due_at)).toBe(w.now() + 15 * MIN)
    expect(w.digest.pending("ses_ctl")).toBe(1)
    expect(heldKeys(w.storage)).toHaveLength(1)
  })

  test("an urgent message to a recipient with nothing held is delivered exactly as without a digest", async () => {
    const w = world()
    w.setIdle(false)
    await w.digest.enable("ses_ctl")
    const result = await w.send("ses_w1", "migration failed on prod", { priority: "urgent", urgent_reason: "blocker: test needs the recipient to act now" })
    expect(result).toEqual({ delivered: true, to: "ses_ctl", title: "Release coordinator", delivery: "steer", priority: "urgent" })
    expect(w.prompts).toHaveLength(1)
    expect(w.prompts[0].text).toStartWith('[cross-session message from ses_w1 "Worker one"]')
    expect(w.prompts[0].text).not.toContain("cross-session digest")
  })

  test("the digest is delivered as one prompt once the interval has passed, oldest first, each item framed", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 15)
    w.advance(1 * MIN)
    await w.send("ses_w2", "second worker: tests green")
    w.advance(3 * MIN)
    await w.send("ses_w1", "first worker: PR opened")
    w.advance(10 * MIN) // 14 min since enabling
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(0)
    w.advance(1 * MIN) // 15 min
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    const text: string = w.prompts[0].text
    expect(text).toStartWith("[cross-session digest: 2 routine messages from other sessions, held since")
    expect(text).toContain("every 15 min")
    expect(text).toContain('--- 1 of 2: from ses_w2 "Worker two" at ')
    expect(text).toContain('--- 2 of 2: from ses_w1 "Worker one" at ')
    expect(text.indexOf("tests green")).toBeLessThan(text.indexOf("PR opened"))
    expect(text).toContain("not from your user")
    expect(w.prompts[0]).toMatchObject({ sessionID: "ses_ctl", delivery: "steer", metadata: { crossSession: { digest: { count: 2, from: ["ses_w2", "ses_w1"] } } } })
    expect(w.digest.pending("ses_ctl")).toBe(0)
    expect(heldKeys(w.storage)).toHaveLength(2)
    expect(await w.digest.inbox.status("ses_ctl")).toMatchObject({ offered: 2 })
    // The next interval counts from this delivery.
    await w.send("ses_w1", "more")
    w.advance(14 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    w.advance(1 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(2)
  })

  test("after a quiet spell a burst still travels together: the digest waits a short gather window", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 15)
    w.advance(120 * MIN)
    await w.send("ses_w1", "a")
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(0)
    w.advance(GATHER_MS - 1)
    await w.send("ses_w2", "b")
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(0)
    w.advance(1)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    expect(w.prompts[0].metadata.crossSession.digest.count).toBe(2)
  })

  test("an idle recipient is woken once by the digest; a busy one gets it as a single steer", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 5)
    await w.send("ses_w1", "a")
    await w.send("ses_w2", "b")
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    expect(w.prompts[0]).toMatchObject({ delivery: "steer", metadata: { crossSession: { digest: { woke: true } } } })

    w.setIdle(false)
    await w.send("ses_w1", "c")
    await w.send("ses_w2", "d")
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(2)
    expect(w.prompts[1]).toMatchObject({ delivery: "steer", metadata: { crossSession: { digest: { woke: false, count: 2 } } } })
  })

  test("an urgent message carries the held digest after it in the same prompt and restarts the interval", async () => {
    const w = world()
    w.setIdle(false)
    await w.digest.enable("ses_ctl", 15)
    await w.send("ses_w1", "routine one")
    w.advance(1 * MIN)
    await w.send("ses_w2", "routine two")
    w.advance(1 * MIN)
    const result = await w.send("ses_w1", "need your decision on the schema change", { priority: "urgent", urgent_reason: "blocker: test needs the recipient to act now" })
    expect(result).toEqual({ delivered: true, to: "ses_ctl", title: "Release coordinator", delivery: "steer", priority: "urgent" })
    expect(w.prompts).toHaveLength(1)
    const text: string = w.prompts[0].text
    expect(text).toStartWith('[cross-session message from ses_w1 "Worker one"]\n\n[urgent — blocker')
    // The urgent frame is complete (with its peer footer) before the carried digest begins.
    expect(text.indexOf("reply with send_message to \"ses_w1\"")).toBeLessThan(text.indexOf("[cross-session digest: 2 routine messages"))
    expect(text).toContain("delivered now with the message above]")
    expect(text.indexOf("routine one")).toBeLessThan(text.indexOf("routine two"))
    expect(w.prompts[0].metadata.crossSession).toMatchObject({ from: "ses_w1", priority: "urgent", carried: { count: 2, remaining: 0, from: ["ses_w1", "ses_w2"] } })
    expect(w.digest.pending("ses_ctl")).toBe(0)
    expect(w.registry.settings.get("ses_ctl")!.last).toBe(w.now())
    w.advance(60 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
  })

  test("a failed digest prompt may have been admitted: it is kept as unknown and never resent automatically", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 5)
    await w.send("ses_w1", "a")
    let fail = true
    const original = w.sessions.prompt
    w.sessions.prompt = async (input: any) => {
      if (fail) throw new Error("reloading")
      return original(input)
    }
    w.advance(5 * MIN)
    await w.digest.tick(0)
    expect(w.digest.pending("ses_ctl")).toBe(0)
    expect(heldKeys(w.storage)).toHaveLength(1)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ pending: 1, delivery_attempts_needing_attention: { admission_unknown: 1 } })
    fail = false
    w.advance(60 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(0)
  })

  test("settings and held messages survive a restart and are delivered once due", async () => {
    const first = world()
    await first.digest.enable("ses_ctl", 10)
    await first.send("ses_w1", "before restart")
    // A new process: fresh registry, same storage.
    const second = world({ start: first.now() + 3 * MIN, storage: first.storage })
    await second.digest.restoreFromStorage()
    expect(second.digest.enabled("ses_ctl")).toBe(true)
    expect(second.registry.settings.get("ses_ctl")!.minutes).toBe(10)
    expect(second.digest.pending("ses_ctl")).toBe(1)
    await second.digest.tick(0)
    expect(second.prompts).toHaveLength(0)
    second.advance(7 * MIN)
    await second.digest.tick(0)
    expect(second.prompts).toHaveLength(1)
    expect(second.prompts[0].text).toContain("before restart")
    expect(heldKeys(second.storage)).toHaveLength(1)
  })

  test("two plugin copies sharing the registry deliver a digest exactly once, and restore only once", async () => {
    const a = world()
    const registry = a.registry
    const b = world({ registry, storage: a.storage, start: a.now() })
    await a.digest.enable("ses_ctl", 5)
    await a.send("ses_w1", "x")
    await b.send("ses_w2", "y")
    expect(a.digest.pending("ses_ctl")).toBe(2)
    a.advance(5 * MIN)
    b.advance(5 * MIN)
    await Promise.all([a.digest.tick(0), b.digest.tick(0), a.digest.flush("ses_ctl"), b.digest.flush("ses_ctl")])
    expect(a.prompts.length + b.prompts.length).toBe(1)
    const delivered = [...a.prompts, ...b.prompts][0]
    expect(delivered.metadata.crossSession.digest.count).toBe(2)
    // Restoring in either copy afterwards (a reload) re-adds nothing.
    await Promise.all([a.digest.restoreFromStorage(), b.digest.restoreFromStorage()])
    expect(a.digest.pending("ses_ctl")).toBe(0)
  })

  test("turning digest mode off delivers anything held at once and is remembered", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 15)
    await w.send("ses_w1", "held")
    const { had, flushed } = await w.digest.disable("ses_ctl")
    expect({ had, flushed }).toEqual({ had: true, flushed: 1 })
    expect(w.prompts).toHaveLength(1)
    expect(w.prompts[0].text).toContain("Digest mode is off for this session now")
    expect(await w.storage.get(cfgKey("ses_ctl"))).toEqual({ off: true })
    const after = await w.send("ses_w1", "now direct")
    expect(after).toMatchObject({ delivered: true })
    expect(w.prompts).toHaveLength(2)
  })

  test("digestText names the Claude Code reply tool for a Claude Code recipient", () => {
    const text = digestText([{ id: "1", to: "ses_ctl", from: "ses_w1", text: "x", at: 0, recipientIsClaudeCode: true }], { minutes: 15 })
    expect(text).toContain("mcp__opencode__send_message")
    expect(readDigestOption(["ses_a"])).toEqual(new Map([["ses_a", 15]]))
    expect(readDigestOption({ ses_a: 30, ses_b: 999 })).toEqual(new Map([["ses_a", 30], ["ses_b", 15]]))
  })
})

describe("subagent relay into a parent in digest mode", () => {
  function relayWorld() {
    const w = world()
    const relay = new ParentRelay(w.sessions, { graceMs: 5, divert: digestDivert(w.digest) })
    const hold = (text: string, priority: "urgent" | "normal") =>
      relay.hold("ses_kid", {
        parentID: "ses_ctl",
        text,
        framed: `[framed] ${text}`,
        metadata: { crossSession: { from: "ses_kid", priority } },
        delivery: "steer",
        priority,
        fromTitle: "Controller's subagent",
      })
    return { ...w, relay, hold }
  }

  test("a normal mid-task message joins the digest; an urgent one is relayed on its own", async () => {
    const w = relayWorld()
    await w.digest.enable("ses_ctl", 15)
    w.hold("progress", "normal")
    await w.relay.toolStarted("ses_kid", "bash")
    expect(w.prompts).toHaveLength(0)
    expect(w.digest.pending("ses_ctl")).toBe(1)
    w.hold("stuck: need a decision", "urgent")
    await w.relay.toolStarted("ses_kid", "bash")
    await Bun.sleep(5)
    expect(w.prompts).toHaveLength(1)
    expect(w.prompts[0].text).toBe("[framed] stuck: need a decision")
    expect(w.digest.pending("ses_ctl")).toBe(1)
  })

  test("a parent not in digest mode is unchanged", async () => {
    const w = relayWorld()
    w.hold("progress", "normal")
    await w.relay.toolStarted("ses_kid", "bash")
    expect(w.prompts.map((prompt) => prompt.text)).toEqual(["[framed] progress"])
  })
})

describe("plugin: opt-in through opencode_meta, options, reload", () => {
  afterEach(() => {
    delete (globalThis as any)[REGISTRY_KEY]
  })

  function ctxFor(options: Record<string, unknown> = {}) {
    const ctx: any = makeMockCtx({
      options,
      sessions: { ses_ctl: sessionInfo("ses_ctl", { title: "Controller" }), ses_w1: sessionInfo("ses_w1", { title: "Worker" }) },
    })
    const prompts: any[] = []
    ctx.session.prompt = async (input: any) => void prompts.push(input)
    return { ctx, prompts }
  }

  const call = async (ctx: any, name: string, input: unknown, sessionID: string) =>
    JSON.parse((await ctx._tools.find((tool: any) => tool.name === name).execute(input, { sessionID })).content)

  test("a session turns digest mode on and off for itself; the setting is persisted", async () => {
    delete (globalThis as any)[REGISTRY_KEY]
    const { ctx, prompts } = ctxFor()
    const cleanup = await plugin.setup(ctx)
    expect(await call(ctx, "opencode_meta", { operation: "message_digest" }, "ses_ctl")).toMatchObject({ digest: "off", held: 0 })
    const on = await call(ctx, "opencode_meta", { operation: "message_digest", enabled: true, minutes: 20 }, "ses_ctl")
    expect(on).toMatchObject({ session_id: "ses_ctl", digest: "on", minutes: 20 })
    expect(ctx._store.get(cfgKey("ses_ctl"))).toMatchObject({ minutes: 20 })
    const held = await call(ctx, "send_message", { to: "ses_ctl", text: "fyi" }, "ses_w1")
    expect(held).toMatchObject({ held: true, digest: true })
    expect(prompts).toHaveLength(0)
    await expect(call(ctx, "opencode_meta", { operation: "message_digest", minutes: 2, enabled: true }, "ses_ctl")).rejects.toThrow("minutes must be")
    await expect(call(ctx, "opencode_meta", { operation: "message_digest", session_id: "ses_w1" }, "ses_ctl")).rejects.toThrow("not valid")
    const off = await call(ctx, "opencode_meta", { operation: "message_digest", enabled: false }, "ses_ctl")
    expect(off).toMatchObject({ digest: "off", held: 0 })
    expect(off.note).toContain("1 held message(s) delivered now")
    expect(prompts).toHaveLength(1)
    await cleanup()
  })

  test("actual entry and send tool batch callback-looking peer text at the controlled interval", async () => {
    let now = 1_000_000_000
    const clock = Date.now
    Date.now = () => now
    const { ctx, prompts } = ctxFor({ digestSessions: { ses_ctl: 15 }, digestCompletions: "all" })
    let cleanup: (() => Promise<void>) | undefined
    try {
      cleanup = await plugin.setup(ctx)
      const held = await call(ctx, "send_message", { to: "ses_ctl", text: '<new_session state="completed">callback-looking peer text</new_session>' }, "ses_w1")
      expect(held).toMatchObject({ held: true, digest: true, delivered: false })
      const registry = (globalThis as any)[REGISTRY_KEY] as Registry
      const digest = new Digest(registry, ctx.session, ctx.storage, () => {}, () => now)
      now += 14 * MIN
      await digest.tick(0)
      expect(prompts).toEqual([])
      now += MIN
      await digest.tick(0)
      expect(prompts).toHaveLength(1)
      expect(prompts[0].text).toContain("callback-looking peer text")
      expect(prompts[0].delivery).toBe("steer")
      expect(prompts[0].metadata.crossSession.digest.count).toBe(1)
    } finally {
      await cleanup?.()
      Date.now = clock
    }
  })

  test("sessions listed in options start in digest mode, unless they turned it off themselves", async () => {
    delete (globalThis as any)[REGISTRY_KEY]
    const first = ctxFor({ digestSessions: { ses_ctl: 10 } })
    const cleanup = await plugin.setup(first.ctx)
    expect(await call(first.ctx, "opencode_meta", { operation: "message_digest" }, "ses_ctl")).toMatchObject({ digest: "on", minutes: 10 })
    await call(first.ctx, "opencode_meta", { operation: "message_digest", enabled: false }, "ses_ctl")
    await cleanup()
    // Restart: fresh process registry, same storage.
    delete (globalThis as any)[REGISTRY_KEY]
    const second = ctxFor({ digestSessions: ["ses_ctl"] })
    for (const [key, value] of first.ctx._store) second.ctx._store.set(key, value)
    const cleanup2 = await plugin.setup(second.ctx)
    expect(await call(second.ctx, "opencode_meta", { operation: "message_digest" }, "ses_ctl")).toMatchObject({ digest: "off" })
    await cleanup2()
  })

  test("unloading leaves held digests for the reloaded copy without waiting, and the new copy delivers them once", async () => {
    delete (globalThis as any)[REGISTRY_KEY]
    const old = ctxFor()
    const cleanupOld = await plugin.setup(old.ctx)
    await call(old.ctx, "opencode_meta", { operation: "message_digest", enabled: true, minutes: 5 }, "ses_ctl")
    await call(old.ctx, "send_message", { to: "ses_ctl", text: "held across reload" }, "ses_w1")
    // During a reload OpenCode answers a prompt only once the reload is over: unloading must not wait on one.
    old.ctx.session.prompt = () => new Promise(() => {})
    await cleanupOld()
    const fresh = ctxFor()
    for (const [key, value] of old.ctx._store) fresh.ctx._store.set(key, value)
    const cleanupNew = await plugin.setup(fresh.ctx)
    const registry = (globalThis as any)[REGISTRY_KEY] as Registry
    const held = [...registry.inbox.entries.get("ses_ctl")!.values()]
    expect(held).toHaveLength(1)
    registry.settings.get("ses_ctl")!.last -= 5 * 60_000
    held[0]!.event.at -= GATHER_MS
    registry.lastTick = 0
    const digest = new Digest(registry, fresh.ctx.session, fresh.ctx.storage)
    await digest.tick(0)
    await digest.tick(0)
    expect(fresh.prompts).toHaveLength(1)
    expect(fresh.prompts[0].text).toContain("held across reload")
    await cleanupNew()
  })
})

describe("native completions bypass digest; old ledger parsing", () => {
  afterEach(() => {
    delete (globalThis as any)[REGISTRY_KEY]
  })

  const subagent = (id: string, extra: Partial<SyntheticEvent> & { state?: string } = {}): SyntheticEvent => ({
    sessionID: "ses_ctl",
    messageID: id,
    source: "subagent",
    description: "review the migration",
    text: `<subagent sessionID="ses_kid" state="${extra.state ?? "completed"}" description="review the migration">\nThe migration is safe.   Two nits.\n</subagent>`,
    metadata: { source: "subagent", childID: "ses_kid", agent: "reviewer", state: extra.state ?? "completed" },
    active: false,
    wake: true,
    ...extra,
  })
  const shell = (id: string, exit: number): SyntheticEvent => ({
    sessionID: "ses_ctl",
    messageID: id,
    source: "shell",
    description: "bun test",
    text: `<shell id="job_1" state="completed" command="bun test">\n${"x".repeat(400)}\n12 pass ${exit ? "3 fail" : "0 fail"}\n</shell>`,
    metadata: { source: "shell", shellID: "sh_1", jobID: "job_1", state: "completed", truncated: false, exit },
    active: false,
    wake: true,
  })

  test("reads subagent and shell completions into short digest items, and ignores other notifications", () => {
    expect(excerpt("<subagent a=\"b\">\n one\n two \n</subagent>", "head")).toBe("one two")
    expect(excerpt("y".repeat(300), "tail", 10)).toBe(`…${"y".repeat(9)}`)
    expect(completionItem(subagent("msg_a"))).toEqual({
      id: "msg_a",
      to: "ses_ctl",
      from: "ses_kid",
      text: "The migration is safe. Two nits.",
      completion: { source: "subagent", state: "completed", agent: "reviewer", description: "review the migration" },
    })
    const failing = completionItem(shell("msg_b", 1))!
    expect(failing).toMatchObject({ from: "job_1", completion: { source: "shell", agent: "bun test", exit: 1 } })
    expect(failing.text.endsWith("12 pass 3 fail")).toBe(true)
    expect(failing.text.length).toBeLessThanOrEqual(240)
    expect(completionItem({ sessionID: "ses_ctl", messageID: "m", text: "restart notice", metadata: { source: "restart" }, wake: true })).toBeUndefined()
    expect(readCompletionMode(undefined)).toBe("off")
    expect(readCompletionMode(false)).toBe("off")
    expect(readCompletionMode("successes")).toBe("off")
  })

  test("all compatibility options and modes leave native and other events byte-for-byte unchanged", async () => {
    const w = world()
    await w.digest.enable("ses_ctl", 15)
    const before = JSON.stringify([...w.storage.store])
    for (const option of [undefined, true, "all", "successes", "off", false]) {
      expect(readCompletionMode(option)).toBe("off")
      for (const mode of ["all", "successes", "off"] as const)
      for (const source of ["subagent", "shell", "new_session", "reminder", "unknown", "user"])
      for (const active of [true, false]) for (const wake of [true, false])
      for (const metadata of [{ state: "completed", exit: 0 }, { state: "error", exit: 1 }, { state: "cancelled" }, { signal: "SIGTERM" }, { timeout: true }]) {
        const event: SyntheticEvent = { sessionID: "ses_ctl", messageID: "native", source, text: "original", active, wake, metadata }
        const original = JSON.stringify(event)
        await completionHook(w.digest, mode)(event)
        expect(JSON.stringify(event)).toBe(original)
      }
    }
    expect(await w.digest.holdCompletion({ ...completionItem(subagent("direct"))! })).toBe(false)
    expect(JSON.stringify([...w.storage.store])).toBe(before)
    expect(w.digest.pending("ses_ctl")).toBe(0)
    expect(w.prompts).toEqual([])
  })

  test("actual plugin setup never registers synthetic or prompt hooks, regardless of old options", async () => {
    for (const option of [undefined, true, "all", "successes", "off", false]) {
      delete (globalThis as any)[REGISTRY_KEY]
      const ctx: any = makeMockCtx({ options: { digestSessions: ["ses_ctl"], digestCompletions: option }, sessions: { ses_ctl: sessionInfo("ses_ctl") } })
      ctx.session.hook = async () => { throw new Error("must never register a session hook") }
      const cleanup = await plugin.setup(ctx)
      const registry = (globalThis as any)[REGISTRY_KEY] as Registry
      expect(registry.held.size).toBe(0)
      expect(ctx._tools.map((tool: any) => tool.name)).toEqual(["opencode_meta", "send_message", "peer_inbox"])
      await cleanup()
    }
    delete (globalThis as any)[REGISTRY_KEY]
    const old: any = makeMockCtx()
    delete old.session.hook
    await (await plugin.setup(old))()
  })

  const seed = async (storage: ReturnType<typeof memoryStorage>, now: number) => {
    await storage.set(cfgKey("ses_ctl"), { minutes: 15, last: now })
    await storage.set(cfgKey("ses_off"), { off: true })
    for (const event of [subagent("old_sub"), shell("old_shell", 2), { ...subagent("old_owner"), source: "new_session" }]) {
      const item = { ...completionItem(event)!, at: now - 60 * MIN }
      await storage.set(`${HELD_PREFIX}ses_ctl:${item.id}`, item)
    }
  }

  test("legacy quarantine excludes records from deadlines, due delivery, urgent delivery and off flush", async () => {
    const w = world()
    await seed(w.storage, w.now())
    const legacy = [...w.storage.store].filter(([key]) => key.startsWith(HELD_PREFIX))
    await w.digest.restoreFromStorage()
    expect(w.digest.describe("ses_ctl")).toMatchObject({ held: 0, legacy_completion_records: 3, held_completions: 3, minutes: 15 })
    expect(w.digest.dueAt("ses_ctl")).toBeUndefined()
    expect(w.digest.nextDelivery("ses_ctl")).toBe(w.now() + 15 * MIN)
    expect(w.registry.off.has("ses_off")).toBe(true)
    expect(digestText(w.registry.held.get("ses_ctl")!, { minutes: 15 })).toBe("")
    await w.digest.tick(0)
    expect(w.prompts).toEqual([])
    // Runtime completion injection cannot turn peer hold into native interception.
    const peer = await w.digest.hold({ to: "ses_ctl", from: "ses_w1", text: "<subagent>finished</subagent>", completion: { source: "shell" } } as any)
    expect(peer).not.toHaveProperty("completion")
    expect(w.digest.pending("ses_ctl")).toBe(1)
    w.advance(15 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(1)
    expect(w.prompts[0].metadata.crossSession.digest).toMatchObject({ count: 1, from: ["ses_w1"] })
    expect(w.prompts[0].metadata.crossSession.digest).not.toHaveProperty("completions")
    await w.send("ses_w1", "peer progress")
    const urgent = await w.send("ses_w2", "need action", { priority: "urgent", urgent_reason: "blocker: recipient must act now" })
    expect(urgent).not.toHaveProperty("carried")
    // The peer message is carried with the urgent one; legacy records never are.
    expect(w.prompts.at(-1).text).toContain("peer progress")
    expect(w.prompts.at(-1).text).not.toContain("old_sub")
    expect((await w.digest.disable("ses_ctl")).flushed).toBe(0)
    expect([...w.storage.store].filter(([key]) => key.startsWith(HELD_PREFIX))).toEqual(legacy)
    const last = w.registry.settings.get("ses_ctl")?.last
    w.advance(60 * MIN)
    await w.digest.tick(0)
    expect(w.prompts).toHaveLength(2)
    expect(w.registry.settings.get("ses_ctl")?.last).toBe(last)
  })

  test("two plugin copies warn once, preserve storage and off choices through cleanup and restart", async () => {
    const storage = memoryStorage()
    await seed(storage, Date.now())
    const snapshot = () => JSON.stringify([...storage.store].filter(([key]) => key.startsWith("digest:")))
    const original = snapshot()
    const warnings: unknown[] = []
    const warn = console.warn
    console.warn = (...args) => { warnings.push(args) }
    try {
      const ctx: any = makeMockCtx({ options: { digestSessions: ["ses_off"] } })
      ctx.storage = storage
      ctx.session.prompt = async () => { throw new Error("no quarantine prompt") }
      ctx.session.synthetic = async () => { throw new Error("no replay") }
      const cleanupA = await plugin.setup(ctx)
      const cleanupB = await plugin.setup(ctx)
      expect(warnings).toHaveLength(1)
      expect(JSON.stringify(warnings)).not.toContain("old_sub")
      expect((globalThis as any)[REGISTRY_KEY].off.has("ses_off")).toBe(true)
      await cleanupA(); await cleanupB()
      expect(snapshot()).toBe(original)
      delete (globalThis as any)[REGISTRY_KEY]
      const cleanupRestart = await plugin.setup(ctx)
      expect(warnings).toHaveLength(2)
      expect(snapshot()).toBe(original)
      await cleanupRestart()
    } finally { console.warn = warn }
  })

  test("actual delivered/cancelled events retire only matching legacy records, never replay them", async () => {
    const ctx: any = makeMockCtx()
    const storage = memoryStorage()
    await seed(storage, Date.now())
    await storage.set(`${HELD_PREFIX}ses_ctl:peer_preserved`, { id: "peer_preserved", to: "ses_ctl", from: "ses_w1", text: "peer", at: Date.now() })
    ctx.storage = storage
    ctx.session.synthetic = async () => { throw new Error("cancelled input must not be resurrected") }
    const cleanup = await plugin.setup(ctx)
    for (const [type, inboxID] of [["session.inbox.cancelled", "old_sub"], ["session.inbox.delivered", "old_shell"]]) {
      ctx.emit({ type, created: 1, data: { sessionID: "ses_ctl", inboxID } })
      await ctx.settle()
      expect(storage.store.has(`${HELD_PREFIX}ses_ctl:${inboxID}`)).toBe(false)
    }
    expect(storage.store.has(`${HELD_PREFIX}ses_ctl:old_owner`)).toBe(true)
    // A held legacy peer message was imported as an inbox original under its own ID; its old key went only after that.
    const imported = `${EVENT_PREFIX}ses_ctl:ses_w1:peer_preserved`
    expect(storage.store.get(imported)).toMatchObject({ id: "peer_preserved", text: "peer", legacy: true, passive: false })
    expect(storage.store.has(`${HELD_PREFIX}ses_ctl:peer_preserved`)).toBe(false)
    ctx.emit({ type: "session.inbox.delivered", created: 1, data: { sessionID: "ses_ctl", inboxID: "peer_preserved" } })
    await ctx.settle()
    expect(storage.store.has(imported)).toBe(true)
    expect((globalThis as any)[REGISTRY_KEY].held.get("ses_ctl").map((item: any) => item.id)).toEqual(["old_owner"])
    await cleanup()
  })

  test("a failed peer flush keeps the peer original beside unchanged quarantined records", async () => {
    const w = world()
    await seed(w.storage, w.now())
    await w.digest.restoreFromStorage()
    const old = JSON.stringify([...w.storage.store].filter(([key]) => key.startsWith(HELD_PREFIX)))
    const peer = await w.digest.hold({ to: "ses_ctl", from: "ses_w1", text: "peer" })
    w.sessions.prompt = async () => { throw new Error("fixture admission failure") }
    w.advance(15 * MIN)
    expect(await w.digest.flush("ses_ctl")).toBe(0)
    expect(w.digest.inbox.status("ses_ctl")).toMatchObject({ pending: 1, delivery_attempts_needing_attention: { admission_unknown: 1 } })
    expect(w.digest.describe("ses_ctl").legacy_completion_records).toBe(3)
    expect(JSON.stringify([...w.storage.store].filter(([key]) => key.startsWith(HELD_PREFIX)))).toBe(old)
    expect(w.storage.store.get(`${EVENT_PREFIX}ses_ctl:${peer.ref}`)).toMatchObject({ text: "peer" })
  })

  test("failed or absent storage removal preserves durable evidence and reports the limitation", async () => {
    for (const missing of [false, true]) {
      const storage = memoryStorage()
      await seed(storage, 100)
      const logs: string[] = []
      const api = { get: async () => undefined, prompt: async () => { throw new Error("no migration") } }
      const store = { ...storage, remove: missing ? undefined : async () => { throw new Error("disk failure") } }
      const digest = new Digest(freshRegistry(), api, store, (message) => logs.push(message))
      await digest.restoreFromStorage()
      expect(digest.inboxSettled("ses_ctl", "unknown")).toBe(false)
      expect(digest.inboxSettled("ses_ctl", "old_sub")).toBe(true)
      await Promise.resolve()
      expect(storage.store.has(`${HELD_PREFIX}ses_ctl:old_sub`)).toBe(true)
      expect(logs.some((message) => message.includes("remains in durable storage"))).toBe(true)
      const restarted = new Digest(freshRegistry(), api, storage)
      await restarted.restoreFromStorage()
      expect(restarted.describe("ses_ctl").legacy_completion_records).toBe(3)
    }
  })
})
