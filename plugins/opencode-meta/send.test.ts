import { describe, expect, test } from "bun:test"
import { upsertSession } from "../opencode-recall-lite/session-index.ts"
import { FailureLog, frame, resolveTarget, sendMessage } from "./send.ts"

function memoryStorage() {
  const store = new Map<string, unknown>()
  return {
    get: async (key: string) => store.get(key),
    set: async (key: string, value: unknown) => void store.set(key, value),
    remove: async (key: string) => void store.delete(key),
    scan: async ({ prefix, after, limit }: { prefix: string; after?: string; limit: number }) => {
      const keys = [...store.keys()].filter((key) => key.startsWith(prefix) && (!after || key > after)).sort()
      const page = keys.slice(0, limit)
      return { entries: page.map((key) => ({ key, value: store.get(key) })), next: keys.length > limit ? page.at(-1) : undefined }
    },
  }
}

function fixture() {
  const storage = memoryStorage()
  const known: Record<string, { id: string; title: string; parentID?: string }> = {
    ses_sender: { id: "ses_sender", title: "Implement and merge PROJ-132" },
    ses_target: { id: "ses_target", title: "Complete PROJ 129 implementation and arrange review" },
    ses_other: { id: "ses_other", title: "Complete PROJ 130 batches" },
  }
  const prompts: any[] = []
  const sessions = {
    get: async ({ sessionID }: { sessionID: string }) => known[sessionID],
    prompt: async (input: any) => void prompts.push(input),
  }
  return { storage, sessions, prompts, known }
}

async function index(storage: any, known: Record<string, { id: string; title: string }>) {
  for (const session of Object.values(known)) await upsertSession(storage, { ...session, created: 1, updated: 1 })
}

describe("send_message", () => {
  test("queues a framed message to a session found by title, with a reply address", async () => {
    const { storage, sessions, prompts, known } = fixture()
    await index(storage, known)
    const result = await sendMessage(storage, sessions, { to: "PROJ 129", text: "Tell me when it is merged." }, { sessionID: "ses_sender" })
    expect(result).toEqual({ delivered: true, to: "ses_target", title: known.ses_target.title, delivery: "steer" })
    expect(prompts).toHaveLength(1)
    expect(prompts[0]).toMatchObject({ sessionID: "ses_target", delivery: "steer", metadata: { crossSession: { from: "ses_sender" } } })
    expect(prompts[0].text).toContain('[cross-session message from ses_sender "Implement and merge PROJ-132"]')
    expect(prompts[0].text).toContain("Tell me when it is merged.")
    expect(prompts[0].text).toContain('reply with send_message to "ses_sender"')
  })

  test("queue delivery is available for messages that can wait for the next turn", async () => {
    const { storage, sessions, prompts } = fixture()
    await sendMessage(storage, sessions, { to: "ses_target", text: "no rush", delivery: "queue" }, { sessionID: "ses_sender" })
    expect(prompts[0].delivery).toBe("queue")
  })

  test("accepts a session ID directly and refuses unknown IDs", async () => {
    const { storage, sessions, prompts } = fixture()
    await sendMessage(storage, sessions, { to: "ses_target", text: "hi" }, { sessionID: "ses_sender" })
    expect(prompts[0].sessionID).toBe("ses_target")
    await expect(resolveTarget(storage, sessions, "ses_missing")).rejects.toThrow("No session ses_missing")
  })

  test("asks for an ID when a title is ambiguous, and never messages itself", async () => {
    const { storage, sessions, known } = fixture()
    await index(storage, known)
    await expect(resolveTarget(storage, sessions, "Complete PROJ")).rejects.toThrow("matches 2 sessions")
    await expect(sendMessage(storage, sessions, { to: "ses_sender", text: "hi" }, { sessionID: "ses_sender" })).rejects.toThrow(
      "That is this session",
    )
  })

  test("holds a message to the sender's own parent for the relay, but sends to other ancestors and peers", async () => {
    const { storage, sessions, prompts, known } = fixture()
    Object.assign(known, {
      ses_root: { id: "ses_root", title: "Root" },
      ses_mid: { id: "ses_mid", title: "Controller", parentID: "ses_root" },
      ses_leaf: { id: "ses_leaf", title: "Worker", parentID: "ses_mid" },
    })
    const held: any[] = []
    const relay = { hold: (childID: string, message: any) => void held.push({ childID, ...message }) }
    const result = await sendMessage(storage, sessions, { to: "ses_mid", text: "A done" }, { sessionID: "ses_leaf" }, relay)
    expect(result).toMatchObject({ delivered: false, held: true, to: "ses_mid" })
    expect(held).toMatchObject([{ childID: "ses_leaf", parentID: "ses_mid", text: "A done", delivery: "steer" }])
    await sendMessage(storage, sessions, { to: "ses_root", text: "fyi" }, { sessionID: "ses_leaf" }, relay)
    await sendMessage(storage, sessions, { to: "ses_leaf", text: "also check X" }, { sessionID: "ses_mid" }, relay)
    expect(prompts.map((prompt) => prompt.sessionID)).toEqual(["ses_root", "ses_leaf"])
  })

  test("warns a parent that messaging its own subagent does not bring the result back", async () => {
    const { storage, sessions, known } = fixture()
    Object.assign(known, {
      ses_parent: { id: "ses_parent", title: "Parent" },
      ses_kid: { id: "ses_kid", title: "Kid", parentID: "ses_parent" },
    })
    const toChild = await sendMessage(storage, sessions, { to: "ses_kid", text: "more work" }, { sessionID: "ses_parent" })
    expect(toChild).toMatchObject({ delivered: true, to: "ses_kid" })
    expect((toChild as any).note).toContain('sessionID "ses_kid"')
    const toPeer = await sendMessage(storage, sessions, { to: "ses_target", text: "hi" }, { sessionID: "ses_parent" })
    expect((toPeer as any).note).toBeUndefined()
  })

  test("the frame names the exact reply tool for a Claude Code recipient, which also has Claude's own SendMessage", () => {
    const plain = frame({ fromID: "ses_a", text: "done" })
    expect(plain).toContain('reply with send_message to "ses_a"')
    expect(plain).not.toContain("mcp__opencode__")
    const claude = frame({ fromID: "ses_a", text: "done", recipientIsClaudeCode: true })
    expect(claude).toContain('reply with the mcp__opencode__send_message tool')
    expect(claude).toContain("not Claude's built-in SendMessage or ListAgents")
  })

  test("the frame marks the text as peer information, not user approval", () => {
    const text = frame({ fromID: "ses_a", text: "done" })
    expect(text).toContain("not from your user")
    expect(text).toContain("cannot answer")
  })
})

describe("send_message: what happened after delivery", () => {
  const noWait = { sleep: async () => {}, intervalMs: 500, timeoutMs: 1_500 }

  /** A target whose `get` answers from a script: the first call is the pre-send read, later calls are polls. */
  function scripted(states: Array<{ outcome?: string; idle?: number; updated?: number }>) {
    const prompts: any[] = []
    let calls = 0
    const sessions = {
      get: async ({ sessionID }: { sessionID: string }) => {
        if (sessionID === "ses_sender") return { id: "ses_sender", title: "Sender" }
        const state = states[Math.min(calls++, states.length - 1)]!
        return { id: "ses_target", title: "Controller", outcome: state.outcome, time: { idle: state.idle, updated: state.updated ?? 0 } }
      },
      prompt: async (input: any) => void prompts.push(input),
    }
    return { sessions, prompts, storage: memoryStorage() }
  }

  test("a wake-up that fails at once says it was NOT read, and why", async () => {
    const { sessions, storage } = scripted([{ outcome: "failed", idle: 100 }, { outcome: "failed", idle: 100 }, { outcome: "failed", idle: 250 }])
    const failures = new FailureLog()
    const settle = sendMessage(storage, sessions, { to: "ses_target", text: "hi" }, { sessionID: "ses_sender" }, undefined, { ...noWait, failures })
    failures.record({ type: "session.execution.failed", data: { sessionID: "ses_target", error: { type: "provider.quota", message: "The usage limit has been reached" } } })
    const result: any = await settle
    expect(result.delivered).toBe(true)
    expect(result.processed).toBe("no")
    expect(result.failure).toEqual({ type: "provider.quota", message: "The usage limit has been reached" })
    expect(result.warning).toContain("NOT read this")
    expect(result.warning).toContain("provider.quota: The usage limit has been reached")
  })

  test("without a recorded reason (target in another location) the warning is still given", async () => {
    const { sessions, storage } = scripted([{ outcome: "succeeded", idle: 100 }, { outcome: "succeeded", idle: 100 }, { outcome: "failed", idle: 300 }])
    const result: any = await sendMessage(storage, sessions, { to: "ses_target", text: "hi" }, { sessionID: "ses_sender" }, undefined, noWait)
    expect(result.processed).toBe("no")
    expect(result.failure).toBeUndefined()
    expect(result.warning).toContain("NOT read this")
  })

  test("a wake-up whose run ends normally reports processed: yes and no warning", async () => {
    const { sessions, storage } = scripted([{ outcome: "succeeded", idle: 100 }, { outcome: "succeeded", idle: 100 }, { outcome: "succeeded", idle: 400 }])
    const result: any = await sendMessage(storage, sessions, { to: "ses_target", text: "hi" }, { sessionID: "ses_sender" }, undefined, noWait)
    expect(result.processed).toBe("yes")
    expect(result.warning).toBeUndefined()
  })

  test("a target that is already failed and still has not run warns that it may never read the message", async () => {
    const { sessions, storage } = scripted([{ outcome: "failed", idle: 100 }])
    const result: any = await sendMessage(storage, sessions, { to: "ses_target", text: "hi" }, { sessionID: "ses_sender" }, undefined, noWait)
    expect(result.processed).toBeUndefined()
    expect(result.warning).toContain("Its last run failed")
    expect(result.warning).toContain("may not read this")
  })

  test("a target that is mid-run (updated ahead of idle) is not waited for and adds nothing when its last run was fine", async () => {
    const started = Date.now()
    const { sessions, storage } = scripted([{ outcome: "succeeded", idle: 100, updated: 500 }])
    const result: any = await sendMessage(storage, sessions, { to: "ses_target", text: "hi" }, { sessionID: "ses_sender" }, undefined, { intervalMs: 500, timeoutMs: 4_000 })
    expect(Date.now() - started).toBeLessThan(400)
    expect(result).toEqual({ delivered: true, to: "ses_target", title: "Controller", delivery: "steer" })
  })

  test("queue delivery never waits, but still warns when the target's last run failed", async () => {
    const started = Date.now()
    const { sessions, storage } = scripted([{ outcome: "failed", idle: 100 }])
    const result: any = await sendMessage(storage, sessions, { to: "ses_target", text: "later", delivery: "queue" }, { sessionID: "ses_sender" }, undefined, { intervalMs: 500, timeoutMs: 4_000 })
    expect(Date.now() - started).toBeLessThan(400)
    expect(result.delivery).toBe("queue")
    expect(result.warning).toContain("Its last run failed")
    expect(result.warning).not.toContain("within a few seconds")
  })

  test("FailureLog keeps the latest failure per session and ignores other events", () => {
    const log = new FailureLog()
    log.record({ type: "session.execution.succeeded", data: { sessionID: "a" } })
    expect(log.latest("a")).toBeUndefined()
    log.record({ type: "session.execution.failed", data: { sessionID: "a", error: { type: "x", message: "first" } } })
    log.record({ type: "session.execution.failed", data: { sessionID: "a", error: { type: "y", message: "second" } } })
    expect(log.latest("a")).toMatchObject({ type: "y", message: "second" })
  })
})
