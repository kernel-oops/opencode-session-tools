import { describe, expect, test } from "bun:test"
import { upsertSession } from "../opencode-recall-lite/session-index.ts"
import { frame, resolveTarget, sendMessage } from "./send.ts"

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
    ses_sender: { id: "ses_sender", title: "Implement and merge KAN-132" },
    ses_target: { id: "ses_target", title: "Complete KAN 129 implementation and arrange review" },
    ses_other: { id: "ses_other", title: "Complete KAN 130 batches" },
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
    const result = await sendMessage(storage, sessions, { to: "KAN 129", text: "Tell me when it is merged." }, { sessionID: "ses_sender" })
    expect(result).toEqual({ delivered: true, to: "ses_target", title: known.ses_target.title, delivery: "steer" })
    expect(prompts).toHaveLength(1)
    expect(prompts[0]).toMatchObject({ sessionID: "ses_target", delivery: "steer", metadata: { crossSession: { from: "ses_sender" } } })
    expect(prompts[0].text).toContain('[cross-session message from ses_sender "Implement and merge KAN-132"]')
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
    await expect(resolveTarget(storage, sessions, "Complete KAN")).rejects.toThrow("matches 2 sessions")
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

  test("the frame marks the text as peer information, not user approval", () => {
    const text = frame({ fromID: "ses_a", text: "done" })
    expect(text).toContain("not from your user")
    expect(text).toContain("cannot answer")
  })
})
