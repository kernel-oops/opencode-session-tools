import { describe, expect, test } from "bun:test"
import { completionHook, completionItem, completionLine, Digest, excerpt, freshRegistry } from "./digest.ts"

describe("native independent-owner completion evidence", () => {
  test("recognises the native completion source and preserves non-acceptance evidence", () => {
    const item = completionItem({ sessionID: "ses_creator", messageID: "msg_notice", wake: true,
      text: '<new_session sessionID="ses_owner" state="succeeded">Execution ended; not acceptance or human authority.</new_session>',
      metadata: { source: "new_session", childID: "ses_owner", state: "succeeded", agent: "build" },
    })
    expect(item?.to).toBe("ses_creator")
    expect(item?.from).toBe("ses_owner")
    expect(item?.completion.source).toBe("new_session")
    expect(item?.text).toContain("not acceptance")
    expect(item?.text).not.toContain("<new_session")
    expect(completionLine({ ...item!, at: 0 })).toContain("independent owner")
  })
  test("legacy failure evidence is marked, not automatically made urgent", () => {
    const item = completionItem({ sessionID: "ses_creator", messageID: "msg_failed", text: "Execution ended", wake: true,
      metadata: { source: "new_session", childID: "ses_owner", state: "failed" },
    })!
    expect(completionLine({ ...item, at: 0 })).toContain("[failed]")
    expect(item).not.toHaveProperty("priority")
  })
  test("bounds native result evidence without exposing a transcript", () => {
    expect(excerpt(`<new_session>${"x".repeat(2000)}</new_session>`, "head", 120).length).toBe(120)
  })
  test("native owner/source/ID and report-only authority survive the inert compatibility hook", async () => {
    const digest = new Digest(freshRegistry(), { get: async () => undefined, prompt: async () => { throw new Error("no prompt") } },
      { get: async () => undefined, set: async () => { throw new Error("no storage write") } })
    const event = { sessionID: "ses_creator", messageID: "msg_notice", source: "new_session", wake: false,
      text: "Execution ended; not acceptance or human authority.", metadata: { childID: "ses_owner", state: "succeeded", source: "new_session" } }
    const before = JSON.stringify(event)
    await completionHook(digest, "all")(event)
    expect(JSON.stringify(event)).toBe(before)
    expect(digest.registry.held.size).toBe(0)
  })
})
