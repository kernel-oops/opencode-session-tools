import { describe, expect, test } from "bun:test"
import plugin from "./index.ts"
import { buildTextDoc, recall, recallGet, score, terms, validateRecallArgs } from "./lib.ts"
import { applyEvent, boundedJson, listSessions, messageText, sessionFromInfo } from "./session-index.ts"
import { makeMockCtx, sessionInfo } from "./mock-ctx.ts"

const userMessage = (text: string) => ({ type: "user", text, time: { created: { epochMillis: 1 } } })
const assistantMessage = (text: string, tool?: string) => ({
  type: "assistant",
  content: [{ type: "text", text }, ...(tool ? [{ type: "tool", name: tool, state: { status: "completed" } }] : [])],
  time: { created: { epochMillis: 2 } },
})

describe("session-index", () => {
  test("converts Session.Info and events into records", () => {
    const record = sessionFromInfo(sessionInfo("ses_1", { parentID: "ses_0" }))!
    expect(record).toMatchObject({ id: "ses_1", parentID: "ses_0", directory: "/tmp/project-a", created: 1_700_000_000_000 })
    expect(messageText(userMessage("hello"))).toEqual({ role: "user", text: "hello", created: 1 })
    expect(messageText(assistantMessage("hi", "read"))!.text).toBe("hi\n[tool:read]")
    expect(messageText({ type: "assistant", content: [{ type: "reasoning", text: "x" }] })).toBeUndefined()
  })

  test("applies created, renamed, settled and deleted events idempotently", async () => {
    const ctx = makeMockCtx()
    const created = { type: "session.created", created: 5, data: { sessionID: "ses_1", projectID: "p", location: { directory: "/d" }, title: "t" } }
    await applyEvent(ctx.storage, created)
    await applyEvent(ctx.storage, created)
    await applyEvent(ctx.storage, { type: "session.renamed", created: 6, data: { sessionID: "ses_1", title: "renamed" } })
    let settled = ""
    await applyEvent(ctx.storage, { type: "session.execution.succeeded", created: 7, data: { sessionID: "ses_1" } }, { onSettled: async (id) => void (settled = id) })
    expect(settled).toBe("ses_1")
    expect(await listSessions(ctx.storage)).toEqual([{ id: "ses_1", projectID: "p", directory: "/d", title: "renamed", created: 5, updated: 7 }])
    await applyEvent(ctx.storage, { type: "session.deleted", created: 8, data: { sessionID: "ses_1" } })
    expect(await listSessions(ctx.storage)).toEqual([])
    expect(await applyEvent(ctx.storage, { type: "unrelated", data: {} })).toBe(false)
  })

  test("bounds serialised output", () => {
    const big = { rows: Array.from({ length: 500 }, (_, i) => ({ i, text: "x".repeat(100) })) }
    const output = boundedJson(big, 4096)
    expect(Buffer.byteLength(output)).toBeLessThanOrEqual(4096)
    expect(JSON.parse(output).output_truncated).toBe(true)
  })
})

describe("recall-lite", () => {
  test("validates arguments", () => {
    expect(() => validateRecallArgs({})).toThrow("query")
    expect(() => validateRecallArgs({ query: "a", limit: 99 })).toThrow("limit")
    expect(validateRecallArgs({ query: " dkim " })).toMatchObject({ query: "dkim", limit: 8 })
    expect(terms("Fix the DKIM signer")).toEqual(["fix", "the", "dkim", "signer"])
  })

  test("builds a bounded text document keeping the newest lines", () => {
    const messages = Array.from({ length: 200 }, (_, i) => userMessage(`line ${i} ${"y".repeat(500)}`))
    const doc = buildTextDoc(messages, 1)
    expect(Buffer.byteLength(doc.text)).toBeLessThanOrEqual(32 * 1024)
    expect(doc.text.endsWith("y".repeat(500))).toBe(true)
    expect(doc.text.includes("line 199 ")).toBe(true)
    expect(doc.text.includes("line 0 ")).toBe(false)
  })

  test("scores titles above bodies and produces snippets", () => {
    const record = { id: "s", title: "DKIM migration", created: 0, updated: 0 }
    const doc = { updated: 0, text: "[user] please check the dkim signer later\n[assistant] done" }
    const result = score(record, doc, "dkim signer")
    expect(result.score).toBeGreaterThan(5)
    expect(result.snippets[0]).toContain("dkim signer")
    expect(score(record, doc, "unrelated").score).toBe(0)
  })

  test("plugin registers tools, indexes settled sessions and answers recall", async () => {
    const ctx = makeMockCtx({
      sessions: { ses_a: sessionInfo("ses_a", { title: "Garmin build" }), ses_b: sessionInfo("ses_b", { title: "Mail", location: { directory: "/tmp/project-b" } }) },
      messages: { ses_a: [userMessage("build the garmin mesh radio firmware"), assistantMessage("compiled", "shell")], ses_b: [userMessage("rotate dkim signer keys")] },
    })
    const cleanup = await plugin.setup(ctx)
    expect(ctx._tools.map((tool: any) => tool.name)).toEqual(["recall", "recall_get"])
    expect(ctx._hooks.context).toHaveLength(1)
    ctx.emit({ type: "session.created", created: 1, data: { sessionID: "ses_a", projectID: "proj_a", location: { directory: "/tmp/project-a" }, title: "Garmin build" } })
    ctx.emit({ type: "session.execution.succeeded", created: 2, data: { sessionID: "ses_a" } })
    ctx.emit({ type: "session.created", created: 3, data: { sessionID: "ses_b", projectID: "proj_b", location: { directory: "/tmp/project-b" }, title: "Mail" } })
    ctx.emit({ type: "session.execution.succeeded", created: 4, data: { sessionID: "ses_b" } })
    await ctx.settle()
    const hits = JSON.parse(await recall(ctx.storage, { query: "dkim signer" }))
    expect(hits.hits.map((hit: any) => hit.session_id)).toEqual(["ses_b"])
    expect(hits.hits[0].snippets[0]).toContain("dkim signer")
    const filtered = JSON.parse(await recall(ctx.storage, { query: "garmin", directory: "/tmp/project-b" }))
    expect(filtered.hits).toEqual([])
    const tool = ctx._tools[0]
    const viaTool = JSON.parse((await tool.execute({ query: "garmin" }, { sessionID: "ses_a" })).content)
    expect(viaTool.hits[0].session_id).toBe("ses_a")
    const got = JSON.parse(await recallGet(ctx.session, ctx.storage, { session_id: "ses_a" }))
    expect(got.session.title).toBe("Garmin build")
    expect(got.transcript).toContain("[tool:shell]")
    const system: any[] = []
    ctx._hooks.context[0]({ system })
    ctx._hooks.context[0]({ system })
    expect(system).toHaveLength(1)
    expect(system[0].text).toContain("[recall-nudge]")
    cleanup()
    if (typeof cleanup === "function") cleanup()
  })

  test("nudge can be disabled by options", async () => {
    const ctx = makeMockCtx({ options: { nudge: false } })
    const cleanup = await plugin.setup(ctx)
    expect(ctx._hooks.context).toBeUndefined()
    cleanup()
  })
})
