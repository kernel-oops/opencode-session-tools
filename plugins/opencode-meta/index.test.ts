import { describe, expect, test } from "bun:test"
import plugin from "./index.ts"
import { executeOperation, validateArgs } from "./lib.ts"
import { makeMockCtx, sessionInfo } from "../opencode-recall-lite/mock-ctx.ts"

const messages = [
  { type: "user", text: "hi", time: { created: { epochMillis: 1 } } },
  {
    type: "assistant",
    content: [
      { type: "text", text: "ok" },
      { type: "tool", name: "read", state: { status: "completed" }, time: { created: { epochMillis: 1_700_000_000_002 } } },
      { type: "tool", name: "read", state: { status: "error" }, time: { created: { epochMillis: 1_700_000_000_003 } } },
      { type: "tool", name: "shell", state: { status: "completed" }, time: { created: { epochMillis: 1_700_000_000_004 } } },
    ],
    time: { created: { epochMillis: 2 } },
  },
]

describe("opencode-meta v2", () => {
  test("plugin entry exposes only the default export", async () => {
    expect(Object.keys(await import("./index.ts"))).toEqual(["default"])
  })

  test("validates arguments and defaults session_id from the tool context", () => {
    expect(() => validateArgs({ operation: "schema" })).toThrow("operation must be one of")
    expect(() => validateArgs({ operation: "session" })).toThrow("session_id is required")
    expect(validateArgs({ operation: "session" }, { sessionID: "ses_x" }).session_id).toBe("ses_x")
    expect(() => validateArgs({ operation: "capabilities", limit: 3 })).toThrow("not valid for operation")
    expect(() => validateArgs({ operation: "recent_sessions", limit: 101 })).toThrow("limit")
  })

  test("session, tree, stats, recent and tool usage come from the API and index", async () => {
    const ctx = makeMockCtx({
      sessions: {
        ses_p: sessionInfo("ses_p", { title: "Parent" }),
        ses_c: sessionInfo("ses_c", { title: "Child", parentID: "ses_p" }),
      },
      messages: { ses_p: messages, ses_c: [] },
    })
    const cleanup = await plugin.setup(ctx)
    expect(ctx._tools.map((tool: any) => tool.name)).toEqual(["opencode_meta", "send_message"])
    ctx.emit({ type: "session.created", created: 10, data: { sessionID: "ses_c", parentID: "ses_p", projectID: "proj_a", location: { directory: "/tmp/project-a" }, title: "Child" } })
    await ctx.settle()
    const run = async (input: unknown) => JSON.parse((await ctx._tools[0].execute(input, { sessionID: "ses_p" })).content)

    const session = await run({ operation: "session" })
    expect(session.session).toMatchObject({ id: "ses_p", title: "Parent", directory: "/tmp/project-a", agent: "build", cost_usd: 0.01 })
    expect(session.session.time.created).toBe("2023-11-14T22:13:20.000Z")

    const tree = await run({ operation: "session_tree" })
    expect(tree.parent).toBeNull()
    expect(tree.children.map((child: any) => child.id)).toEqual(["ses_c"])
    const childTree = await run({ operation: "session_tree", session_id: "ses_c" })
    expect(childTree.parent.id).toBe("ses_p")

    const stats = await run({ operation: "session_stats" })
    expect(stats.totals).toEqual({ messages: 2, tool_calls: 3 })
    expect(stats.tools).toEqual([
      { name: "read", calls: 2, errors: 1 },
      { name: "shell", calls: 1, errors: 0 },
    ])

    const recent = await run({ operation: "recent_sessions", days: 3650 })
    expect(recent.sessions.map((item: any) => item.id).sort()).toEqual(["ses_c", "ses_p"])
    const recentOther = await run({ operation: "recent_sessions", directory: "/nowhere" })
    expect(recentOther.sessions).toEqual([])

    const usage = await run({ operation: "tool_usage", days: 3650 })
    expect(usage.usage).toEqual([
      { tool_name: "read", calls: 2, sessions: 1, last_used: "2023-11-14T22:13:20.003Z" },
      { tool_name: "shell", calls: 1, sessions: 1, last_used: "2023-11-14T22:13:20.004Z" },
    ])
    const onlyShell = await run({ operation: "tool_usage", tool_name: "shell", days: 3650 })
    expect(onlyShell.usage.map((row: any) => row.tool_name)).toEqual(["shell"])

    const caps = await run({ operation: "capabilities" })
    expect(caps.app.version).toBe("2.0.3-test")
    expect(caps.index.sessions_indexed).toBe(2)
    expect(caps.operations).toContain("capabilities")

    await expect(run({ operation: "session", session_id: "missing" })).rejects.toThrow("session not found")
    cleanup()
  })

  test("executeOperation rejects unknown sessions without an index entry", async () => {
    const ctx = makeMockCtx()
    await expect(
      executeOperation(validateArgs({ operation: "session_stats", session_id: "nope" }), ctx.session, ctx.storage, ctx.app, ctx.location),
    ).rejects.toThrow("session not found")
  })
})
