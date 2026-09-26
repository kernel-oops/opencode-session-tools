import { describe, expect, test } from "bun:test"
import { isTurnEnd, ParentRelay } from "./relay.ts"

function setup(graceMs = 5) {
  const prompts: any[] = []
  const relay = new ParentRelay({ prompt: async (input) => void prompts.push(input) }, { graceMs })
  const hold = (text: string) =>
    relay.hold("ses_child", { parentID: "ses_parent", text, framed: `[framed] ${text}`, metadata: { crossSession: { from: "ses_child" } }, delivery: "steer" })
  return { prompts, relay, hold }
}

const completed = (output: string, status = "completed") => ({
  tool: "subagent",
  sessionID: "ses_parent",
  status: "completed",
  result: {
    output: { sessionID: "ses_child", status, output },
    content: status === "completed" ? `<subagent sessionID="ses_child" state="completed">\n${output}\n</subagent>` : output,
    metadata: { sessionID: "ses_child", status },
  },
})

describe("ParentRelay", () => {
  test("delivers held messages as soon as the child calls another tool (mid-task progress)", async () => {
    const { prompts, relay, hold } = setup()
    hold("A done")
    await relay.toolStarted("ses_child", "send_message")
    expect(prompts).toHaveLength(0)
    await relay.toolStarted("ses_child", "bash")
    expect(prompts).toMatchObject([{ sessionID: "ses_parent", text: "[framed] A done", delivery: "steer" }])
    expect(relay.pending("ses_child")).toBe(0)
  })

  test("merges messages held at the end of a foreground run into the subagent result, before the final reply", async () => {
    const { prompts, relay, hold } = setup()
    hold("Full details: X, Y, Z")
    await relay.turnEnded("ses_child")
    const event: any = completed("Short summary.")
    relay.subagentFinished(event)
    expect(event.result.output.output).toBe("[Sent to you with send_message during this run:]\n\nFull details: X, Y, Z\n\n[Final reply:]\n\nShort summary.")
    expect(event.result.content).toStartWith('<subagent sessionID="ses_child" state="completed">\n[Sent to you')
    expect(event.result.content).toEndWith("Short summary.\n</subagent>")
    await Bun.sleep(20)
    expect(prompts).toHaveLength(0)
  })

  test("delivers at the end of a background run, beside the completion notice", async () => {
    const { prompts, relay, hold } = setup()
    relay.subagentFinished(completed("working in the background", "running"))
    hold("Full details")
    await relay.turnEnded("ses_child")
    expect(prompts).toMatchObject([{ sessionID: "ses_parent", text: "[framed] Full details" }])
  })

  test("never drops a held message: delivers it if no merge happens after the turn ends", async () => {
    const { prompts, relay, hold } = setup()
    hold("details")
    await relay.turnEnded("ses_child")
    expect(prompts).toHaveLength(0)
    await Bun.sleep(20)
    expect(prompts).toHaveLength(1)
    hold("left over")
    await relay.dispose()
    expect(prompts).toHaveLength(2)
  })

  test("recognises turn-end events", () => {
    expect(isTurnEnd({ type: "session.execution.succeeded", data: { sessionID: "ses_a" } })).toBe("ses_a")
    expect(isTurnEnd({ type: "session.execution.interrupted", data: { sessionID: "ses_a" } })).toBe("ses_a")
    expect(isTurnEnd({ type: "session.step.ended", data: { sessionID: "ses_a" } })).toBeUndefined()
  })
})
