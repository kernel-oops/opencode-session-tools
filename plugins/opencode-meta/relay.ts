// Holds a subagent's send_message to its own parent until it is clear whether the message is mid-task progress
// or part of the subagent's final report. A subagent's final reply already reaches its parent, so a message sent
// just before finishing would otherwise arrive separately from, and often duplicate, that reply.
//
// - The subagent calls another tool: it is still working, so the held messages are delivered straight away.
// - Its turn ends and it ran in the foreground: the messages are merged into the parent's subagent tool result.
// - Its turn ends and it runs in the background: the messages are delivered now, beside its completion notice.
// Nothing is dropped: if the merge does not happen within MERGE_GRACE_MS of the turn ending, they are delivered.

export const MERGE_GRACE_MS = 30_000
const SUBAGENT_TOOL = "subagent"

export interface Held {
  parentID: string
  text: string
  framed: string
  metadata: Record<string, unknown>
  delivery: "steer" | "queue"
}

export interface RelayDelivery {
  prompt(input: { sessionID: string; text: string; delivery?: "queue" | "steer"; metadata?: Record<string, unknown> }): Promise<unknown>
}

export class ParentRelay {
  private held = new Map<string, Held[]>()
  private background = new Set<string>()
  private timers = new Map<string, ReturnType<typeof setTimeout>>()

  constructor(
    private sessions: RelayDelivery,
    private options: { graceMs?: number; onError?: (error: unknown) => void } = {},
  ) {}

  hold(childID: string, message: Held) {
    const list = this.held.get(childID) ?? []
    list.push(message)
    this.held.set(childID, list)
  }

  pending(childID: string) {
    return this.held.get(childID)?.length ?? 0
  }

  /** Delivers everything held for this child as ordinary steered messages. */
  async flush(childID: string) {
    this.clearTimer(childID)
    const list = this.held.get(childID)
    if (!list?.length) return
    this.held.delete(childID)
    for (const message of list) {
      await this.sessions
        .prompt({ sessionID: message.parentID, text: message.framed, delivery: message.delivery, metadata: message.metadata })
        .catch((error) => this.options.onError?.(error))
    }
  }

  /** tool.execute.before: any further tool call in the child means the held messages were mid-task. */
  async toolStarted(sessionID: string, tool: string) {
    if (tool === "send_message" || !this.held.has(sessionID)) return
    await this.flush(sessionID)
  }

  /** tool.execute.after for the parent's subagent tool: records background runs, merges into foreground results. */
  subagentFinished(event: any) {
    if (event?.tool !== SUBAGENT_TOOL || event.status !== "completed") return
    const result = event.result
    const childID: string | undefined = result?.output?.sessionID ?? result?.metadata?.sessionID
    if (!childID) return
    if (result?.output?.status === "running") {
      this.background.add(childID)
      return
    }
    this.background.delete(childID)
    const list = this.held.get(childID)
    if (!list?.length) return
    this.held.delete(childID)
    this.clearTimer(childID)
    const block = [
      "[Sent to you with send_message during this run:]",
      ...list.map((message) => message.text),
      "[Final reply:]",
    ].join("\n\n")
    const output = typeof result.output?.output === "string" ? { ...result.output, output: `${block}\n\n${result.output.output}` } : result.output
    let content = result.content
    if (typeof content === "string") {
      const opening = /^<subagent[^>]*>\n/.exec(content)
      content = opening ? `${opening[0]}${block}\n\n${content.slice(opening[0].length)}` : `${block}\n\n${content}`
    }
    // Replace rather than mutate, so the change survives however the host copies the hook event.
    event.result = { ...result, ...(output === undefined ? {} : { output }), content }
  }

  /** The child's turn ended (succeeded, failed or interrupted). */
  async turnEnded(childID: string) {
    if (!this.held.has(childID)) return
    if (this.background.has(childID)) return this.flush(childID)
    this.clearTimer(childID)
    const timer = setTimeout(() => void this.flush(childID), this.options.graceMs ?? MERGE_GRACE_MS)
    ;(timer as { unref?: () => void }).unref?.()
    this.timers.set(childID, timer)
  }

  async dispose() {
    for (const childID of [...this.held.keys()]) await this.flush(childID)
  }

  private clearTimer(childID: string) {
    const timer = this.timers.get(childID)
    if (timer) clearTimeout(timer)
    this.timers.delete(childID)
  }
}

export function isTurnEnd(event: any): string | undefined {
  const type = event?.type
  if (type !== "session.execution.succeeded" && type !== "session.execution.failed" && type !== "session.execution.interrupted") return
  const sessionID = event?.data?.sessionID
  return typeof sessionID === "string" ? sessionID : undefined
}
