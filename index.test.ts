import { expect, test } from "bun:test"
import plugin from "./index.ts"
import { makeMockCtx } from "./plugins/opencode-recall-lite/mock-ctx.ts"

test("the combined entry registers all four tools, a single session indexer and the relay's turn-end watcher", async () => {
  const ctx: any = makeMockCtx()
  let subscriptions = 0
  const subscribe = ctx.event.subscribe
  ctx.event.subscribe = (...args: any[]) => {
    subscriptions++
    return subscribe(...args)
  }
  const stop = await plugin.setup(ctx)
  expect(ctx._tools.map((tool: any) => tool.name).sort()).toEqual(["opencode_meta", "recall", "recall_get", "send_message"])
  // One subscription for the shared session indexer, one for the parent relay's turn-end watcher.
  expect(subscriptions).toBe(2)
  expect(Object.keys(await import("./index.ts"))).toEqual(["default"])
  await stop()
})
