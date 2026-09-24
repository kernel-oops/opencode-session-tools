import { setupRecall } from "./lib.ts"

// Plain object rather than `Plugin.define` from "@opencode/plugin": a local plugin outside the
// OpenCode repository cannot resolve that package at runtime, and `define` only returns its argument.
export default {
  id: "opencode-recall-lite",
  async setup(ctx: any) {
    return setupRecall(ctx)
  },
}
