import { setupMeta } from "./lib.ts"

// Plain object rather than `Plugin.define` from "@opencode/plugin": a local plugin outside the
// OpenCode repository cannot resolve that package at runtime, and `define` only returns its argument.
export default {
  id: "opencode-meta",
  async setup(ctx: any) {
    return setupMeta(ctx)
  },
}
