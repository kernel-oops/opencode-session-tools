import { setupMeta } from "./plugins/opencode-meta/lib.ts"
import { setupRecall } from "./plugins/opencode-recall-lite/lib.ts"

// npm entry: both plugins as one, sharing a single session index in this plugin's storage. recall-lite
// maintains the index; opencode-meta's tools read the same one.
// Plain object rather than `Plugin.define`: a plugin outside the OpenCode repository cannot resolve
// "@opencode/plugin" at runtime, and `define` only returns its argument.
export default {
  id: "opencode-session-tools",
  async setup(ctx: any) {
    const stopRecall = await setupRecall(ctx)
    const stopMeta = await setupMeta(ctx, { indexing: false })
    return () => {
      stopMeta()
      stopRecall()
    }
  },
}
