// Test-only mock of the v2 Promise plugin context surface these plugins use.
export interface Emitted { type: string; created: number; data: Record<string, unknown> }

export function makeMockCtx(seed: { sessions?: Record<string, any>; messages?: Record<string, any[]>; options?: Record<string, unknown> } = {}) {
  const store = new Map<string, unknown>()
  const tools: any[] = []
  const hooks: Record<string, Function[]> = {}
  const listeners = new Set<(event: Emitted | null) => void>()
  const sessions = { ...(seed.sessions ?? {}) }
  const messages = { ...(seed.messages ?? {}) }
  const ctx = {
    app: { name: "opencode", version: "2.0.3-test", channel: "prod" },
    location: { directory: "/tmp/project-a", project: { id: "proj_a" } },
    options: seed.options ?? {},
    storage: {
      get: async (key: string) => store.get(key),
      set: async (key: string, value: unknown) => void store.set(key, JSON.parse(JSON.stringify(value))),
      remove: async (key: string) => void store.delete(key),
      scan: async ({ prefix, after, limit = 100 }: { prefix: string; after?: string; limit?: number }) => {
        const keys = [...store.keys()].filter((key) => key.startsWith(prefix)).sort()
        const start = after ? keys.indexOf(after) + 1 : 0
        const page = keys.slice(start, start + limit)
        const next = start + limit < keys.length ? page[page.length - 1] : undefined
        return { entries: page.map((key) => ({ key, value: store.get(key) })), next }
      },
    },
    session: {
      get: async ({ sessionID }: { sessionID: string }) => sessions[sessionID],
      context: async ({ sessionID }: { sessionID: string }) => messages[sessionID] ?? [],
      hook: async (name: string, callback: Function) => {
        ;(hooks[name] ??= []).push(callback)
        return { dispose: async () => {} }
      },
    },
    event: {
      subscribe: ({ signal }: { signal?: AbortSignal } = {}) => {
        const queue: Emitted[] = []
        let wake: (() => void) | undefined
        const listener = (event: Emitted | null) => {
          if (event) queue.push(event)
          wake?.()
        }
        listeners.add(listener)
        signal?.addEventListener("abort", () => {
          listeners.delete(listener)
          wake?.()
        })
        return {
          async *[Symbol.asyncIterator]() {
            while (!signal?.aborted) {
              if (queue.length) {
                yield queue.shift()!
                continue
              }
              await new Promise<void>((resolve) => (wake = resolve))
              wake = undefined
            }
          },
        }
      },
    },
    tool: {
      transform: async (callback: Function) => {
        callback({ add: (tool: any) => tools.push(tool), list: () => tools.map((tool) => ({ id: tool.name })) })
        return { dispose: async () => {} }
      },
    },
    _store: store,
    _tools: tools,
    _hooks: hooks,
    _sessions: sessions,
    _messages: messages,
    emit(event: Emitted) {
      for (const listener of listeners) listener(event)
    },
    async settle() {
      for (let i = 0; i < 20; i += 1) await new Promise((resolve) => setTimeout(resolve, 5))
    },
  }
  return ctx
}

export function sessionInfo(id: string, extra: Record<string, unknown> = {}) {
  return {
    id,
    projectID: "proj_a",
    location: { directory: "/tmp/project-a" },
    title: `Session ${id}`,
    agent: "build",
    cost: 0.01,
    tokens: { input: 10, output: 5 },
    time: { created: { epochMillis: 1_700_000_000_000 }, updated: { epochMillis: 1_700_000_100_000 } },
    ...extra,
  }
}
