import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import type { BbPluginApi } from "@get-bb/plugin-sdk";
import {
  type Down,
  type Up,
  extractWaveMeta,
  mapThreadContextUsage,
  metaKey,
} from "./protocol.ts";
import rexWaveBridge from "./server.ts";

test("protocol fixtures round-trip and helpers match BB 0.6.132 contracts", () => {
  const raw = readFileSync(new URL("./protocol.fixtures.json", import.meta.url), "utf8");
  const parsed = JSON.parse(raw) as { up: Up[]; down: Down[] };
  assert.equal(parsed.up.length, 6);
  assert.equal(parsed.down.length, 11);

  const nestedMeta = extractWaveMeta({
    rexWave: { waveId: "auth-v2", role: "verifier", attempt: 2, roomId: "#p/wave-auth-v2" },
  });
  assert.deepEqual(nestedMeta, {
    waveId: "auth-v2",
    role: "verifier",
    attempt: 2,
    roomId: "#p/wave-auth-v2",
  });
  assert.equal(metaKey(nestedMeta!), "spawn:auth-v2/verifier/2");

  const mapped = mapThreadContextUsage({
    usage: {
      usedTokens: 42_000,
      modelContextWindow: 200_000,
      estimated: false,
      cost: { amount: 0.42, currency: "USD" },
      snapshot: {
        autoCompactAtTokens: 160_000,
        capturedAt: "2026-11-16T10:00:00Z",
        contextWindowTokens: 200_000,
        estimated: false,
        model: "gpt-6.1-sol",
        providerSessionId: "sess_1",
        providerTurnId: "turn_1",
        usedTokens: 42_000,
        categories: [
          { id: "messages", label: "Messages", kind: "used", tokens: 30_000, entries: [] },
          { id: "tools", label: "Tools", kind: "used", tokens: 12_000, entries: [] },
        ],
      },
    },
  });
  assert.deepEqual(mapped, {
    used_tokens: 42_000,
    model_context_window: 200_000,
    estimated: false,
    auto_compact_at_tokens: 160_000,
    cost_usd: 0.42,
    categories: [
      { id: "messages", label: "Messages", kind: "used", tokens: 30_000 },
      { id: "tools", label: "Tools", kind: "used", tokens: 12_000 },
    ],
  });
});

test("rexWaveBridge enforces stop-settle-before-archive, afterSeq watch reconciliation, turn.failed, and tool filtering", async () => {
  const sdkOrder: string[] = [];
  const kv = new Map<string, unknown>();
  const threadMeta = new Map<string, Record<string, unknown>>();
  const threadRecords = new Map<string, { id: string; projectId: string; originPluginId: string; status: string }>();
  const listeners = new Map<string, Array<(payload: unknown) => unknown>>();
  const registeredTools = new Map<
    string,
    { execute: (params: unknown, ctx: { threadId: string; projectId: string; signal: AbortSignal }) => Promise<unknown> }
  >();
  let configureFn: ((ctx: unknown) => { tools: unknown[]; skills: string[]; instructions?: string }) | null = null;
  let wsFactory: (() => {
    onOpen?: (ws: { send: (s: string) => void; close: () => void; readyState: number }) => void;
    onMessage?: (ws: unknown, data: string) => Promise<void>;
    onClose?: (ws: unknown) => void;
  }) | null = null;
  let stopPollsRemaining = 0;

  const mockBb = {
    pluginId: "rex-wave",
    log: { info() {}, warn() {}, error() {}, debug() {} },
    storage: {
      kv: {
        async get(k: string) {
          return kv.get(k) ?? null;
        },
        async set(k: string, v: unknown) {
          kv.set(k, v);
        },
      },
    },
    http: {
      experimental_websocket(_path: string, handler: typeof wsFactory) {
        wsFactory = handler;
      },
    },
    events: {
      on(name: string, fn: (payload: unknown) => unknown) {
        const list = listeners.get(name) ?? [];
        list.push(fn);
        listeners.set(name, list);
      },
    },
    agents: {
      registerTool(def: {
        name: string;
        execute: (params: unknown, ctx: { threadId: string; projectId: string; signal: AbortSignal }) => Promise<unknown>;
      }) {
        registeredTools.set(def.name, def);
      },
      configure(fn: NonNullable<typeof configureFn>) {
        configureFn = fn;
      },
    },
    onDispose() {},
    sdk: {
      threads: {
        async spawn(args: Record<string, unknown>) {
          sdkOrder.push(`spawn:${(args.environment as { environmentProviderId: string }).environmentProviderId}`);
          const id = "th_spawned_1";
          const rec = { id, projectId: String(args.projectId), originPluginId: "rex-wave", status: "active" };
          threadRecords.set(id, rec);
          threadMeta.set(id, (args.pluginMetadata as Record<string, unknown>) ?? {});
          return rec;
        },
        async get({ threadId }: { threadId: string }) {
          const rec = threadRecords.get(threadId);
          if (!rec) throw new Error("not found");
          if (rec.status === "stopping" && stopPollsRemaining > 0) {
            stopPollsRemaining -= 1;
            if (stopPollsRemaining === 0) rec.status = "idle";
          }
          sdkOrder.push(`get:${threadId}:${rec.status}`);
          return { ...rec };
        },
        async list({ offset = 0, limit = 200 }: { offset?: number; limit?: number } = {}) {
          return [...threadRecords.values()].slice(offset, offset + limit);
        },
        async getPluginMetadata({ threadId }: { threadId: string }) {
          return threadMeta.get(threadId) ?? {};
        },
        async experimental_listPluginMetadata({ threadIds }: { threadIds: string[] }) {
          return {
            threads: threadIds
              .filter((id) => threadMeta.has(id))
              .map((threadId) => ({ threadId, metadata: threadMeta.get(threadId)! })),
          };
        },
        async send(args: { threadId: string; mode: string }) {
          sdkOrder.push(`send:${args.threadId}:${args.mode}`);
          return { ok: true, delivery: "sent" };
        },
        interactions: {
          async resolve(args: { threadId: string; interactionId: string }) {
            sdkOrder.push(`resolve:${args.threadId}:${args.interactionId}`);
            return {};
          },
        },
        async stop({ threadId }: { threadId: string }) {
          const rec = threadRecords.get(threadId);
          if (rec) rec.status = stopPollsRemaining > 0 ? "stopping" : "idle";
          sdkOrder.push(`stop:${threadId}`);
          return { ok: true };
        },
        async archive({ threadId }: { threadId: string }) {
          sdkOrder.push(`archive:${threadId}`);
          return { archivedThreadIds: [threadId] };
        },
        events: {
          async list({ afterSeq }: { afterSeq?: string }) {
            if (afterSeq === "5") return [{ seq: 8 }];
            return [];
          },
        },
        async context() {
          return {
            usage: {
              usedTokens: 10_000,
              modelContextWindow: 128_000,
              estimated: true,
              snapshot: undefined,
            },
          };
        },
      },
      plugins: {
        async callRpc() {
          return null;
        },
      },
    },
  } as unknown as BbPluginApi;

  await rexWaveBridge(mockBb);
  assert.ok(registeredTools.has("rex_room_post"));
  assert.equal(registeredTools.has("rex_run_code"), false);

  const nonWaveCfg = configureFn!({
    pluginMetadata: {},
    project: { id: "proj_1" },
    provider: { id: "codex", model: "gpt-6.1-sol" },
  });
  assert.deepEqual(nonWaveCfg.tools, []);

  const waveCfg = configureFn!({
    pluginMetadata: { rexWave: { waveId: "w1", role: "author", attempt: 1, roomId: "#proj_1/wave-w1" } },
    project: { id: "proj_1" },
    provider: { id: "pi", model: "openrouter/model" },
  });
  assert.ok(waveCfg.tools.includes("rex_room_post"));
  assert.equal(waveCfg.tools.includes("rex_run_code"), false);
  assert.equal(waveCfg.instructions?.includes("rex_run_code"), false);
  assert.ok(waveCfg.instructions?.includes("@author"));

  const sentUp: Up[] = [];
  const socket = {
    readyState: 1,
    send(payload: string) {
      sentUp.push(JSON.parse(payload) as Up);
    },
    close() {},
  };
  const session = wsFactory!();
  session.onOpen!(socket);
  assert.equal(sentUp[0]?.t, "hello");

  await session.onMessage!(
    socket,
    JSON.stringify({
      t: "spawn",
      req_id: 1,
      args: {
        projectId: "proj_1",
        environment: { type: "provider", environmentProviderId: "project-checkout", inputs: {} },
        prompt: "go",
        pluginMetadata: {
          rexWave: { waveId: "w1", role: "author", attempt: 1, roomId: "#proj_1/wave-w1" },
        },
      },
    } satisfies Down),
  );
  assert.equal(sdkOrder[0], "spawn:project-checkout");

  kv.clear();
  await session.onMessage!(
    socket,
    JSON.stringify({
      t: "find_by_meta",
      req_id: 2,
      meta: { waveId: "w1", role: "author", attempt: 1, roomId: "#proj_1/wave-w1" },
    } satisfies Down),
  );
  const findRes = sentUp.find((m) => m.t === "result" && m.req_id === 2);
  assert.deepEqual(findRes, { t: "result", req_id: 2, ok: true, value: { thread_id: "th_spawned_1" } });

  await session.onMessage!(
    socket,
    JSON.stringify({ t: "watch_thread", thread_id: "th_spawned_1", after_seq: 5 } satisfies Down),
  );
  assert.ok(sentUp.some((m) => m.t === "thread_events" && m.thread_id === "th_spawned_1" && m.sequence === 8));

  for (const fn of listeners.get("turn.failed") ?? []) {
    await fn({
      threadId: "th_spawned_1",
      requestId: "req_1",
      turnId: "turn_1",
      errorInfo: { category: "rate-limit", httpStatusCode: 429, providerCode: "too_many_requests" },
      inputAccepted: false,
      rateLimits: null,
    });
  }
  assert.ok(
    sentUp.some(
      (m) => m.t === "thread" && m.signal === "failed" && m.error === "rate-limit (too_many_requests)",
    ),
  );

  const ac = new AbortController();
  const toolPromise = registeredTools.get("rex_room_post")!.execute(
    { body: "hi" },
    { threadId: "th_spawned_1", projectId: "proj_1", signal: ac.signal },
  );
  await new Promise((r) => setTimeout(r, 5));
  const toolCallMsg = sentUp.find((m): m is Extract<Up, { t: "tool_call" }> => m.t === "tool_call");
  assert.ok(toolCallMsg);
  await session.onMessage!(
    socket,
    JSON.stringify({ t: "tool_result", call_id: toolCallMsg.call_id, ok: true, text: "{\"seq\":1}" } satisfies Down),
  );
  const toolRes = (await toolPromise) as { isError: boolean; content: Array<{ text: string }> };
  assert.equal(toolRes.isError, false);
  assert.equal(toolRes.content[0].text, "{\"seq\":1}");

  sdkOrder.length = 0;
  stopPollsRemaining = 2;
  await session.onMessage!(socket, JSON.stringify({ t: "retire", req_id: 9, thread_id: "th_spawned_1" } satisfies Down));
  assert.deepEqual(sdkOrder, [
    "stop:th_spawned_1",
    "get:th_spawned_1:stopping",
    "get:th_spawned_1:idle",
    "archive:th_spawned_1",
  ]);
});
