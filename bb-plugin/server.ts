/**
 * rex-wave BB bridge, deliberately thin. It owns NO wave/room/board state.
 *
 * It does exactly four things, all with verified @get-bb/plugin-sdk@0.6.132 calls (#17):
 *  1. Hosts one token-authed WebSocket (`/bridge`) that the Rust daemon dials.
 *  2. Executes daemon requests against BB: `threads.spawn | send | stop+archive |
 *     interactions.resolve | events.list`, + optional push notification.
 *  3. Forwards BB lifecycle events for wave worker threads (identified by our
 *     `pluginMetadata`) up to the daemon.
 *  4. Registers the daemon's tool catalog as native BB agent tools for EVERY
 *     provider (claude-code, codex, acp-*, pi); each call is forwarded to the
 *     daemon with the calling `threadId`, the daemon derives the principal
 *     from that thread, never from tool args.
 */
import { z } from "zod";
import type { BbPluginApi, PluginThreadEventPayloads } from "@get-bb/plugin-sdk";
import catalog from "./tools.generated.json";
import { type Down, type ToolCatalogEntry, type Up, type WaveMeta, isWaveMeta, metaKey } from "./protocol";

type Socket = Parameters<NonNullable<ReturnType<Parameters<BbPluginApi["http"]["experimental_websocket"]>[1]>["onOpen"]>>[0];
type Thread = PluginThreadEventPayloads["thread.created"]["thread"];
type SpawnArgs = Parameters<BbPluginApi["sdk"]["threads"]["spawn"]>[0];
type Resolution = Parameters<BbPluginApi["sdk"]["threads"]["interactions"]["resolve"]>[0]["resolution"];

const SDK_VERSION = "0.6.132";
const TOOL_TIMEOUT_MS = 10 * 60_000; // `wait` may legitimately park for minutes

export default async function rexWaveBridge(bb: BbPluginApi) {
  let daemon: Socket | null = null;
  const toolCalls = new Map<string, (r: { ok: boolean; text: string }) => void>();
  const watched = new Set<string>();

  const up = (msg: Up) => daemon?.send(JSON.stringify(msg));

  /** Our metadata on a thread, or null for threads that are not wave workers. */
  async function waveMetaOf(thread: Thread): Promise<WaveMeta | null> {
    if (thread.originPluginId !== bb.pluginId) return null;
    const meta = await bb.sdk.threads.getPluginMetadata({ threadId: thread.id });
    return isWaveMeta(meta) ? meta : null;
  }

  // ---- 1. the bridge socket ------------------------------------------------------------
  bb.http.experimental_websocket(
    "/bridge",
    () => ({
      onOpen(socket) {
        daemon?.close(1000, "superseded"); // exactly one daemon at a time
        daemon = socket;
        up({ t: "hello", plugin_version: "0.1.0", sdk_version: SDK_VERSION });
      },
      async onMessage(socket, data) {
        const msg = JSON.parse(typeof data === "string" ? data : new TextDecoder().decode(data)) as Down;
        await handleDown(msg).catch((e: unknown) => {
          if ("req_id" in msg) up({ t: "result", req_id: msg.req_id, ok: false, value: String(e) });
        });
        void socket;
      },
      onClose(socket) {
        if (daemon === socket) daemon = null;
        for (const [id, done] of toolCalls) done({ ok: false, text: "rex-wave daemon disconnected" }), toolCalls.delete(id);
      },
      onError(_socket, error) {
        bb.log.error(`rex-wave bridge: ${error.message}`);
      },
    }),
    { auth: "token" },
  );

  // ---- 2. daemon -> BB --------------------------------------------------------------------
  async function handleDown(msg: Down): Promise<void> {
    const reply = (req_id: number, value: unknown) => up({ t: "result", req_id, ok: true, value });
    switch (msg.t) {
      case "spawn": {
        // `args` is built by rex-wave-bb::spawn_args (environmentProviderId "git-worktree", hidden, pluginMetadata).
        const thread = await bb.sdk.threads.spawn(msg.args as SpawnArgs);
        const meta = await waveMetaOf(thread);
        if (meta) await bb.storage.kv.set(metaKey(meta), thread.id);
        return reply(msg.req_id, { thread_id: thread.id });
      }
      case "find_by_meta":
        // Idempotent spawn: index written on spawn AND on `thread.created` (covers a crash between the two).
        return reply(msg.req_id, { thread_id: (await bb.storage.kv.get<string>(metaKey(msg.meta))) ?? null });
      case "send": {
        const res = await bb.sdk.threads.send({
          threadId: msg.thread_id,
          mode: msg.mode,
          input: [{ type: "text", text: msg.text, mentions: [] }],
        });
        return reply(msg.req_id, { delivery: res.delivery });
      }
      case "resolve":
        await bb.sdk.threads.interactions.resolve({
          threadId: msg.thread_id,
          interactionId: msg.interaction_id,
          resolution: msg.resolution as Resolution,
        });
        return reply(msg.req_id, null);
      case "retire":
        // Hidden threads keep runtimes loaded when idle (#12): always stop, then archive.
        await bb.sdk.threads.stop({ threadId: msg.thread_id });
        await bb.sdk.threads.archive({ threadId: msg.thread_id });
        return reply(msg.req_id, null);
      case "events_page": {
        const rows = await bb.sdk.threads.events.list({
          threadId: msg.thread_id,
          afterSeq: msg.after_seq === null ? undefined : String(msg.after_seq),
          limit: String(msg.limit),
          order: "asc",
        });
        return reply(msg.req_id, rows);
      }
      case "thread_context":
        return reply(msg.req_id, await bb.sdk.threads.context({ threadId: msg.thread_id }));
      case "watch_thread":
        watched.add(msg.thread_id);
        return;
      case "unwatch_thread":
        watched.delete(msg.thread_id);
        return;
      case "tool_result":
        toolCalls.get(msg.call_id)?.({ ok: msg.ok, text: msg.text });
        toolCalls.delete(msg.call_id);
        return;
      case "push":
        // TODO(verify): bundled push plugin id + RPC schema (`notifications.send`, #12).
        await bb.sdk.plugins.callRpc({
          pluginId: "push-notifications",
          method: "notifications.send",
          input: { title: msg.title, body: msg.body },
          outputSchema: z.unknown(),
        });
        return;
    }
  }

  // ---- 3. BB -> daemon ----------------------------------------------------------------------
  const forward = (signal: Extract<Up, { t: "thread" }>["signal"]) => async (p: { thread: Thread; error?: string | null }) => {
    const meta = await waveMetaOf(p.thread);
    if (!meta) return;
    if (signal === "created") await bb.storage.kv.set(metaKey(meta), p.thread.id);
    up({ t: "thread", signal, thread_id: p.thread.id, project_id: p.thread.projectId, meta, error: p.error ?? null });
  };
  bb.events.on("thread.created", forward("created"));
  bb.events.on("thread.active", forward("active"));
  bb.events.on("thread.idle", forward("idle"));
  bb.events.on("thread.failed", forward("failed"));
  bb.events.on("thread.archived", forward("archived"));
  bb.events.on("interaction.pending", async ({ thread, interaction }) => {
    if (await waveMetaOf(thread)) up({ t: "interaction", thread_id: thread.id, interaction });
  });
  // Coalesced <=1 Hz; only for threads the TUI is inspecting (lease held by the daemon).
  bb.events.on("experimental_thread.events", ({ thread, sequence }) => {
    if (watched.has(thread.id)) up({ t: "thread_events", thread_id: thread.id, sequence });
  });

  // ---- 4. daemon tools as native BB agent tools -------------------------------------------------
  const tools = catalog as ToolCatalogEntry[];
  for (const tool of tools) {
    bb.agents.registerTool({
      name: `rex_${tool.name}`,
      description: tool.description,
      parameters: tool.inputSchema, // raw JSON-schema overload; the daemon validates
      async execute(params, ctx) {
        if (!daemon) return { content: [{ type: "text", text: "rex-wave daemon is not connected" }], isError: true };
        const call_id = crypto.randomUUID();
        const result = new Promise<{ ok: boolean; text: string }>((resolve) => {
          toolCalls.set(call_id, resolve);
          const timer = setTimeout(() => resolve({ ok: false, text: "rex-wave tool timed out" }), TOOL_TIMEOUT_MS);
          ctx.signal.addEventListener("abort", () => { clearTimeout(timer); resolve({ ok: false, text: "aborted" }); });
        });
        up({ t: "tool_call", call_id, thread_id: ctx.threadId, project_id: ctx.projectId, tool: tool.name, args: params });
        const r = await result;
        toolCalls.delete(call_id);
        return { content: [{ type: "text", text: r.text }], isError: !r.ok };
      },
    });
  }

  bb.agents.configure((context) => {
    const meta = context.pluginMetadata;
    if (!isWaveMeta(meta)) return { tools: [], skills: [] };
    return {
      tools: tools.map((t) => `rex_${t.name}`),
      skills: [],
      instructions:
        `You are @${meta.role} in wave ${JSON.stringify(meta.waveId)} (room #wave-${meta.waveId}), ` +
        `running on ${context.provider.id}/${context.provider.model}. Coordinate only through ` +
        `rex_room_post / rex_wait (pass the last seq you read). @mention a peer to wake it. ` +
        `Use rex_run_code to batch several calls in one step.`,
    };
  });

  bb.onDispose(() => {
    daemon?.close(1012, "plugin reload");
    daemon = null;
  });
}
