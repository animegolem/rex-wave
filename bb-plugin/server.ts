import { z } from "zod";
import type { BbPluginApi, PluginThreadEventPayloads } from "@get-bb/plugin-sdk";
import catalog from "./tools.generated.json" with { type: "json" };
import {
  type Down,
  type ToolCatalogEntry,
  type Up,
  type WaveMeta,
  extractWaveMeta,
  mapThreadContextUsage,
  metaKey,
} from "./protocol.ts";

type Socket = Parameters<
  NonNullable<ReturnType<Parameters<BbPluginApi["http"]["experimental_websocket"]>[1]>["onOpen"]>
>[0];
type Thread = PluginThreadEventPayloads["thread.created"]["thread"];
type TurnFailed = PluginThreadEventPayloads["turn.failed"];
type SpawnArgs = Parameters<BbPluginApi["sdk"]["threads"]["spawn"]>[0];
type Resolution = Parameters<BbPluginApi["sdk"]["threads"]["interactions"]["resolve"]>[0]["resolution"];

const SDK_VERSION = "0.6.132";
const TOOL_TIMEOUT_MS = 10 * 60_000;
const STOP_SETTLE_POLLS = 30;
const STOP_SETTLE_MS = 50;

function formatTurnFailure(ev: TurnFailed): string {
  if (!ev.errorInfo) return "turn failed";
  const detail =
    ev.errorInfo.providerCode ?? (ev.errorInfo.httpStatusCode !== null ? String(ev.errorInfo.httpStatusCode) : null);
  return detail ? `${ev.errorInfo.category} (${detail})` : ev.errorInfo.category;
}

function turnFailureStopReason(ev: TurnFailed): string | undefined {
  const normalize = (s: string | null | undefined) =>
    s ? s.trim().toLowerCase().replaceAll("-", "_") : undefined;
  const cat = normalize(ev.errorInfo?.category);
  const code = normalize(ev.errorInfo?.providerCode);
  const isRefusal = (s: string | undefined) =>
    s !== undefined &&
    (s === "policy" ||
      s === "refusal" ||
      s === "refused" ||
      s === "safety" ||
      s === "content_filter" ||
      s === "cyber_safeguard" ||
      s.includes("safeguard") ||
      s.includes("refusal") ||
      s.includes("content_filter"));
  if (isRefusal(code)) return ev.errorInfo?.providerCode ?? undefined;
  if (isRefusal(cat)) return ev.errorInfo?.category;
  return undefined;
}

export default async function rexWaveBridge(bb: BbPluginApi) {
  let daemon: Socket | null = null;
  const toolCalls = new Map<string, (r: { ok: boolean; text: string }) => void>();
  const watched = new Map<string, number | null>();

  const up = (msg: Up) => daemon?.send(JSON.stringify(msg));

  async function waveMetaOf(thread: Pick<Thread, "id" | "originPluginId">): Promise<WaveMeta | null> {
    if (thread.originPluginId !== bb.pluginId) return null;
    const meta = await bb.sdk.threads.getPluginMetadata({ threadId: thread.id });
    return extractWaveMeta(meta);
  }

  async function waveMetaByThreadId(threadId: string): Promise<{ thread: Thread; meta: WaveMeta } | null> {
    try {
      const thread = await bb.sdk.threads.get({ threadId });
      const meta = await waveMetaOf(thread);
      return meta ? { thread, meta } : null;
    } catch {
      return null;
    }
  }

  async function lookupThreadByMeta(target: WaveMeta): Promise<string | null> {
    const cached = await bb.storage.kv.get<string>(metaKey(target));
    if (cached) return cached;
    for (let offset = 0; ; offset += 200) {
      const threads = await bb.sdk.threads.list({
        originPluginId: bb.pluginId,
        includeHidden: true,
        limit: 200,
        offset,
      });
      if (threads.length === 0) return null;
      const metaList = await bb.sdk.threads.experimental_listPluginMetadata({
        threadIds: threads.map((t) => t.id),
      });
      for (const row of metaList.threads) {
        const m = extractWaveMeta(row.metadata);
        if (m && m.waveId === target.waveId && m.role === target.role && m.attempt === target.attempt) {
          await bb.storage.kv.set(metaKey(m), row.threadId);
          return row.threadId;
        }
      }
      if (threads.length < 200) return null;
    }
  }

  bb.http.experimental_websocket(
    "/bridge",
    () => ({
      onOpen(socket) {
        daemon?.close(1000, "superseded");
        daemon = socket;
        watched.clear();
        up({ t: "hello", plugin_version: "0.1.0", sdk_version: SDK_VERSION });
      },
      async onMessage(socket, data) {
        let msg: Down;
        try {
          msg = JSON.parse(typeof data === "string" ? data : new TextDecoder().decode(data)) as Down;
        } catch (e) {
          bb.log.error(`rex-wave bridge invalid JSON frame: ${e instanceof Error ? e.message : String(e)}`);
          return;
        }
        await handleDown(msg).catch((e: unknown) => {
          const text = e instanceof Error ? e.message : String(e);
          if ("req_id" in msg) up({ t: "result", req_id: msg.req_id, ok: false, value: text });
        });
        void socket;
      },
      onClose(socket) {
        if (daemon === socket) {
          daemon = null;
          watched.clear();
        }
        for (const [id, done] of toolCalls) {
          done({ ok: false, text: "rex-wave daemon disconnected" });
          toolCalls.delete(id);
        }
      },
      onError(_socket, error) {
        bb.log.error(`rex-wave bridge: ${error.message}`);
      },
    }),
    { auth: "token" },
  );

  async function handleDown(msg: Down): Promise<void> {
    const reply = (req_id: number, value: unknown) => up({ t: "result", req_id, ok: true, value });
    switch (msg.t) {
      case "spawn": {
        const spawnArgs = msg.args as SpawnArgs;
        const thread = await bb.sdk.threads.spawn(spawnArgs);
        const meta = extractWaveMeta(spawnArgs.pluginMetadata) ?? (await waveMetaOf(thread));
        if (meta) await bb.storage.kv.set(metaKey(meta), thread.id);
        return reply(msg.req_id, { thread_id: thread.id });
      }
      case "find_by_meta":
        return reply(msg.req_id, { thread_id: await lookupThreadByMeta(msg.meta) });
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
      case "retire": {
        // Hidden BB threads keep provider runtimes resident when archived while active or stopping.
        await bb.sdk.threads.stop({ threadId: msg.thread_id });
        for (let i = 0; i < STOP_SETTLE_POLLS; i++) {
          const cur = await bb.sdk.threads.get({ threadId: msg.thread_id });
          if (cur.status !== "stopping" && cur.status !== "active") break;
          if (i === STOP_SETTLE_POLLS - 1) {
            throw new Error(`thread ${msg.thread_id} remained ${cur.status} after stop`);
          }
          await new Promise((r) => setTimeout(r, STOP_SETTLE_MS));
        }
        await bb.sdk.threads.archive({ threadId: msg.thread_id });
        watched.delete(msg.thread_id);
        return reply(msg.req_id, null);
      }
      case "events_page": {
        const rows = await bb.sdk.threads.events.list({
          threadId: msg.thread_id,
          afterSeq: msg.after_seq === null ? undefined : String(msg.after_seq),
          beforeSeq:
            msg.before_seq === undefined || msg.before_seq === null
              ? undefined
              : String(msg.before_seq),
          limit: String(msg.limit),
          order: msg.order ?? "asc",
        });
        return reply(msg.req_id, rows);
      }
      case "thread_context": {
        const ctx = await bb.sdk.threads.context({ threadId: msg.thread_id });
        return reply(msg.req_id, mapThreadContextUsage(ctx));
      }
      case "watch_thread": {
        const afterSeq = msg.after_seq ?? null;
        watched.set(msg.thread_id, afterSeq);
        const tail = await bb.sdk.threads.events.list({
          threadId: msg.thread_id,
          afterSeq: afterSeq === null ? undefined : String(afterSeq),
          limit: "1",
          order: "desc",
        });
        if (tail.length > 0 && watched.has(msg.thread_id)) {
          const seq = tail[0].seq;
          watched.set(msg.thread_id, seq);
          up({ t: "thread_events", thread_id: msg.thread_id, sequence: seq });
        }
        return;
      }
      case "unwatch_thread":
        watched.delete(msg.thread_id);
        return;
      case "tool_result":
        toolCalls.get(msg.call_id)?.({ ok: msg.ok, text: msg.text });
        toolCalls.delete(msg.call_id);
        return;
      case "push":
        await bb.sdk.plugins
          .callRpc({
            pluginId: "push-notifications",
            method: "notifications.send",
            input: { title: msg.title, body: msg.body },
            outputSchema: z.unknown(),
          })
          .catch((e: unknown) => {
            bb.log.warn(`rex-wave push notification failed: ${e instanceof Error ? e.message : String(e)}`);
          });
        return;
    }
  }

  const forward =
    (signal: Extract<Up, { t: "thread" }>["signal"]) =>
    async (p: { thread: Thread; error?: string | null }) => {
      const meta = await waveMetaOf(p.thread);
      if (!meta) return;
      if (signal === "created") await bb.storage.kv.set(metaKey(meta), p.thread.id);
      up({
        t: "thread",
        signal,
        thread_id: p.thread.id,
        project_id: p.thread.projectId,
        meta,
        error: p.error ?? null,
      });
    };

  bb.events.on("thread.created", forward("created"));
  bb.events.on("thread.active", forward("active"));
  bb.events.on("thread.idle", forward("idle"));
  bb.events.on("thread.failed", forward("failed"));
  bb.events.on("thread.archived", forward("archived"));
  bb.events.on("thread.deleted", forward("archived"));

  bb.events.on("turn.failed", async (ev) => {
    const resolved = await waveMetaByThreadId(ev.threadId);
    if (!resolved) return;
    const stopReason = turnFailureStopReason(ev);
    up({
      t: "thread",
      signal: "failed",
      thread_id: resolved.thread.id,
      project_id: resolved.thread.projectId,
      meta: resolved.meta,
      error: formatTurnFailure(ev),
      ...(stopReason ? { stop_reason: stopReason } : {}),
    });
  });

  bb.events.on("interaction.pending", async ({ thread, interaction }) => {
    const meta = await waveMetaOf(thread);
    if (meta) {
      up({ t: "interaction", thread_id: thread.id, meta, interaction });
    }
  });

  bb.events.on("experimental_thread.events", ({ thread, sequence }) => {
    if (!watched.has(thread.id)) return;
    const prev = watched.get(thread.id) ?? null;
    if (prev !== null && sequence <= prev) return;
    watched.set(thread.id, sequence);
    up({ t: "thread_events", thread_id: thread.id, sequence });
  });

  const tools = catalog as ToolCatalogEntry[];
  for (const tool of tools) {
    bb.agents.registerTool({
      name: `rex_${tool.name}`,
      description: tool.description,
      parameters: tool.inputSchema,
      async execute(params, ctx) {
        if (!daemon) {
          return { content: [{ type: "text", text: "rex-wave daemon is not connected" }], isError: true };
        }
        const resolved = await waveMetaByThreadId(ctx.threadId);
        if (!resolved) {
          return { content: [{ type: "text", text: "thread is not a rex-wave worker" }], isError: true };
        }
        const call_id = crypto.randomUUID();
        const r = await new Promise<{ ok: boolean; text: string }>((resolve) => {
          let settled = false;
          const finish = (outcome: { ok: boolean; text: string }) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            ctx.signal.removeEventListener("abort", onAbort);
            toolCalls.delete(call_id);
            resolve(outcome);
          };
          const onAbort = () => finish({ ok: false, text: "aborted" });
          const timer = setTimeout(() => finish({ ok: false, text: "rex-wave tool timed out" }), TOOL_TIMEOUT_MS);
          ctx.signal.addEventListener("abort", onAbort, { once: true });
          toolCalls.set(call_id, finish);
          up({
            t: "tool_call",
            call_id,
            thread_id: ctx.threadId,
            project_id: ctx.projectId,
            meta: resolved.meta,
            tool: tool.name,
            args: params,
          });
        });
        return { content: [{ type: "text", text: r.text }], isError: !r.ok };
      },
    });
  }

  bb.agents.configure((context) => {
    const meta = extractWaveMeta(context.pluginMetadata);
    if (!meta) return { tools: [], skills: [] };
    const provenanceBits: string[] = [];
    if (meta.runStamp) provenanceBits.push(`run_stamp=${JSON.stringify(meta.runStamp)}`);
    if (meta.outputDir) provenanceBits.push(`output_dir=${JSON.stringify(meta.outputDir)}`);
    const provenanceSuffix =
      provenanceBits.length > 0
        ? ` Provenance: ${provenanceBits.join(", ")} (never treat pre-existing files without matching run_stamp as completed work).`
        : "";
    return {
      tools: tools.map((t) => `rex_${t.name}`),
      skills: [],
      instructions:
        `You are @${meta.role} in wave ${JSON.stringify(meta.waveId)} (room #wave-${meta.waveId}, ` +
        `roomId ${JSON.stringify(meta.roomId)}, project ${JSON.stringify(context.project.id)}), ` +
        `running on ${context.provider.id}/${context.provider.model}. ` +
        `Coordinate through rex_room_post, rex_wait (pass the last seq you read), and rex_run_code. ` +
        `@mention a peer to wake it.${provenanceSuffix}`,
    };
  });

  bb.onDispose(() => {
    daemon?.close(1012, "plugin reload");
    daemon = null;
    watched.clear();
  });
}
