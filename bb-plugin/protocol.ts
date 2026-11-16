/**
 * Wire protocol between `bb-plugin/server.ts` and the Rust daemon
 * (`rex-wave-bb::protocol`, private there). One WebSocket, both directions.
 *
 * Drift guard: both sides round-trip `protocol.fixtures.json` in tests
 * (`cargo test -p rex-wave-bb`, `npm test`), so a field renamed on one side
 * fails CI on the other.
 */

/** Seeded as `pluginMetadata` on every wave worker thread; read back on every event. */
export type WaveMeta = {
  waveId: string;
  role: string;
  attempt: number;
  roomId: string;
};

export type ThreadSignal = "created" | "active" | "idle" | "failed" | "archived";

/** Plugin -> daemon. */
export type Up =
  | { t: "hello"; plugin_version: string; sdk_version: string }
  | {
      t: "thread";
      signal: ThreadSignal;
      thread_id: string;
      project_id: string;
      meta: WaveMeta | null;
      error: string | null;
    }
  | { t: "interaction"; thread_id: string; interaction: unknown }
  | { t: "thread_events"; thread_id: string; sequence: number }
  | {
      t: "tool_call";
      call_id: string;
      thread_id: string;
      project_id: string;
      tool: string;
      args: unknown;
    }
  | { t: "result"; req_id: number; ok: boolean; value: unknown };

/** Daemon -> plugin. `args` / `resolution` arrive already in BB SDK shape. */
export type Down =
  | { t: "spawn"; req_id: number; args: unknown }
  | { t: "find_by_meta"; req_id: number; meta: WaveMeta }
  | {
      t: "send";
      req_id: number;
      thread_id: string;
      mode: "auto" | "start" | "steer" | "steer-if-active" | "queue-if-active";
      text: string;
    }
  | {
      t: "resolve";
      req_id: number;
      thread_id: string;
      interaction_id: string;
      resolution: unknown;
    }
  | { t: "retire"; req_id: number; thread_id: string }
  | {
      t: "events_page";
      req_id: number;
      thread_id: string;
      after_seq: number | null;
      limit: number;
    }
  | { t: "thread_context"; req_id: number; thread_id: string }
  | { t: "watch_thread"; thread_id: string }
  | { t: "unwatch_thread"; thread_id: string }
  | { t: "tool_result"; call_id: string; ok: boolean; text: string }
  | { t: "push"; title: string; body: string };

/** Shape of `tools.generated.json` (written by `rex-wave schema --bb`). */
export type ToolCatalogEntry = {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
};

export function isWaveMeta(v: unknown): v is WaveMeta {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  return (
    typeof o.waveId === "string" &&
    typeof o.role === "string" &&
    typeof o.attempt === "number" &&
    typeof o.roomId === "string"
  );
}

export function metaKey(m: WaveMeta): string {
  return `spawn:${m.waveId}/${m.role}/${m.attempt}`;
}
