import type { BbPluginApi } from "@get-bb/plugin-sdk";

type ThreadContextResult = Awaited<ReturnType<BbPluginApi["sdk"]["threads"]["context"]>>;

export type WaveMeta = {
  waveId: string;
  role: string;
  attempt: number;
  roomId: string;
};

export type ThreadSignal = "created" | "active" | "idle" | "failed" | "archived";

export type ContextCategoryKind = "used" | "reserved" | "deferred" | "free";

export type ContextCategory = {
  id: string;
  label: string;
  kind: ContextCategoryKind;
  tokens: number;
};

export type ThreadContextUsage = {
  used_tokens: number;
  model_context_window: number;
  estimated: boolean;
  auto_compact_at_tokens: number | null;
  cost_usd: number | null;
  categories: ContextCategory[];
};

export type WaveWorkspace =
  | { workspace: "managed_worktree"; base: { kind: "default" } | { kind: "named"; name: string } }
  | { workspace: "project_default" }
  | { workspace: "existing_path"; path: string };

export type SpawnEnvironment =
  | {
      type: "provider";
      environmentProviderId: "git-worktree";
      inputs:
        | { branch: { kind: "default" } | { kind: "named"; name: string } }
        | { kind: "existing"; path: string };
    }
  | {
      type: "provider";
      environmentProviderId: "project-checkout";
      inputs: Record<string, never>;
    };

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
  | { t: "interaction"; thread_id: string; meta?: WaveMeta | null; interaction: unknown }
  | { t: "thread_events"; thread_id: string; sequence: number }
  | {
      t: "tool_call";
      call_id: string;
      thread_id: string;
      project_id: string;
      meta?: WaveMeta | null;
      tool: string;
      args: unknown;
    }
  | { t: "result"; req_id: number; ok: boolean; value: unknown };

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
  | { t: "watch_thread"; thread_id: string; after_seq?: number | null }
  | { t: "unwatch_thread"; thread_id: string }
  | { t: "tool_result"; call_id: string; ok: boolean; text: string }
  | { t: "push"; title: string; body: string };

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

export function extractWaveMeta(pluginMetadata: unknown): WaveMeta | null {
  if (isWaveMeta(pluginMetadata)) return pluginMetadata;
  if (typeof pluginMetadata === "object" && pluginMetadata !== null) {
    const nested = (pluginMetadata as Record<string, unknown>).rexWave;
    if (isWaveMeta(nested)) return nested;
  }
  return null;
}

export function metaKey(m: WaveMeta): string {
  return `spawn:${m.waveId}/${m.role}/${m.attempt}`;
}

export function buildSpawnEnvironment(ws: WaveWorkspace): SpawnEnvironment {
  switch (ws.workspace) {
    case "managed_worktree":
      return {
        type: "provider",
        environmentProviderId: "git-worktree",
        inputs: { branch: ws.base },
      };
    case "existing_path":
      return {
        type: "provider",
        environmentProviderId: "git-worktree",
        inputs: { kind: "existing", path: ws.path },
      };
    case "project_default":
      return {
        type: "provider",
        environmentProviderId: "project-checkout",
        inputs: {},
      };
  }
}

export function mapThreadContextUsage(res: ThreadContextResult): ThreadContextUsage | null {
  if (!res.usage) return null;
  const u = res.usage;
  const costUsd = u.cost && u.cost.currency.toUpperCase() === "USD" ? u.cost.amount : null;
  const categories: ContextCategory[] = u.snapshot
    ? u.snapshot.categories.map((c) => ({
        id: c.id,
        label: c.label,
        kind: c.kind,
        tokens: c.tokens,
      }))
    : [];
  return {
    used_tokens: u.usedTokens,
    model_context_window: u.modelContextWindow,
    estimated: u.estimated,
    auto_compact_at_tokens: u.snapshot ? u.snapshot.autoCompactAtTokens : null,
    cost_usd: costUsd,
    categories,
  };
}
