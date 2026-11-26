//! N-way `HostRouter` multiplexing `AgentHost` across arbitrary runner backends
//! (`Bb`, `T3`, `Acp`, `Claude`, `Codex`) over `Arc<dyn AgentHost>`.

use crate::{AgentHost, BoxFut, HostError, SpawnRequest};
use rex_wave_api::config::{HarnessProvider, RexConfig, WorkspaceStrategy};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{
    AgentSpec, BaseBranch, InteractionResolution, Observation, SteerDeliveryMode, WaveStart,
    WaveView, Workspace,
};
use rex_wave_api::{
    ApiError, HostInteractionId, HostThreadId, InvalidId, ProviderId, RoleName, RunnerKind,
    WaveCommand, WaveSlug, WireReply,
};
use rex_wave_runtime::{DecideCtx, Service};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedProvider {
    pub target: RunnerKind,
    pub provider: ProviderId,
}

fn normalize_provider_for_target(raw: &str, target: RunnerKind) -> Result<ProviderId, HostError> {
    let norm = raw.trim();
    let lower = norm.to_ascii_lowercase();
    let mapped = match target {
        RunnerKind::T3 => match lower.as_str() {
            "agy" | "antigravity" => "antigravity",
            "claude" | "claude-code" => "claudeAgent",
            "open-code" => "opencode",
            _ => norm,
        },
        RunnerKind::Bb => match lower.as_str() {
            "claude" | "claudeagent" => "claude-code",
            _ => norm,
        },
        RunnerKind::Acp | RunnerKind::Claude | RunnerKind::Codex => match lower.as_str() {
            "agy" => "antigravity",
            _ => norm,
        },
    };
    ProviderId::parse(mapped).map_err(|e| HostError::Rejected(e.to_string()))
}

/// Applies per-provider model, reasoning, and billing-account defaults from `cfg`
/// when a `SpawnRequest`'s `AgentSpec` left them unset.
pub fn apply_harness_defaults(agent: &mut AgentSpec, cfg: &RexConfig) {
    let Some(prof) = cfg.harnesses.for_provider_id(&agent.provider) else {
        return;
    };
    if agent.model.is_none() {
        agent.model = prof.model.clone();
    }
    if agent.reasoning.is_none() {
        agent.reasoning = prof.reasoning.clone();
    }
    if agent.account.is_none() {
        agent.account = prof.account.clone();
    }
}

/// Resolves which runner owns `provider` without operator config overrides.
pub fn route_provider(
    provider: &ProviderId,
    default_runner: RunnerKind,
) -> Result<RoutedProvider, HostError> {
    route_provider_with_config(provider, default_runner, None)
}

/// Resolves which runner owns `provider`, honoring explicit `<runner>:` prefixes first,
/// then per-provider `[runners]` overrides, then `antigravity` -> `T3`, then
/// `runners.default_runner`, and finally `default_runner`.
pub fn route_provider_with_config(
    provider: &ProviderId,
    default_runner: RunnerKind,
    cfg: Option<&RexConfig>,
) -> Result<RoutedProvider, HostError> {
    let raw = provider.as_str().trim();
    if let Some((target, clean)) = RunnerKind::strip_provider_prefix(raw) {
        if clean.is_empty() {
            return Err(HostError::Rejected(format!(
                "empty provider name after '{}:' prefix",
                target.as_str()
            )));
        }
        if target == RunnerKind::Bb
            && (clean.eq_ignore_ascii_case("antigravity") || clean.eq_ignore_ascii_case("agy"))
        {
            return Err(HostError::Rejected(
                "provider 'antigravity' must route through T3 Code or ACP, never BB".into(),
            ));
        }
        let parsed = ProviderId::parse(clean).map_err(|e| HostError::Rejected(e.to_string()))?;
        return Ok(RoutedProvider {
            target,
            provider: parsed,
        });
    }

    if let Some(c) = cfg
        && let Some(slot) = HarnessProvider::parse_slot(raw)
        && let Some(route) = c.runners.override_for(slot)
    {
        let target = route
            .resolve_for_provider(raw)
            .map_err(HostError::Rejected)?;
        return Ok(RoutedProvider {
            target,
            provider: normalize_provider_for_target(raw, target)?,
        });
    }

    if raw.eq_ignore_ascii_case("antigravity") || raw.eq_ignore_ascii_case("agy") {
        let target = match cfg.and_then(|c| c.runners.default_runner) {
            Some(rex_wave_api::config::RunnerRoute::Native) => RunnerKind::Acp,
            Some(rex_wave_api::config::RunnerRoute::T3)
            | Some(rex_wave_api::config::RunnerRoute::Bb) => RunnerKind::T3,
            None if matches!(
                default_runner,
                RunnerKind::Acp | RunnerKind::Claude | RunnerKind::Codex
            ) =>
            {
                RunnerKind::Acp
            }
            None => RunnerKind::T3,
        };
        return Ok(RoutedProvider {
            target,
            provider: normalize_provider_for_target(raw, target)?,
        });
    }

    if let Some(route) = cfg.and_then(|c| c.runners.default_runner) {
        let target = route
            .resolve_for_provider(raw)
            .map_err(HostError::Rejected)?;
        return Ok(RoutedProvider {
            target,
            provider: normalize_provider_for_target(raw, target)?,
        });
    }

    if matches!(
        default_runner,
        RunnerKind::Acp | RunnerKind::Claude | RunnerKind::Codex
    ) && let Some(slot) = HarnessProvider::parse_slot(raw)
    {
        let target = rex_wave_api::config::RunnerRoute::Native
            .resolve_for_provider(slot.slug())
            .map_err(HostError::Rejected)?;
        return Ok(RoutedProvider {
            target,
            provider: normalize_provider_for_target(raw, target)?,
        });
    }

    Ok(RoutedProvider {
        target: default_runner,
        provider: normalize_provider_for_target(raw, default_runner)?,
    })
}

pub fn route_thread(thread: &HostThreadId) -> RunnerKind {
    thread.runner_kind()
}

/// Resolves and stamps `<runner>:<provider>` plus `[harnesses.<slot>]` defaults
/// onto `agent` so they are durably recorded in `StreamId::Wave`.
pub fn resolve_agent_spec_for_wave(
    agent: &mut AgentSpec,
    default_runner: RunnerKind,
    cfg: Option<&RexConfig>,
    (omitted_perm, omitted_ws): (bool, bool),
    allow_account_default: bool,
) -> Result<(RunnerKind, bool), ApiError> {
    let mut account_applied = false;
    if let Some(c) = cfg
        && let Some(prof) = c.harnesses.for_provider_id(&agent.provider)
    {
        if agent.model.is_none() {
            agent.model = prof.model.clone();
        }
        if agent.reasoning.is_none() {
            agent.reasoning = prof.reasoning.clone();
        }
        if allow_account_default && agent.account.is_none() && prof.account.is_some() {
            agent.account = prof.account.clone();
            account_applied = true;
        }
        if omitted_perm && let Some(p) = prof.permission {
            agent.permission = p;
        }
        if omitted_ws && let Some(w) = prof.workspace {
            agent.workspace = match w {
                WorkspaceStrategy::ProjectDefault => Workspace::ProjectDefault,
                WorkspaceStrategy::ManagedWorktree => Workspace::ManagedWorktree {
                    base: BaseBranch::Default,
                },
            };
        }
    }

    let routed =
        route_provider_with_config(&agent.provider, default_runner, cfg).map_err(|msg| {
            ApiError::Invalid(InvalidId {
                what: "provider".into(),
                reason: msg.to_string(),
            })
        })?;
    let pinned_provider = ProviderId::parse(format!(
        "{}:{}",
        routed.target.as_str(),
        routed.provider.as_str()
    ))
    .map_err(ApiError::Invalid)?;
    agent.provider = pinned_provider;
    Ok((routed.target, account_applied))
}

fn compute_wave_start_digest(ws: &WaveStart) -> String {
    let defaults_mask = (ws.config_defaults.permission, ws.config_defaults.workspace);
    let effective_worker_masks: Vec<(bool, bool)> = (0..ws.spec.workers.len())
        .map(|idx| {
            ws.config_defaults
                .worker_masks
                .get(idx)
                .copied()
                .unwrap_or(defaults_mask)
        })
        .collect();
    let bytes =
        serde_json::to_vec(&(&ws.spec, defaults_mask, &effective_worker_masks)).unwrap_or_default();
    let digest = Sha256::digest(&bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Service wrapper around `Registry<Waves>` that resolves runner routes and
/// `[harnesses.<slot>]` defaults once at `WaveStart` / `WaveAddWorker` time so
/// the chosen runner and effective `AgentSpec` are durably journaled in `StreamId::Wave`.
pub struct WaveService {
    inner: Arc<dyn Service<WaveCommand>>,
    default_runner: RunnerKind,
    config_rx: Option<watch::Receiver<RexConfig>>,
    host: std::sync::OnceLock<Arc<dyn AgentHost>>,
}

impl WaveService {
    pub fn new(
        inner: Arc<dyn Service<WaveCommand>>,
        default_runner: RunnerKind,
        config_rx: Option<watch::Receiver<RexConfig>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            default_runner,
            config_rx,
            host: std::sync::OnceLock::new(),
        })
    }

    pub fn with_state_dir(self: Arc<Self>, _state_dir: PathBuf) -> Arc<Self> {
        self
    }

    pub fn bind_host(&self, host: Arc<dyn AgentHost>) {
        let _ = self.host.set(host);
    }
}

impl Service<WaveCommand> for WaveService {
    fn dispatch(
        &self,
        mut cmd: WaveCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let cfg = self.config_rx.as_ref().map(|rx| rx.borrow().clone());
        let default_runner = self.default_runner;
        Box::pin(async move {
            match &mut cmd {
                WaveCommand::WaveStart(ws) => {
                    ws.config_defaults.account_from_config = false;
                    ws.config_defaults.request_digest = Some(compute_wave_start_digest(ws));
                    let defaults_mask =
                        (ws.config_defaults.permission, ws.config_defaults.workspace);
                    let caller_defaults_had_account = ws.spec.defaults.account.is_some();
                    let _ = resolve_agent_spec_for_wave(
                        &mut ws.spec.defaults,
                        default_runner,
                        cfg.as_ref(),
                        defaults_mask,
                        false,
                    )?;
                    let explicit_wave_acct = ws.spec.defaults.account.clone();
                    for (idx, w) in ws.spec.workers.iter_mut().enumerate() {
                        if let Some(mut agent) = w.agent.take() {
                            let w_mask = ws
                                .config_defaults
                                .worker_masks
                                .get(idx)
                                .copied()
                                .unwrap_or(defaults_mask);
                            let worker_had_account = agent.account.is_some();
                            let _ = resolve_agent_spec_for_wave(
                                &mut agent,
                                default_runner,
                                cfg.as_ref(),
                                w_mask,
                                !caller_defaults_had_account && !worker_had_account,
                            )?;
                            if agent.account.is_none() && caller_defaults_had_account {
                                agent.account = explicit_wave_acct.clone();
                            }
                            w.agent = Some(agent);
                        } else {
                            let mut agent = ws.spec.defaults.clone();
                            let _ = resolve_agent_spec_for_wave(
                                &mut agent,
                                default_runner,
                                cfg.as_ref(),
                                (false, false),
                                !caller_defaults_had_account,
                            )?;
                            w.agent = Some(agent);
                        }
                    }
                }
                WaveCommand::WaveAddWorker(wa) => {
                    wa.config_defaults.account_from_config = false;
                    wa.config_defaults.request_digest = None;
                    let existing_defaults = match self
                        .inner
                        .dispatch(
                            WaveCommand::WaveView(WaveView {
                                wave: wa.wave.clone(),
                            }),
                            ctx.clone(),
                        )
                        .await
                    {
                        Ok(WireReply::WaveView(snap)) => Some(snap.spec.defaults),
                        _ => None,
                    };
                    let wave_has_paying_account = existing_defaults
                        .as_ref()
                        .is_some_and(|d| d.account.is_some());
                    if let Some(ref mut agent) = wa.worker.agent {
                        let caller_had_account = agent.account.is_some();
                        let mask = (wa.config_defaults.permission, wa.config_defaults.workspace);
                        let (_, applied_acct) = resolve_agent_spec_for_wave(
                            agent,
                            default_runner,
                            cfg.as_ref(),
                            mask,
                            !caller_had_account && !wave_has_paying_account,
                        )?;
                        wa.config_defaults.account_from_config = applied_acct;
                    } else if let Some(mut agent) = existing_defaults {
                        let (_, applied_acct) = resolve_agent_spec_for_wave(
                            &mut agent,
                            default_runner,
                            cfg.as_ref(),
                            (false, false),
                            !wave_has_paying_account,
                        )?;
                        wa.config_defaults.account_from_config = applied_acct;
                        wa.worker.agent = Some(agent);
                    }
                }
                WaveCommand::WaveResolve(wr) => {
                    if let Some(host) = self.host.get() {
                        let snap = match self
                            .inner
                            .dispatch(
                                WaveCommand::WaveView(WaveView {
                                    wave: wr.wave.clone(),
                                }),
                                ctx.clone(),
                            )
                            .await?
                        {
                            WireReply::WaveView(s) => s,
                            other => {
                                return Err(ApiError::Internal(format!(
                                    "unexpected WaveView reply: {other:?}"
                                )));
                            }
                        };
                        let (_, _, phase) = snap
                            .workers
                            .iter()
                            .find(|(r, _, _)| r == &wr.role)
                            .ok_or_else(|| ApiError::NotFound {
                                what: format!("worker @{}/{}", wr.wave, wr.role),
                            })?;
                        match phase {
                            rex_wave_api::wave::WorkerPhase::Live {
                                thread,
                                activity: rex_wave_api::wave::Activity::Blocked { interaction },
                            } => {
                                if interaction.id != wr.interaction {
                                    return Err(ApiError::conflict(format!(
                                        "worker @{}/{} is blocked on {}, not {}",
                                        wr.wave, wr.role, interaction.id, wr.interaction
                                    )));
                                }
                                match host
                                    .validate_resolve(thread, &wr.interaction, &wr.resolution)
                                    .await
                                {
                                    Ok(()) => {}
                                    Err(HostError::Rejected(reason)) => {
                                        return Err(ApiError::Invalid(InvalidId {
                                            what: "resolution".into(),
                                            reason,
                                        }));
                                    }
                                    Err(HostError::Unreachable(msg)) => {
                                        return Err(ApiError::Transport(msg));
                                    }
                                }
                            }
                            other => {
                                return Err(ApiError::conflict(format!(
                                    "worker @{}/{} has no pending interaction ({other:?})",
                                    wr.wave, wr.role
                                )));
                            }
                        }
                    }
                }
                _ => {}
            }
            self.inner.dispatch(cmd, ctx).await
        })
    }
}

type PinnedAttemptKey = (WaveSlug, RoleName, u32);

type PinnedRouterRow = (WaveSlug, RoleName, u32, RunnerKind, AgentSpec);

#[derive(Default)]
struct PinnedRouterStore {
    order: VecDeque<PinnedAttemptKey>,
    entries: BTreeMap<PinnedAttemptKey, (RunnerKind, AgentSpec)>,
}

fn load_router_pins(path: &Path) -> PinnedRouterStore {
    let Ok(bytes) = rex_wave_fs::read_regular_bounded(path, 2 * 1024 * 1024) else {
        return PinnedRouterStore::default();
    };
    let Ok(rows) = serde_json::from_slice::<Vec<PinnedRouterRow>>(&bytes) else {
        return PinnedRouterStore::default();
    };
    let mut store = PinnedRouterStore::default();
    for (w, r, a, runner, agent) in rows {
        let key = (w, r, a);
        if !store.entries.contains_key(&key) {
            store.order.push_back(key.clone());
        }
        store.entries.insert(key, (runner, agent));
    }
    store
}

fn save_router_pins(path: &Path, store: &PinnedRouterStore) -> io::Result<()> {
    let rows: Vec<PinnedRouterRow> = store
        .order
        .iter()
        .filter_map(|k| {
            store
                .entries
                .get(k)
                .map(|(runner, agent)| (k.0.clone(), k.1.clone(), k.2, *runner, agent.clone()))
        })
        .collect();
    let bytes = serde_json::to_vec(&rows).map_err(io::Error::other)?;
    rex_wave_fs::write_atomic_path(path, &bytes, 0o600, false)
}

/// Looks up a persisted runner pin for a legacy bare-provider worker attempt.
pub fn lookup_pinned_runner(
    path: &Path,
    wave: &WaveSlug,
    role: &RoleName,
    attempt: u32,
) -> Option<RunnerKind> {
    let store = load_router_pins(path);
    store
        .entries
        .get(&(wave.clone(), role.clone(), attempt))
        .map(|(r, _)| *r)
}

/// N-way `AgentHost` multiplexer backed by `Arc<dyn AgentHost>` slots per [`RunnerKind`].
#[derive(Clone)]
pub struct HostRouter {
    hosts: BTreeMap<RunnerKind, Arc<dyn AgentHost>>,
    default_runner: RunnerKind,
    config_rx: Option<watch::Receiver<RexConfig>>,
    pin_store: Option<PathBuf>,
    pinned_attempts: Arc<Mutex<PinnedRouterStore>>,
}

impl HostRouter {
    pub fn new(default_runner: RunnerKind) -> Self {
        HostRouter {
            hosts: BTreeMap::new(),
            default_runner,
            config_rx: None,
            pin_store: None,
            pinned_attempts: Arc::new(Mutex::new(PinnedRouterStore::default())),
        }
    }

    pub fn from_entries(
        default_runner: RunnerKind,
        entries: impl IntoIterator<Item = (RunnerKind, Arc<dyn AgentHost>)>,
    ) -> Self {
        HostRouter {
            hosts: entries.into_iter().collect(),
            default_runner,
            config_rx: None,
            pin_store: None,
            pinned_attempts: Arc::new(Mutex::new(PinnedRouterStore::default())),
        }
    }

    pub fn with_config_watch(mut self, rx: watch::Receiver<RexConfig>) -> Self {
        self.config_rx = Some(rx);
        self
    }

    pub fn with_pin_store(mut self, path: PathBuf) -> Self {
        let loaded = load_router_pins(&path);
        self.pin_store = Some(path);
        self.pinned_attempts = Arc::new(Mutex::new(loaded));
        self
    }

    pub fn with_host<H: AgentHost>(mut self, kind: RunnerKind, host: H) -> Self {
        self.hosts.insert(kind, Arc::new(host));
        self
    }

    pub fn with_optional_host<H: AgentHost>(self, kind: RunnerKind, host: Option<H>) -> Self {
        match host {
            Some(h) => self.with_host(kind, h),
            None => self,
        }
    }

    pub fn default_runner(&self) -> RunnerKind {
        self.default_runner
    }

    pub fn get(&self, kind: RunnerKind) -> Option<&Arc<dyn AgentHost>> {
        self.hosts.get(&kind)
    }
}

impl AgentHost for HostRouter {
    fn spawn(&self, mut req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>> {
        let resolved: Result<(RunnerKind, AgentSpec), HostError> =
            if RunnerKind::strip_provider_prefix(req.agent.provider.as_str()).is_some() {
                let mut effective_agent = req.agent.clone();
                route_provider_with_config(&effective_agent.provider, self.default_runner, None)
                    .map(|routed| {
                        effective_agent.provider = routed.provider;
                        (routed.target, effective_agent)
                    })
            } else {
                let key = (req.wave.clone(), req.role.clone(), req.attempt);
                let mut pinned = self
                    .pinned_attempts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if let Some(existing) = pinned.entries.get(&key).cloned() {
                    Ok(existing)
                } else {
                    let cfg = self.config_rx.as_ref().map(|rx| rx.borrow().clone());
                    let mut effective_agent = req.agent.clone();
                    match route_provider_with_config(
                        &effective_agent.provider,
                        self.default_runner,
                        cfg.as_ref(),
                    ) {
                        Ok(routed) => {
                            effective_agent.provider = routed.provider;
                            let pair = (routed.target, effective_agent);
                            let mut evicted: Option<(PinnedAttemptKey, (RunnerKind, AgentSpec))> =
                                None;
                            if pinned.entries.len() >= 1024
                                && let Some(oldest) = pinned.order.pop_front()
                                && let Some(val) = pinned.entries.remove(&oldest)
                            {
                                evicted = Some((oldest, val));
                            }
                            pinned.order.push_back(key.clone());
                            pinned.entries.insert(key.clone(), pair.clone());
                            if let Some(ref path) = self.pin_store
                                && let Err(e) = save_router_pins(path, &pinned)
                            {
                                pinned.entries.remove(&key);
                                pinned.order.pop_back();
                                if let Some((old_k, old_v)) = evicted {
                                    pinned.order.push_front(old_k.clone());
                                    pinned.entries.insert(old_k, old_v);
                                }
                                return Box::pin(async move {
                                    Err(HostError::Unreachable(format!("persist router pin: {e}")))
                                });
                            }
                            Ok(pair)
                        }
                        Err(e) => Err(e),
                    }
                }
            };
        Box::pin(async move {
            let (target, effective_agent) = resolved?;
            let is_bare_antigravity = target == RunnerKind::T3
                && effective_agent
                    .provider
                    .as_str()
                    .eq_ignore_ascii_case("antigravity");
            req.agent = effective_agent;
            match self.hosts.get(&target) {
                Some(host) => host.spawn(req).await,
                None if is_bare_antigravity => Err(HostError::Rejected(
                    "provider 'antigravity' requires T3 Code, but no T3 host is configured".into(),
                )),
                None => Err(HostError::Rejected(format!(
                    "{} host is not configured",
                    target.display_name()
                ))),
            }
        })
    }

    fn tell(
        &self,
        thread: &HostThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let target = route_thread(thread);
        let thread = thread.clone();
        let body = body.clone();
        Box::pin(async move {
            match self.hosts.get(&target) {
                Some(host) => host.tell(&thread, &body, mode).await,
                None => Err(HostError::Unreachable(format!(
                    "{} host is not configured",
                    target.display_name()
                ))),
            }
        })
    }

    fn resolve(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let target = route_thread(thread);
        let thread = thread.clone();
        let interaction = interaction.clone();
        let resolution = resolution.clone();
        Box::pin(async move {
            match self.hosts.get(&target) {
                Some(host) => host.resolve(&thread, &interaction, &resolution).await,
                None => Err(HostError::Unreachable(format!(
                    "{} host is not configured",
                    target.display_name()
                ))),
            }
        })
    }

    fn validate_resolve(
        &self,
        thread: &HostThreadId,
        interaction: &HostInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        let target = route_thread(thread);
        let thread = thread.clone();
        let interaction = interaction.clone();
        let resolution = resolution.clone();
        Box::pin(async move {
            match self.hosts.get(&target) {
                Some(host) => {
                    host.validate_resolve(&thread, &interaction, &resolution)
                        .await
                }
                None => Err(HostError::Unreachable(format!(
                    "{} host is not configured",
                    target.display_name()
                ))),
            }
        })
    }

    fn retire(&self, thread: &HostThreadId) -> BoxFut<'_, Result<(), HostError>> {
        let target = route_thread(thread);
        let thread = thread.clone();
        Box::pin(async move {
            match self.hosts.get(&target) {
                Some(host) => host.retire(&thread).await,
                None => Err(HostError::Unreachable(format!(
                    "{} host is not configured",
                    target.display_name()
                ))),
            }
        })
    }

    fn check_live(
        &self,
        thread: &HostThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        let target = route_thread(thread);
        let thread = thread.clone();
        Box::pin(async move {
            match self.hosts.get(&target) {
                Some(host) => host.check_live(&thread).await,
                None => Err(HostError::Unreachable(format!(
                    "{} host is not configured",
                    target.display_name()
                ))),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wave_room;
    use rex_wave_api::config::RunnerRoute;
    use rex_wave_api::wave::{AgentSpec, PermissionMode, Workspace};
    use rex_wave_api::{ProjectId, RoleName, WaveSlug};
    use std::sync::Mutex;

    struct StubHost {
        kind: RunnerKind,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl AgentHost for StubHost {
        fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>> {
            let prefix = self.kind.thread_prefix().unwrap_or("th_bb_");
            let tid = HostThreadId::parse(format!("{prefix}{}:{}", req.wave, req.role)).unwrap();
            self.seen.lock().unwrap().push(format!(
                "spawn:{}:{}",
                self.kind.as_str(),
                req.agent.provider
            ));
            Box::pin(async move { Ok(tid) })
        }

        fn tell(
            &self,
            thread: &HostThreadId,
            _body: &Body,
            _mode: SteerDeliveryMode,
        ) -> BoxFut<'_, Result<(), HostError>> {
            self.seen
                .lock()
                .unwrap()
                .push(format!("tell:{}:{thread}", self.kind.as_str()));
            Box::pin(async { Ok(()) })
        }

        fn resolve(
            &self,
            thread: &HostThreadId,
            _interaction: &HostInteractionId,
            _resolution: &InteractionResolution,
        ) -> BoxFut<'_, Result<(), HostError>> {
            self.seen
                .lock()
                .unwrap()
                .push(format!("resolve:{}:{thread}", self.kind.as_str()));
            Box::pin(async { Ok(()) })
        }

        fn retire(&self, thread: &HostThreadId) -> BoxFut<'_, Result<(), HostError>> {
            self.seen
                .lock()
                .unwrap()
                .push(format!("retire:{}:{thread}", self.kind.as_str()));
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn n_way_host_router_dispatches_five_runners_over_dyn_agent_host() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut router = HostRouter::new(RunnerKind::Bb);
        for kind in [
            RunnerKind::Bb,
            RunnerKind::T3,
            RunnerKind::Acp,
            RunnerKind::Claude,
            RunnerKind::Codex,
        ] {
            router = router.with_host(
                kind,
                StubHost {
                    kind,
                    seen: seen.clone(),
                },
            );
        }

        let wave = WaveSlug::parse("w1").unwrap();
        let proj = ProjectId::parse("/workspace/demo").unwrap();
        let mk = |role: &str, prov: &str| SpawnRequest {
            wave: wave.clone(),
            role: RoleName::parse(role).unwrap(),
            attempt: 1,
            project: proj.clone(),
            agent: AgentSpec {
                provider: ProviderId::parse(prov).unwrap(),
                model: None,
                account: None,
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::Full,
                workspace: Workspace::ProjectDefault,
            },
            room: wave_room(&wave, &proj),
            prompt: Body::parse("go").unwrap(),
            title: "[w1] @r".into(),
            run_stamp: "w1:r:a1".into(),
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        };

        let dyn_host: Arc<dyn AgentHost> = Arc::new(router);
        let t_ag = dyn_host.spawn(mk("r1", "antigravity")).await.unwrap();
        let t_acp = dyn_host.spawn(mk("r2", "acp:opencode")).await.unwrap();
        let t_cl = dyn_host
            .spawn(mk("r3", "claude:claude-code"))
            .await
            .unwrap();
        let t_cx = dyn_host.spawn(mk("r4", "codex:codex")).await.unwrap();
        let t_bb = dyn_host.spawn(mk("r5", "pi")).await.unwrap();

        assert_eq!(t_ag.runner_kind(), RunnerKind::T3);
        assert_eq!(t_acp.runner_kind(), RunnerKind::Acp);
        assert_eq!(t_cl.runner_kind(), RunnerKind::Claude);
        assert_eq!(t_cx.runner_kind(), RunnerKind::Codex);
        assert_eq!(t_bb.runner_kind(), RunnerKind::Bb);

        for tid in [&t_ag, &t_acp, &t_cl, &t_cx, &t_bb] {
            dyn_host
                .tell(tid, &Body::parse("ping").unwrap(), SteerDeliveryMode::Auto)
                .await
                .unwrap();
            dyn_host.retire(tid).await.unwrap();
        }

        let events = seen.lock().unwrap().clone();
        assert!(events.contains(&"spawn:t3:antigravity".to_owned()));
        assert!(events.contains(&"spawn:acp:opencode".to_owned()));
        assert!(events.contains(&"spawn:claude:claude-code".to_owned()));
        assert!(events.contains(&"spawn:codex:codex".to_owned()));
        assert!(events.contains(&"spawn:bb:pi".to_owned()));
        assert!(events.contains(&format!("retire:acp:{t_acp}")));
        assert!(events.contains(&format!("retire:claude:{t_cl}")));
        assert!(events.contains(&format!("retire:codex:{t_cx}")));
    }

    #[tokio::test]
    async fn host_router_applies_live_config_routes_and_harness_defaults() {
        use rex_wave_api::ModelId;
        use rex_wave_api::config::RunnerRoute;

        struct CaptureHost {
            kind: RunnerKind,
            captured: Arc<Mutex<Vec<(RunnerKind, AgentSpec)>>>,
        }

        impl AgentHost for CaptureHost {
            fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>> {
                let prefix = self.kind.thread_prefix().unwrap_or("th_bb_");
                let tid =
                    HostThreadId::parse(format!("{prefix}{}:{}", req.wave, req.role)).unwrap();
                self.captured.lock().unwrap().push((self.kind, req.agent));
                Box::pin(async move { Ok(tid) })
            }

            fn tell(
                &self,
                _thread: &HostThreadId,
                _body: &Body,
                _mode: SteerDeliveryMode,
            ) -> BoxFut<'_, Result<(), HostError>> {
                Box::pin(async { Ok(()) })
            }

            fn resolve(
                &self,
                _thread: &HostThreadId,
                _interaction: &HostInteractionId,
                _resolution: &InteractionResolution,
            ) -> BoxFut<'_, Result<(), HostError>> {
                Box::pin(async { Ok(()) })
            }

            fn retire(&self, _thread: &HostThreadId) -> BoxFut<'_, Result<(), HostError>> {
                Box::pin(async { Ok(()) })
            }
        }

        let captured = Arc::new(Mutex::new(Vec::new()));
        let (cfg_tx, cfg_rx) = watch::channel(RexConfig::default());
        let mut router = HostRouter::new(RunnerKind::Bb).with_config_watch(cfg_rx);
        for kind in [
            RunnerKind::Bb,
            RunnerKind::T3,
            RunnerKind::Acp,
            RunnerKind::Claude,
            RunnerKind::Codex,
        ] {
            router = router.with_host(
                kind,
                CaptureHost {
                    kind,
                    captured: captured.clone(),
                },
            );
        }

        let wave = WaveSlug::parse("w-cfg").unwrap();
        let proj = ProjectId::parse("/workspace/demo").unwrap();
        let mk = |role: &str, prov: &str, model: Option<&str>| {
            let mut agent = AgentSpec {
                provider: ProviderId::parse(prov).unwrap(),
                model: model.map(|m| ModelId::parse(m).unwrap()),
                account: None,
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::Full,
                workspace: Workspace::ProjectDefault,
            };
            let cur_cfg = cfg_tx.borrow().clone();
            resolve_agent_spec_for_wave(
                &mut agent,
                RunnerKind::Bb,
                Some(&cur_cfg),
                (false, false),
                true,
            )
            .unwrap();
            SpawnRequest {
                wave: wave.clone(),
                role: RoleName::parse(role).unwrap(),
                attempt: 1,
                project: proj.clone(),
                agent,
                room: wave_room(&wave, &proj),
                prompt: Body::parse("go").unwrap(),
                title: "[w-cfg] @r".into(),
                run_stamp: "w-cfg:r:a1".into(),
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            }
        };

        // Before config updates, codex follows daemon default (Bb) with no harness defaults.
        let r0_req = mk("r0", "codex", None);
        assert_eq!(r0_req.agent.provider.as_str(), "bb:codex");
        let t0 = router.spawn(r0_req.clone()).await.unwrap();
        assert_eq!(t0.runner_kind(), RunnerKind::Bb);

        // Live-update config via watch channel without restarting the router.
        let mut next_cfg = RexConfig::default();
        next_cfg.runners.default_runner = Some(RunnerRoute::T3);
        next_cfg.runners.claude = Some(RunnerRoute::Native);
        next_cfg.harnesses.claude.model = Some(ModelId::parse("claude-opus-5-5").unwrap());
        next_cfg.harnesses.claude.reasoning = Some("max".into());
        next_cfg.harnesses.claude.account = Some("team-a".into());
        next_cfg.harnesses.codex.model = Some(ModelId::parse("gpt-6.2-codex").unwrap());
        next_cfg.harnesses.codex.reasoning = Some("xhigh".into());
        next_cfg.harnesses.codex.account = Some("team-b".into());
        cfg_tx.send_replace(next_cfg);

        // Per-provider route override + harness defaults fill unset model/reasoning/account.
        let t1 = router.spawn(mk("r1", "claude", None)).await.unwrap();
        assert_eq!(t1.runner_kind(), RunnerKind::Claude);

        // Global default_runner routes codex -> T3 while keeping explicit model override.
        let t2 = router
            .spawn(mk("r2", "codex", Some("explicit-model")))
            .await
            .unwrap();
        assert_eq!(t2.runner_kind(), RunnerKind::T3);

        // Explicit runner prefix still overrides config and resolves codex harness defaults.
        let t3 = router.spawn(mk("r3", "bb:codex", None)).await.unwrap();
        assert_eq!(t3.runner_kind(), RunnerKind::Bb);

        // Respawning r0 (attempt=2) after config changed preserves its journaled route and model=None.
        let mut r0_respawn = r0_req;
        r0_respawn.attempt = 2;
        let t0_a2 = router.spawn(r0_respawn).await.unwrap();
        assert_eq!(t0_a2.runner_kind(), RunnerKind::Bb);

        let log = captured.lock().unwrap();
        assert_eq!(log.len(), 5);
        assert_eq!(log[0].0, RunnerKind::Bb);
        assert_eq!(log[0].1.model, None);

        assert_eq!(log[1].0, RunnerKind::Claude);
        assert_eq!(
            log[1].1.model.as_ref().map(ModelId::as_str),
            Some("claude-opus-5-5")
        );
        assert_eq!(log[1].1.reasoning.as_deref(), Some("max"));
        assert_eq!(log[1].1.account.as_deref(), Some("team-a"));

        assert_eq!(log[2].0, RunnerKind::T3);
        assert_eq!(
            log[2].1.model.as_ref().map(ModelId::as_str),
            Some("explicit-model")
        );
        assert_eq!(log[2].1.reasoning.as_deref(), Some("xhigh"));
        assert_eq!(log[2].1.account.as_deref(), Some("team-b"));

        assert_eq!(log[3].0, RunnerKind::Bb);
        assert_eq!(
            log[3].1.model.as_ref().map(ModelId::as_str),
            Some("gpt-6.2-codex")
        );

        assert_eq!(log[4].0, RunnerKind::Bb);
        assert_eq!(log[4].1.model, None);
    }

    #[tokio::test]
    async fn retry_same_attempt_after_lost_reply_and_route_change_stays_on_first_runner() {
        struct FlakyBbHost {
            calls: Arc<Mutex<Vec<(u32, String)>>>,
        }
        impl AgentHost for FlakyBbHost {
            fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<HostThreadId, HostError>> {
                let n = {
                    let mut g = self.calls.lock().unwrap();
                    g.push((req.attempt, req.agent.provider.as_str().to_owned()));
                    g.len()
                };
                let tid = HostThreadId::parse(format!("th_bb_{}:{}", req.wave, req.role)).unwrap();
                Box::pin(async move {
                    if n == 1 {
                        Err(HostError::Unreachable("lost spawn reply".into()))
                    } else {
                        Ok(tid)
                    }
                })
            }
            fn tell(
                &self,
                _t: &HostThreadId,
                _b: &Body,
                _m: SteerDeliveryMode,
            ) -> BoxFut<'_, Result<(), HostError>> {
                Box::pin(async { Ok(()) })
            }
            fn resolve(
                &self,
                _t: &HostThreadId,
                _i: &HostInteractionId,
                _r: &InteractionResolution,
            ) -> BoxFut<'_, Result<(), HostError>> {
                Box::pin(async { Ok(()) })
            }
            fn retire(&self, _t: &HostThreadId) -> BoxFut<'_, Result<(), HostError>> {
                Box::pin(async { Ok(()) })
            }
        }

        let bb_calls = Arc::new(Mutex::new(Vec::new()));
        let t3_seen = Arc::new(Mutex::new(Vec::new()));
        let pin_dir = tempfile::tempdir().unwrap();
        let pin_path = pin_dir.path().join("router-pins.json");
        let (cfg_tx, cfg_rx) = watch::channel(RexConfig::default());
        let mk_router = || {
            HostRouter::new(RunnerKind::Bb)
                .with_config_watch(cfg_rx.clone())
                .with_pin_store(pin_path.clone())
                .with_host(
                    RunnerKind::Bb,
                    FlakyBbHost {
                        calls: bb_calls.clone(),
                    },
                )
                .with_host(
                    RunnerKind::T3,
                    StubHost {
                        kind: RunnerKind::T3,
                        seen: t3_seen.clone(),
                    },
                )
        };
        let router = mk_router();

        let wave = WaveSlug::parse("w-retry").unwrap();
        let proj = ProjectId::parse("/tmp/proj").unwrap();
        let mk_req = |attempt: u32| SpawnRequest {
            wave: wave.clone(),
            project: proj.clone(),
            role: RoleName::parse("worker").unwrap(),
            attempt,
            agent: AgentSpec {
                provider: ProviderId::parse("codex").unwrap(),
                model: None,
                account: None,
                reasoning: None,
                service_tier: None,
                permission: PermissionMode::Full,
                workspace: Workspace::ProjectDefault,
            },
            room: wave_room(&wave, &proj),
            prompt: Body::parse("go").unwrap(),
            title: "[w-retry] @worker".into(),
            run_stamp: format!("w-retry:worker:attempt-{attempt}:1"),
            output_dir: None,
            writes: Vec::new(),
            return_schema: None,
        };

        assert!(matches!(
            router.spawn(mk_req(1)).await,
            Err(HostError::Unreachable(_))
        ));

        let mut switched = RexConfig::default();
        switched.runners.codex = Some(RunnerRoute::T3);
        cfg_tx.send_replace(switched);

        // Recreate a brand-new HostRouter (simulating daemon restart) after config flipped to T3;
        // `router-pins.json` keeps the legacy bare-provider attempt=1 pinned to BB.
        let restarted_router = mk_router();
        let retried = restarted_router.spawn(mk_req(1)).await.unwrap();
        assert_eq!(retried.runner_kind(), RunnerKind::Bb);
        assert_eq!(bb_calls.lock().unwrap().len(), 2);
        assert!(
            t3_seen.lock().unwrap().is_empty(),
            "attempt 1 retry across restart must not cross over to T3"
        );

        let next_attempt = restarted_router.spawn(mk_req(2)).await.unwrap();
        assert_eq!(next_attempt.runner_kind(), RunnerKind::T3);
        assert_eq!(t3_seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unregistered_runner_returns_rejected_so_conductor_fails_fast() {
        let mut cfg = RexConfig::default();
        cfg.runners.codex = Some(RunnerRoute::Native);
        let (_tx, rx) = watch::channel(cfg);
        let router = HostRouter::new(RunnerKind::Bb).with_config_watch(rx);
        let wave = WaveSlug::parse("w-missing").unwrap();
        let proj = ProjectId::parse("/tmp/proj").unwrap();
        let res = router
            .spawn(SpawnRequest {
                wave: wave.clone(),
                project: proj.clone(),
                role: RoleName::parse("dev").unwrap(),
                attempt: 1,
                agent: AgentSpec {
                    provider: ProviderId::parse("codex").unwrap(),
                    model: None,
                    account: None,
                    reasoning: None,
                    service_tier: None,
                    permission: PermissionMode::Full,
                    workspace: Workspace::ProjectDefault,
                },
                room: wave_room(&wave, &proj),
                prompt: Body::parse("go").unwrap(),
                title: "[w-missing] @dev".into(),
                run_stamp: "w-missing:dev:attempt-1:1".into(),
                output_dir: None,
                writes: Vec::new(),
                return_schema: None,
            })
            .await;
        assert!(
            matches!(res, Err(HostError::Rejected(_))),
            "expected Rejected for unregistered host, got {res:?}"
        );
    }
}
