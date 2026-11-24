//! N-way `HostRouter` multiplexing `AgentHost` across arbitrary runner backends
//! (`Bb`, `T3`, `Acp`, `Claude`, `Codex`) over `Arc<dyn AgentHost>`.

use crate::{AgentHost, BoxFut, HostError, SpawnRequest};
use rex_wave_api::room::Body;
use rex_wave_api::wave::{InteractionResolution, Observation, SteerDeliveryMode};
use rex_wave_api::{HostInteractionId, HostThreadId, ProviderId, RunnerKind};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedProvider {
    pub target: RunnerKind,
    pub provider: ProviderId,
}

/// Resolves which runner owns `provider` and strips any explicit `<runner>:` prefix.
///
/// Bare `antigravity` routes to `RunnerKind::T3` regardless of `default_runner`,
/// and `bb:antigravity` is rejected outright. Explicit `acp:antigravity` routes
/// to `RunnerKind::Acp` for native ACP launches.
pub fn route_provider(
    provider: &ProviderId,
    default_runner: RunnerKind,
) -> Result<RoutedProvider, HostError> {
    let raw = provider.as_str().trim();
    if let Some((target, clean)) = RunnerKind::strip_provider_prefix(raw) {
        if clean.is_empty() {
            return Err(HostError::Rejected(format!(
                "empty provider name after '{}:' prefix",
                target.as_str()
            )));
        }
        if target == RunnerKind::Bb && clean.eq_ignore_ascii_case("antigravity") {
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

    if raw.eq_ignore_ascii_case("antigravity") {
        let parsed = ProviderId::parse(raw).map_err(|e| HostError::Rejected(e.to_string()))?;
        return Ok(RoutedProvider {
            target: RunnerKind::T3,
            provider: parsed,
        });
    }

    Ok(RoutedProvider {
        target: default_runner,
        provider: provider.clone(),
    })
}

pub fn route_thread(thread: &HostThreadId) -> RunnerKind {
    thread.runner_kind()
}

/// N-way `AgentHost` multiplexer backed by `Arc<dyn AgentHost>` slots per [`RunnerKind`].
#[derive(Clone)]
pub struct HostRouter {
    hosts: BTreeMap<RunnerKind, Arc<dyn AgentHost>>,
    default_runner: RunnerKind,
}

impl HostRouter {
    pub fn new(default_runner: RunnerKind) -> Self {
        HostRouter {
            hosts: BTreeMap::new(),
            default_runner,
        }
    }

    pub fn from_entries(
        default_runner: RunnerKind,
        entries: impl IntoIterator<Item = (RunnerKind, Arc<dyn AgentHost>)>,
    ) -> Self {
        HostRouter {
            hosts: entries.into_iter().collect(),
            default_runner,
        }
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
        let routed = route_provider(&req.agent.provider, self.default_runner);
        Box::pin(async move {
            let routed = routed?;
            let is_bare_antigravity = routed.target == RunnerKind::T3
                && routed.provider.as_str().eq_ignore_ascii_case("antigravity");
            req.agent.provider = routed.provider;
            match self.hosts.get(&routed.target) {
                Some(host) => host.spawn(req).await,
                None if is_bare_antigravity => Err(HostError::Rejected(
                    "provider 'antigravity' requires T3 Code, but no T3 host is configured".into(),
                )),
                None => Err(HostError::Unreachable(format!(
                    "{} host is not configured",
                    routed.target.display_name()
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
}
