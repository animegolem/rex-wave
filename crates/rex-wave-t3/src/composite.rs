//! Multi-host routing: `CompositeHost` (`AgentHost`) and `CompositeFeed` (`AgentFeed`).
//!
//! Structurally enforces the 2026-11-21 routing rule: bare `antigravity` ALWAYS routes
//! to `T3Host`, and `bb:antigravity` is rejected outright.

use crate::protocol::is_t3_thread_id;
use rex_wave_api::read::ThreadPageView;
use rex_wave_api::room::Body;
use rex_wave_api::wave::{InteractionResolution, Observation, SteerDeliveryMode};
use rex_wave_api::{
    ApiError, BbInteractionId, BbProjectId, BbSeq, BbThreadId, Family, FrameStream, ProviderId,
    RoleName, WaveSlug,
};
use rex_wave_runtime::{AgentFeed, BoxFut};
use rex_wave_waves::{AgentHost, HostError, SpawnRequest};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunnerTarget {
    Bb,
    T3,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedProvider {
    pub target: RunnerTarget,
    pub provider: ProviderId,
}

/// Resolves which host owns `provider` and strips any explicit `t3:` / `bb:` prefix.
///
/// Bare `antigravity` ALWAYS routes to `RunnerTarget::T3` regardless of `default_runner`,
/// and `bb:antigravity` is rejected to structurally enforce the T3-only Gemini policy.
pub fn route_provider(
    provider: &ProviderId,
    default_runner: RunnerTarget,
) -> Result<RoutedProvider, HostError> {
    let raw = provider.as_str().trim();
    if let Some(rest) = raw.strip_prefix("t3:") {
        let clean = rest.trim();
        if clean.is_empty() {
            return Err(HostError::Rejected(
                "empty provider name after 't3:' prefix".into(),
            ));
        }
        let parsed = ProviderId::parse(clean).map_err(|e| HostError::Rejected(e.to_string()))?;
        return Ok(RoutedProvider {
            target: RunnerTarget::T3,
            provider: parsed,
        });
    }

    if let Some(rest) = raw.strip_prefix("bb:") {
        let clean = rest.trim();
        if clean.is_empty() {
            return Err(HostError::Rejected(
                "empty provider name after 'bb:' prefix".into(),
            ));
        }
        if clean.eq_ignore_ascii_case("antigravity") {
            return Err(HostError::Rejected(
                "provider 'antigravity' must route through T3 Code, never BB".into(),
            ));
        }
        let parsed = ProviderId::parse(clean).map_err(|e| HostError::Rejected(e.to_string()))?;
        return Ok(RoutedProvider {
            target: RunnerTarget::Bb,
            provider: parsed,
        });
    }

    if raw.eq_ignore_ascii_case("antigravity") {
        let parsed = ProviderId::parse(raw).map_err(|e| HostError::Rejected(e.to_string()))?;
        return Ok(RoutedProvider {
            target: RunnerTarget::T3,
            provider: parsed,
        });
    }

    Ok(RoutedProvider {
        target: default_runner,
        provider: provider.clone(),
    })
}

pub fn route_thread(thread: &BbThreadId) -> RunnerTarget {
    if is_t3_thread_id(thread) {
        RunnerTarget::T3
    } else {
        RunnerTarget::Bb
    }
}

/// Multiplexes `AgentHost` calls across an optional BB host and an optional T3 host.
#[derive(Clone)]
pub struct CompositeHost<B, T> {
    bb: Option<B>,
    t3: Option<T>,
    default_runner: RunnerTarget,
}

impl<B, T> CompositeHost<B, T> {
    pub fn new(bb: Option<B>, t3: Option<T>, default_runner: RunnerTarget) -> Self {
        CompositeHost {
            bb,
            t3,
            default_runner,
        }
    }

    pub fn both(bb: B, t3: T, default_runner: RunnerTarget) -> Self {
        Self::new(Some(bb), Some(t3), default_runner)
    }

    pub fn default_runner(&self) -> RunnerTarget {
        self.default_runner
    }
}

impl<B: AgentHost, T: AgentHost> AgentHost for CompositeHost<B, T> {
    fn spawn(
        &self,
        mut req: SpawnRequest,
    ) -> impl Future<Output = Result<BbThreadId, HostError>> + Send {
        let routed = route_provider(&req.agent.provider, self.default_runner);
        async move {
            let routed = routed?;
            let is_antigravity = routed.provider.as_str().eq_ignore_ascii_case("antigravity");
            req.agent.provider = routed.provider;
            match routed.target {
                RunnerTarget::T3 => match &self.t3 {
                    Some(t3) => t3.spawn(req).await,
                    None if is_antigravity => Err(HostError::Rejected(
                        "provider 'antigravity' requires T3 Code, but no T3 host is configured"
                            .into(),
                    )),
                    None => Err(HostError::Unreachable("T3 host is not configured".into())),
                },
                RunnerTarget::Bb => match &self.bb {
                    Some(bb) => bb.spawn(req).await,
                    None => Err(HostError::Unreachable("BB host is not configured".into())),
                },
            }
        }
    }

    fn tell(
        &self,
        thread: &BbThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> impl Future<Output = Result<(), HostError>> + Send {
        let target = route_thread(thread);
        let thread = thread.clone();
        let body = body.clone();
        async move {
            match target {
                RunnerTarget::T3 => match &self.t3 {
                    Some(t3) => t3.tell(&thread, &body, mode).await,
                    None => Err(HostError::Unreachable("T3 host is not configured".into())),
                },
                RunnerTarget::Bb => match &self.bb {
                    Some(bb) => bb.tell(&thread, &body, mode).await,
                    None => Err(HostError::Unreachable("BB host is not configured".into())),
                },
            }
        }
    }

    fn resolve(
        &self,
        thread: &BbThreadId,
        interaction: &BbInteractionId,
        resolution: &InteractionResolution,
    ) -> impl Future<Output = Result<(), HostError>> + Send {
        let target = route_thread(thread);
        let thread = thread.clone();
        let interaction = interaction.clone();
        let resolution = resolution.clone();
        async move {
            match target {
                RunnerTarget::T3 => match &self.t3 {
                    Some(t3) => t3.resolve(&thread, &interaction, &resolution).await,
                    None => Err(HostError::Unreachable("T3 host is not configured".into())),
                },
                RunnerTarget::Bb => match &self.bb {
                    Some(bb) => bb.resolve(&thread, &interaction, &resolution).await,
                    None => Err(HostError::Unreachable("BB host is not configured".into())),
                },
            }
        }
    }

    fn retire(&self, thread: &BbThreadId) -> impl Future<Output = Result<(), HostError>> + Send {
        let target = route_thread(thread);
        let thread = thread.clone();
        async move {
            match target {
                RunnerTarget::T3 => match &self.t3 {
                    Some(t3) => t3.retire(&thread).await,
                    None => Err(HostError::Unreachable("T3 host is not configured".into())),
                },
                RunnerTarget::Bb => match &self.bb {
                    Some(bb) => bb.retire(&thread).await,
                    None => Err(HostError::Unreachable("BB host is not configured".into())),
                },
            }
        }
    }

    fn check_live(
        &self,
        thread: &BbThreadId,
    ) -> impl Future<Output = Result<Option<Observation>, HostError>> + Send {
        let target = route_thread(thread);
        let thread = thread.clone();
        async move {
            match target {
                RunnerTarget::T3 => match &self.t3 {
                    Some(t3) => t3.check_live(&thread).await,
                    None => Err(HostError::Unreachable("T3 host is not configured".into())),
                },
                RunnerTarget::Bb => match &self.bb {
                    Some(bb) => bb.check_live(&thread).await,
                    None => Err(HostError::Unreachable("BB host is not configured".into())),
                },
            }
        }
    }
}

pub type ThreadIdResolver =
    Arc<dyn Fn(&WaveSlug, &RoleName) -> BoxFut<'static, Option<BbThreadId>> + Send + Sync>;

/// Multiplexes `AgentFeed` across BB and T3 feeds, routing by `t3:` thread-ID prefix
/// and merging both `project_names` watch channels.
pub struct CompositeFeed {
    bb: Option<Arc<dyn AgentFeed>>,
    t3: Option<Arc<dyn AgentFeed>>,
    resolver: Option<ThreadIdResolver>,
    merged_project_names: Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>>,
}

impl CompositeFeed {
    pub fn new(bb: Option<Arc<dyn AgentFeed>>, t3: Option<Arc<dyn AgentFeed>>) -> Self {
        let merged_project_names = merge_project_name_watches(
            bb.as_ref().and_then(|f| f.project_names()),
            t3.as_ref().and_then(|f| f.project_names()),
        );
        CompositeFeed {
            bb,
            t3,
            resolver: None,
            merged_project_names,
        }
    }

    pub fn with_thread_resolver(mut self, resolver: ThreadIdResolver) -> Self {
        self.resolver = Some(resolver);
        self
    }

    async fn resolve_target(&self, wave: &WaveSlug, role: &RoleName) -> Option<RunnerTarget> {
        let resolver = self.resolver.as_ref()?;
        let thread = resolver(wave, role).await?;
        Some(route_thread(&thread))
    }
}

fn merge_maps(
    bb: &BTreeMap<BbProjectId, String>,
    t3: &BTreeMap<BbProjectId, String>,
) -> Arc<BTreeMap<BbProjectId, String>> {
    let mut out = t3.clone();
    for (k, v) in bb {
        out.insert(k.clone(), v.clone());
    }
    Arc::new(out)
}

fn merge_project_name_watches(
    bb_rx: Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>>,
    t3_rx: Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>>,
) -> Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
    match (bb_rx, t3_rx) {
        (None, None) => None,
        (Some(rx), None) | (None, Some(rx)) => Some(rx),
        (Some(mut bb), Some(mut t3)) => {
            let initial = merge_maps(&bb.borrow_and_update(), &t3.borrow_and_update());
            let (tx, rx) = watch::channel(initial);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let mut bb_open = true;
                    let mut t3_open = true;
                    while bb_open || t3_open {
                        tokio::select! {
                            biased;
                            () = tx.closed() => break,
                            res = bb.changed(), if bb_open => {
                                if res.is_err() {
                                    bb_open = false;
                                    continue;
                                }
                            }
                            res = t3.changed(), if t3_open => {
                                if res.is_err() {
                                    t3_open = false;
                                    continue;
                                }
                            }
                        }
                        let next = merge_maps(&bb.borrow_and_update(), &t3.borrow_and_update());
                        tx.send_if_modified(|cur| {
                            if **cur != *next {
                                *cur = next;
                                true
                            } else {
                                false
                            }
                        });
                    }
                });
            }
            Some(rx)
        }
    }
}

impl AgentFeed for CompositeFeed {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<BbSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            if let Some(target) = self.resolve_target(&wave, &role).await {
                return match target {
                    RunnerTarget::T3 => match &self.t3 {
                        Some(f) => f.page(&wave, &role, after, limit).await,
                        None => Err(ApiError::Unavailable {
                            family: Family::Wave,
                        }),
                    },
                    RunnerTarget::Bb => match &self.bb {
                        Some(f) => f.page(&wave, &role, after, limit).await,
                        None => Err(ApiError::Unavailable {
                            family: Family::Wave,
                        }),
                    },
                };
            }

            if let Some(t3) = &self.t3 {
                match t3.page(&wave, &role, after, limit).await {
                    Ok(page) => return Ok(page),
                    Err(ApiError::NotFound { .. }) if self.bb.is_some() => {}
                    Err(e) => return Err(e),
                }
            }
            if let Some(bb) = &self.bb {
                return bb.page(&wave, &role, after, limit).await;
            }
            Err(ApiError::Unavailable {
                family: Family::Wave,
            })
        })
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            if let Some(target) = self.resolve_target(&wave, &role).await {
                return match target {
                    RunnerTarget::T3 => match &self.t3 {
                        Some(f) => f.live(&wave, &role).await,
                        None => Err(ApiError::Unavailable {
                            family: Family::Wave,
                        }),
                    },
                    RunnerTarget::Bb => match &self.bb {
                        Some(f) => f.live(&wave, &role).await,
                        None => Err(ApiError::Unavailable {
                            family: Family::Wave,
                        }),
                    },
                };
            }

            if let Some(t3) = &self.t3 {
                match t3.live(&wave, &role).await {
                    Ok(stream) => return Ok(stream),
                    Err(ApiError::NotFound { .. }) if self.bb.is_some() => {}
                    Err(e) => return Err(e),
                }
            }
            if let Some(bb) = &self.bb {
                return bb.live(&wave, &role).await;
            }
            Err(ApiError::Unavailable {
                family: Family::Wave,
            })
        })
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
        self.merged_project_names.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_provider_and_thread_routing_rules() {
        let p = |s: &str| ProviderId::parse(s).unwrap();

        let bare_ag = route_provider(&p("antigravity"), RunnerTarget::Bb).unwrap();
        assert_eq!(bare_ag.target, RunnerTarget::T3);
        assert_eq!(bare_ag.provider.as_str(), "antigravity");

        assert!(route_provider(&p("bb:antigravity"), RunnerTarget::Bb).is_err());

        let t3_claude = route_provider(&p("t3:claudeAgent"), RunnerTarget::Bb).unwrap();
        assert_eq!(t3_claude.target, RunnerTarget::T3);
        assert_eq!(t3_claude.provider.as_str(), "claudeAgent");

        let bb_claude = route_provider(&p("bb:claude-code"), RunnerTarget::T3).unwrap();
        assert_eq!(bb_claude.target, RunnerTarget::Bb);
        assert_eq!(bb_claude.provider.as_str(), "claude-code");

        let def_bb = route_provider(&p("codex"), RunnerTarget::Bb).unwrap();
        assert_eq!(def_bb.target, RunnerTarget::Bb);
        assert_eq!(def_bb.provider.as_str(), "codex");

        let def_t3 = route_provider(&p("codex"), RunnerTarget::T3).unwrap();
        assert_eq!(def_t3.target, RunnerTarget::T3);
        assert_eq!(def_t3.provider.as_str(), "codex");

        assert_eq!(
            route_thread(&BbThreadId::parse("t3:w1:r1:a1").unwrap()),
            RunnerTarget::T3
        );
        assert_eq!(
            route_thread(&BbThreadId::parse("th_bb_123").unwrap()),
            RunnerTarget::Bb
        );
    }
}
