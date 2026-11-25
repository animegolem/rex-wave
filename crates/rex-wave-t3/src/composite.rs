//! Compatibility adapters (`CompositeHost`, `CompositeFeed`) delegating to the
//! N-way [`rex_wave_waves::HostRouter`] and [`rex_wave_runtime::FeedRouter`].

use rex_wave_api::read::ThreadPageView;
use rex_wave_api::room::Body;
use rex_wave_api::wave::{InteractionResolution, Observation, SteerDeliveryMode};
use rex_wave_api::{
    ApiError, BbInteractionId, BbProjectId, BbSeq, BbThreadId, FrameStream, ProviderId, RoleName,
    RunnerKind, WaveSlug,
};
use rex_wave_runtime::{AgentFeed, BoxFut, FeedRouter};
use rex_wave_waves::{AgentHost, HostError, HostRouter, SpawnRequest};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::watch;

pub use rex_wave_runtime::ThreadIdResolver;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunnerTarget {
    Bb,
    T3,
}

impl From<RunnerTarget> for RunnerKind {
    fn from(t: RunnerTarget) -> Self {
        match t {
            RunnerTarget::Bb => RunnerKind::Bb,
            RunnerTarget::T3 => RunnerKind::T3,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedProvider {
    pub target: RunnerTarget,
    pub provider: ProviderId,
}

pub fn route_provider(
    provider: &ProviderId,
    default_runner: RunnerTarget,
) -> Result<RoutedProvider, HostError> {
    let routed = rex_wave_waves::route_provider(provider, default_runner.into())?;
    let target = match routed.target {
        RunnerKind::Bb => RunnerTarget::Bb,
        RunnerKind::T3 => RunnerTarget::T3,
        other => {
            return Err(HostError::Rejected(format!(
                "runner '{}' requires N-way HostRouter",
                other.as_str()
            )));
        }
    };
    Ok(RoutedProvider {
        target,
        provider: routed.provider,
    })
}

pub fn route_thread(thread: &BbThreadId) -> RunnerTarget {
    match rex_wave_waves::route_thread(thread) {
        RunnerKind::T3 => RunnerTarget::T3,
        _ => RunnerTarget::Bb,
    }
}

/// Two-runner compatibility wrapper around [`HostRouter`].
#[derive(Clone)]
pub struct CompositeHost<B, T> {
    inner: HostRouter,
    default_runner: RunnerTarget,
    _marker: std::marker::PhantomData<fn() -> (B, T)>,
}

impl<B: AgentHost, T: AgentHost> CompositeHost<B, T> {
    pub fn new(bb: Option<B>, t3: Option<T>, default_runner: RunnerTarget) -> Self {
        let inner = HostRouter::new(default_runner.into())
            .with_optional_host(RunnerKind::Bb, bb)
            .with_optional_host(RunnerKind::T3, t3);
        CompositeHost {
            inner,
            default_runner,
            _marker: std::marker::PhantomData,
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
    fn spawn(&self, req: SpawnRequest) -> BoxFut<'_, Result<BbThreadId, HostError>> {
        self.inner.spawn(req)
    }

    fn tell(
        &self,
        thread: &BbThreadId,
        body: &Body,
        mode: SteerDeliveryMode,
    ) -> BoxFut<'_, Result<(), HostError>> {
        self.inner.tell(thread, body, mode)
    }

    fn resolve(
        &self,
        thread: &BbThreadId,
        interaction: &BbInteractionId,
        resolution: &InteractionResolution,
    ) -> BoxFut<'_, Result<(), HostError>> {
        self.inner.resolve(thread, interaction, resolution)
    }

    fn retire(&self, thread: &BbThreadId) -> BoxFut<'_, Result<(), HostError>> {
        self.inner.retire(thread)
    }

    fn check_live(
        &self,
        thread: &BbThreadId,
    ) -> BoxFut<'_, Result<Option<Observation>, HostError>> {
        self.inner.check_live(thread)
    }
}

/// Two-runner compatibility wrapper around [`FeedRouter`].
pub struct CompositeFeed {
    inner: FeedRouter,
}

impl CompositeFeed {
    pub fn new(bb: Option<Arc<dyn AgentFeed>>, t3: Option<Arc<dyn AgentFeed>>) -> Self {
        let mut entries = Vec::new();
        if let Some(t) = t3 {
            entries.push((RunnerKind::T3, t));
        }
        if let Some(b) = bb {
            entries.push((RunnerKind::Bb, b));
        }
        CompositeFeed {
            inner: FeedRouter::new(entries),
        }
    }

    pub fn with_thread_resolver(mut self, resolver: ThreadIdResolver) -> Self {
        self.inner = self.inner.with_thread_resolver(resolver);
        self
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
        self.inner.page(wave, role, after, limit)
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        self.inner.live(wave, role)
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<BbProjectId, String>>>> {
        self.inner.project_names()
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
