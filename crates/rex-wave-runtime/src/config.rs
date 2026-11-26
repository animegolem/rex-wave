//! Pure `Family::Config` decider (`StreamId::Config`) and serialized file-sync wrapper.

use crate::{BoxFut, DecideCtx, Decider, EntryMeta, Hub, Registry, Service};
use rex_wave_api::config::{
    ConfigEvent, ConfigSnapshot, ConfigValueView, RexConfig, canonicalize_config_key,
};
use rex_wave_api::{ApiError, ConfigCommand, Envelope, Event, IdemKey, Seq, StreamId, WireReply};
use rex_wave_store::Journal;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

#[derive(Clone, Debug, Default)]
pub struct ConfigState {
    pub head: Seq,
    pub config: RexConfig,
}

/// Pure journaled `StreamId::Config` decider (zero I/O).
pub struct ConfigDecider;

impl Decider for ConfigDecider {
    type State = ConfigState;
    type Cmd = ConfigCommand;
    type Event = ConfigEvent;

    fn address(_cmd: &ConfigCommand, _ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        Ok(StreamId::Config)
    }

    fn idem(cmd: &ConfigCommand) -> Option<IdemKey> {
        match cmd {
            ConfigCommand::ConfigSet(c) => c.idem.clone(),
            ConfigCommand::ConfigShow(_)
            | ConfigCommand::ConfigGet(_)
            | ConfigCommand::ConfigSyncFile(_) => None,
        }
    }

    fn decide(
        state: &ConfigState,
        cmd: &ConfigCommand,
        _ctx: &DecideCtx,
    ) -> Result<Vec<ConfigEvent>, ApiError> {
        match cmd {
            ConfigCommand::ConfigShow(_) => Ok(vec![]),
            ConfigCommand::ConfigGet(c) => {
                let _ = state.config.get_key(&c.key)?;
                Ok(vec![])
            }
            ConfigCommand::ConfigSet(c) => {
                let mut next = state.config.clone();
                let (canon, norm_val) = next.set_key(&c.key, c.value.as_deref())?;
                if next == state.config {
                    return Ok(vec![]);
                }
                Ok(vec![ConfigEvent::Updated {
                    key: canon.to_owned(),
                    value: norm_val,
                    config: Box::new(next),
                }])
            }
            ConfigCommand::ConfigSyncFile(c) => {
                c.config.validate()?;
                if c.config == state.config {
                    return Ok(vec![]);
                }
                Ok(vec![ConfigEvent::SyncedFromFile {
                    config: Box::new(c.config.clone()),
                }])
            }
        }
    }

    fn evolve(state: &mut ConfigState, event: &ConfigEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        state.config = event.config().clone();
    }

    fn reply(state: &ConfigState, cmd: ConfigCommand, _committed: &[Envelope]) -> WireReply {
        let mk_snapshot = || ConfigSnapshot {
            head: state.head,
            config: state.config.clone(),
            config_path: None,
            effective_default_runner: state.config.runners.default_runner,
            runners_health: Vec::new(),
            couriers_health: Vec::new(),
            persist_error: None,
        };
        match cmd {
            ConfigCommand::ConfigShow(_) => WireReply::ConfigShow(mk_snapshot()),
            ConfigCommand::ConfigGet(c) => {
                let canon = canonicalize_config_key(&c.key).unwrap_or(c.key.as_str());
                let val = state.config.get_key(canon).ok().flatten();
                WireReply::ConfigGet(ConfigValueView {
                    head: state.head,
                    key: canon.to_owned(),
                    value: val,
                })
            }
            ConfigCommand::ConfigSet(_) => WireReply::ConfigSet(mk_snapshot()),
            ConfigCommand::ConfigSyncFile(_) => WireReply::ConfigSyncFile(mk_snapshot()),
        }
    }

    fn wrap(event: ConfigEvent) -> Event {
        Event::Config(event)
    }

    fn unwrap(event: &Event) -> Option<&ConfigEvent> {
        match event {
            Event::Config(e) => Some(e),
            _ => None,
        }
    }
}

/// Optional boundary & persistence hook used by the daemon composition root to:
/// 1. enforce `protected-roots` before any journal append,
/// 2. sync `config.toml` atomically in monotonic `Seq` order after commit, and
/// 3. attach live runner & courier health onto [`ConfigSnapshot`].
pub trait ConfigHook: Send + Sync + 'static {
    fn validate_candidate(&self, next: &RexConfig) -> Result<(), ApiError>;
    fn on_committed(&self, head: Seq, config: &RexConfig) -> BoxFut<'_, Result<(), ApiError>>;
    fn augment_snapshot<'a>(&'a self, snap: &'a mut ConfigSnapshot) -> BoxFut<'a, ()>;
}

/// Reads the latest committed `(head, RexConfig)` from `journal` by replaying `StreamId::Config`.
pub fn replay_config_from_journal(journal: &Journal) -> Result<(Seq, RexConfig), ApiError> {
    let mut after = Seq::ORIGIN;
    let mut head = Seq::ORIGIN;
    let mut cfg = RexConfig::default();
    loop {
        let page = journal
            .read(&StreamId::Config, after, 512)
            .map_err(crate::store_error)?;
        if page.is_empty() {
            break;
        }
        for env in page {
            after = env.seq;
            head = env.seq;
            if let Event::Config(ev) = env.event {
                cfg = ev.config().clone();
            }
        }
    }
    Ok((head, cfg))
}

/// Service owner for `Family::Config`, pairing pure `Registry<ConfigDecider>` with
/// a live `watch::Receiver<RexConfig>` and serialized `config.toml` persistence.
pub struct ConfigService {
    inner: Arc<dyn Service<ConfigCommand>>,
    tx: watch::Sender<RexConfig>,
    synced_seq: Mutex<Seq>,
    hook: Option<Arc<dyn ConfigHook>>,
}

impl ConfigService {
    pub fn new(journal: Journal, hub: Hub) -> Result<Arc<Self>, ApiError> {
        Self::with_hook(journal, hub, None)
    }

    pub fn with_hook(
        journal: Journal,
        hub: Hub,
        hook: Option<Arc<dyn ConfigHook>>,
    ) -> Result<Arc<Self>, ApiError> {
        let (initial_head, initial_cfg) = replay_config_from_journal(&journal)?;
        let (tx, _) = watch::channel(initial_cfg);
        let inner: Arc<dyn Service<ConfigCommand>> = Registry::<ConfigDecider>::new(journal, hub);
        Ok(Arc::new(Self {
            inner,
            tx,
            synced_seq: Mutex::new(initial_head),
            hook,
        }))
    }

    pub fn subscribe(&self) -> watch::Receiver<RexConfig> {
        self.tx.subscribe()
    }

    pub fn current(&self) -> RexConfig {
        self.tx.borrow().clone()
    }
}

impl Service<ConfigCommand> for ConfigService {
    fn dispatch(
        &self,
        cmd: ConfigCommand,
        ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            match &cmd {
                ConfigCommand::ConfigShow(_) | ConfigCommand::ConfigGet(_) => {
                    let mut reply = self.inner.dispatch(cmd, ctx).await?;
                    if let WireReply::ConfigShow(snap) = &mut reply
                        && let Some(ref hook) = self.hook
                    {
                        hook.augment_snapshot(snap).await;
                    }
                    Ok(reply)
                }
                ConfigCommand::ConfigSet(_) | ConfigCommand::ConfigSyncFile(_) => {
                    let mut last_seq = self.synced_seq.lock().await;
                    if let Some(ref hook) = self.hook {
                        match &cmd {
                            ConfigCommand::ConfigSet(c) => {
                                let mut candidate = self.tx.borrow().clone();
                                candidate.set_key(&c.key, c.value.as_deref())?;
                                hook.validate_candidate(&candidate)?;
                            }
                            ConfigCommand::ConfigSyncFile(c) => {
                                c.config.validate()?;
                                hook.validate_candidate(&c.config)?;
                            }
                            ConfigCommand::ConfigShow(_) | ConfigCommand::ConfigGet(_) => {}
                        }
                    }

                    let mut reply = self.inner.dispatch(cmd, ctx).await?;
                    match &mut reply {
                        WireReply::ConfigSet(snap) => {
                            if snap.head > *last_seq {
                                self.tx.send_replace(snap.config.clone());
                                if let Some(ref hook) = self.hook {
                                    match hook.on_committed(snap.head, &snap.config).await {
                                        Ok(()) => {
                                            *last_seq = snap.head;
                                        }
                                        Err(err) => {
                                            let msg = err.to_string();
                                            eprintln!(
                                                "rex-wave config: committed seq #{} to journal, but config.toml sync failed: {msg}",
                                                snap.head.0
                                            );
                                            snap.persist_error = Some(msg);
                                        }
                                    }
                                } else {
                                    *last_seq = snap.head;
                                }
                            }
                            drop(last_seq);
                            if let Some(ref hook) = self.hook {
                                hook.augment_snapshot(snap).await;
                            }
                        }
                        WireReply::ConfigSyncFile(snap) => {
                            if snap.head > *last_seq {
                                self.tx.send_replace(snap.config.clone());
                                *last_seq = snap.head;
                            }
                            drop(last_seq);
                            if let Some(ref hook) = self.hook {
                                hook.augment_snapshot(snap).await;
                            }
                        }
                        _ => {}
                    }
                    Ok(reply)
                }
            }
        })
    }
}
