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
            mail_failures: Vec::new(),
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
/// 1. enforce `protected-roots` and TOML limits before any journal append,
/// 2. sync `config.toml` atomically in monotonic `Seq` order after commit, and
/// 3. attach live runner & courier health onto [`ConfigSnapshot`].
pub trait ConfigHook: Send + Sync + 'static {
    fn validate_candidate<'a>(
        &'a self,
        current: &'a RexConfig,
        next: &'a RexConfig,
        will_export: bool,
    ) -> BoxFut<'a, Result<(), ApiError>>;
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

struct ConfigWriteCoordinator {
    synced_seq: Seq,
    persist_error: Option<(Seq, String)>,
}

/// Service owner for `Family::Config`, pairing pure `Registry<ConfigDecider>` with
/// a live `watch::Receiver<RexConfig>` and cancellation-safe serialized `config.toml` persistence.
pub struct ConfigService {
    inner: Arc<dyn Service<ConfigCommand>>,
    tx: watch::Sender<RexConfig>,
    persist_err_tx: watch::Sender<Option<String>>,
    coord: Arc<Mutex<ConfigWriteCoordinator>>,
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
        let (persist_err_tx, _) = watch::channel(None);
        let inner: Arc<dyn Service<ConfigCommand>> = Registry::<ConfigDecider>::new(journal, hub);
        Ok(Arc::new(Self {
            inner,
            tx,
            persist_err_tx,
            coord: Arc::new(Mutex::new(ConfigWriteCoordinator {
                synced_seq: initial_head,
                persist_error: None,
            })),
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
                    if let WireReply::ConfigShow(snap) = &mut reply {
                        if let Some(ref hook) = self.hook {
                            hook.augment_snapshot(snap).await;
                        }
                        if snap.persist_error.is_none() {
                            snap.persist_error = self.persist_err_tx.borrow().clone();
                        }
                    }
                    Ok(reply)
                }
                ConfigCommand::ConfigSet(_) | ConfigCommand::ConfigSyncFile(_) => {
                    // Run validation, journal commit, and file export inside a daemon-owned
                    // non-cancellable task holding `coord` to completion so a disconnected UDS
                    // caller can never release the lock while an older blocking export is still in flight.
                    let inner = self.inner.clone();
                    let tx = self.tx.clone();
                    let persist_err_tx = self.persist_err_tx.clone();
                    let coord = self.coord.clone();
                    let hook = self.hook.clone();
                    let join = tokio::spawn(async move {
                        let mut state = coord.lock().await;
                        if let Some(ref h) = hook {
                            let cur = tx.borrow().clone();
                            match &cmd {
                                ConfigCommand::ConfigSet(c) => {
                                    let mut candidate = cur.clone();
                                    candidate.set_key(&c.key, c.value.as_deref())?;
                                    h.validate_candidate(&cur, &candidate, true).await?;
                                }
                                ConfigCommand::ConfigSyncFile(c) => {
                                    c.config.validate()?;
                                    h.validate_candidate(&cur, &c.config, false).await?;
                                }
                                ConfigCommand::ConfigShow(_) | ConfigCommand::ConfigGet(_) => {}
                            }
                        }

                        let mut reply = inner.dispatch(cmd, ctx).await?;
                        match &mut reply {
                            WireReply::ConfigSet(snap) => {
                                if snap.head > state.synced_seq {
                                    tx.send_replace(snap.config.clone());
                                    if let Some(ref h) = hook {
                                        match h.on_committed(snap.head, &snap.config).await {
                                            Ok(()) => {
                                                state.synced_seq = snap.head;
                                                state.persist_error = None;
                                                persist_err_tx.send_replace(None);
                                            }
                                            Err(err) => {
                                                let msg = err.to_string();
                                                eprintln!(
                                                    "rex-wave config: committed seq #{} to journal, but config.toml sync failed: {msg}",
                                                    snap.head.0
                                                );
                                                state.persist_error =
                                                    Some((snap.head, msg.clone()));
                                                persist_err_tx.send_replace(Some(msg.clone()));
                                                snap.persist_error = Some(msg);
                                            }
                                        }
                                    } else {
                                        state.synced_seq = snap.head;
                                        state.persist_error = None;
                                        persist_err_tx.send_replace(None);
                                    }
                                } else if let Some((_, ref msg)) = state.persist_error {
                                    snap.persist_error = Some(msg.clone());
                                }
                                drop(state);
                                if let Some(ref h) = hook {
                                    h.augment_snapshot(snap).await;
                                }
                            }
                            WireReply::ConfigSyncFile(snap) => {
                                if snap.head > state.synced_seq {
                                    tx.send_replace(snap.config.clone());
                                    state.synced_seq = snap.head;
                                    state.persist_error = None;
                                    persist_err_tx.send_replace(None);
                                }
                                drop(state);
                                if let Some(ref h) = hook {
                                    h.augment_snapshot(snap).await;
                                }
                            }
                            _ => {}
                        }
                        Ok::<WireReply, ApiError>(reply)
                    });

                    join.await
                        .map_err(|e| ApiError::Internal(format!("config writer task join: {e}")))?
                }
            }
        })
    }
}
