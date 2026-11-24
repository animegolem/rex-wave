use crate::read::ReadSide;
use crate::{BoxFut, DecideCtx, Service, now};
use rex_wave_api::wave::{Activity, Observation, WaveObserve, WaveView, WorkerPhase};
use rex_wave_api::*;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::watch;

/// One authenticated connection (or in-process client): its principal and its
/// single stage slot. Created by the transport after `Hello`, or by
/// [`LocalClient::new`].
#[derive(Clone)]
pub struct Session {
    me: Principal,
    stage: Arc<watch::Sender<FocusGeneration>>,
}

impl Session {
    pub fn new(me: Principal) -> Self {
        Session {
            me,
            stage: Arc::new(watch::channel(FocusGeneration::default()).0),
        }
    }

    pub fn me(&self) -> &Principal {
        &self.me
    }

    /// Claim the stage slot: every older stage watch on this session ends.
    pub(crate) fn focus(&self) -> (FocusGeneration, watch::Receiver<FocusGeneration>) {
        let mut generation = FocusGeneration::default();
        self.stage.send_modify(|g| {
            *g = g.next();
            generation = *g;
        });
        (generation, self.stage.subscribe())
    }
}

/// The daemon's front desk. One slot per family; built by struct literal in
/// the composition root so a missing owner is a compile error.
pub struct Router {
    pub room: Arc<dyn Service<RoomCommand>>,
    pub wave: Arc<dyn Service<WaveCommand>>,
    pub board: Arc<dyn Service<BoardCommand>>,
    pub inbox: Arc<dyn Service<InboxCommand>>,
    pub mail: Arc<dyn Service<MailCommand>>,
    pub presence: Arc<dyn Service<PresenceCommand>>,
    pub read: Arc<ReadSide>,
}

pub use rex_wave_api::authorize;

type ActiveWaitKey = (usize, WaveSlug, RoleName);

/// Upper bound applied when a `Principal::Worker` issues a `Wait` with
/// `timeout_ms: None` so an abandoned call cannot leave a worker `Parked` forever.
pub const MAX_UNBOUNDED_WORKER_WAIT_MS: u64 = 10 * 60 * 1000;

fn active_worker_waits() -> &'static Mutex<HashMap<ActiveWaitKey, usize>> {
    static MAP: OnceLock<Mutex<HashMap<ActiveWaitKey, usize>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn has_active_wait(key: &ActiveWaitKey) -> bool {
    active_worker_waits()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(key)
}

fn increment_active_wait(key: &ActiveWaitKey) -> usize {
    let mut map = active_worker_waits()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let entry = map.entry(key.clone()).or_insert(0);
    *entry += 1;
    *entry
}

fn decrement_active_wait(key: &ActiveWaitKey) -> usize {
    let mut map = active_worker_waits()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let Some(count) = map.get_mut(key) else {
        return 0;
    };
    *count = count.saturating_sub(1);
    let remaining = *count;
    if remaining == 0 {
        map.remove(key);
    }
    remaining
}

fn resolve_room_cmd(
    read: &ReadSide,
    by: &Principal,
    mut cmd: RoomCommand,
) -> Result<RoomCommand, ApiError> {
    use rex_wave_api::room::RoomRef;
    use rex_wave_store::DmIdemResolution;
    let idem = cmd.effective_idem();
    let slot = match &mut cmd {
        RoomCommand::RoomEnsure(c) => &mut c.room,
        RoomCommand::RoomPost(c) => &mut c.room,
        RoomCommand::RoomJoin(c) => &mut c.room,
        RoomCommand::RoomLeave(c) => &mut c.room,
        RoomCommand::RoomLabel(c) => &mut c.room,
        RoomCommand::TaskClaim(c) => &mut c.room,
        RoomCommand::TaskHeartbeat(c) => &mut c.room,
        RoomCommand::TaskTransition(c) => &mut c.room,
        RoomCommand::TaskUpdate(c) => &mut c.room,
        RoomCommand::RoomTasks(c) => &mut c.room,
    };
    let allow_legacy_fallback = matches!(slot, RoomRef::Dm { .. });
    let resolved = slot.resolve(by)?;
    let target = match read
        .journal
        .resolve_dm_room_with_idem(resolved, by, idem.as_ref(), allow_legacy_fallback)
        .map_err(crate::store_error)?
    {
        DmIdemResolution::Route(room) => room,
        DmIdemResolution::CrossAuthor { .. } => {
            return Err(ApiError::conflict(format!(
                "idempotency key {:?} in this DM was already used by another participant",
                idem.as_ref().map(IdemKey::as_str).unwrap_or_default()
            )));
        }
        DmIdemResolution::Ambiguous(_) => {
            return Err(ApiError::conflict(format!(
                "ambiguous idempotency key {:?} exists on both legacy and canonical DM histories; specify an explicit RoomId or a fresh idem key",
                idem.as_ref().map(IdemKey::as_str).unwrap_or_default()
            )));
        }
    };
    *slot = RoomRef::Id { id: target };
    Ok(cmd)
}

fn system_ctx() -> DecideCtx {
    DecideCtx::new(
        Principal::System {
            part: SystemPart::WaveConductor,
        },
        now(),
    )
}

async fn restore_worker_working(
    key: &ActiveWaitKey,
    wave_svc: &Arc<dyn Service<WaveCommand>>,
    wave: &WaveSlug,
    role: &RoleName,
) {
    if has_active_wait(key) {
        return;
    }
    let still_parked = match wave_svc
        .dispatch(
            WaveCommand::WaveView(WaveView { wave: wave.clone() }),
            system_ctx(),
        )
        .await
    {
        Ok(WireReply::WaveView(snap)) => snap.workers.iter().any(|(r, _, phase)| {
            r == role
                && matches!(
                    phase,
                    WorkerPhase::Live {
                        activity: Activity::Parked { .. },
                        ..
                    }
                )
        }),
        _ => false,
    };
    if still_parked && !has_active_wait(key) {
        let _ = wave_svc
            .dispatch(
                WaveCommand::WaveObserve(WaveObserve {
                    wave: wave.clone(),
                    role: role.clone(),
                    observation: Observation::Activity {
                        activity: Activity::Working,
                    },
                }),
                system_ctx(),
            )
            .await;
    }
}

struct WorkerWaitGuard {
    key: ActiveWaitKey,
    wave_svc: Arc<dyn Service<WaveCommand>>,
    wave: WaveSlug,
    role: RoleName,
    completed: bool,
}

impl WorkerWaitGuard {
    fn enter(wave_svc: Arc<dyn Service<WaveCommand>>, wave: WaveSlug, role: RoleName) -> Self {
        let key = (
            Arc::as_ptr(&wave_svc).cast::<()>() as usize,
            wave.clone(),
            role.clone(),
        );
        increment_active_wait(&key);
        WorkerWaitGuard {
            key,
            wave_svc,
            wave,
            role,
            completed: false,
        }
    }

    async fn finish(mut self) {
        self.completed = true;
        if decrement_active_wait(&self.key) == 0 {
            restore_worker_working(&self.key, &self.wave_svc, &self.wave, &self.role).await;
        }
    }
}

impl Drop for WorkerWaitGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if decrement_active_wait(&self.key) == 0
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let key = self.key.clone();
            let wave_svc = self.wave_svc.clone();
            let wave = self.wave.clone();
            let role = self.role.clone();
            handle.spawn(async move {
                restore_worker_working(&key, &wave_svc, &wave, &role).await;
            });
        }
    }
}

impl Router {
    /// Constructs a partial-deployment or test [`Router`] where every command
    /// family defaults to [`Unavailable`] over `read`. Callers override only
    /// the families under test while production composition roots still name
    /// every field explicitly.
    pub fn unavailable_with(read: Arc<ReadSide>) -> Self {
        Router {
            room: Arc::new(Unavailable(Family::Room)),
            wave: Arc::new(Unavailable(Family::Wave)),
            board: Arc::new(Unavailable(Family::Board)),
            inbox: Arc::new(Unavailable(Family::Inbox)),
            mail: Arc::new(Unavailable(Family::Mail)),
            presence: Arc::new(Unavailable(Family::Presence)),
            read,
        }
    }

    pub async fn dispatch(&self, by: &Principal, cmd: WireCommand) -> Result<WireReply, ApiError> {
        authorize(cmd.meta().audience, by)?;
        let ctx = DecideCtx::new(by.clone(), now());
        match cmd.route() {
            Routed::Room(c) => {
                let resolved = resolve_room_cmd(&self.read, by, c)?;
                self.room.dispatch(resolved, ctx).await
            }
            Routed::Wave(c) => self.wave.dispatch(c, ctx).await,
            Routed::Board(c) => self.board.dispatch(c, ctx).await,
            Routed::Inbox(mut c) => {
                match &mut c {
                    InboxCommand::InboxMarkRead(m) => {
                        m.room = self
                            .read
                            .journal
                            .resolve_dm_room(m.room.clone())
                            .map_err(crate::store_error)?;
                    }
                    InboxCommand::SetArchived(s) => {
                        if let rex_wave_api::read::ArchiveTarget::Room { room } = &mut s.target {
                            *room = self
                                .read
                                .journal
                                .resolve_dm_room(room.clone())
                                .map_err(crate::store_error)?;
                        }
                    }
                    _ => {}
                }
                self.inbox.dispatch(c, ctx).await
            }
            Routed::Mail(mut c) => {
                if let MailCommand::MailPromote(p) = &mut c {
                    p.room = self
                        .read
                        .journal
                        .resolve_dm_room(p.room.clone())
                        .map_err(crate::store_error)?;
                }
                self.mail.dispatch(c, ctx).await
            }
            Routed::Presence(c) => self.presence.dispatch(c, ctx).await,
            Routed::Read(ReadCommand::Wait(mut wait)) => {
                if wait.timeout_ms != Some(0)
                    && matches!(by, Principal::Worker { .. })
                    && wait.timeout_ms.is_none()
                {
                    wait.timeout_ms = Some(MAX_UNBOUNDED_WORKER_WAIT_MS);
                }
                let prepared = self.read.prepare_wait(by.clone(), wait)?;
                let guard = if prepared.timeout_ms() != Some(0)
                    && let Principal::Worker { wave, role } = by
                {
                    let (room, since) = prepared.parked_scope();
                    let guard =
                        WorkerWaitGuard::enter(self.wave.clone(), wave.clone(), role.clone());
                    let _ = self
                        .wave
                        .dispatch(
                            WaveCommand::WaveObserve(WaveObserve {
                                wave: wave.clone(),
                                role: role.clone(),
                                observation: Observation::Activity {
                                    activity: Activity::Parked { room, since },
                                },
                            }),
                            system_ctx(),
                        )
                        .await;
                    Some(guard)
                } else {
                    None
                };
                let outcome = self
                    .read
                    .run_prepared_wait(prepared)
                    .await
                    .map(WireReply::Wait);
                if let Some(g) = guard {
                    g.finish().await;
                }
                outcome
            }
            Routed::Read(c) => self.read.dispatch(c, ctx).await,
        }
    }

    pub async fn watch(
        &self,
        session: &Session,
        interest: Interest,
        from: Cursor,
    ) -> Result<FrameStream, ApiError> {
        self.read.watch(session.clone(), interest, from).await
    }
}

/// A family with no owner in this process. Every call fails with
/// `ApiError::Unavailable`; useful in tests and partial deployments.
pub struct Unavailable(pub Family);

impl<C: Send + 'static> Service<C> for Unavailable {
    fn dispatch(&self, _cmd: C, _ctx: DecideCtx) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let family = self.0;
        Box::pin(async move { Err(ApiError::Unavailable { family }) })
    }
}

/// Forwards a family to another process (e.g. a split-out Kanban app) through
/// the same `Client` trait. The remote authorizes the forwarding daemon's own
/// session principal; carrying `ctx.by()` across is an open question (see
/// ARCHITECTURE.md).
pub struct Remote<C: Client> {
    pub client: C,
    pub family: Family,
}

impl<C, F> Service<F> for Remote<C>
where
    C: Client + 'static,
    F: Into<WireCommand> + Send + 'static,
{
    fn dispatch(&self, cmd: F, _ctx: DecideCtx) -> BoxFut<'_, Result<WireReply, ApiError>> {
        let wire: WireCommand = cmd.into();
        Box::pin(async move {
            self.client.call_wire(wire).await.map_err(|e| match e {
                ApiError::Transport(_) => ApiError::Unavailable {
                    family: self.family,
                },
                other => other,
            })
        })
    }
}

/// In-process `Client`. Used by reactors (with a `System` principal), tests,
/// and any embedded UI. Same semantics as the remote client by construction.
#[derive(Clone)]
pub struct LocalClient {
    router: Arc<Router>,
    session: Session,
}

impl LocalClient {
    pub fn new(router: Arc<Router>, me: Principal) -> Self {
        LocalClient {
            router,
            session: Session::new(me),
        }
    }
}

impl Client for LocalClient {
    fn me(&self) -> &Principal {
        self.session.me()
    }
    fn call_wire(
        &self,
        cmd: WireCommand,
    ) -> impl Future<Output = Result<WireReply, ApiError>> + Send {
        self.router.dispatch(self.session.me(), cmd)
    }
    fn watch(
        &self,
        interest: Interest,
        from: Cursor,
    ) -> impl Future<Output = Result<FrameStream, ApiError>> + Send {
        self.router.watch(&self.session, interest, from)
    }
}

/// Host-bridge backend abstraction shared by `BbBridge`, `T3Bridge`, and native
/// runners so bridges can run in-process against `Arc<Router>` or against
/// custom test backends.
pub trait BridgeBackend: Send + Sync + 'static {
    type C: Client + Clone + 'static;
    fn client(&self, me: Principal) -> Self::C;
    fn handles_worker_wait_parking(&self) -> bool {
        false
    }
    fn parked_room_and_since(
        &self,
        worker: &Principal,
        wait: &rex_wave_api::read::Wait,
    ) -> Option<(Option<rex_wave_api::room::RoomId>, Seq)> {
        crate::fallback_parked_room_and_since(worker, wait)
    }
}

impl BridgeBackend for Arc<Router> {
    type C = LocalClient;
    fn client(&self, me: Principal) -> LocalClient {
        LocalClient::new(self.clone(), me)
    }
    fn handles_worker_wait_parking(&self) -> bool {
        true
    }
    fn parked_room_and_since(
        &self,
        worker: &Principal,
        wait: &rex_wave_api::read::Wait,
    ) -> Option<(Option<rex_wave_api::room::RoomId>, Seq)> {
        self.read.parked_room_and_since(worker, wait)
    }
}
