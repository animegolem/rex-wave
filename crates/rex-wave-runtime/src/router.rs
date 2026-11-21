use crate::read::ReadSide;
use crate::{BoxFut, DecideCtx, Service, now};
use rex_wave_api::room::RoomId;
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

/// Pure audience check. Finer policy belongs to deciders.
pub fn authorize(audience: Audience, by: &Principal) -> Result<(), ApiError> {
    let ok = match (audience, by) {
        (Audience::System, by) => matches!(by, Principal::System { .. }),
        (Audience::Operator, by) => matches!(by, Principal::Operator { .. }),
        (Audience::Member, _) => true,
    };
    if ok {
        Ok(())
    } else {
        Err(ApiError::Denied {
            principal: by.to_string(),
            action: format!("{audience:?} command"),
        })
    }
}

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

fn parked_room_and_since(
    worker: &Principal,
    wait: &rex_wave_api::read::Wait,
) -> (Option<RoomId>, Seq) {
    match &wait.interest {
        Interest::Room { room } | Interest::RoomFiltered { room, .. } => match room.resolve(worker)
        {
            Ok(id) => {
                let since = wait.after.at(&StreamId::Room { room: id.clone() });
                (Some(id), since)
            }
            Err(_) => (None, Seq::ORIGIN),
        },
        Interest::Streams { streams } if streams.len() == 1 => match &streams[0] {
            StreamId::Room { room } => (Some(room.clone()), wait.after.at(&streams[0])),
            _ => (None, Seq::ORIGIN),
        },
        _ => (None, Seq::ORIGIN),
    }
}

fn system_ctx() -> DecideCtx {
    DecideCtx {
        by: Principal::System {
            part: SystemPart::WaveConductor,
        },
        at: now(),
    }
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
    pub async fn dispatch(&self, by: &Principal, cmd: WireCommand) -> Result<WireReply, ApiError> {
        authorize(cmd.meta().audience, by)?;
        let ctx = DecideCtx {
            by: by.clone(),
            at: now(),
        };
        match cmd.route() {
            Routed::Room(c) => self.room.dispatch(c, ctx).await,
            Routed::Wave(c) => self.wave.dispatch(c, ctx).await,
            Routed::Board(c) => self.board.dispatch(c, ctx).await,
            Routed::Inbox(c) => self.inbox.dispatch(c, ctx).await,
            Routed::Mail(c) => self.mail.dispatch(c, ctx).await,
            Routed::Presence(c) => self.presence.dispatch(c, ctx).await,
            Routed::Read(ReadCommand::Wait(mut wait)) => {
                let guard = if wait.timeout_ms != Some(0)
                    && let Principal::Worker { wave, role } = by
                {
                    if wait.timeout_ms.is_none() {
                        wait.timeout_ms = Some(MAX_UNBOUNDED_WORKER_WAIT_MS);
                    }
                    let (room, since) = parked_room_and_since(by, &wait);
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
                let outcome = self.read.dispatch(ReadCommand::Wait(wait), ctx).await;
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
/// session principal; carrying `ctx.by` across is an open question (see
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
