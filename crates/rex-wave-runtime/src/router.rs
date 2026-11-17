use crate::read::ReadSide;
use crate::{BoxFut, DecideCtx, Service, now};
use rex_wave_api::*;
use std::future::Future;
use std::sync::Arc;
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
