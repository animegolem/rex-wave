use crate::hub::Hub;
use crate::{BoxFut, DecideCtx, Decider, EntryMeta, Service, store_error};
use rex_wave_api::{ApiError, Envelope, StreamId, WireReply};
use rex_wave_store::{Appended, Journal, StreamWriter};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const MAILBOX: usize = 64;
const DEFAULT_IDLE: Duration = Duration::from_secs(300);
const REHYDRATE_PAGE: usize = 512;

struct ActorMsg<C> {
    cmd: C,
    ctx: DecideCtx,
    reply: oneshot::Sender<Result<WireReply, ApiError>>,
}

type Mailbox<C> = mpsc::Sender<ActorMsg<C>>;

/// Builds the empty state a stream actor folds its journal into. Runs on every
/// (re)hydration, so passivated actors come back with the same seed.
type StateSeed<S> = std::sync::Arc<dyn Fn() -> S + Send + Sync>;

enum RegistryMsg<C> {
    Lookup {
        stream: StreamId,
        reply: oneshot::Sender<Mailbox<C>>,
    },
    Gone {
        stream: StreamId,
        incarnation: u64,
    },
}

/// Spawn-on-demand actors for one decider, one per stream. Implements
/// `Service<D::Cmd>`.
///
/// The stream map is owned by a registry task, not shared behind a lock.
/// Callers ask it for a stream's mailbox and then talk to the stream actor
/// directly, so the registry is never on the path of a command's work.
pub struct Registry<D: Decider> {
    tx: mpsc::UnboundedSender<RegistryMsg<D::Cmd>>,
}

impl<D: Decider> Registry<D> {
    pub fn new(journal: Journal, hub: Hub) -> std::sync::Arc<Self> {
        Self::with_idle(journal, hub, DEFAULT_IDLE)
    }

    /// `idle` is how long a stream actor waits for a command before it
    /// passivates (drops its state and its writer claim).
    pub fn with_idle(journal: Journal, hub: Hub, idle: Duration) -> std::sync::Arc<Self> {
        Self::with_seed(journal, hub, idle, std::sync::Arc::new(D::State::default))
    }

    /// Like [`Registry::new`], but every stream actor starts from a clone of
    /// `initial` instead of `D::State::default()`. For read-only configuration
    /// a decider consults (its `evolve` must leave that part untouched).
    pub fn with_initial_state(journal: Journal, hub: Hub, initial: D::State) -> std::sync::Arc<Self>
    where
        D::State: Clone + Sync,
    {
        Self::with_seed(
            journal,
            hub,
            DEFAULT_IDLE,
            std::sync::Arc::new(move || initial.clone()),
        )
    }

    fn with_seed(
        journal: Journal,
        hub: Hub,
        idle: Duration,
        seed: StateSeed<D::State>,
    ) -> std::sync::Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let owner = RegistryTask::<D> {
            journal,
            hub,
            idle,
            seed,
            me: tx.downgrade(),
            live: HashMap::new(),
            waiting: HashMap::new(),
            incarnations: 0,
        };
        tokio::spawn(owner.run(rx));
        std::sync::Arc::new(Registry { tx })
    }

    async fn mailbox(&self, stream: StreamId) -> Result<Mailbox<D::Cmd>, ApiError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(RegistryMsg::Lookup { stream, reply })
            .map_err(|_| ApiError::Internal("registry stopped".into()))?;
        rx.await
            .map_err(|_| ApiError::Internal("registry dropped lookup".into()))
    }
}

impl<D: Decider> Service<D::Cmd> for Registry<D> {
    fn dispatch(&self, cmd: D::Cmd, ctx: DecideCtx) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            let stream = D::address(&cmd, &ctx)?;
            let (reply, rx) = oneshot::channel();
            let mut msg = ActorMsg { cmd, ctx, reply };
            let mut attempts = 0;
            loop {
                let mailbox = self.mailbox(stream.clone()).await?;
                match mailbox.send(msg).await {
                    Ok(()) => break,
                    // The actor passivated between lookup and send; the
                    // registry respawns it once its claim is released.
                    Err(mpsc::error::SendError(back)) if attempts < 8 => {
                        msg = back;
                        attempts += 1;
                    }
                    Err(_) => return Err(ApiError::Internal("stream actor unavailable".into())),
                }
            }
            rx.await
                .map_err(|_| ApiError::Internal("actor dropped reply".into()))?
        })
    }
}

struct Slot<C> {
    mailbox: Mailbox<C>,
    incarnation: u64,
}

struct RegistryTask<D: Decider> {
    journal: Journal,
    hub: Hub,
    idle: Duration,
    seed: StateSeed<D::State>,
    me: mpsc::WeakUnboundedSender<RegistryMsg<D::Cmd>>,
    live: HashMap<StreamId, Slot<D::Cmd>>,
    waiting: HashMap<StreamId, Vec<oneshot::Sender<Mailbox<D::Cmd>>>>,
    incarnations: u64,
}

impl<D: Decider> RegistryTask<D> {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<RegistryMsg<D::Cmd>>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                RegistryMsg::Lookup { stream, reply } => match self.live.get(&stream) {
                    Some(slot) if !slot.mailbox.is_closed() => {
                        let _ = reply.send(slot.mailbox.clone());
                    }
                    // Draining: a new actor could not claim the writer yet.
                    Some(_) => self.waiting.entry(stream).or_default().push(reply),
                    None => {
                        let mailbox = self.spawn(stream);
                        let _ = reply.send(mailbox);
                    }
                },
                RegistryMsg::Gone {
                    stream,
                    incarnation,
                } => {
                    if self
                        .live
                        .get(&stream)
                        .is_some_and(|s| s.incarnation == incarnation)
                    {
                        self.live.remove(&stream);
                        if let Some(waiters) = self.waiting.remove(&stream) {
                            let mailbox = self.spawn(stream);
                            for w in waiters {
                                let _ = w.send(mailbox.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    fn spawn(&mut self, stream: StreamId) -> Mailbox<D::Cmd> {
        self.incarnations += 1;
        let (mailbox, inbox) = mpsc::channel(MAILBOX);
        let gone = GoneOnDrop::<D> {
            registry: self.me.clone(),
            stream: Some(stream.clone()),
            incarnation: self.incarnations,
        };
        tokio::spawn(run_actor::<D>(
            stream.clone(),
            self.journal.clone(),
            self.hub.clone(),
            (self.seed)(),
            inbox,
            self.idle,
            gone,
        ));
        self.live.insert(
            stream,
            Slot {
                mailbox: mailbox.clone(),
                incarnation: self.incarnations,
            },
        );
        mailbox
    }
}

struct GoneOnDrop<D: Decider> {
    registry: mpsc::WeakUnboundedSender<RegistryMsg<D::Cmd>>,
    stream: Option<StreamId>,
    incarnation: u64,
}

impl<D: Decider> Drop for GoneOnDrop<D> {
    fn drop(&mut self) {
        if let (Some(registry), Some(stream)) = (self.registry.upgrade(), self.stream.take()) {
            let _ = registry.send(RegistryMsg::Gone {
                stream,
                incarnation: self.incarnation,
            });
        }
    }
}

async fn run_actor<D: Decider>(
    stream: StreamId,
    journal: Journal,
    hub: Hub,
    initial: D::State,
    mut inbox: mpsc::Receiver<ActorMsg<D::Cmd>>,
    idle: Duration,
    gone: GoneOnDrop<D>,
) {
    let mut actor = match StreamActor::<D>::rehydrate(&journal, stream, hub, initial) {
        Ok(actor) => actor,
        Err(e) => {
            inbox.close();
            while let Some(msg) = inbox.recv().await {
                let _ = msg.reply.send(Err(e.clone()));
            }
            drop(gone);
            return;
        }
    };
    loop {
        match tokio::time::timeout(idle, inbox.recv()).await {
            Ok(Some(msg)) => {
                if !actor.handle(msg) {
                    break;
                }
            }
            Ok(None) => break,
            Err(_idle) => {
                inbox.close();
                while let Some(msg) = inbox.recv().await {
                    actor.handle(msg);
                }
                break;
            }
        }
    }
    // The writer claim must be released before the registry hears `Gone`,
    // or the replacement actor's claim would fail.
    drop(actor);
    drop(gone);
}

/// One stream's actor: owns its state and its unique writer.
struct StreamActor<D: Decider> {
    writer: StreamWriter,
    state: D::State,
    hub: Hub,
    stale: bool,
}

impl<D: Decider> StreamActor<D> {
    fn rehydrate(
        journal: &Journal,
        stream: StreamId,
        hub: Hub,
        initial: D::State,
    ) -> Result<Self, ApiError> {
        let writer = journal.claim(stream).map_err(store_error)?;
        let mut actor = StreamActor::<D> {
            writer,
            state: initial,
            hub,
            stale: false,
        };
        let mut after = rex_wave_api::Seq::ORIGIN;
        loop {
            let page = journal
                .read(actor.writer.stream(), after, REHYDRATE_PAGE)
                .map_err(store_error)?;
            let Some(last) = page.last() else { break };
            after = last.seq;
            for e in &page {
                actor.evolve(e);
            }
        }
        Ok(actor)
    }

    fn evolve(&mut self, e: &Envelope) {
        if let Some(event) = D::unwrap(&e.event) {
            D::evolve(
                &mut self.state,
                event,
                &EntryMeta {
                    seq: e.seq,
                    at: e.at,
                    by: e.by.clone(),
                },
            );
        }
    }

    /// Returns false when the actor must stop because its view of the
    /// journal is stale (another process wrote the stream).
    fn handle(&mut self, msg: ActorMsg<D::Cmd>) -> bool {
        let ActorMsg { cmd, ctx, reply } = msg;
        let _ = reply.send(self.execute(cmd, &ctx));
        !self.stale
    }

    fn execute(&mut self, cmd: D::Cmd, ctx: &DecideCtx) -> Result<WireReply, ApiError> {
        let idem = D::idem(&cmd);
        if let Some(key) = &idem
            && let Some(prior) = self.writer.lookup(key).map_err(store_error)?
        {
            return Ok(D::reply(&self.state, cmd, &prior));
        }
        let events = D::decide(&self.state, &cmd, ctx)?;
        if events.is_empty() {
            return Ok(D::reply(&self.state, cmd, &[]));
        }
        let events = events.into_iter().map(D::wrap).collect();
        let appended = self
            .writer
            .append(ctx.by(), ctx.at(), idem.as_ref(), events)
            .map_err(|e| {
                self.stale = matches!(e, rex_wave_store::StoreError::HeadMoved { .. });
                store_error(e)
            })?;
        match appended {
            Appended::Fresh(committed) => {
                for e in &committed {
                    self.evolve(e);
                }
                self.hub.publish(&committed);
                Ok(D::reply(&self.state, cmd, &committed))
            }
            Appended::Replayed(prior) => Ok(D::reply(&self.state, cmd, &prior)),
        }
    }
}
