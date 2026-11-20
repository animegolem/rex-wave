//! # rex-wave-transport
//!
//! The only crate that knows bytes. Everything above it speaks `api::Client`.
//!
//! Wire: one multiplexed connection per client, frames are
//! `u32-BE length || JSON`. Calls and watches carry a client-chosen `id`, so
//! many watches (sidebar tree, stage) share one socket and a stage retarget or
//! drop is `Unwatch{old}` + `Watch{new}` with no reconnect.
//!
//! Auth happens once per connection, in `Hello`, and yields the session
//! [`Principal`]:
//! * Unix socket + peer uid == daemon uid -> `Operator{seat}` (TUI, CLI);
//! * `Token` minted by `rex-wave token --seat sam` -> `Seat{sam}` (MCP);
//! * the BB bridge runs in-process and never uses this crate.
//!
//! [`RemoteClient`] outlives its socket. When the connection drops (a daemon
//! restart unlinks and rebinds the socket), it redials with backoff, sends
//! `Hello` again, and re-opens every live watch from the last cursor it
//! delivered, so callers keep one `FrameStream` across the restart. Calls in
//! flight at the drop fail with the cause, since the daemon may or may not have
//! run them.

use futures_util::StreamExt;
use rex_wave_api::*;
use rex_wave_runtime::{Router, Session};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinSet};

const MAX_FRAME: usize = 16 * 1024 * 1024;
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const WATCH_BUFFER: usize = 128;
const REDIAL_MIN: Duration = Duration::from_millis(50);
const REDIAL_MAX: Duration = Duration::from_secs(2);
const MAX_LOG_LINE: usize = 512;

fn log_daemon(args: std::fmt::Arguments<'_>) {
    let line = args.to_string();
    if line.len() <= MAX_LOG_LINE {
        eprintln!("rex-wave daemon: {line}");
    } else {
        let mut end = MAX_LOG_LINE;
        while !line.is_char_boundary(end) {
            end -= 1;
        }
        eprintln!("rex-wave daemon: {}...", &line[..end]);
    }
}

/// Daemon-side log line on stderr. Only the serving half logs: a client may be
/// drawing a full-screen TUI on the same terminal.
macro_rules! daemon_log {
    ($($arg:tt)*) => {
        log_daemon(format_args!($($arg)*))
    };
}

mod wire {
    use rex_wave_api::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "t", rename_all = "snake_case")]
    pub enum ClientFrame {
        Hello {
            credential: super::Credential,
            client: String,
        },
        Call {
            id: u64,
            cmd: WireCommand,
        },
        Watch {
            id: u64,
            interest: Interest,
            from: Cursor,
        },
        Unwatch {
            id: u64,
        },
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "t", rename_all = "snake_case")]
    pub enum ServerFrame {
        Welcome {
            result: Result<Principal, ApiError>,
            daemon: String,
        },
        Reply {
            id: u64,
            result: Result<WireReply, ApiError>,
        },
        WatchOpened {
            id: u64,
            result: Result<(), ApiError>,
        },
        Frame {
            id: u64,
            frame: Frame,
        },
        WatchEnded {
            id: u64,
            error: Option<ApiError>,
        },
    }

    impl ServerFrame {
        pub fn kind(&self) -> &'static str {
            match self {
                ServerFrame::Welcome { .. } => "welcome",
                ServerFrame::Reply { .. } => "reply",
                ServerFrame::WatchOpened { .. } => "watch_opened",
                ServerFrame::Frame { .. } => "frame",
                ServerFrame::WatchEnded { .. } => "watch_ended",
            }
        }

        /// The frame to send instead when this one cannot be encoded: the
        /// same answer slot, carrying `error`. A watch frame ends its watch.
        pub fn fallback(&self, error: ApiError) -> ServerFrame {
            match self {
                ServerFrame::Welcome { daemon, .. } => ServerFrame::Welcome {
                    result: Err(error),
                    daemon: daemon.clone(),
                },
                ServerFrame::Reply { id, .. } => ServerFrame::Reply {
                    id: *id,
                    result: Err(error),
                },
                ServerFrame::WatchOpened { id, .. } => ServerFrame::WatchOpened {
                    id: *id,
                    result: Err(error),
                },
                ServerFrame::Frame { id, .. } | ServerFrame::WatchEnded { id, .. } => {
                    ServerFrame::WatchEnded {
                        id: *id,
                        error: Some(error),
                    }
                }
            }
        }
    }
}

fn encode<T: Serialize>(msg: &T) -> io::Result<Vec<u8>> {
    let bytes =
        serde_json::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if bytes.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds 16 MiB",
        ));
    }
    Ok(bytes)
}

async fn write_bytes<W: AsyncWrite + Unpin>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    w.write_u32(bytes.len() as u32).await?;
    w.write_all(bytes).await?;
    w.flush().await
}

async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, msg: &T) -> io::Result<()> {
    write_bytes(w, &encode(msg)?).await
}

/// Write `frame`, or its [`wire::ServerFrame::fallback`] if it cannot be
/// encoded, so one bad payload answers one request instead of closing the
/// connection. Errors only when the socket itself fails.
async fn write_server_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    conn: u64,
    frame: &wire::ServerFrame,
) -> io::Result<()> {
    let bytes = match encode(frame) {
        Ok(bytes) => bytes,
        Err(e) => {
            let kind = frame.kind();
            daemon_log!(
                "conn #{conn}: cannot encode {kind} frame ({e}); sending an internal error"
            );
            encode(&frame.fallback(ApiError::Internal(format!(
                "daemon could not encode {kind} frame: {e}"
            ))))?
        }
    };
    write_bytes(w, &bytes).await
}

async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(r: &mut R) -> io::Result<Option<T>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds 16 MiB",
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    let msg =
        serde_json::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(msg))
}

/// Aborts a spawned task when dropped, so a connection's helper tasks die with it.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Where the daemon listens.
#[derive(Clone, Debug)]
pub enum Endpoint {
    /// Default: `$XDG_STATE_HOME/rex-wave/daemon.sock` (mode 0600).
    Unix(PathBuf),
    /// Reserved for a browser / embedded GUI over loopback with a token.
    WebSocket(String),
}

impl Endpoint {
    pub fn default_unix() -> Self {
        let base = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
            .unwrap_or_else(std::env::temp_dir);
        Endpoint::Unix(base.join("rex-wave/daemon.sock"))
    }
}

/// How a client proves who it is. Sent once, in `Hello`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Credential {
    /// Trust the Unix peer uid (operator on the same account).
    PeerUid,
    /// A capability token naming one seat, minted by the daemon.
    Token { token: String },
}

/// Maps a credential (plus the kernel-verified peer uid, on Unix) to a principal.
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(
        &self,
        peer_uid: Option<u32>,
        credential: &Credential,
    ) -> Result<Principal, ApiError>;
}

/// Standard authenticator: `PeerUid` succeeds when the peer's Unix uid equals
/// `daemon_uid`, and `Token` looks up a seat in a token map.
pub struct TableAuthenticator {
    pub operator: SeatName,
    pub daemon_uid: Option<u32>,
    pub tokens: BTreeMap<String, SeatName>,
}

impl Authenticator for TableAuthenticator {
    fn authenticate(
        &self,
        peer_uid: Option<u32>,
        credential: &Credential,
    ) -> Result<Principal, ApiError> {
        match credential {
            Credential::PeerUid => match (peer_uid, self.daemon_uid) {
                (Some(peer), Some(daemon)) if peer == daemon => Ok(Principal::Operator {
                    seat: self.operator.clone(),
                }),
                _ => Err(ApiError::Denied {
                    principal: "anonymous".into(),
                    action: "peer-uid hello".into(),
                }),
            },
            Credential::Token { token } => match self.tokens.get(token) {
                Some(seat) => Ok(Principal::Seat { seat: seat.clone() }),
                None => Err(ApiError::Denied {
                    principal: "anonymous".into(),
                    action: "invalid token".into(),
                }),
            },
        }
    }
}

/// Where and how to (re)dial. Kept for the life of a [`RemoteClient`].
struct Dialer {
    endpoint: Endpoint,
    credential: Credential,
    client_name: String,
}

impl Dialer {
    /// Connect, send `Hello`, await `Welcome`.
    async fn dial(&self) -> Result<(Principal, OwnedReadHalf, OwnedWriteHalf), ApiError> {
        let stream = match &self.endpoint {
            Endpoint::Unix(path) => UnixStream::connect(path)
                .await
                .map_err(|e| ApiError::Transport(e.to_string()))?,
            Endpoint::WebSocket(_) => {
                return Err(ApiError::Transport(
                    "websocket endpoint is not enabled".into(),
                ));
            }
        };
        let (mut reader, mut writer) = stream.into_split();
        write_frame(
            &mut writer,
            &wire::ClientFrame::Hello {
                credential: self.credential.clone(),
                client: self.client_name.clone(),
            },
        )
        .await
        .map_err(|e| ApiError::Transport(e.to_string()))?;
        let welcome: wire::ServerFrame =
            tokio::time::timeout(HELLO_TIMEOUT, read_frame(&mut reader))
                .await
                .map_err(|_| ApiError::Timeout)?
                .map_err(|e| ApiError::Transport(e.to_string()))?
                .ok_or_else(|| ApiError::Transport("server closed before welcome".into()))?;
        match welcome {
            wire::ServerFrame::Welcome { result, .. } => Ok((result?, reader, writer)),
            other => Err(ApiError::Transport(format!(
                "expected welcome, got {other:?}"
            ))),
        }
    }
}

enum ClientReq {
    Call {
        cmd: WireCommand,
        reply: oneshot::Sender<Result<WireReply, ApiError>>,
    },
    Watch {
        interest: Interest,
        from: Cursor,
        reply: oneshot::Sender<Result<FrameStream, ApiError>>,
    },
    /// Sent by a dropped [`FrameStream`].
    Unwatch { id: u64 },
}

/// A client session that survives daemon restarts. Implements `api::Client`.
#[derive(Clone)]
pub struct RemoteClient {
    me: Principal,
    req_tx: mpsc::UnboundedSender<ClientReq>,
    connected: watch::Receiver<bool>,
    /// Why the session ended for good, once it has.
    closed: Arc<OnceLock<ApiError>>,
}

impl std::fmt::Debug for RemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteClient")
            .field("me", &self.me)
            .field("connected", &self.connected())
            .finish()
    }
}

impl RemoteClient {
    /// Connect, send `Hello`, await `Welcome`. Bound to one principal for life:
    /// a redial that authenticates as anyone else ends the session.
    pub async fn connect(
        endpoint: Endpoint,
        credential: Credential,
        client_name: &str,
    ) -> Result<Self, ApiError> {
        let dialer = Arc::new(Dialer {
            endpoint,
            credential,
            client_name: client_name.to_owned(),
        });
        let (me, reader, writer) = dialer.dial().await?;
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let (connected_tx, connected) = watch::channel(true);
        let closed = Arc::new(OnceLock::new());
        let session = Connection {
            dialer,
            me: me.clone(),
            req_rx,
            unwatch_tx: req_tx.downgrade(),
            connected: connected_tx,
            closed: closed.clone(),
            next_id: 1,
            watches: BTreeMap::new(),
            generations: Generations::default(),
        };
        tokio::spawn(session.run(reader, writer));
        Ok(RemoteClient {
            me,
            req_tx,
            connected,
            closed,
        })
    }

    /// Whether the socket is up right now. False while redialing.
    pub fn connected(&self) -> bool {
        *self.connected.borrow()
    }

    /// Follows [`RemoteClient::connected`], for status badges.
    pub fn watch_connection(&self) -> watch::Receiver<bool> {
        self.connected.clone()
    }

    fn request<T: Send>(
        &self,
        req: impl FnOnce(oneshot::Sender<Result<T, ApiError>>) -> ClientReq + Send,
    ) -> impl Future<Output = Result<T, ApiError>> + Send {
        let tx = self.req_tx.clone();
        let closed = self.closed.clone();
        async move {
            let gone = || {
                closed
                    .get()
                    .cloned()
                    .unwrap_or_else(|| ApiError::Transport("connection closed".into()))
            };
            let (reply, rx) = oneshot::channel();
            tx.send(req(reply)).map_err(|_| gone())?;
            rx.await.map_err(|_| gone())?
        }
    }
}

impl Client for RemoteClient {
    fn me(&self) -> &Principal {
        &self.me
    }

    fn call_wire(
        &self,
        cmd: WireCommand,
    ) -> impl Future<Output = Result<WireReply, ApiError>> + Send {
        self.request(|reply| ClientReq::Call { cmd, reply })
    }

    fn watch(
        &self,
        interest: Interest,
        from: Cursor,
    ) -> impl Future<Output = Result<FrameStream, ApiError>> + Send {
        self.request(|reply| ClientReq::Watch {
            interest,
            from,
            reply,
        })
    }
}

struct UnwatchGuard {
    id: u64,
    req_tx: mpsc::UnboundedSender<ClientReq>,
    armed: bool,
}

impl Drop for UnwatchGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.req_tx.send(ClientReq::Unwatch { id: self.id });
        }
    }
}

fn frame_stream(rx: mpsc::Receiver<Result<Frame, ApiError>>, guard: UnwatchGuard) -> FrameStream {
    Box::pin(futures_util::stream::unfold(
        (rx, guard),
        |(mut rx, mut guard)| async move {
            match rx.recv().await {
                Some(item) => Some((item, (rx, guard))),
                None => {
                    // The watch already ended; no Unwatch needed.
                    guard.armed = false;
                    None
                }
            }
        },
    ))
}

/// Maps each connection's stage generations into one increasing sequence.
/// The daemon numbers generations per session, so after a redial they restart
/// at 1, and a caller that drops frames older than its newest generation (the
/// TUI does) would drop every frame from the new session.
#[derive(Default)]
struct Generations {
    offset: u64,
    high: u64,
}

impl Generations {
    /// Called as each connection starts.
    fn rebase(&mut self) {
        self.offset = self.high;
    }

    fn map(&mut self, generation: FocusGeneration) -> FocusGeneration {
        let mapped = generation.0 + self.offset;
        self.high = self.high.max(mapped);
        FocusGeneration(mapped)
    }
}

/// A watch the caller still holds, with what it takes to re-open it.
struct LiveWatch {
    interest: Interest,
    /// Where to resume: the caller has every frame up to here.
    resume: Cursor,
    tx: mpsc::Sender<Result<Frame, ApiError>>,
    caught_up: bool,
    /// The generation the caller saw in `StageFocused`; a re-opened stage
    /// watch keeps it.
    stage: Option<FocusGeneration>,
}

impl LiveWatch {
    /// Record `frame` and return what the caller should see, or `None` for a
    /// frame the caller already has the meaning of (a re-opened watch's second
    /// `CaughtUp` or `StageFocused`).
    fn admit(&mut self, frame: Frame, generations: &mut Generations) -> Option<Frame> {
        match frame {
            Frame::StageFocused { generation, target } => {
                if self.stage.is_some() {
                    return None;
                }
                let generation = generations.map(generation);
                self.stage = Some(generation);
                Some(Frame::StageFocused { generation, target })
            }
            Frame::Stage { generation, inner } => {
                let generation = match self.stage {
                    Some(pinned) => pinned,
                    None => generations.map(generation),
                };
                let inner = self.track(*inner)?;
                Some(Frame::Stage {
                    generation,
                    inner: Box::new(inner),
                })
            }
            other => self.track(other),
        }
    }

    fn track(&mut self, frame: Frame) -> Option<Frame> {
        match &frame {
            Frame::Committed(env) => self.resume.advance(&env.stream, env.seq),
            Frame::CaughtUp { cursor } => {
                // `cursor` lists every stream that existed when this watch
                // started, so clear `from_head` before a redial can mistake a
                // stream created while down for one to skip.
                self.resume = pin_caught_up(cursor);
                if std::mem::replace(&mut self.caught_up, true) {
                    return None;
                }
            }
            _ => {}
        }
        Some(frame)
    }
}

fn pin_caught_up(cursor: &Cursor) -> Cursor {
    if !cursor.from_head() {
        return cursor.clone();
    }
    let mut value = serde_json::to_value(cursor).expect("Cursor serializes");
    if let Some(obj) = value.as_object_mut() {
        obj.insert("from_head".into(), serde_json::Value::Bool(false));
    }
    serde_json::from_value(value).expect("Cursor deserializes")
}

/// A watch sent to the daemon whose `WatchOpened` has not arrived.
struct Opening {
    reply: oneshot::Sender<Result<FrameStream, ApiError>>,
    watch: LiveWatch,
    stream: FrameStream,
}

enum ConnEvent {
    Read(io::Result<Option<wire::ServerFrame>>),
    WriteFailed(io::Error),
}

enum Redial {
    Connected(OwnedReadHalf, OwnedWriteHalf),
    Shutdown,
    Fatal(ApiError),
}

/// The task behind a [`RemoteClient`]: owns the socket, redials it, and keeps
/// watches open across redials.
struct Connection {
    dialer: Arc<Dialer>,
    me: Principal,
    req_rx: mpsc::UnboundedReceiver<ClientReq>,
    /// Weak, so this task ends once every client handle and stream is gone.
    unwatch_tx: mpsc::WeakUnboundedSender<ClientReq>,
    connected: watch::Sender<bool>,
    closed: Arc<OnceLock<ApiError>>,
    next_id: u64,
    watches: BTreeMap<u64, LiveWatch>,
    generations: Generations,
}

impl Connection {
    async fn run(mut self, mut reader: OwnedReadHalf, mut writer: OwnedWriteHalf) {
        let mut delay = REDIAL_MIN;
        loop {
            self.set_connected(true);
            let started = tokio::time::Instant::now();
            let Some(cause) = self.serve(reader, writer).await else {
                return;
            };
            if started.elapsed() >= REDIAL_MAX {
                delay = REDIAL_MIN;
            }
            self.set_connected(false);
            match self.redial(&cause, &mut delay).await {
                Redial::Connected(r, w) => (reader, writer) = (r, w),
                Redial::Shutdown => return,
                Redial::Fatal(e) => {
                    let _ = self.closed.set(e.clone());
                    for (_, w) in std::mem::take(&mut self.watches) {
                        let _ = w.tx.send(Err(e.clone())).await;
                    }
                    return;
                }
            }
        }
    }

    /// Notify `watch_connection` receivers only on a real change.
    fn set_connected(&self, up: bool) {
        self.connected
            .send_if_modified(|was| std::mem::replace(was, up) != up);
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Serve one socket until it drops (returning why) or every handle is
    /// gone (returning `None`).
    async fn serve(&mut self, reader: OwnedReadHalf, mut writer: OwnedWriteHalf) -> Option<String> {
        // `read_frame` is not cancel-safe: dropping it part-way through a frame
        // loses the bytes it has already taken off the socket, and every later
        // frame is then read from the wrong offset. Read on a task of its own
        // and select on the channel, whose `recv` can be dropped safely.
        let (ev_tx, mut ev_rx) = mpsc::channel(WATCH_BUFFER);
        let read_tx = ev_tx.clone();
        let mut reader = reader;
        let reader_task = tokio::spawn(async move {
            loop {
                let incoming = read_frame::<_, wire::ServerFrame>(&mut reader).await;
                let last = !matches!(incoming, Ok(Some(_)));
                if read_tx.send(ConnEvent::Read(incoming)).await.is_err() || last {
                    break;
                }
            }
        });
        let _reader = AbortOnDrop(reader_task.abort_handle());
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let writer_task = tokio::spawn(async move {
            while let Some(bytes) = out_rx.recv().await {
                if let Err(e) = write_bytes(&mut writer, &bytes).await {
                    let _ = ev_tx.send(ConnEvent::WriteFailed(e)).await;
                    break;
                }
            }
        });
        let _writer = AbortOnDrop(writer_task.abort_handle());

        self.generations.rebase();
        // A session holds at most one stage watch. If the socket died before
        // `WatchEnded` arrived for a superseded stage, drop the older one now
        // so two `Watch` frames cannot race to claim the new session's slot.
        if let Some(&latest_stage) = self
            .watches
            .iter()
            .rev()
            .find_map(|(id, w)| matches!(w.interest, Interest::Stage { .. }).then_some(id))
        {
            self.watches.retain(|&id, w| {
                id == latest_stage || !matches!(w.interest, Interest::Stage { .. })
            });
        }
        for (&id, w) in &self.watches {
            let reopen = wire::ClientFrame::Watch {
                id,
                interest: w.interest.clone(),
                from: w.resume.clone(),
            };
            if let Ok(bytes) = encode(&reopen) {
                let _ = out_tx.send(bytes);
            }
        }

        let mut calls: HashMap<u64, oneshot::Sender<Result<WireReply, ApiError>>> = HashMap::new();
        let mut opening: HashMap<u64, Opening> = HashMap::new();
        let cause = loop {
            tokio::select! {
                req = self.req_rx.recv() => match req {
                    Some(req) => self.submit(req, &out_tx, &mut calls, &mut opening),
                    None => return None,
                },
                ev = ev_rx.recv() => match ev {
                    Some(ConnEvent::Read(Ok(Some(frame)))) => {
                        self.receive(frame, &out_tx, &mut calls, &mut opening).await;
                    }
                    Some(ConnEvent::Read(Ok(None))) | None => {
                        break "daemon closed the connection".to_owned();
                    }
                    Some(ConnEvent::Read(Err(e))) => break format!("reading from daemon: {e}"),
                    Some(ConnEvent::WriteFailed(e)) => break format!("writing to daemon: {e}"),
                },
            }
        };
        for (_, reply) in calls {
            let _ = reply.send(Err(ApiError::Transport(cause.clone())));
        }
        for (_, o) in opening {
            let _ = o.reply.send(Err(ApiError::Transport(cause.clone())));
        }
        Some(cause)
    }

    fn submit(
        &mut self,
        req: ClientReq,
        out_tx: &mpsc::UnboundedSender<Vec<u8>>,
        calls: &mut HashMap<u64, oneshot::Sender<Result<WireReply, ApiError>>>,
        opening: &mut HashMap<u64, Opening>,
    ) {
        match req {
            ClientReq::Call { cmd, reply } => {
                let id = self.take_id();
                match encode(&wire::ClientFrame::Call { id, cmd }) {
                    Ok(bytes) => {
                        calls.insert(id, reply);
                        let _ = out_tx.send(bytes);
                    }
                    Err(e) => {
                        let _ = reply.send(Err(ApiError::Transport(format!("encoding call: {e}"))));
                    }
                }
            }
            ClientReq::Watch {
                interest,
                from,
                reply,
            } => {
                let id = self.take_id();
                let frame = wire::ClientFrame::Watch {
                    id,
                    interest: interest.clone(),
                    from: from.clone(),
                };
                let bytes = match encode(&frame) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        let _ =
                            reply.send(Err(ApiError::Transport(format!("encoding watch: {e}"))));
                        return;
                    }
                };
                // The requester holds a sender until it hears back, so this
                // upgrade only fails if it has already given up.
                let Some(req_tx) = self.unwatch_tx.upgrade() else {
                    return;
                };
                let (tx, rx) = mpsc::channel(WATCH_BUFFER);
                let guard = UnwatchGuard {
                    id,
                    req_tx,
                    armed: true,
                };
                opening.insert(
                    id,
                    Opening {
                        reply,
                        watch: LiveWatch {
                            interest,
                            resume: from,
                            tx,
                            caught_up: false,
                            stage: None,
                        },
                        stream: frame_stream(rx, guard),
                    },
                );
                let _ = out_tx.send(bytes);
            }
            ClientReq::Unwatch { id } => {
                if self.watches.remove(&id).is_some()
                    && let Ok(bytes) = encode(&wire::ClientFrame::Unwatch { id })
                {
                    let _ = out_tx.send(bytes);
                }
            }
        }
    }

    async fn receive(
        &mut self,
        frame: wire::ServerFrame,
        out_tx: &mpsc::UnboundedSender<Vec<u8>>,
        calls: &mut HashMap<u64, oneshot::Sender<Result<WireReply, ApiError>>>,
        opening: &mut HashMap<u64, Opening>,
    ) {
        match frame {
            wire::ServerFrame::Reply { id, result } => {
                if let Some(reply) = calls.remove(&id) {
                    let _ = reply.send(result);
                }
            }
            wire::ServerFrame::WatchOpened { id, result } => {
                if let Some(o) = opening.remove(&id) {
                    match result {
                        Ok(()) => {
                            self.watches.insert(id, o.watch);
                            let _ = o.reply.send(Ok(o.stream));
                        }
                        Err(e) => {
                            let _ = o.reply.send(Err(e));
                        }
                    }
                } else if let Err(e) = result
                    && let Some(w) = self.watches.remove(&id)
                {
                    // A re-opened watch the daemon now refuses.
                    let _ = w.tx.send(Err(e)).await;
                }
            }
            wire::ServerFrame::Frame { id, frame } => {
                let Some(w) = self.watches.get_mut(&id) else {
                    return;
                };
                let Some(frame) = w.admit(frame, &mut self.generations) else {
                    return;
                };
                if w.tx.send(Ok(frame)).await.is_err() {
                    self.watches.remove(&id);
                    if let Ok(bytes) = encode(&wire::ClientFrame::Unwatch { id }) {
                        let _ = out_tx.send(bytes);
                    }
                }
            }
            wire::ServerFrame::WatchEnded { id, error } => {
                if let Some(w) = self.watches.remove(&id)
                    && let Some(e) = error
                {
                    if let Ok(bytes) = encode(&wire::ClientFrame::Unwatch { id }) {
                        let _ = out_tx.send(bytes);
                    }
                    let _ = w.tx.send(Err(e)).await;
                }
            }
            wire::ServerFrame::Welcome { .. } => {}
        }
    }

    /// Redial with backoff until connected as the same principal.
    async fn redial(&mut self, cause: &str, delay: &mut Duration) -> Redial {
        let dialer = self.dialer.clone();
        loop {
            let wait = *delay;
            *delay = (*delay * 2).min(REDIAL_MAX);
            if self
                .while_down(tokio::time::sleep(wait), cause)
                .await
                .is_none()
            {
                return Redial::Shutdown;
            }
            match self.while_down(dialer.dial(), cause).await {
                None => return Redial::Shutdown,
                Some(Ok((me, reader, writer))) if me == self.me => {
                    return Redial::Connected(reader, writer);
                }
                Some(Ok((me, ..))) => {
                    return Redial::Fatal(ApiError::Transport(format!(
                        "daemon now authenticates this client as {me}, not {}",
                        self.me
                    )));
                }
                Some(Err(e @ ApiError::Denied { .. })) => return Redial::Fatal(e),
                Some(Err(_)) => {}
            }
        }
    }

    /// Drive `fut` while the socket is down, refusing calls and new watches
    /// with `cause`. `None` once every handle is gone.
    async fn while_down<F: Future>(&mut self, fut: F, cause: &str) -> Option<F::Output> {
        tokio::pin!(fut);
        let refusal = || ApiError::Transport(format!("reconnecting to daemon ({cause})"));
        loop {
            tokio::select! {
                out = &mut fut => return Some(out),
                req = self.req_rx.recv() => match req? {
                    ClientReq::Call { reply, .. } => {
                        let _ = reply.send(Err(refusal()));
                    }
                    ClientReq::Watch { reply, .. } => {
                        let _ = reply.send(Err(refusal()));
                    }
                    ClientReq::Unwatch { id } => {
                        self.watches.remove(&id);
                    }
                },
            }
        }
    }
}

/// Bind `endpoint`, recording the daemon process's own Unix uid from the bound
/// socket inode so callers do not need `unsafe` `geteuid()`.
pub fn bind_unix(path: &std::path::Path) -> io::Result<(UnixListener, u32)> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    let uid = std::fs::symlink_metadata(path)?.uid();
    Ok((listener, uid))
}

/// Serve `router` on an already-bound `UnixListener` until cancelled.
/// Cancelling closes every connection, as a daemon exit would.
pub async fn serve_listener(
    listener: UnixListener,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
) -> io::Result<()> {
    if let Ok(addr) = listener.local_addr()
        && let Some(path) = addr.as_pathname()
    {
        daemon_log!("listening on {}", path.display());
    }
    let mut conns = JoinSet::new();
    let mut next_conn = 0u64;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.inspect_err(|e| daemon_log!("accept failed: {e}"))?;
                next_conn += 1;
                let conn = next_conn;
                let peer_uid = stream.peer_cred().ok().map(|c| c.uid());
                let router = router.clone();
                let auth = auth.clone();
                conns.spawn(async move {
                    if let Err(e) = handle_conn(conn, stream, peer_uid, router, auth).await {
                        daemon_log!("conn #{conn}: {e}");
                    }
                });
            }
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
}

/// Serve `router` on `endpoint` until cancelled. One task per connection, one
/// task per watch; replies complete in whatever order they finish.
pub async fn serve(
    endpoint: Endpoint,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
) -> io::Result<()> {
    match endpoint {
        Endpoint::Unix(path) => {
            let (listener, _) = bind_unix(&path)?;
            serve_listener(listener, router, auth).await
        }
        Endpoint::WebSocket(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "websocket endpoint is not enabled",
        )),
    }
}

/// Errors that point at the daemon or what it fronts, as opposed to a caller
/// asking for something it may not have.
fn worth_logging(e: &ApiError) -> bool {
    matches!(e, ApiError::Internal(_) | ApiError::Transport(_))
}

async fn handle_conn(
    conn: u64,
    stream: UnixStream,
    peer_uid: Option<u32>,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
) -> io::Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let first: Option<wire::ClientFrame> =
        tokio::time::timeout(HELLO_TIMEOUT, read_frame(&mut reader))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "hello timed out"))??;
    let Some(wire::ClientFrame::Hello { credential, client }) = first else {
        daemon_log!("conn #{conn}: closed without hello");
        return Ok(());
    };
    let welcome = auth.authenticate(peer_uid, &credential);
    write_server_frame(
        &mut writer,
        conn,
        &wire::ServerFrame::Welcome {
            result: welcome.clone(),
            daemon: env!("CARGO_PKG_VERSION").into(),
        },
    )
    .await?;
    let me = match welcome {
        Ok(me) => me,
        Err(e) => {
            daemon_log!("conn #{conn}: refused {client}: {e}");
            return Ok(());
        }
    };
    daemon_log!("conn #{conn}: {client} connected as {me}");

    let session = Session::new(me.clone());
    let (out_tx, mut out_rx) = mpsc::channel::<wire::ServerFrame>(WATCH_BUFFER * 2);
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            write_server_frame(&mut writer, conn, &frame).await?;
        }
        Ok::<(), io::Error>(())
    });
    let _writer = AbortOnDrop(writer_task.abort_handle());

    let mut watches: HashMap<u64, AbortOnDrop> = HashMap::new();
    let ended = loop {
        let frame = match read_frame::<_, wire::ClientFrame>(&mut reader).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break "client hung up".to_owned(),
            Err(e) => break format!("read failed: {e}"),
        };
        match frame {
            wire::ClientFrame::Hello { .. } => break "second hello".to_owned(),
            wire::ClientFrame::Call { id, cmd } => {
                let router = router.clone();
                let by = session.me().clone();
                let out_tx = out_tx.clone();
                let name = cmd.meta().name;
                tokio::spawn(async move {
                    let result = router.dispatch(&by, cmd).await;
                    if let Err(e) = &result
                        && worth_logging(e)
                    {
                        daemon_log!("conn #{conn}: {name} failed: {e}");
                    }
                    let _ = out_tx.send(wire::ServerFrame::Reply { id, result }).await;
                });
            }
            wire::ClientFrame::Watch { id, interest, from } => {
                let router = router.clone();
                let session = session.clone();
                let out_tx = out_tx.clone();
                let handle = tokio::spawn(async move {
                    let mut stream = match router.watch(&session, interest, from).await {
                        Ok(s) => {
                            if out_tx
                                .send(wire::ServerFrame::WatchOpened { id, result: Ok(()) })
                                .await
                                .is_err()
                            {
                                return;
                            }
                            s
                        }
                        Err(e) => {
                            if worth_logging(&e) {
                                daemon_log!("conn #{conn}: watch {id} refused: {e}");
                            }
                            let _ = out_tx
                                .send(wire::ServerFrame::WatchOpened { id, result: Err(e) })
                                .await;
                            return;
                        }
                    };
                    let mut end_err = None;
                    while let Some(item) = stream.next().await {
                        match item {
                            Ok(frame) => {
                                if out_tx
                                    .send(wire::ServerFrame::Frame { id, frame })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(e) => {
                                if worth_logging(&e) {
                                    daemon_log!("conn #{conn}: watch {id} ended: {e}");
                                }
                                end_err = Some(e);
                                break;
                            }
                        }
                    }
                    let _ = out_tx
                        .send(wire::ServerFrame::WatchEnded { id, error: end_err })
                        .await;
                });
                // Replacing an entry aborts the watch it held.
                watches.insert(id, AbortOnDrop(handle.abort_handle()));
            }
            wire::ClientFrame::Unwatch { id } => {
                watches.remove(&id);
            }
        }
    };

    drop(watches);
    drop(out_tx);
    match writer_task.await {
        Ok(Err(e)) => daemon_log!("conn #{conn}: {me} disconnected ({ended}; write failed: {e})"),
        _ => daemon_log!("conn #{conn}: {me} disconnected ({ended})"),
    }
    Ok(())
}
