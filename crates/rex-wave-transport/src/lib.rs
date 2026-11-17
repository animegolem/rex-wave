//! # rex-wave-transport
//!
//! The only crate that knows bytes. Everything above it speaks `api::Client`.
//!
//! Wire: one multiplexed connection per client, frames are
//! `u32-BE length || JSON`. Calls and watches carry a client-chosen `id`, so
//! many watches (sidebar tree, stage) share one socket and a stage retarget or
//! drop is `Unwatch{old}` + `Watch{new}` with no reconnect.
//!
//! Auth happens once, in `Hello`, and yields the session [`Principal`]:
//! * Unix socket + peer uid == daemon uid -> `Operator{seat}` (TUI, CLI);
//! * `Token` minted by `rex-wave token --seat sam` -> `Seat{sam}` (MCP);
//! * the BB bridge runs in-process and never uses this crate.

use futures_util::StreamExt;
use rex_wave_api::*;
use rex_wave_runtime::{Router, Session};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

const MAX_FRAME: usize = 16 * 1024 * 1024;
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const WATCH_BUFFER: usize = 128;

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
}

async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, msg: &T) -> io::Result<()> {
    let bytes =
        serde_json::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if bytes.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds 16 MiB",
        ));
    }
    w.write_u32(bytes.len() as u32).await?;
    w.write_all(&bytes).await?;
    w.flush().await
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
}

/// A connected client session. Implements `api::Client`.
#[derive(Clone)]
pub struct RemoteClient {
    me: Principal,
    req_tx: mpsc::UnboundedSender<ClientReq>,
}

impl std::fmt::Debug for RemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteClient")
            .field("me", &self.me)
            .finish()
    }
}

impl RemoteClient {
    /// Connect, send `Hello`, await `Welcome`. Bound to one principal for life.
    pub async fn connect(
        endpoint: Endpoint,
        credential: Credential,
        client_name: &str,
    ) -> Result<Self, ApiError> {
        let stream = match endpoint {
            Endpoint::Unix(path) => UnixStream::connect(&path)
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
                credential,
                client: client_name.to_owned(),
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
        let me = match welcome {
            wire::ServerFrame::Welcome { result, .. } => result?,
            other => {
                return Err(ApiError::Transport(format!(
                    "expected welcome, got {other:?}"
                )));
            }
        };

        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<wire::ClientFrame>();
        tokio::spawn(async move {
            while let Some(frame) = out_rx.recv().await {
                if write_frame(&mut writer, &frame).await.is_err() {
                    break;
                }
            }
        });

        let (req_tx, req_rx) = mpsc::unbounded_channel();
        tokio::spawn(run_client(reader, out_tx, req_rx));
        Ok(RemoteClient { me, req_tx })
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
        let tx = self.req_tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(ClientReq::Call { cmd, reply })
                .map_err(|_| ApiError::Transport("connection closed".into()))?;
            rx.await
                .map_err(|_| ApiError::Transport("connection closed".into()))?
        }
    }

    fn watch(
        &self,
        interest: Interest,
        from: Cursor,
    ) -> impl Future<Output = Result<FrameStream, ApiError>> + Send {
        let tx = self.req_tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            tx.send(ClientReq::Watch {
                interest,
                from,
                reply,
            })
            .map_err(|_| ApiError::Transport("connection closed".into()))?;
            rx.await
                .map_err(|_| ApiError::Transport("connection closed".into()))?
        }
    }
}

struct UnwatchGuard {
    id: u64,
    out_tx: mpsc::UnboundedSender<wire::ClientFrame>,
    armed: bool,
}

impl Drop for UnwatchGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.out_tx.send(wire::ClientFrame::Unwatch { id: self.id });
        }
    }
}

async fn run_client<R: AsyncRead + Unpin + Send + 'static>(
    mut reader: R,
    out_tx: mpsc::UnboundedSender<wire::ClientFrame>,
    mut req_rx: mpsc::UnboundedReceiver<ClientReq>,
) {
    let mut next_id = 1u64;
    let mut calls: HashMap<u64, oneshot::Sender<Result<WireReply, ApiError>>> = HashMap::new();
    let mut opening: HashMap<
        u64,
        (
            oneshot::Sender<Result<FrameStream, ApiError>>,
            mpsc::Sender<Result<Frame, ApiError>>,
            FrameStream,
        ),
    > = HashMap::new();
    let mut watches: HashMap<u64, mpsc::Sender<Result<Frame, ApiError>>> = HashMap::new();

    let reason = loop {
        tokio::select! {
            req = req_rx.recv() => match req {
                Some(ClientReq::Call { cmd, reply }) => {
                    let id = next_id;
                    next_id += 1;
                    calls.insert(id, reply);
                    if out_tx.send(wire::ClientFrame::Call { id, cmd }).is_err() {
                        break "connection writer closed".to_owned();
                    }
                }
                Some(ClientReq::Watch { interest, from, reply }) => {
                    let id = next_id;
                    next_id += 1;
                    let (tx, rx) = mpsc::channel(WATCH_BUFFER);
                    let guard = UnwatchGuard { id, out_tx: out_tx.clone(), armed: true };
                    let stream: FrameStream = Box::pin(futures_util::stream::unfold((rx, guard), |(mut rx, mut guard)| async move {
                        match rx.recv().await {
                            Some(item) => Some((item, (rx, guard))),
                            None => {
                                // Server already ended the watch; no Unwatch needed.
                                guard.armed = false;
                                None
                            }
                        }
                    }));
                    opening.insert(id, (reply, tx, stream));
                    if out_tx.send(wire::ClientFrame::Watch { id, interest, from }).is_err() {
                        break "connection writer closed".to_owned();
                    }
                }
                None => return,
            },
            incoming = read_frame::<_, wire::ServerFrame>(&mut reader) => match incoming {
                Ok(Some(wire::ServerFrame::Reply { id, result })) => {
                    if let Some(reply) = calls.remove(&id) {
                        let _ = reply.send(result);
                    }
                }
                Ok(Some(wire::ServerFrame::WatchOpened { id, result })) => {
                    if let Some((reply, tx, stream)) = opening.remove(&id) {
                        match result {
                            Ok(()) => {
                                watches.insert(id, tx);
                                let _ = reply.send(Ok(stream));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                }
                Ok(Some(wire::ServerFrame::Frame { id, frame })) => {
                    let closed = match watches.get(&id) {
                        Some(tx) => tx.send(Ok(frame)).await.is_err(),
                        None => false,
                    };
                    if closed {
                        watches.remove(&id);
                    }
                }
                Ok(Some(wire::ServerFrame::WatchEnded { id, error })) => {
                    if let Some(tx) = watches.remove(&id)
                        && let Some(e) = error
                    {
                        let _ = tx.send(Err(e)).await;
                    }
                }
                Ok(Some(wire::ServerFrame::Welcome { .. })) => {}
                Ok(None) => break "server closed connection".to_owned(),
                Err(e) => break e.to_string(),
            }
        }
    };

    let err = || ApiError::Transport(reason.clone());
    for (_, reply) in calls {
        let _ = reply.send(Err(err()));
    }
    for (_, (reply, _, _)) in opening {
        let _ = reply.send(Err(err()));
    }
    for (_, tx) in watches {
        let _ = tx.send(Err(err())).await;
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
pub async fn serve_listener(
    listener: UnixListener,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let peer_uid = stream.peer_cred().ok().map(|c| c.uid());
        let router = router.clone();
        let auth = auth.clone();
        tokio::spawn(async move {
            let _ = handle_conn(stream, peer_uid, router, auth).await;
        });
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

async fn handle_conn(
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
    let Some(wire::ClientFrame::Hello { credential, .. }) = first else {
        return Ok(());
    };
    let me = match auth.authenticate(peer_uid, &credential) {
        Ok(me) => {
            write_frame(
                &mut writer,
                &wire::ServerFrame::Welcome {
                    result: Ok(me.clone()),
                    daemon: env!("CARGO_PKG_VERSION").into(),
                },
            )
            .await?;
            me
        }
        Err(e) => {
            let _ = write_frame(
                &mut writer,
                &wire::ServerFrame::Welcome {
                    result: Err(e),
                    daemon: env!("CARGO_PKG_VERSION").into(),
                },
            )
            .await;
            return Ok(());
        }
    };

    let session = Session::new(me);
    let (out_tx, mut out_rx) = mpsc::channel::<wire::ServerFrame>(WATCH_BUFFER * 2);
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            if write_frame(&mut writer, &frame).await.is_err() {
                break;
            }
        }
    });

    let mut watches: HashMap<u64, AbortHandle> = HashMap::new();
    while let Ok(Some(frame)) = read_frame::<_, wire::ClientFrame>(&mut reader).await {
        match frame {
            wire::ClientFrame::Hello { .. } => break,
            wire::ClientFrame::Call { id, cmd } => {
                let router = router.clone();
                let by = session.me().clone();
                let out_tx = out_tx.clone();
                tokio::spawn(async move {
                    let result = router.dispatch(&by, cmd).await;
                    let _ = out_tx.send(wire::ServerFrame::Reply { id, result }).await;
                });
            }
            wire::ClientFrame::Watch { id, interest, from } => {
                if let Some(prev) = watches.remove(&id) {
                    prev.abort();
                }
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
                                end_err = Some(e);
                                break;
                            }
                        }
                    }
                    let _ = out_tx
                        .send(wire::ServerFrame::WatchEnded { id, error: end_err })
                        .await;
                });
                watches.insert(id, handle.abort_handle());
            }
            wire::ClientFrame::Unwatch { id } => {
                if let Some(handle) = watches.remove(&id) {
                    handle.abort();
                }
            }
        }
    }

    for (_, handle) in watches {
        handle.abort();
    }
    drop(out_tx);
    let _ = writer_task.await;
    Ok(())
}
