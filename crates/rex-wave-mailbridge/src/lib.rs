//! # rex-wave-mailbridge, the sandboxed `MailBridgeAdapter`
//!
//! Rooms are weather; letters are canon. This crate is the only door
//! between them, and in this phase it opens **only onto a sandbox**.
//!
//! * [`SandboxRoot`] is the non-prod guard, encoded as a type: the only
//!   constructor refuses any path that is, contains, or lives under a
//!   production mail root (`~/agent-folder/interchange`, `~/.local/state/carrier`,
//!   the live `roster.json`), and requires a `.rex-wave-sandbox` marker file.
//!   Every filesystem operation in this crate takes a `&SandboxRoot`.
//! * `promote_to_letter` honors house-mail laws: **bank before announce** (I2:
//!   the file is durably written *before* the `Banked` journal entry that the
//!   Herald announces), **never delete** (BoxMonotone: write-if-absent; identical
//!   bytes are an idempotent no-op), and **ACK only exact bytes** (sha256).
//! * Projections speak `mailbox.item.schema.json` v1 verbatim (`MailboxItem`,
//!   `WakeRecord`, derived `wake_id`).
//! * The wake-bell sink is an HTTP ingress shaped like wake-bell's `Sink`; a
//!   cron fire becomes a DM from `System{WakeBell}`, never a Herdr prompt.
//!
//! Unlike the pure deciders, the mail actor performs I/O *inside* the actor
//! (it is the single writer of the sandbox directory), because I2 requires the
//! effect to precede the journal entry.

use rex_wave_api::mail::*;
use rex_wave_api::*;
use rex_wave_runtime::{BoxFut, DecideCtx, LocalClient, Reactor, Service};
use rex_wave_store::{Journal, StreamWriter};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    #[error("{0} is (or is under) a production mail root")]
    Production(PathBuf),
    #[error("{0} lacks the .rex-wave-sandbox marker")]
    Unmarked(PathBuf),
    #[error("io: {0}")]
    Io(String),
}

/// A verified non-production mail root. Unforgeable outside this module.
#[derive(Clone, Debug)]
pub struct SandboxRoot {
    root: PathBuf,
}

impl SandboxRoot {
    /// The only constructor. Canonicalizes, checks the prod denylist both
    /// ways (root under prod, prod under root), requires the marker file.
    pub fn verify(path: &Path) -> Result<Self, SandboxError> {
        let _ = path;
        todo!("SandboxRoot::verify")
    }
    pub fn path(&self) -> &Path {
        &self.root
    }
}

/// Pure: `wake_id` per schema 1, first 16 hex of
/// `sha256("wake/1\n" + seat + "\n" + box + "/" + file + "\n" + full_sha256)`.
pub fn wake_id(seat: &SeatName, mailbox: &str, file: &str, full_sha256: &Sha256Hex) -> WakeId {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(format!("wake/1\n{seat}\n{mailbox}/{file}\n{full_sha256}").as_bytes());
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    WakeId(hex[..16].to_owned())
}

/// Pure: render a room range as a letter (YAML frontmatter `from,to,subject,topic,round,priority`
/// + quoted transcript with `room/seq` provenance). Same input -> same bytes.
pub fn render_letter(req: &MailPromote, by: &Principal, entries: &[Envelope]) -> (String, Vec<u8>) {
    let _ = (req, by, entries);
    todo!("render_letter -> (file name '<topic>-r<round>.md', bytes)")
}

/// Pure: project a banked letter into a v1 `MailboxItem`.
pub fn project(root: &SandboxRoot, path: &Path, bytes: &[u8]) -> Result<MailboxItem, SandboxError> {
    let _ = (root, path, bytes);
    todo!("project")
}

/// The mail actor: single writer of both the sandbox directory and the `Mail` stream.
pub struct MailBridge {
    root: SandboxRoot,
    /// Reads room ranges for promotion straight from the journal (read-only;
    /// room *events* are part of the shared api contract, room *code* is not imported).
    reader: Journal,
    writer: Mutex<StreamWriter>,
}

impl MailBridge {
    pub fn new(root: SandboxRoot, reader: Journal, writer: StreamWriter) -> Self {
        MailBridge { root, reader, writer: Mutex::new(writer) }
    }

    /// promote: read range -> render -> write-if-absent + fsync -> append Banked -> reply.
    async fn promote(&self, req: MailPromote, ctx: DecideCtx) -> Result<LetterRef, ApiError> {
        let _ = (req, ctx, &self.root, &self.reader, &self.writer);
        todo!("MailBridge::promote")
    }

    /// Scan the sandbox `to-<seat>/` boxes and emit `Projected` for unseen files
    /// (read-through projection; bodies stay at source).
    pub async fn rescan(&self) -> Result<u32, ApiError> {
        todo!("MailBridge::rescan")
    }
}

impl Service<MailCommand> for MailBridge {
    fn dispatch(&self, cmd: MailCommand, ctx: DecideCtx) -> BoxFut<'_, Result<WireReply, ApiError>> {
        Box::pin(async move {
            match cmd {
                MailCommand::MailPromote(req) => Ok(WireReply::MailPromote(self.promote(req, ctx).await?)),
                MailCommand::MailAck(a) => {
                    let _ = a;
                    todo!("verify on-disk sha256 == a.sha256, append ack row in sandbox acks.jsonl, journal Acked")
                }
                MailCommand::MailWakeBell(b) => {
                    let _ = b;
                    todo!("journal BellRang (idempotent on bell_id); BellCourier delivers the DM")
                }
            }
        })
    }
}

/// HTTP ingress for wake-bell (`POST /sink` with `MailWakeBell` JSON). Binds
/// loopback only; forwards as `System{WakeBell}` through the given client.
pub async fn serve_wake_bell_sink<C: Client + 'static>(addr: SocketAddr, client: C) -> std::io::Result<()> {
    let _ = (addr, client);
    todo!("serve_wake_bell_sink: tokio TcpListener, minimal HTTP/1.1 POST parse, client.call(MailWakeBell)")
}

/// Reactor: turns `MailEvent::BellRang` into a DM `room_post` from
/// `System{WakeBell}` to the seat (idem = `bell:<bell_id>`, so a replay never
/// double-delivers, wake-bell's own at-least-once retries collapse here).
pub struct BellCourier;

impl Reactor for BellCourier {
    const NAME: &'static str = "bell-courier";
    fn interest(&self) -> Interest {
        Interest::Streams { streams: vec![StreamId::Mail] }
    }
    fn react(&mut self, frame: Frame, client: &LocalClient) -> impl Future<Output = Result<(), ApiError>> + Send {
        let _ = (frame, client);
        async move { todo!("BellCourier::react") }
    }
}
