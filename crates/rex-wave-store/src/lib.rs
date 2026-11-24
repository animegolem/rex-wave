//! # rex-wave-store, the journal
//!
//! One append-only table keyed by `(stream, seq)`:
//!
//! ```sql
//! CREATE TABLE entries(
//!   stream TEXT NOT NULL,     -- canonical JSON of StreamId
//!   seq    INTEGER NOT NULL,  -- 1.. per stream, gapless
//!   at     INTEGER NOT NULL,  -- UnixMs
//!   by     TEXT NOT NULL,     -- JSON of Principal
//!   idem   TEXT,              -- idempotency key, on the first row of a batch
//!   batch  INTEGER NOT NULL,  -- rows in this row's batch (first row only)
//!   event  TEXT NOT NULL,     -- JSON of Event
//!   PRIMARY KEY(stream, seq),
//!   UNIQUE(stream, idem)
//! ) STRICT;
//! ```
//!
//! Invariants, encoded rather than documented:
//! * **Single writer per stream.** [`Journal::claim`] hands out at most one
//!   [`StreamWriter`] per stream per process. It is not `Clone`, and dropping
//!   it releases the claim.
//! * **Gapless seq.** Assigned inside the append transaction from the writer's
//!   cached head; a mismatch with the table is [`StoreError::HeadMoved`]
//!   (another process wrote the file).
//! * **Idempotent append.** A batch whose `idem` already exists on the stream
//!   returns the original envelopes instead of appending.
//! * **Event fits stream.** `Event::fits` is checked on append.
//!
//! The SQLite schema is private; nothing outside this crate sees rusqlite.

use rex_wave_api::{
    Envelope, Event, IdemKey, Principal, Seq, StreamId, UnixMs,
    room::{PrincipalPair, RoomId},
};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

pub const JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("stream {0:?} already has a live writer")]
    AlreadyClaimed(StreamId),
    #[error("event does not belong on stream {0:?}")]
    WrongStream(StreamId),
    #[error("head moved underneath writer (expected {expected:?}, found {found:?})")]
    HeadMoved { expected: Seq, found: Seq },
    #[error("unsupported journal schema version {found} (max supported: {max_supported})")]
    UnsupportedSchemaVersion { found: u32, max_supported: u32 },
    #[error("storage: {0}")]
    Backend(String),
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Backend(e.to_string())
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Backend(format!("codec: {e}"))
    }
}

/// Result of an append: freshly committed, or the envelopes an earlier
/// append with the same idempotency key committed.
#[derive(Debug)]
pub enum Appended {
    Fresh(Vec<Envelope>),
    Replayed(Vec<Envelope>),
}

impl Appended {
    pub fn envelopes(&self) -> &[Envelope] {
        match self {
            Appended::Fresh(v) | Appended::Replayed(v) => v,
        }
    }
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS entries(
  stream TEXT NOT NULL,
  seq    INTEGER NOT NULL,
  at     INTEGER NOT NULL,
  by     TEXT NOT NULL,
  idem   TEXT,
  batch  INTEGER NOT NULL,
  event  TEXT NOT NULL,
  PRIMARY KEY(stream, seq),
  UNIQUE(stream, idem)
) STRICT;
";

/// Sealed initialization handle passed to [`CommitHook::on_init`]. Keeps
/// `rusqlite` private to `rex-wave-store`.
pub struct InitContext<'a> {
    conn: &'a Connection,
}

impl InitContext<'_> {
    pub fn execute_ddl(&mut self, sql: &str) -> Result<(), StoreError> {
        self.conn.execute_batch(sql)?;
        Ok(())
    }
}

/// Sealed transaction handle passed to [`CommitHook::on_append`] inside the
/// same `IMMEDIATE` SQLite transaction that appends `entries`.
pub struct TxContext<'a, 'conn> {
    tx: &'a rusqlite::Transaction<'conn>,
}

impl TxContext<'_, '_> {
    pub fn execute_sql(&self, sql: &str, text_params: &[&str]) -> Result<usize, StoreError> {
        let count = self
            .tx
            .execute(sql, rusqlite::params_from_iter(text_params.iter()))?;
        Ok(count)
    }
}

/// Atomic side-table / outbox hook invoked inside [`StreamWriter::append`]'s
/// `IMMEDIATE` transaction before `COMMIT`, plus an optional post-commit wakeup.
pub trait CommitHook: Send + Sync + 'static {
    fn on_init(&self, _ctx: &mut InitContext<'_>) -> Result<(), StoreError> {
        Ok(())
    }

    fn on_append(
        &self,
        _tx: &TxContext<'_, '_>,
        _committed: &[Envelope],
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn after_commit(&self, _committed: &[Envelope]) {}
}

struct Inner {
    conn: Mutex<Connection>,
    claimed: Mutex<HashSet<String>>,
    hook: Option<Arc<dyn CommitHook>>,
}

impl Inner {
    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
    fn claimed(&self) -> MutexGuard<'_, HashSet<String>> {
        self.claimed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The SQLite (WAL) journal. Cheap to clone; clones share one connection.
#[derive(Clone)]
pub struct Journal {
    inner: Arc<Inner>,
}

fn key(stream: &StreamId) -> String {
    serde_json::to_string(stream).expect("stream ids serialize")
}

fn read_rows(
    conn: &Connection,
    stream: &StreamId,
    after: Seq,
    upto: Option<Seq>,
    limit: usize,
) -> Result<Vec<Envelope>, StoreError> {
    let mut stmt = conn.prepare_cached(
        "SELECT seq, at, by, event FROM entries WHERE stream = ?1 AND seq > ?2 AND seq <= ?3 ORDER BY seq LIMIT ?4",
    )?;
    let upto = upto.map_or(i64::MAX, |s| s.0 as i64);
    let rows = stmt.query_map(
        params![key(stream), after.0 as i64, upto, limit as i64],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        },
    )?;
    let mut out = Vec::new();
    for row in rows {
        let (seq, at, by, event) = row?;
        out.push(Envelope {
            stream: stream.clone(),
            seq: Seq(seq as u64),
            at: UnixMs(at as u64),
            by: serde_json::from_str(&by)?,
            event: serde_json::from_str(&event)?,
        });
    }
    Ok(out)
}

fn head_of(conn: &Connection, stream: &StreamId) -> Result<Seq, StoreError> {
    let max: Option<i64> = conn.query_row(
        "SELECT max(seq) FROM entries WHERE stream = ?1",
        params![key(stream)],
        |r| r.get(0),
    )?;
    Ok(Seq(max.unwrap_or(0) as u64))
}

fn find_idem(
    conn: &Connection,
    stream: &StreamId,
    idem: &IdemKey,
) -> Result<Option<Vec<Envelope>>, StoreError> {
    let first: Option<(i64, i64)> = conn
        .query_row(
            "SELECT seq, batch FROM entries WHERE stream = ?1 AND idem = ?2",
            params![key(stream), idem.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((seq, batch)) = first else {
        return Ok(None);
    };
    let first = Seq(seq as u64);
    let last = Seq((seq + batch - 1) as u64);
    Ok(Some(read_rows(
        conn,
        stream,
        Seq(first.0 - 1),
        Some(last),
        batch as usize,
    )?))
}

/// Outcome of resolving a DM room for an idempotent command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DmIdemResolution {
    Route(RoomId),
    CrossAuthor { room: RoomId },
    Ambiguous(Vec<RoomId>),
}

fn legacy_dm_candidates(a: &Principal, b: &Principal) -> Vec<RoomId> {
    let mut pairs = Vec::new();
    if let Principal::Seat { seat } = a {
        pairs.push((Principal::Operator { seat: seat.clone() }, b.clone()));
    }
    if let Principal::Seat { seat } = b {
        pairs.push((a.clone(), Principal::Operator { seat: seat.clone() }));
    }
    if let (Principal::Seat { seat: sa }, Principal::Seat { seat: sb }) = (a, b) {
        pairs.push((
            Principal::Operator { seat: sa.clone() },
            Principal::Operator { seat: sb.clone() },
        ));
    }
    pairs
        .into_iter()
        .filter_map(|(la, lb)| PrincipalPair::new(la, lb).ok())
        .map(|pair| RoomId::Direct { pair })
        .collect()
}

impl Journal {
    /// Open (creating if needed) the journal at `path` in WAL mode.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        Self::open_with_hook(path, None)
    }

    /// Open the journal at `path` in WAL mode with an optional [`CommitHook`].
    pub fn open_with_hook(
        path: &Path,
        hook: Option<Arc<dyn CommitHook>>,
    ) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(StoreError::Backend(format!(
                "journal_mode is {mode}, want wal"
            )));
        }
        conn.execute_batch("PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;")?;
        Self::init(conn, hook)
    }

    /// In-memory journal for tests and embedded demos.
    pub fn in_memory() -> Result<Self, StoreError> {
        Self::in_memory_with_hook(None)
    }

    /// In-memory journal with an optional [`CommitHook`].
    pub fn in_memory_with_hook(hook: Option<Arc<dyn CommitHook>>) -> Result<Self, StoreError> {
        Self::init(Connection::open_in_memory()?, hook)
    }

    fn init(mut conn: Connection, hook: Option<Arc<dyn CommitHook>>) -> Result<Self, StoreError> {
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let raw_ver: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        let found = u32::try_from(raw_ver)
            .map_err(|_| StoreError::Backend(format!("negative SQLite user_version: {raw_ver}")))?;
        if found > JOURNAL_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchemaVersion {
                found,
                max_supported: JOURNAL_SCHEMA_VERSION,
            });
        }
        tx.execute_batch(SCHEMA)?;
        if found < JOURNAL_SCHEMA_VERSION {
            tx.pragma_update(None, "user_version", JOURNAL_SCHEMA_VERSION)?;
        }
        if let Some(ref h) = hook {
            let mut init_ctx = InitContext { conn: &tx };
            h.on_init(&mut init_ctx)?;
        }
        tx.commit()?;
        Ok(Journal {
            inner: Arc::new(Inner {
                conn: Mutex::new(conn),
                claimed: Mutex::new(HashSet::new()),
                hook,
            }),
        })
    }

    /// Current SQLite `PRAGMA user_version` recorded in this journal.
    pub fn schema_version(&self) -> Result<u32, StoreError> {
        let raw: i64 = self
            .inner
            .conn()
            .query_row("PRAGMA user_version", [], |r| r.get(0))?;
        u32::try_from(raw)
            .map_err(|_| StoreError::Backend(format!("negative SQLite user_version: {raw}")))
    }

    /// The unique writer for `stream`. Fails while another one is alive.
    pub fn claim(&self, stream: StreamId) -> Result<StreamWriter, StoreError> {
        let k = key(&stream);
        if !self.inner.claimed().insert(k.clone()) {
            return Err(StoreError::AlreadyClaimed(stream));
        }
        let mut writer = StreamWriter {
            stream,
            key: k,
            head: Seq::ORIGIN,
            inner: self.inner.clone(),
        };
        writer.head = head_of(&self.inner.conn(), &writer.stream)?;
        Ok(writer)
    }

    /// Entries with `seq > after`, ascending, at most `limit`.
    pub fn read(
        &self,
        stream: &StreamId,
        after: Seq,
        limit: usize,
    ) -> Result<Vec<Envelope>, StoreError> {
        read_rows(&self.inner.conn(), stream, after, None, limit)
    }

    /// Current head of a stream (`Seq::ORIGIN` if empty).
    pub fn head(&self, stream: &StreamId) -> Result<Seq, StoreError> {
        head_of(&self.inner.conn(), stream)
    }

    /// Current heads of `streams` read from a single consistent SQLite snapshot
    /// (`Seq::ORIGIN` for any empty stream), without scanning unrelated streams.
    pub fn heads(&self, streams: &[StreamId]) -> Result<Vec<(StreamId, Seq)>, StoreError> {
        if streams.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.inner.conn();
        let tx = conn.transaction()?;
        let mut out = Vec::with_capacity(streams.len());
        {
            let mut stmt = tx.prepare_cached("SELECT max(seq) FROM entries WHERE stream = ?1")?;
            for s in streams {
                let max: Option<i64> = stmt.query_row(params![key(s)], |r| r.get(0))?;
                out.push((s.clone(), Seq(max.unwrap_or(0) as u64)));
            }
        }
        Ok(out)
    }

    /// Non-destructive compatibility lookup for pre-PR-26 `(Operator, peer)`
    /// DM streams:
    /// - Never modifies SQLite rows (preserving append-only immutability,
    ///   `Herald` dedupe keys, `Inbox` cursors, `idem` keys, and `Banked` ranges).
    /// - If `room` is not a canonical `(Seat, peer)` DM, or `room` already has
    ///   committed entries (`head > 0`), returns `room` unchanged.
    /// - Otherwise (`room` is a canonical `(Seat, peer)` DM with `head == 0`),
    ///   checks whether a pre-upgrade `(Operator, peer)` stream already exists
    ///   (`head > 0`) and returns it so existing operator DM history continues
    ///   without splitting.
    pub fn resolve_dm_room(&self, room: RoomId) -> Result<RoomId, StoreError> {
        let RoomId::Direct { ref pair } = room else {
            return Ok(room);
        };
        let (a, b): (Principal, Principal) = pair.clone().into();
        if a.is_operator() || b.is_operator() {
            return Ok(room);
        }
        let conn = self.inner.conn();
        if head_of(&conn, &StreamId::Room { room: room.clone() })? > Seq::ORIGIN {
            return Ok(room);
        }
        for legacy_room in legacy_dm_candidates(&a, &b) {
            if head_of(
                &conn,
                &StreamId::Room {
                    room: legacy_room.clone(),
                },
            )? > Seq::ORIGIN
            {
                return Ok(legacy_room);
            }
        }
        Ok(room)
    }

    /// Resolve a DM room for a command carrying an optional idempotency key:
    /// - When `allow_legacy_fallback` is false (explicit `RoomRef::Id`), keeps
    ///   empty-canonical -> single-legacy routing so an explicit canonical ID
    ///   never forks a legacy-only DM, while skipping cross-history `idem`
    ///   lookup so an explicit `RoomRef::Id` still disambiguates split journals.
    /// - When `allow_legacy_fallback` is true (`RoomRef::Dm`), checks both
    ///   canonical and pre-PR-26 `(Operator, peer)` histories for `idem`,
    ///   routing to the unique stream where `by` (`same_actor`) committed `idem`,
    ///   returning [`DmIdemResolution::Ambiguous`] when `idem` exists for `by`
    ///   on multiple histories, and [`DmIdemResolution::CrossAuthor`] when
    ///   `idem` was committed by another participant.
    pub fn resolve_dm_room_with_idem(
        &self,
        room: RoomId,
        by: &Principal,
        idem: Option<&IdemKey>,
        allow_legacy_fallback: bool,
    ) -> Result<DmIdemResolution, StoreError> {
        let RoomId::Direct { ref pair } = room else {
            return Ok(DmIdemResolution::Route(room));
        };
        let (a, b): (Principal, Principal) = pair.clone().into();
        let conn = self.inner.conn();

        if a.is_operator() || b.is_operator() {
            if let Some(key) = idem
                && let Some(batch) = find_idem(&conn, &StreamId::Room { room: room.clone() }, key)?
                && batch.first().is_some_and(|env| !env.by.same_actor(by))
            {
                return Ok(DmIdemResolution::CrossAuthor { room });
            }
            return Ok(DmIdemResolution::Route(room));
        }

        let canon_exists = head_of(&conn, &StreamId::Room { room: room.clone() })? > Seq::ORIGIN;
        let mut legacy_existing = Vec::new();
        for cand in legacy_dm_candidates(&a, &b) {
            if head_of(&conn, &StreamId::Room { room: cand.clone() })? > Seq::ORIGIN {
                legacy_existing.push(cand);
            }
        }

        if !allow_legacy_fallback {
            let pinned = if canon_exists || legacy_existing.is_empty() {
                room
            } else {
                legacy_existing.remove(0)
            };
            if let Some(key) = idem
                && let Some(batch) = find_idem(
                    &conn,
                    &StreamId::Room {
                        room: pinned.clone(),
                    },
                    key,
                )?
                && batch.first().is_some_and(|env| !env.by.same_actor(by))
            {
                return Ok(DmIdemResolution::CrossAuthor { room: pinned });
            }
            return Ok(DmIdemResolution::Route(pinned));
        }
        if let Some(key) = idem {
            let mut to_check = Vec::with_capacity(1 + legacy_existing.len());
            if canon_exists {
                to_check.push(room.clone());
            }
            to_check.extend(legacy_existing.iter().cloned());

            let mut same_actor_rooms = Vec::new();
            let mut other_actor_rooms = Vec::new();
            for r in to_check {
                if let Some(batch) = find_idem(&conn, &StreamId::Room { room: r.clone() }, key)? {
                    if batch.first().is_some_and(|env| env.by.same_actor(by)) {
                        same_actor_rooms.push(r);
                    } else {
                        other_actor_rooms.push(r);
                    }
                }
            }
            if same_actor_rooms.len() == 1 {
                return Ok(DmIdemResolution::Route(same_actor_rooms.remove(0)));
            }
            if same_actor_rooms.len() > 1 {
                return Ok(DmIdemResolution::Ambiguous(same_actor_rooms));
            }
            if let Some(other_room) = other_actor_rooms.into_iter().next() {
                return Ok(DmIdemResolution::CrossAuthor { room: other_room });
            }
        }
        if canon_exists || legacy_existing.is_empty() {
            Ok(DmIdemResolution::Route(room))
        } else {
            Ok(DmIdemResolution::Route(legacy_existing.remove(0)))
        }
    }

    /// Every stream with at least one entry, with its head.
    pub fn streams(&self) -> Result<Vec<(StreamId, Seq)>, StoreError> {
        let conn = self.inner.conn();
        let mut stmt = conn.prepare_cached(
            "SELECT stream, max(seq) FROM entries GROUP BY stream ORDER BY stream",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        let mut out = Vec::new();
        for row in rows {
            let (stream, head) = row?;
            out.push((serde_json::from_str(&stream)?, Seq(head as u64)));
        }
        Ok(out)
    }
}

/// The single write capability for one stream.
pub struct StreamWriter {
    stream: StreamId,
    key: String,
    head: Seq,
    inner: Arc<Inner>,
}

impl StreamWriter {
    pub fn stream(&self) -> &StreamId {
        &self.stream
    }

    pub fn head(&self) -> Seq {
        self.head
    }

    /// The envelopes an earlier append with `idem` committed, if any.
    pub fn lookup(&self, idem: &IdemKey) -> Result<Option<Vec<Envelope>>, StoreError> {
        find_idem(&self.inner.conn(), &self.stream, idem)
    }

    /// Append a batch in one transaction. All or nothing.
    pub fn append(
        &mut self,
        by: &Principal,
        at: UnixMs,
        idem: Option<&IdemKey>,
        events: Vec<Event>,
    ) -> Result<Appended, StoreError> {
        if events.iter().any(|e| !e.fits(&self.stream)) {
            return Err(StoreError::WrongStream(self.stream.clone()));
        }
        let mut conn = self.inner.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if let Some(idem) = idem
            && let Some(prior) = find_idem(&tx, &self.stream, idem)?
        {
            return Ok(Appended::Replayed(prior));
        }
        let found = head_of(&tx, &self.stream)?;
        if found != self.head {
            return Err(StoreError::HeadMoved {
                expected: self.head,
                found,
            });
        }
        let by_json = serde_json::to_string(by)?;
        let batch = events.len() as i64;
        let mut committed = Vec::with_capacity(events.len());
        let mut seq = self.head;
        {
            let mut insert = tx.prepare_cached(
                "INSERT INTO entries(stream, seq, at, by, idem, batch, event) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for (i, event) in events.into_iter().enumerate() {
                seq = seq.next();
                let (row_idem, row_batch) = if i == 0 {
                    (idem.map(IdemKey::as_str), batch)
                } else {
                    (None, 0)
                };
                insert.execute(params![
                    self.key,
                    seq.0 as i64,
                    at.0 as i64,
                    by_json,
                    row_idem,
                    row_batch,
                    serde_json::to_string(&event)?
                ])?;
                committed.push(Envelope {
                    stream: self.stream.clone(),
                    seq,
                    at,
                    by: by.clone(),
                    event,
                });
            }
        }
        if let Some(ref hook) = self.inner.hook {
            let tx_ctx = TxContext { tx: &tx };
            hook.on_append(&tx_ctx, &committed)?;
        }
        tx.commit()?;
        self.head = seq;
        if let Some(ref hook) = self.inner.hook {
            hook.after_commit(&committed);
        }
        Ok(Appended::Fresh(committed))
    }
}

impl Drop for StreamWriter {
    fn drop(&mut self) {
        self.inner.claimed().remove(&self.key);
    }
}

#[cfg(test)]
mod legacy_dm_tests {
    use super::*;
    use rex_wave_api::SeatName;
    use rex_wave_api::room::{Body, Mentions, RoomEvent, RoomOrigin, RoomRef};

    #[test]
    fn resolves_pre_pr26_operator_dm_without_mutating_single_or_split_journals() {
        let dir = tempfile::tempdir().unwrap();
        let op = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };
        let alice = Principal::Seat {
            seat: SeatName::parse("alice").unwrap(),
        };
        let legacy_room = RoomId::Direct {
            pair: PrincipalPair::new(op.clone(), alice.clone()).unwrap(),
        };
        let canon_room = RoomRef::Dm {
            with: alice.clone(),
        }
        .resolve(&op)
        .unwrap();
        assert_ne!(legacy_room, canon_room);

        // Case 1: Only the pre-PR-26 (Operator, Seat) stream exists.
        let single_db = dir.path().join("single.sqlite3");
        {
            let j = Journal::open(&single_db).unwrap();
            let mut w = j
                .claim(StreamId::Room {
                    room: legacy_room.clone(),
                })
                .unwrap();
            w.append(
                &op,
                UnixMs(1_000),
                Some(&IdemKey::parse("op-1").unwrap()),
                vec![
                    Event::Room(RoomEvent::Opened {
                        id: legacy_room.clone(),
                        origin: RoomOrigin::Manual,
                        topic: None,
                    }),
                    Event::Room(RoomEvent::Posted {
                        body: Body::parse("pre-upgrade dm").unwrap(),
                        mentions: Mentions::default(),
                        re: None,
                        after: None,
                        labels: Vec::new(),
                        task: None,
                        idem: None,
                        origin: rex_wave_api::room::PostOrigin::Direct,
                    }),
                ],
            )
            .unwrap();
        }
        let reopened = Journal::open(&single_db).unwrap();
        assert_eq!(
            reopened.resolve_dm_room(canon_room.clone()).unwrap(),
            legacy_room,
            "single legacy DM stream continues without splitting"
        );
        assert_eq!(
            reopened
                .resolve_dm_room_with_idem(canon_room.clone(), &op, None, false)
                .unwrap(),
            DmIdemResolution::Route(legacy_room.clone()),
            "explicit canonical RoomRef::Id write on legacy-only DM continues legacy stream without forking"
        );
        assert_eq!(
            reopened
                .head(&StreamId::Room {
                    room: legacy_room.clone()
                })
                .unwrap(),
            Seq(2)
        );

        // Case 2: Both (Seat, Seat) and (Operator, Seat) already exist in a 3e50dc4 journal.
        let split_db = dir.path().join("split.sqlite3");
        {
            let j = Journal::open(&split_db).unwrap();
            let same_idem = IdemKey::parse("same-idem").unwrap();
            let mut w_canon = j
                .claim(StreamId::Room {
                    room: canon_room.clone(),
                })
                .unwrap();
            w_canon
                .append(
                    &alice,
                    UnixMs(1_000),
                    Some(&same_idem),
                    vec![
                        Event::Room(RoomEvent::Opened {
                            id: canon_room.clone(),
                            origin: RoomOrigin::Manual,
                            topic: None,
                        }),
                        Event::Room(RoomEvent::Posted {
                            body: Body::parse("canon post").unwrap(),
                            mentions: Mentions::default(),
                            re: None,
                            after: None,
                            labels: Vec::new(),
                            task: None,
                            idem: None,
                            origin: rex_wave_api::room::PostOrigin::Direct,
                        }),
                    ],
                )
                .unwrap();
            let mut w_legacy = j
                .claim(StreamId::Room {
                    room: legacy_room.clone(),
                })
                .unwrap();
            w_legacy
                .append(
                    &op,
                    UnixMs(2_000),
                    Some(&same_idem),
                    vec![
                        Event::Room(RoomEvent::Opened {
                            id: legacy_room.clone(),
                            origin: RoomOrigin::Manual,
                            topic: None,
                        }),
                        Event::Room(RoomEvent::Posted {
                            body: Body::parse("legacy post").unwrap(),
                            mentions: Mentions::default(),
                            re: None,
                            after: None,
                            labels: Vec::new(),
                            task: None,
                            idem: None,
                            origin: rex_wave_api::room::PostOrigin::Direct,
                        }),
                    ],
                )
                .unwrap();
        }
        let split = Journal::open(&split_db).unwrap();
        assert_eq!(
            split.resolve_dm_room(canon_room.clone()).unwrap(),
            canon_room
        );
        assert_eq!(
            split.resolve_dm_room(legacy_room.clone()).unwrap(),
            legacy_room
        );
        assert_eq!(
            split
                .head(&StreamId::Room {
                    room: canon_room.clone()
                })
                .unwrap(),
            Seq(2)
        );
        assert_eq!(
            split
                .head(&StreamId::Room {
                    room: legacy_room.clone()
                })
                .unwrap(),
            Seq(2)
        );
        let same_idem = IdemKey::parse("same-idem").unwrap();
        assert_eq!(
            split
                .resolve_dm_room_with_idem(canon_room.clone(), &op, Some(&same_idem), true)
                .unwrap(),
            DmIdemResolution::Route(legacy_room.clone()),
            "operator retry via RoomRef::Dm resolves to operator's legacy batch"
        );
        assert_eq!(
            split
                .resolve_dm_room_with_idem(canon_room.clone(), &op, Some(&same_idem), false)
                .unwrap(),
            DmIdemResolution::CrossAuthor {
                room: canon_room.clone()
            },
            "explicit canonical RoomRef::Id stays pinned to canonical stream and flags cross-author idem"
        );
        assert_eq!(
            split
                .resolve_dm_room_with_idem(canon_room.clone(), &alice, Some(&same_idem), true)
                .unwrap(),
            DmIdemResolution::Route(canon_room.clone()),
            "peer retry resolves to peer's canonical batch"
        );
        let bob = Principal::Seat {
            seat: SeatName::parse("bob").unwrap(),
        };
        assert!(matches!(
            split
                .resolve_dm_room_with_idem(canon_room.clone(), &bob, Some(&same_idem), true)
                .unwrap(),
            DmIdemResolution::CrossAuthor { .. }
        ));
    }

    #[test]
    fn user_version_upgrades_from_zero_and_rejects_future_versions() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ver.sqlite3");
        {
            let raw = Connection::open(&db_path).unwrap();
            raw.execute_batch(SCHEMA).unwrap();
            let v0: i64 = raw
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
            assert_eq!(v0, 0);
        }
        let upgraded = Journal::open(&db_path).unwrap();
        assert_eq!(upgraded.schema_version().unwrap(), JOURNAL_SCHEMA_VERSION);
        drop(upgraded);

        {
            let raw = Connection::open(&db_path).unwrap();
            raw.pragma_update(None, "user_version", JOURNAL_SCHEMA_VERSION + 7)
                .unwrap();
        }
        let err = match Journal::open(&db_path) {
            Ok(_) => panic!("future schema version must be rejected"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            StoreError::UnsupportedSchemaVersion {
                found,
                max_supported: JOURNAL_SCHEMA_VERSION,
            } if found == JOURNAL_SCHEMA_VERSION + 7
        ));
    }

    #[test]
    fn commit_hook_is_atomic_with_append_and_skips_idempotent_replays() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct TestHook {
            fail: AtomicBool,
            on_append_calls: AtomicUsize,
            after_commit_calls: AtomicUsize,
        }

        impl CommitHook for TestHook {
            fn on_init(&self, ctx: &mut InitContext<'_>) -> Result<(), StoreError> {
                ctx.execute_ddl(
                    "CREATE TABLE IF NOT EXISTS side_index(seq INTEGER PRIMARY KEY, stream TEXT NOT NULL) STRICT;",
                )
            }

            fn on_append(
                &self,
                tx: &TxContext<'_, '_>,
                committed: &[Envelope],
            ) -> Result<(), StoreError> {
                self.on_append_calls.fetch_add(1, Ordering::SeqCst);
                if self.fail.load(Ordering::SeqCst) {
                    return Err(StoreError::Backend("injected hook failure".into()));
                }
                for env in committed {
                    let s = env.seq.0.to_string();
                    let k = key(&env.stream);
                    tx.execute_sql(
                        "INSERT INTO side_index(seq, stream) VALUES (CAST(?1 AS INTEGER), ?2)",
                        &[&s, &k],
                    )?;
                }
                Ok(())
            }

            fn after_commit(&self, _committed: &[Envelope]) {
                self.after_commit_calls.fetch_add(1, Ordering::SeqCst);
            }
        }

        let hook = Arc::new(TestHook {
            fail: AtomicBool::new(true),
            on_append_calls: AtomicUsize::new(0),
            after_commit_calls: AtomicUsize::new(0),
        });
        let j = Journal::in_memory_with_hook(Some(hook.clone())).unwrap();
        let alice = Principal::Seat {
            seat: SeatName::parse("alice").unwrap(),
        };
        let room = RoomRef::parse_str("#general")
            .unwrap()
            .resolve(&alice)
            .unwrap();
        let stream = StreamId::Room { room: room.clone() };
        let mut w = j.claim(stream.clone()).unwrap();
        let idem = IdemKey::parse("post-1").unwrap();
        let ev = Event::Room(RoomEvent::Opened {
            id: room,
            origin: RoomOrigin::Manual,
            topic: None,
        });

        // Failing hook rolls back `entries` and leaves `head` at ORIGIN.
        assert!(
            w.append(&alice, UnixMs(100), Some(&idem), vec![ev.clone()])
                .is_err()
        );
        assert_eq!(w.head(), Seq::ORIGIN);
        assert_eq!(j.head(&stream).unwrap(), Seq::ORIGIN);
        assert_eq!(hook.on_append_calls.load(Ordering::SeqCst), 1);
        assert_eq!(hook.after_commit_calls.load(Ordering::SeqCst), 0);

        // Succeeding retry commits both `entries` and `side_index`.
        hook.fail.store(false, Ordering::SeqCst);
        let first = w
            .append(&alice, UnixMs(100), Some(&idem), vec![ev.clone()])
            .unwrap();
        assert!(matches!(first, Appended::Fresh(_)));
        assert_eq!(w.head(), Seq(1));
        assert_eq!(hook.on_append_calls.load(Ordering::SeqCst), 2);
        assert_eq!(hook.after_commit_calls.load(Ordering::SeqCst), 1);

        // Idempotent replay does NOT re-invoke `on_append` or `after_commit`.
        let replay = w
            .append(&alice, UnixMs(200), Some(&idem), vec![ev])
            .unwrap();
        assert!(matches!(replay, Appended::Replayed(_)));
        assert_eq!(hook.on_append_calls.load(Ordering::SeqCst), 2);
        assert_eq!(hook.after_commit_calls.load(Ordering::SeqCst), 1);
    }
}
