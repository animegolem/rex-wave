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
    ArchiveTarget, Envelope, Event, IdemKey, Principal, Seq, StreamId, UnixMs, WireCommand,
    notify::{InboxEvent, InboxMarkRead, NotifyKind},
    room::{PrincipalPair, RoomEvent, RoomId},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("stream {0:?} already has a live writer")]
    AlreadyClaimed(StreamId),
    #[error("event does not belong on stream {0:?}")]
    WrongStream(StreamId),
    #[error("head moved underneath writer (expected {expected:?}, found {found:?})")]
    HeadMoved { expected: Seq, found: Seq },
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

struct Inner {
    conn: Mutex<Connection>,
    claimed: Mutex<HashSet<String>>,
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

impl Journal {
    /// Open (creating if needed) the journal at `path` in WAL mode.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(StoreError::Backend(format!(
                "journal_mode is {mode}, want wal"
            )));
        }
        conn.execute_batch("PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;")?;
        Self::init(conn)
    }

    /// In-memory journal for tests and embedded demos.
    pub fn in_memory() -> Result<Self, StoreError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(mut conn: Connection) -> Result<Self, StoreError> {
        conn.execute_batch(SCHEMA)?;
        migrate_legacy_operator_dms(&mut conn)?;
        Ok(Journal {
            inner: Arc::new(Inner {
                conn: Mutex::new(conn),
                claimed: Mutex::new(HashSet::new()),
            }),
        })
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
        tx.commit()?;
        self.head = seq;
        Ok(Appended::Fresh(committed))
    }
}

impl Drop for StreamWriter {
    fn drop(&mut self) {
        self.inner.claimed().remove(&self.key);
    }
}

struct LegacyRow {
    seq: Seq,
    at: i64,
    by_json: String,
    idem: Option<String>,
    batch: i64,
    event: Event,
}

fn remap_seq(seq: Seq, map: &HashMap<Seq, Seq>) -> Seq {
    map.get(&seq).copied().unwrap_or(seq)
}

fn remap_room_event(ev: RoomEvent, canonical_room: &RoomId, map: &HashMap<Seq, Seq>) -> RoomEvent {
    match ev {
        RoomEvent::Opened { origin, topic, .. } => RoomEvent::Opened {
            id: canonical_room.clone(),
            origin,
            topic,
        },
        RoomEvent::Posted {
            body,
            mentions,
            re,
            after,
            labels,
            mut task,
            idem,
        } => {
            if let Some(ref mut spec) = task {
                spec.needs = spec.needs.iter().map(|&s| remap_seq(s, map)).collect();
            }
            RoomEvent::Posted {
                body,
                mentions,
                re: re.map(|s| remap_seq(s, map)),
                after: after.map(|s| remap_seq(s, map)),
                labels,
                task,
                idem,
            }
        }
        RoomEvent::Labeled {
            target,
            added,
            removed,
            before,
            after,
        } => RoomEvent::Labeled {
            target: remap_seq(target, map),
            added,
            removed,
            before,
            after,
        },
        RoomEvent::TaskClaimed { task, rev, lease } => RoomEvent::TaskClaimed {
            task: remap_seq(task, map),
            rev,
            lease,
        },
        RoomEvent::TaskHeartbeat {
            task,
            epoch,
            expires_at,
        } => RoomEvent::TaskHeartbeat {
            task: remap_seq(task, map),
            epoch,
            expires_at,
        },
        RoomEvent::TaskTransitioned { task, rev, status } => RoomEvent::TaskTransitioned {
            task: remap_seq(task, map),
            rev,
            status,
        },
        RoomEvent::TaskUpdated {
            task,
            rev,
            title,
            needs_add,
            needs_remove,
            priority,
            due_ms,
            clear_due,
        } => RoomEvent::TaskUpdated {
            task: remap_seq(task, map),
            rev,
            title,
            needs_add: needs_add.into_iter().map(|s| remap_seq(s, map)).collect(),
            needs_remove: needs_remove
                .into_iter()
                .map(|s| remap_seq(s, map))
                .collect(),
            priority,
            due_ms,
            clear_due,
        },
        other => other,
    }
}

fn remap_inbox_event(
    ev: InboxEvent,
    migrated: &BTreeMap<RoomId, (RoomId, HashMap<Seq, Seq>)>,
) -> Option<InboxEvent> {
    match ev {
        InboxEvent::CursorAdvanced { room, upto } => {
            let (canon, map) = migrated.get(&room)?;
            Some(InboxEvent::CursorAdvanced {
                room: canon.clone(),
                upto: remap_seq(upto, map),
            })
        }
        InboxEvent::ArchivedSet {
            target: ArchiveTarget::Room { room },
            archived,
        } => {
            let (canon, _) = migrated.get(&room)?;
            Some(InboxEvent::ArchivedSet {
                target: ArchiveTarget::Room {
                    room: canon.clone(),
                },
                archived,
            })
        }
        InboxEvent::Raised {
            id,
            rank,
            mut draft,
        } => {
            if !remap_draft(&mut draft, migrated) {
                return None;
            }
            Some(InboxEvent::Raised { id, rank, draft })
        }
        InboxEvent::Updated {
            id,
            rank,
            mut draft,
        } => {
            if !remap_draft(&mut draft, migrated) {
                return None;
            }
            Some(InboxEvent::Updated { id, rank, draft })
        }
        _ => None,
    }
}

fn remap_draft(
    draft: &mut rex_wave_api::notify::NotificationDraft,
    migrated: &BTreeMap<RoomId, (RoomId, HashMap<Seq, Seq>)>,
) -> bool {
    let mut changed = false;
    match &mut draft.kind {
        NotifyKind::DirectMessage { room, seq } | NotifyKind::Mention { room, seq } => {
            if let Some((canon, map)) = migrated.get(room) {
                *room = canon.clone();
                *seq = remap_seq(*seq, map);
                changed = true;
            }
        }
        _ => {}
    }
    for action in &mut draft.actions {
        if let WireCommand::InboxMarkRead(InboxMarkRead { room, upto }) = action.run.as_mut()
            && let Some((canon, map)) = migrated.get(room)
        {
            *room = canon.clone();
            *upto = remap_seq(*upto, map);
            changed = true;
        }
    }
    changed
}

fn disambiguate_idem(raw: &str, used: &mut HashSet<String>) -> String {
    if used.insert(raw.to_owned()) {
        return raw.to_owned();
    }
    for n in 1u32.. {
        let prefix = format!("legacy-op-{n}:");
        let keep = 256usize.saturating_sub(prefix.len());
        let mut end = raw.len().min(keep);
        while end > 0 && !raw.is_char_boundary(end) {
            end -= 1;
        }
        let candidate = format!("{prefix}{}", &raw[..end]);
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("idem disambiguation exhausted")
}

fn load_legacy_rows(tx: &Transaction<'_>, stream_key: &str) -> Result<Vec<LegacyRow>, StoreError> {
    let mut stmt = tx.prepare(
        "SELECT seq, at, by, idem, batch, event FROM entries WHERE stream = ?1 ORDER BY seq",
    )?;
    let rows = stmt.query_map(params![stream_key], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, i64>(4)?,
            r.get::<_, String>(5)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (seq, at, by_json, idem, batch, event_json) = row?;
        out.push(LegacyRow {
            seq: Seq(seq as u64),
            at,
            by_json,
            idem,
            batch,
            event: serde_json::from_str(&event_json)?,
        });
    }
    Ok(out)
}

/// Migrate pre-PR-26 `(Operator { seat }, peer)` DM streams into canonical
/// `(Seat { seat }, peer)` DM streams, merging losslessly if both already
/// exist in the journal.
fn migrate_legacy_operator_dms(conn: &mut Connection) -> Result<(), StoreError> {
    let needs_scan: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entries \
         WHERE (stream LIKE '%\"room\":\"direct\"%' AND stream LIKE '%\"kind\":\"operator\"%') \
            OR (stream LIKE '%\"stream\":\"inbox\"%' AND event LIKE '%\"room\":\"direct\"%' AND event LIKE '%\"kind\":\"operator\"%')",
        [],
        |r| r.get(0),
    )?;
    if needs_scan == 0 {
        return Ok(());
    }

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let stream_keys: Vec<String> = {
        let mut stmt = tx.prepare(
            "SELECT DISTINCT stream FROM entries WHERE stream LIKE '%\"room\":\"direct\"%' AND stream LIKE '%\"kind\":\"operator\"%' ORDER BY stream",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    let mut migrated_rooms: BTreeMap<RoomId, (RoomId, HashMap<Seq, Seq>)> = BTreeMap::new();

    for legacy_key in stream_keys {
        let StreamId::Room {
            room: RoomId::Direct { pair },
        } = serde_json::from_str::<StreamId>(&legacy_key)?
        else {
            continue;
        };
        let legacy_room = RoomId::Direct { pair: pair.clone() };
        let (a, b): (Principal, Principal) = pair.into();
        if !(a.is_operator() || b.is_operator()) || a.same_actor(&b) {
            continue;
        }
        let Ok(canon_pair) = PrincipalPair::new(a.as_dm_member(), b.as_dm_member()) else {
            continue;
        };
        let canon_room = RoomId::Direct { pair: canon_pair };
        let canon_stream = StreamId::Room {
            room: canon_room.clone(),
        };
        let canon_key = key(&canon_stream);
        if canon_key == legacy_key {
            continue;
        }

        let legacy_rows = load_legacy_rows(&tx, &legacy_key)?;
        let existing_head = head_of(&tx, &canon_stream)?;
        let mut seq_map: HashMap<Seq, Seq> = HashMap::new();

        if existing_head == Seq::ORIGIN {
            tx.execute("DELETE FROM entries WHERE stream = ?1", params![legacy_key])?;
            let mut insert = tx.prepare(
                "INSERT INTO entries(stream, seq, at, by, idem, batch, event) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for row in legacy_rows {
                seq_map.insert(row.seq, row.seq);
                let event = match row.event {
                    Event::Room(re) => Event::Room(remap_room_event(re, &canon_room, &seq_map)),
                    other => other,
                };
                insert.execute(params![
                    canon_key,
                    row.seq.0 as i64,
                    row.at,
                    row.by_json,
                    row.idem,
                    row.batch,
                    serde_json::to_string(&event)?,
                ])?;
            }
        } else {
            let mut used_idems: HashSet<String> = {
                let mut stmt =
                    tx.prepare("SELECT idem FROM entries WHERE stream = ?1 AND idem IS NOT NULL")?;
                let rows = stmt.query_map(params![canon_key], |r| r.get::<_, String>(0))?;
                rows.collect::<Result<HashSet<_>, _>>()?
            };

            let mut groups: Vec<(Option<String>, Vec<LegacyRow>)> = Vec::new();
            let mut idx = 0;
            while idx < legacy_rows.len() {
                let b_len = usize::try_from(legacy_rows[idx].batch.max(1)).unwrap_or(1);
                let end = (idx + b_len).min(legacy_rows.len());
                let group_idem = legacy_rows[idx].idem.clone();
                let mut slice = Vec::with_capacity(end - idx);
                for r in legacy_rows.iter().skip(idx).take(end - idx) {
                    slice.push(LegacyRow {
                        seq: r.seq,
                        at: r.at,
                        by_json: r.by_json.clone(),
                        idem: r.idem.clone(),
                        batch: r.batch,
                        event: r.event.clone(),
                    });
                }
                groups.push((group_idem, slice));
                idx = end;
            }

            tx.execute("DELETE FROM entries WHERE stream = ?1", params![legacy_key])?;
            let mut next_seq = existing_head;
            let mut insert = tx.prepare(
                "INSERT INTO entries(stream, seq, at, by, idem, batch, event) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;

            for (group_idem, rows) in groups {
                let surviving: Vec<LegacyRow> = rows
                    .into_iter()
                    .filter(|r| !matches!(r.event, Event::Room(RoomEvent::Opened { .. })))
                    .collect();
                if surviving.is_empty() {
                    continue;
                }
                let batch_len = surviving.len() as i64;
                let resolved_idem = group_idem.map(|raw| disambiguate_idem(&raw, &mut used_idems));
                for (i, row) in surviving.into_iter().enumerate() {
                    next_seq = next_seq.next();
                    seq_map.insert(row.seq, next_seq);
                    let event = match row.event {
                        Event::Room(re) => Event::Room(remap_room_event(re, &canon_room, &seq_map)),
                        other => other,
                    };
                    let (row_idem, row_batch) = if i == 0 {
                        (resolved_idem.as_deref(), batch_len)
                    } else {
                        (None, 0)
                    };
                    insert.execute(params![
                        canon_key,
                        next_seq.0 as i64,
                        row.at,
                        row.by_json,
                        row_idem,
                        row_batch,
                        serde_json::to_string(&event)?,
                    ])?;
                }
            }
        }

        migrated_rooms.insert(legacy_room, (canon_room, seq_map));
    }

    if !migrated_rooms.is_empty() {
        let inbox_rows: Vec<(String, i64, String)> = {
            let mut stmt = tx.prepare(
                "SELECT stream, seq, event FROM entries WHERE stream LIKE '%\"stream\":\"inbox\"%' AND event LIKE '%\"room\":\"direct\"%' AND event LIKE '%\"kind\":\"operator\"%'",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let mut update =
            tx.prepare("UPDATE entries SET event = ?1 WHERE stream = ?2 AND seq = ?3")?;
        for (stream_k, seq_i, ev_json) in inbox_rows {
            if let Ok(Event::Inbox(iev)) = serde_json::from_str::<Event>(&ev_json)
                && let Some(remapped) = remap_inbox_event(iev, &migrated_rooms)
            {
                let updated = serde_json::to_string(&Event::Inbox(remapped))?;
                update.execute(params![updated, stream_k, seq_i])?;
            }
        }
    }

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod legacy_dm_tests {
    use super::*;
    use rex_wave_api::SeatName;
    use rex_wave_api::room::{Body, Mentions, RoomOrigin};

    fn insert_pre_pr26_batch(
        path: &Path,
        stream: &StreamId,
        by: &Principal,
        idem: Option<&str>,
        events: Vec<Event>,
    ) {
        let mut conn = Connection::open(path).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        let tx = conn.transaction().unwrap();
        let k = key(stream);
        let head: i64 = tx
            .query_row(
                "SELECT COALESCE(max(seq), 0) FROM entries WHERE stream = ?1",
                params![&k],
                |r| r.get(0),
            )
            .unwrap();
        let batch = events.len() as i64;
        for (i, ev) in events.into_iter().enumerate() {
            let seq = head + (i as i64) + 1;
            let (row_idem, row_batch) = if i == 0 { (idem, batch) } else { (None, 0) };
            tx.execute(
                "INSERT INTO entries(stream, seq, at, by, idem, batch, event) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    &k,
                    seq,
                    1_000i64 + seq,
                    serde_json::to_string(by).unwrap(),
                    row_idem,
                    row_batch,
                    serde_json::to_string(&ev).unwrap(),
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    #[test]
    fn migrates_single_and_split_legacy_operator_dm_streams_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let op = Principal::Operator {
            seat: SeatName::parse("owner").unwrap(),
        };
        let owner_seat = Principal::Seat {
            seat: SeatName::parse("owner").unwrap(),
        };
        let alice = Principal::Seat {
            seat: SeatName::parse("alice").unwrap(),
        };
        let legacy_room = RoomId::Direct {
            pair: PrincipalPair::new(op.clone(), alice.clone()).unwrap(),
        };
        let canon_room = RoomId::Direct {
            pair: PrincipalPair::new(owner_seat.clone(), alice.clone()).unwrap(),
        };
        let legacy_stream = StreamId::Room {
            room: legacy_room.clone(),
        };
        let canon_stream = StreamId::Room {
            room: canon_room.clone(),
        };

        // Case 1: Only the legacy (Operator, Seat) stream exists.
        let single_db = dir.path().join("single.sqlite3");
        insert_pre_pr26_batch(
            &single_db,
            &legacy_stream,
            &op,
            Some("post-1"),
            vec![
                Event::Room(RoomEvent::Opened {
                    id: legacy_room.clone(),
                    origin: RoomOrigin::Manual,
                    topic: None,
                }),
                Event::Room(RoomEvent::Posted {
                    body: Body::parse("hello from 3e50dc4").unwrap(),
                    mentions: Mentions::default(),
                    re: None,
                    after: None,
                    labels: Vec::new(),
                    task: None,
                    idem: None,
                }),
            ],
        );
        let reopened = Journal::open(&single_db).unwrap();
        assert_eq!(reopened.head(&legacy_stream).unwrap(), Seq::ORIGIN);
        assert_eq!(reopened.head(&canon_stream).unwrap(), Seq(2));
        let entries = reopened.read(&canon_stream, Seq::ORIGIN, 10).unwrap();
        assert!(matches!(
            &entries[0].event,
            Event::Room(RoomEvent::Opened { id, .. }) if id == &canon_room
        ));

        // Case 2: Both (Seat, Seat) and (Operator, Seat) already exist with threaded replies.
        let split_db = dir.path().join("split.sqlite3");
        insert_pre_pr26_batch(
            &split_db,
            &canon_stream,
            &alice,
            Some("same-idem"),
            vec![
                Event::Room(RoomEvent::Opened {
                    id: canon_room.clone(),
                    origin: RoomOrigin::Manual,
                    topic: None,
                }),
                Event::Room(RoomEvent::Posted {
                    body: Body::parse("from alice").unwrap(),
                    mentions: Mentions::default(),
                    re: None,
                    after: None,
                    labels: Vec::new(),
                    task: None,
                    idem: None,
                }),
            ],
        );
        insert_pre_pr26_batch(
            &split_db,
            &legacy_stream,
            &op,
            Some("same-idem"),
            vec![
                Event::Room(RoomEvent::Opened {
                    id: legacy_room.clone(),
                    origin: RoomOrigin::Manual,
                    topic: None,
                }),
                Event::Room(RoomEvent::Posted {
                    body: Body::parse("root from op").unwrap(),
                    mentions: Mentions::default(),
                    re: None,
                    after: None,
                    labels: Vec::new(),
                    task: None,
                    idem: None,
                }),
            ],
        );
        insert_pre_pr26_batch(
            &split_db,
            &legacy_stream,
            &op,
            Some("reply-idem"),
            vec![Event::Room(RoomEvent::Posted {
                body: Body::parse("reply to op root").unwrap(),
                mentions: Mentions::default(),
                re: Some(Seq(2)),
                after: None,
                labels: Vec::new(),
                task: None,
                idem: None,
            })],
        );

        let merged = Journal::open(&split_db).unwrap();
        assert_eq!(merged.head(&legacy_stream).unwrap(), Seq::ORIGIN);
        assert_eq!(merged.head(&canon_stream).unwrap(), Seq(4));
        let all = merged.read(&canon_stream, Seq::ORIGIN, 10).unwrap();
        assert_eq!(all.len(), 4);
        assert!(matches!(
            &all[3].event,
            Event::Room(RoomEvent::Posted { re: Some(Seq(3)), body, .. })
                if body.as_str() == "reply to op root"
        ));
    }
}
