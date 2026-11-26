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
    mail::MailEvent,
    room::{PrincipalPair, RoomId},
};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{BTreeSet, HashSet};
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
CREATE TABLE IF NOT EXISTS mail_fts_state(
  id       INTEGER PRIMARY KEY CHECK (id = 1),
  last_seq INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS mail_fts_meta(
  seq        INTEGER PRIMARY KEY,
  sha256     TEXT NOT NULL UNIQUE,
  from_seat  TEXT NOT NULL,
  recipients TEXT NOT NULL,
  subject    TEXT NOT NULL,
  path       TEXT NOT NULL,
  created_at INTEGER NOT NULL
) STRICT;
CREATE VIRTUAL TABLE IF NOT EXISTS mail_fts USING fts5(
  sha256 UNINDEXED,
  msg_id,
  from_seat,
  recipients,
  subject,
  topic,
  body,
  path,
  tokenize = 'porter unicode61'
);
";

/// One ranked match returned by [`Journal::search_mail`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailFtsMatch {
    pub seq: Seq,
    pub sha256: String,
    pub from_seat: String,
    pub subject: String,
    pub path: String,
    pub created_at: UnixMs,
    pub snippet: String,
}

fn normalize_fts_seat(raw: &str) -> String {
    raw.trim().trim_start_matches('@').to_ascii_lowercase()
}

fn escape_fts_phrase(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for ch in raw.chars() {
        if ch == '"' {
            out.push_str("\"\"");
        } else if !ch.is_control() {
            out.push(ch);
        }
    }
    out.push('"');
    out
}

fn build_fts5_query(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut tokens: Vec<(String, bool)> = Vec::new();
    let mut chars = trimmed.chars().peekable();
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch == '"' {
            chars.next();
            let mut phrase = String::new();
            while let Some(c) = chars.next() {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        phrase.push('"');
                    } else {
                        break;
                    }
                } else {
                    phrase.push(c);
                }
            }
            if !phrase.trim().is_empty() {
                tokens.push((escape_fts_phrase(&phrase), false));
            }
            continue;
        }
        let mut word = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() {
                break;
            }
            word.push(c);
            chars.next();
        }
        if matches!(word.as_str(), "AND" | "OR" | "NOT") {
            tokens.push((word, true));
        } else if let Some(prefix) = word.strip_suffix('*')
            && !prefix.is_empty()
            && prefix
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-'))
        {
            tokens.push((format!("{}*", escape_fts_phrase(prefix)), false));
        } else {
            tokens.push((escape_fts_phrase(&word), false));
        }
    }

    let mut out = Vec::with_capacity(tokens.len());
    let mut last_was_op = true;
    for (idx, (tok, is_op)) in tokens.iter().enumerate() {
        if *is_op {
            let next_is_term = tokens.get(idx + 1).is_some_and(|(_, next_op)| !next_op);
            if !last_was_op && next_is_term {
                out.push(tok.clone());
                last_was_op = true;
            }
        } else {
            out.push(tok.clone());
            last_was_op = false;
        }
    }
    if out.is_empty() {
        Some(escape_fts_phrase(trimmed))
    } else {
        Some(out.join(" "))
    }
}

fn index_mail_envelope(tx: &rusqlite::Transaction<'_>, env: &Envelope) -> Result<bool, StoreError> {
    if env.stream != StreamId::Mail {
        return Ok(false);
    }
    let Event::Mail(MailEvent::MessageCommitted { message, .. }) = &env.event else {
        return Ok(false);
    };
    let from_seat = normalize_fts_seat(&message.from);
    let mut recips = BTreeSet::new();
    for s in message.to.iter().chain(message.cc.iter()) {
        recips.insert(s.as_str().to_owned());
    }
    let recipients_padded = if recips.is_empty() {
        String::new()
    } else {
        format!(" {} ", recips.into_iter().collect::<Vec<_>>().join(" "))
    };
    let path_str = message.path.to_string_lossy();
    let inserted = tx.execute(
        "INSERT OR IGNORE INTO mail_fts_meta(seq, sha256, from_seat, recipients, subject, path, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            env.seq.0 as i64,
            message.sha256.as_str(),
            from_seat,
            recipients_padded,
            message.subject,
            path_str.as_ref(),
            message.created_at.0 as i64,
        ],
    )?;
    if inserted > 0 {
        let topic_str = message.topic.as_deref().unwrap_or("");
        tx.execute(
            "INSERT INTO mail_fts(rowid, sha256, msg_id, from_seat, recipients, subject, topic, body, path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                env.seq.0 as i64,
                message.sha256.as_str(),
                message.id,
                from_seat,
                recipients_padded,
                message.subject,
                topic_str,
                message.body,
                path_str.as_ref(),
            ],
        )?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn set_mail_fts_watermark(tx: &rusqlite::Transaction<'_>, seq: Seq) -> Result<(), StoreError> {
    tx.execute(
        "INSERT INTO mail_fts_state(id, last_seq) VALUES (1, ?1)
         ON CONFLICT(id) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)",
        params![seq.0 as i64],
    )?;
    Ok(())
}

fn mail_fts_watermark(conn: &Connection) -> Result<Seq, StoreError> {
    let last: Option<i64> = conn
        .query_row(
            "SELECT last_seq FROM mail_fts_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    Ok(Seq(last.unwrap_or(0).max(0) as u64))
}

fn backfill_mail_fts_tx(tx: &rusqlite::Transaction<'_>) -> Result<usize, StoreError> {
    let mail_head = head_of(tx, &StreamId::Mail)?;
    let last = mail_fts_watermark(tx)?;
    let meta_count: i64 = tx.query_row("SELECT COUNT(*) FROM mail_fts_meta", [], |r| r.get(0))?;
    let fts_count: i64 = tx.query_row("SELECT COUNT(*) FROM mail_fts", [], |r| r.get(0))?;
    let mut cursor = if fts_count < meta_count {
        tx.execute("DELETE FROM mail_fts", [])?;
        tx.execute("DELETE FROM mail_fts_meta", [])?;
        tx.execute("DELETE FROM mail_fts_state", [])?;
        Seq::ORIGIN
    } else if mail_head <= last {
        return Ok(0);
    } else {
        last
    };

    let mut indexed = 0usize;
    while cursor < mail_head {
        let page = read_rows(tx, &StreamId::Mail, cursor, Some(mail_head), 512)?;
        if page.is_empty() {
            break;
        }
        for env in &page {
            cursor = env.seq;
            if index_mail_envelope(tx, env)? {
                indexed += 1;
            }
        }
    }
    set_mail_fts_watermark(tx, mail_head)?;
    Ok(indexed)
}

fn strip_sql_leading_trivia(mut sql: &str) -> &str {
    loop {
        let trimmed = sql.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            sql = match rest.find('\n') {
                Some(idx) => &rest[idx + 1..],
                None => "",
            };
        } else if let Some(rest) = trimmed.strip_prefix("/*") {
            sql = match rest.find("*/") {
                Some(idx) => &rest[idx + 2..],
                None => "",
            };
        } else {
            return trimmed;
        }
    }
}

fn leading_sql_verb(sql: &str) -> &str {
    let clean = strip_sql_leading_trivia(sql);
    let end = clean
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(clean.len());
    &clean[..end]
}

fn top_level_semicolons(sql: &str) -> Vec<usize> {
    let mut semis = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut in_bracket = false;
    while i < bytes.len() {
        let b = bytes[i];
        if !in_single && !in_double && !in_backtick && !in_bracket {
            if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
                continue;
            }
            match b {
                b'\'' => in_single = true,
                b'"' => in_double = true,
                b'`' => in_backtick = true,
                b'[' => in_bracket = true,
                b';' => semis.push(i),
                _ => {}
            }
        } else if in_single && b == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_single = false;
        } else if in_double && b == b'"' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_double = false;
        } else if in_backtick && b == b'`' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'`' {
                i += 2;
                continue;
            }
            in_backtick = false;
        } else if in_bracket && b == b']' {
            in_bracket = false;
        }
        i += 1;
    }
    semis
}

fn is_incomplete_sqlite_err(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(_, Some(msg))
            if msg.eq_ignore_ascii_case("incomplete input")
    )
}

/// Sealed initialization handle passed to [`CommitHook::on_init`]. Keeps
/// `rusqlite` and transaction control private to `rex-wave-store`.
pub struct InitContext<'a> {
    conn: &'a Connection,
}

impl InitContext<'_> {
    pub fn execute_ddl(&mut self, sql: &str) -> Result<(), StoreError> {
        if self.conn.is_autocommit() {
            return Err(StoreError::Backend(
                "init hook called outside an active SQLite transaction".into(),
            ));
        }
        let mut start = 0usize;
        while start < sql.len() {
            let rem = strip_sql_leading_trivia(&sql[start..]);
            if rem.is_empty() {
                break;
            }
            let verb = leading_sql_verb(rem);
            if !verb.eq_ignore_ascii_case("CREATE") {
                return Err(StoreError::Backend(format!(
                    "init hook DDL only allows CREATE statements (got {verb:?})"
                )));
            }
            let semis = top_level_semicolons(&sql[start..]);
            let mut executed = false;
            for rel_semi in semis {
                let end = start + rel_semi;
                let candidate = &sql[start..=end];
                match self.conn.prepare(candidate) {
                    Ok(mut stmt) => {
                        if self.conn.is_autocommit() {
                            return Err(StoreError::Backend(
                                "init hook called outside an active SQLite transaction".into(),
                            ));
                        }
                        stmt.execute([])?;
                        if self.conn.is_autocommit() {
                            return Err(StoreError::Backend(
                                "init hook illegally terminated enclosing SQLite transaction"
                                    .into(),
                            ));
                        }
                        start = end + 1;
                        executed = true;
                        break;
                    }
                    Err(e) if is_incomplete_sqlite_err(&e) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            if !executed {
                if self.conn.is_autocommit() {
                    return Err(StoreError::Backend(
                        "init hook called outside an active SQLite transaction".into(),
                    ));
                }
                let mut stmt = self.conn.prepare(&sql[start..])?;
                stmt.execute([])?;
                if self.conn.is_autocommit() {
                    return Err(StoreError::Backend(
                        "init hook illegally terminated enclosing SQLite transaction".into(),
                    ));
                }
                break;
            }
        }
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
        if self.tx.is_autocommit() {
            return Err(StoreError::Backend(
                "commit hook called after enclosing SQLite transaction ended".into(),
            ));
        }
        let verb = leading_sql_verb(sql);
        if !matches!(
            verb.to_ascii_uppercase().as_str(),
            "INSERT" | "UPDATE" | "DELETE" | "REPLACE"
        ) {
            return Err(StoreError::Backend(format!(
                "commit hook SQL only allows single INSERT/UPDATE/DELETE/REPLACE statements (got {verb:?})"
            )));
        }
        let mut stmt = self.tx.prepare(sql)?;
        let count = stmt.execute(rusqlite::params_from_iter(text_params.iter()))?;
        if self.tx.is_autocommit() {
            return Err(StoreError::Backend(
                "commit hook illegally terminated enclosing SQLite transaction".into(),
            ));
        }
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
        let existing_fts_sql: Option<String> = tx
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mail_fts'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(ref ddl) = existing_fts_sql
            && (!ddl.contains("msg_id") || !ddl.contains("topic"))
        {
            tx.execute_batch(
                "DROP TABLE IF EXISTS mail_fts;
                 DELETE FROM mail_fts_meta;
                 DELETE FROM mail_fts_state;",
            )?;
        }
        tx.execute_batch(SCHEMA)?;
        if found < JOURNAL_SCHEMA_VERSION {
            tx.pragma_update(None, "user_version", JOURNAL_SCHEMA_VERSION)?;
        }
        backfill_mail_fts_tx(&tx)?;
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
    ///   `room` pinned to its exact stream (matching `Read` and `Wait` on
    ///   `RoomRef::Id`) while verifying `idem` authorship.
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

        if !allow_legacy_fallback || a.is_operator() || b.is_operator() {
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

    /// Ensures all committed `StreamId::Mail` messages are indexed in SQLite FTS5.
    /// Returns the number of newly indexed messages.
    pub fn backfill_mail_fts(&self) -> Result<usize, StoreError> {
        let mut conn = self.inner.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let count = backfill_mail_fts_tx(&tx)?;
        tx.commit()?;
        Ok(count)
    }

    /// Full-text search over dissolved `StreamId::Mail` messages via SQLite FTS5.
    /// Automatically backfills if unindexed `StreamId::Mail` entries exist.
    pub fn search_mail(
        &self,
        query: &str,
        seat_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MailFtsMatch>, StoreError> {
        let Some(fts_query) = build_fts5_query(query) else {
            return Ok(Vec::new());
        };
        let cap = limit.clamp(1, 1_000) as i64;
        let seat_norm = seat_filter.map(normalize_fts_seat);
        let seat_needle = seat_norm
            .as_deref()
            .map(|s| format!(" {s} "))
            .unwrap_or_default();

        let mut conn = self.inner.conn();
        if head_of(&conn, &StreamId::Mail)? > mail_fts_watermark(&conn)? {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            backfill_mail_fts_tx(&tx)?;
            tx.commit()?;
        }

        let run_query = |expr: &str| -> Result<Vec<MailFtsMatch>, rusqlite::Error> {
            let mut stmt = conn.prepare_cached(
                "SELECT
                   m.seq,
                   m.sha256,
                   m.from_seat,
                   m.subject,
                   m.path,
                   m.created_at,
                   snippet(mail_fts, 6, '[', ']', '...', 18) AS snip
                 FROM mail_fts
                 JOIN mail_fts_meta m ON m.seq = mail_fts.rowid
                 WHERE mail_fts MATCH ?1
                   AND (?2 IS NULL OR m.from_seat = ?2 OR instr(m.recipients, ?3) > 0)
                 ORDER BY bm25(mail_fts), m.created_at DESC, m.seq DESC
                 LIMIT ?4",
            )?;
            let rows = stmt.query_map(
                params![expr, seat_norm.as_deref(), seat_needle.as_str(), cap],
                |r| {
                    Ok(MailFtsMatch {
                        seq: Seq(r.get::<_, i64>(0)? as u64),
                        sha256: r.get(1)?,
                        from_seat: r.get(2)?,
                        subject: r.get(3)?,
                        path: r.get(4)?,
                        created_at: UnixMs(r.get::<_, i64>(5)? as u64),
                        snippet: r.get(6)?,
                    })
                },
            )?;
            rows.collect()
        };

        match run_query(&fts_query) {
            Ok(hits) => Ok(hits),
            Err(rusqlite::Error::SqliteFailure(ffi_err, _))
                if ffi_err.code == rusqlite::ErrorCode::Unknown =>
            {
                let literal = escape_fts_phrase(query.trim());
                Ok(run_query(&literal)?)
            }
            Err(e) => Err(e.into()),
        }
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
        let committed = {
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
            if self.stream == StreamId::Mail {
                for env in &committed {
                    index_mail_envelope(&tx, env)?;
                }
                set_mail_fts_watermark(&tx, seq)?;
            }
            if let Some(ref hook) = self.inner.hook {
                let tx_ctx = TxContext { tx: &tx };
                hook.on_append(&tx_ctx, &committed)?;
                if tx.is_autocommit() {
                    return Err(StoreError::Backend(
                        "commit hook illegally terminated enclosing SQLite transaction".into(),
                    ));
                }
            }
            tx.commit()?;
            self.head = seq;
            committed
        };
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
            DmIdemResolution::Route(canon_room.clone()),
            "explicit canonical RoomRef::Id stays pinned to exact canonical stream"
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
        use std::sync::OnceLock;
        use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

        struct TestHook {
            fail: AtomicBool,
            try_commit_sql: AtomicBool,
            try_rollback_recovery: AtomicBool,
            on_append_calls: AtomicUsize,
            after_commit_calls: AtomicUsize,
            observed_head_in_after_commit: AtomicU64,
            journal: OnceLock<Journal>,
            stream: StreamId,
        }

        impl CommitHook for TestHook {
            fn on_init(&self, ctx: &mut InitContext<'_>) -> Result<(), StoreError> {
                assert!(ctx.execute_ddl("COMMIT;").is_err());
                assert!(ctx.execute_ddl("PRAGMA user_version = 99;").is_err());
                ctx.execute_ddl(
                    "CREATE TABLE IF NOT EXISTS side_index(seq INTEGER PRIMARY KEY, stream TEXT NOT NULL) STRICT;
                     CREATE TABLE IF NOT EXISTS side_audit(cnt INTEGER NOT NULL) STRICT;
                     CREATE TRIGGER IF NOT EXISTS side_index_audit AFTER INSERT ON side_index
                     BEGIN
                         INSERT INTO side_audit(cnt) VALUES (NEW.seq);
                     END;",
                )
            }

            fn on_append(
                &self,
                tx: &TxContext<'_, '_>,
                committed: &[Envelope],
            ) -> Result<(), StoreError> {
                self.on_append_calls.fetch_add(1, Ordering::SeqCst);
                if self.try_commit_sql.load(Ordering::SeqCst) {
                    return tx.execute_sql("COMMIT", &[]).map(|_| ());
                }
                if self.try_rollback_recovery.load(Ordering::SeqCst) {
                    tx.execute_sql(
                        "INSERT INTO side_index(seq, stream) VALUES (1, 'seed')",
                        &[],
                    )?;
                    let _ = tx.execute_sql(
                        "INSERT OR ROLLBACK INTO side_index(seq, stream) VALUES (1, 'dup')",
                        &[],
                    );
                    // Enclosing transaction was rolled back into autocommit mode; any recovery
                    // write MUST be rejected before executing so nothing persists outside a tx.
                    return tx
                        .execute_sql(
                            "INSERT INTO side_index(seq, stream) VALUES (99, 'escaped')",
                            &[],
                        )
                        .map(|_| ());
                }
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
                if let Some(j) = self.journal.get() {
                    let h = j
                        .head(&self.stream)
                        .expect("journal unlocked in after_commit");
                    self.observed_head_in_after_commit
                        .store(h.0, Ordering::SeqCst);
                }
                self.after_commit_calls.fetch_add(1, Ordering::SeqCst);
            }
        }

        let alice = Principal::Seat {
            seat: SeatName::parse("alice").unwrap(),
        };
        let room = RoomRef::parse_str("#general")
            .unwrap()
            .resolve(&alice)
            .unwrap();
        let stream = StreamId::Room { room: room.clone() };
        let hook = Arc::new(TestHook {
            fail: AtomicBool::new(true),
            try_commit_sql: AtomicBool::new(true),
            try_rollback_recovery: AtomicBool::new(true),
            on_append_calls: AtomicUsize::new(0),
            after_commit_calls: AtomicUsize::new(0),
            observed_head_in_after_commit: AtomicU64::new(0),
            journal: OnceLock::new(),
            stream: stream.clone(),
        });
        let j = Journal::in_memory_with_hook(Some(hook.clone())).unwrap();
        let _ = hook.journal.set(j.clone());
        let mut w = j.claim(stream.clone()).unwrap();
        let idem = IdemKey::parse("post-1").unwrap();
        let ev = Event::Room(RoomEvent::Opened {
            id: room,
            origin: RoomOrigin::Manual,
            topic: None,
        });

        // Transaction-control SQL (`COMMIT`) inside `on_append` is rejected with 0 rows committed.
        assert!(
            w.append(&alice, UnixMs(100), Some(&idem), vec![ev.clone()])
                .is_err()
        );
        assert_eq!(w.head(), Seq::ORIGIN);
        assert_eq!(j.head(&stream).unwrap(), Seq::ORIGIN);
        assert_eq!(hook.after_commit_calls.load(Ordering::SeqCst), 0);
        hook.try_commit_sql.store(false, Ordering::SeqCst);

        // Constraint-induced `INSERT OR ROLLBACK` followed by a recovery write is rejected
        // BEFORE executing in autocommit mode, leaving 0 rows in `side_index` or `entries`.
        assert!(
            w.append(&alice, UnixMs(100), Some(&idem), vec![ev.clone()])
                .is_err()
        );
        assert_eq!(w.head(), Seq::ORIGIN);
        assert_eq!(j.head(&stream).unwrap(), Seq::ORIGIN);
        let leaked_rows: i64 = j
            .inner
            .conn()
            .query_row("SELECT COUNT(*) FROM side_index", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            leaked_rows, 0,
            "recovery write must not execute in autocommit mode"
        );
        hook.try_rollback_recovery.store(false, Ordering::SeqCst);

        // Failing hook rolls back `entries` and leaves `head` at ORIGIN.
        assert!(
            w.append(&alice, UnixMs(100), Some(&idem), vec![ev.clone()])
                .is_err()
        );
        assert_eq!(w.head(), Seq::ORIGIN);
        assert_eq!(j.head(&stream).unwrap(), Seq::ORIGIN);
        assert_eq!(hook.on_append_calls.load(Ordering::SeqCst), 3);
        assert_eq!(hook.after_commit_calls.load(Ordering::SeqCst), 0);

        // Succeeding retry commits `entries`, `side_index`, and the `CREATE TRIGGER` audit row,
        // and `after_commit` reads the committed head on the cloned Journal without deadlocking.
        hook.fail.store(false, Ordering::SeqCst);
        let first = w
            .append(&alice, UnixMs(100), Some(&idem), vec![ev.clone()])
            .unwrap();
        assert!(matches!(first, Appended::Fresh(_)));
        assert_eq!(w.head(), Seq(1));
        assert_eq!(hook.observed_head_in_after_commit.load(Ordering::SeqCst), 1);
        assert_eq!(hook.on_append_calls.load(Ordering::SeqCst), 4);
        assert_eq!(hook.after_commit_calls.load(Ordering::SeqCst), 1);
        let trigger_rows: i64 = j
            .inner
            .conn()
            .query_row("SELECT COUNT(*) FROM side_audit WHERE cnt = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(trigger_rows, 1);

        // Idempotent replay does NOT re-invoke `on_append` or `after_commit`.
        let replay = w
            .append(&alice, UnixMs(200), Some(&idem), vec![ev])
            .unwrap();
        assert!(matches!(replay, Appended::Replayed(_)));
        assert_eq!(hook.on_append_calls.load(Ordering::SeqCst), 4);
        assert_eq!(hook.after_commit_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mail_fts5_indexes_transactionally_filters_by_seat_and_backfills_legacy_rows() {
        use rex_wave_api::mail::{LetterPriority, MailEvent, MailMessage, MailOrigin};
        use rex_wave_api::{SeatName, Sha256Hex};
        use std::path::PathBuf;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts.sqlite3");
        let alice = SeatName::parse("alice").unwrap();
        let bob = SeatName::parse("bob").unwrap();
        let carol = SeatName::parse("carol").unwrap();

        let msg1 = MailMessage {
            sha256: Sha256Hex::parse("a".repeat(64)).unwrap(),
            id: "1795500000001-alice-to-bob-11111111".into(),
            from: "alice".into(),
            to: vec![bob.clone()],
            cc: Vec::new(),
            subject: "OAuth refresh token rotation".into(),
            body: "Rotate auth-v2 refresh tokens with atomic WAL commit.".into(),
            path: PathBuf::from("mail/1795500000001-alice-to-bob-11111111.md"),
            priority: LetterPriority::High,
            topic: Some("auth-v2".into()),
            in_reply_to: None,
            refs: Vec::new(),
            created_at: UnixMs(1_795_500_000_001),
            created_at_iso: "2026-11-26T00:00:00Z".into(),
            receipt_only: false,
            room: None,
            raw_markdown: "---\nfrom: alice\nto: bob\n---\n\nRotate auth-v2 refresh tokens.\n"
                .into(),
            raw_bytes_b64: None,
            origin: MailOrigin::Send,
        };
        let msg2 = MailMessage {
            sha256: Sha256Hex::parse("b".repeat(64)).unwrap(),
            id: "1795500000002-carol-to-alice-22222222".into(),
            from: "carol".into(),
            to: vec![alice.clone()],
            cc: Vec::new(),
            subject: "Quarterly storage review".into(),
            body: "SQLite FTS5 indexing covers every dissolved letter.".into(),
            path: PathBuf::from("mail/1795500000002-carol-to-alice-22222222.md"),
            priority: LetterPriority::Normal,
            topic: None,
            in_reply_to: None,
            refs: Vec::new(),
            created_at: UnixMs(1_795_500_000_002),
            created_at_iso: "2026-11-26T00:00:01Z".into(),
            receipt_only: false,
            room: None,
            raw_markdown: "---\nfrom: carol\nto: alice\n---\n\nSQLite FTS5 indexing.\n".into(),
            raw_bytes_b64: None,
            origin: MailOrigin::Send,
        };

        {
            let j = Journal::open(&db_path).unwrap();
            let mut w = j.claim(StreamId::Mail).unwrap();
            w.append(
                &Principal::Seat {
                    seat: alice.clone(),
                },
                msg1.created_at,
                Some(&IdemKey::parse("m1").unwrap()),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(msg1.clone()),
                    mutated_after_ack: false,
                    prior_ack_sha: None,
                    send_idem: None,
                })],
            )
            .unwrap();
            w.append(
                &Principal::Seat {
                    seat: carol.clone(),
                },
                msg2.created_at,
                Some(&IdemKey::parse("m2").unwrap()),
                vec![Event::Mail(MailEvent::MessageCommitted {
                    message: Box::new(msg2.clone()),
                    mutated_after_ack: false,
                    prior_ack_sha: None,
                    send_idem: None,
                })],
            )
            .unwrap();

            // Hyphenated term must match as phrase, not FTS5 NOT operator.
            let hyphen = j.search_mail("auth-v2", None, 10).unwrap();
            assert_eq!(hyphen.len(), 1);
            assert_eq!(hyphen[0].sha256, msg1.sha256.as_str());

            // Seat filter restricts hits to sender/recipient participation.
            let bob_hits = j.search_mail("SQLite", Some("bob"), 10).unwrap();
            assert!(bob_hits.is_empty(), "bob must not see carol->alice mail");
            let alice_hits = j.search_mail("SQLite", Some("alice"), 10).unwrap();
            assert_eq!(alice_hits.len(), 1);
            assert_eq!(alice_hits[0].sha256, msg2.sha256.as_str());

            // Simulate pre-FTS5 legacy rows by clearing mail_fts + state and reopening.
            j.inner
                .conn()
                .execute_batch(
                    "DELETE FROM mail_fts; DELETE FROM mail_fts_meta; DELETE FROM mail_fts_state;",
                )
                .unwrap();
        }

        let reopened = Journal::open(&db_path).unwrap();
        let hits = reopened.search_mail("rotation", None, 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].sha256, msg1.sha256.as_str());
        assert_eq!(reopened.backfill_mail_fts().unwrap(), 0);
    }
}
