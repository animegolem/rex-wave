//! Two-way reconciler between room task-threads and Emacs `.org` files.
//!
//! Honors `ABI-0.md` Clause B-4 (ignoring `.#*` lock symlinks and `#*#`
//! autosaves, deferring writes while `.#<filename>` exists), handles real Emacs
//! Org planning lines (`SCHEDULED:`, `DEADLINE:`, `CLOSED:`) and column-aligned
//! `:PROPERTIES:` drawers, preserves Emacs-supplied `:ID:` strings, unknown
//! drawers, preamble lines, `**` subheadings, and body prose losslessly, and
//! refuses to overwrite stale `.org` edits on `RevisionConflict`.

use rex_kanban::org::{OrgDocument, OrgError, parse_document, render_document};
use rex_kanban::{CardRevision, Lane, LaneName, OrgId, OrgPriority, Priority, TaskCard, Title};
use rex_wave_api::notify::{InboxRaise, NotificationDraft, NotifyKind};
use rex_wave_api::room::{
    Body, RoomId, RoomLabel, RoomPost, RoomRef, RoomScope, RoomTasks, TaskClaim, TaskSpec,
    TaskStatus, TaskTransition, TaskUpdate, TaskView,
};
use rex_wave_api::{
    ApiError, Client, ConflictData, IdemKey, Label, LeaseEpoch, Principal, Seq, UnixMs,
};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

pub const CANONICAL_TODO_HEADER: &str = "#+TODO: IDEA TODO STRT WAIT BLKD | DONE CNCL";

const PROP_REX_SEQ: &str = "REX_SEQ";
const PROP_REX_EPOCH: &str = "REX_EPOCH";
const PROP_REX_LEASE: &str = "REX_LEASE";

/// Mode of org files and conflict sidecars this module writes.
const ORG_FILE_MODE: u32 = 0o644;
/// The largest org file synced; a task board is far smaller.
const MAX_ORG_FILE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug)]
pub enum OrgSyncError {
    Parse(OrgError),
    UnsupportedStatus {
        keyword: String,
        title: String,
    },
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Utf8 {
        path: PathBuf,
        source: std::string::FromUtf8Error,
    },
    Api(ApiError),
}

impl std::fmt::Display for OrgSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrgSyncError::Parse(err) => write!(f, "org parse error: {err}"),
            OrgSyncError::UnsupportedStatus { keyword, title } => {
                write!(f, "invalid heading status `{keyword}` for `{title}`")
            }
            OrgSyncError::Io { path, source } => {
                write!(f, "io error on {}: {source}", path.display())
            }
            OrgSyncError::Utf8 { path, source } => {
                write!(f, "file {} is not valid UTF-8: {source}", path.display())
            }
            OrgSyncError::Api(err) => write!(f, "daemon api error: {err}"),
        }
    }
}

impl std::error::Error for OrgSyncError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OrgSyncError::Parse(err) => Some(err),
            OrgSyncError::Io { source, .. } => Some(source),
            OrgSyncError::Utf8 { source, .. } => Some(source),
            OrgSyncError::Api(err) => Some(err),
            OrgSyncError::UnsupportedStatus { .. } => None,
        }
    }
}

impl From<OrgError> for OrgSyncError {
    fn from(err: OrgError) -> Self {
        OrgSyncError::Parse(err)
    }
}

impl From<ApiError> for OrgSyncError {
    fn from(err: ApiError) -> Self {
        OrgSyncError::Api(err)
    }
}

/// Snapshot of one task's synced state from the previous reconciliation cycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaselineTask {
    pub org_id: OrgId,
    pub seq: Seq,
    pub rev: u64,
    pub title: Title,
    pub status: TaskStatus,
    pub priority: Priority,
    pub due_ms: Option<UnixMs>,
    pub needs: Vec<OrgId>,
    pub labels: Vec<Label>,
}

/// An un-ID'd `.org` heading already created in the room whose minted `:ID:`
/// has not yet been written back to disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCreatedTask {
    pub ordinal: usize,
    pub title: String,
    pub occurrence: usize,
    pub org_id: OrgId,
}

/// Last-synced snapshot used to detect local Emacs edits and diff by `:ID:`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrgSyncState {
    pub last_synced_bytes: Option<Vec<u8>>,
    pub tasks: BTreeMap<OrgId, BaselineTask>,
    pub pending_created: Vec<PendingCreatedTask>,
}

/// Controls conflict sidecar output and optional `NotifyKind::OrgConflict` delivery.
#[derive(Clone, Debug, Default)]
pub struct OrgSyncOptions {
    pub write_conflict_sidecar: bool,
    pub notify_principal: Option<Principal>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrgConflictKind {
    RevisionConflict {
        expected: u64,
        actual: u64,
    },
    LeaseFenced {
        expected: LeaseEpoch,
        actual: LeaseEpoch,
        holder: Option<Principal>,
    },
    UnmetNeeds {
        unmet: Vec<Seq>,
    },
    UnresolvedDependency {
        missing: OrgId,
    },
    DependencyCycle,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrgSyncConflict {
    pub path: PathBuf,
    pub org_id: OrgId,
    pub seq: Option<Seq>,
    pub kind: OrgConflictKind,
    pub detail: String,
}

impl OrgSyncConflict {
    pub fn to_notification_draft(&self) -> NotificationDraft {
        let path_str = self.path.to_string_lossy().into_owned();
        NotificationDraft {
            kind: NotifyKind::OrgConflict {
                path: path_str.clone(),
                org_id: self.org_id.clone(),
                detail: self.detail.clone(),
            },
            title: format!("Org sync conflict on {}", self.org_id),
            preview: format!("{path_str}: {}", self.detail),
            actions: Vec::new(),
            dedupe: format!("org-conflict:{path_str}:{}", self.org_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrgToRoomOutcome {
    IgnoredEphemeral,
    DeferredLock {
        lock_path: PathBuf,
    },
    Synced {
        created: Vec<OrgId>,
        updated: Vec<OrgId>,
        transitioned: Vec<OrgId>,
        labeled: Vec<OrgId>,
        wrote_back: bool,
    },
    Conflict {
        applied: Vec<OrgId>,
        conflicts: Vec<OrgSyncConflict>,
        sidecar_path: Option<PathBuf>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoomToOrgOutcome {
    IgnoredEphemeral,
    DeferredLock { lock_path: PathBuf },
    DeferredLocalEdits,
    Unchanged,
    Written { tasks_synced: usize },
}

/// True when `path` is an Emacs lock symlink (`.#*`) or autosave file (`#*#`).
pub fn is_emacs_lock_or_autosave(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
        return false;
    };
    name.starts_with(".#") || (name.len() >= 2 && name.starts_with('#') && name.ends_with('#'))
}

/// Path of the Emacs Clause B-4 lock symlink `.#<filename>` for `target`.
pub fn emacs_lock_path(target: &Path) -> Option<PathBuf> {
    let name = target.file_name()?.to_str()?;
    let lock_name = format!(".#{name}");
    Some(match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(lock_name),
        _ => PathBuf::from(lock_name),
    })
}

/// True when an Emacs `.#<filename>` lock exists for `target`.
/// Judged without following links, because Emacs lock files are dangling symlinks.
pub fn has_emacs_lock(target: &Path) -> bool {
    emacs_lock_path(target).is_some_and(|lock| rex_wave_fs::entry_exists(&lock))
}

/// Path of the conflict sidecar `<filename>.rex-conflict` alongside `target`.
pub fn conflict_sidecar_path(target: &Path) -> PathBuf {
    let mut os = target.as_os_str().to_os_string();
    os.push(".rex-conflict");
    PathBuf::from(os)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct EmacsOrgDocument {
    doc: OrgDocument,
    /// Org planning lines (`SCHEDULED:`, `DEADLINE:`, `CLOSED:`) per top-level heading,
    /// sitting between the `*` heading and `:PROPERTIES:` in GNU Emacs Org output.
    planning: Vec<Vec<String>>,
}

fn is_top_heading(line: &str) -> bool {
    line == "*" || line.starts_with("* ") || line.starts_with("*\t")
}

fn is_planning_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("SCHEDULED:")
        || trimmed.starts_with("DEADLINE:")
        || trimmed.starts_with("CLOSED:")
}

fn normalize_known_property_line(line: &str) -> Cow<'_, str> {
    let trimmed = line.trim();
    let Some(after_open) = trimmed.strip_prefix(':') else {
        return Cow::Borrowed(line);
    };
    let Some((key, after_close)) = after_open.split_once(':') else {
        return Cow::Borrowed(line);
    };
    let known = matches!(
        key,
        "ID" | "REX_CARD"
            | "CARD"
            | "REX_REV"
            | "REV"
            | "REVISION"
            | "REX_NEEDS"
            | "REX_SEQ"
            | "REX_EPOCH"
            | "REX_LEASE"
            | "ASSIGNEE"
            | "DUE_MS"
            | "PRIORITY"
            | "LINKS"
    );
    if !known {
        return Cow::Borrowed(line);
    }
    let val = after_close.trim();
    if val.is_empty() {
        Cow::Owned(format!(":{key}:"))
    } else {
        Cow::Owned(format!(":{key}: {val}"))
    }
}

/// Parse an Emacs `.org` document, preserving planning lines between a heading
/// and its `:PROPERTIES:` drawer and normalizing Emacs column-aligned property
/// whitespace (`:ID:       <uuid>`) before delegating to `rex_kanban::org`.
fn parse_emacs_document(text: &str) -> Result<EmacsOrgDocument, OrgError> {
    if text.is_empty() {
        return Ok(EmacsOrgDocument::default());
    }
    let trimmed = text.strip_suffix('\n').unwrap_or(text);
    let raw_lines: Vec<&str> = if trimmed.is_empty() && text == "\n" {
        vec![""]
    } else {
        trimmed
            .split('\n')
            .map(|l| l.strip_suffix('\r').unwrap_or(l))
            .collect()
    };

    let mut cleaned_lines: Vec<Cow<'_, str>> = Vec::with_capacity(raw_lines.len());
    let mut planning: Vec<Vec<String>> = Vec::new();
    let mut idx = 0;

    while idx < raw_lines.len() {
        let line = raw_lines[idx];
        if is_top_heading(line) {
            cleaned_lines.push(Cow::Borrowed(line));
            idx += 1;

            let mut task_planning = Vec::new();
            while idx < raw_lines.len() && is_planning_line(raw_lines[idx]) {
                task_planning.push(raw_lines[idx].to_owned());
                idx += 1;
            }
            planning.push(task_planning);

            if idx < raw_lines.len() && raw_lines[idx].trim() == ":PROPERTIES:" {
                cleaned_lines.push(Cow::Borrowed(raw_lines[idx]));
                idx += 1;
                while idx < raw_lines.len() && !is_top_heading(raw_lines[idx]) {
                    let prop_line = raw_lines[idx];
                    if prop_line.trim() == ":END:" {
                        cleaned_lines.push(Cow::Borrowed(prop_line));
                        idx += 1;
                        break;
                    }
                    cleaned_lines.push(normalize_known_property_line(prop_line));
                    idx += 1;
                }
            }
        } else {
            cleaned_lines.push(Cow::Borrowed(line));
            idx += 1;
        }
    }

    let mut cleaned = String::with_capacity(text.len());
    for line in &cleaned_lines {
        cleaned.push_str(line);
        cleaned.push('\n');
    }
    let doc = parse_document(&cleaned)?;
    Ok(EmacsOrgDocument { doc, planning })
}

fn render_emacs_document(edoc: &EmacsOrgDocument) -> String {
    let base = render_document(&edoc.doc);
    if edoc.planning.iter().all(Vec::is_empty) {
        return base;
    }
    let mut out = String::with_capacity(base.len() + 128);
    let mut task_idx = 0usize;
    for line in base.lines() {
        out.push_str(line);
        out.push('\n');
        if is_top_heading(line) {
            if let Some(lines) = edoc.planning.get(task_idx) {
                for pl in lines {
                    out.push_str(pl);
                    out.push('\n');
                }
            }
            task_idx += 1;
        }
    }
    out
}

fn task_status_to_lane(status: TaskStatus) -> LaneName {
    match status {
        TaskStatus::Open => Lane::Todo.lane_name(),
        TaskStatus::Claimed => Lane::Doing.lane_name(),
        TaskStatus::Review => Lane::Review.lane_name(),
        TaskStatus::Blocked => Lane::Blocked.lane_name(),
        TaskStatus::Done => Lane::Done.lane_name(),
        TaskStatus::Cancelled => LaneName::parse("cncl").expect("static cncl lane"),
    }
}

fn task_status_from_lane(lane: &LaneName) -> Option<TaskStatus> {
    match lane.as_str() {
        "idea" | "todo" => Some(TaskStatus::Open),
        "doing" => Some(TaskStatus::Claimed),
        "review" => Some(TaskStatus::Review),
        "blocked" => Some(TaskStatus::Blocked),
        "done" => Some(TaskStatus::Done),
        "cncl" => Some(TaskStatus::Cancelled),
        _ => None,
    }
}

fn room_title_label(room: &RoomRef, resolved: Option<&RoomId>) -> String {
    if let Some(id) = resolved {
        return format_room_id(id);
    }
    match room {
        RoomRef::Id { id } => format_room_id(id),
        RoomRef::Channel {
            scope: RoomScope::Global,
            name,
        } => format!("#{name}"),
        RoomRef::Channel {
            scope: RoomScope::Project { project },
            name,
        } => format!("#proj:{project}/{name}"),
        RoomRef::Dm { with } => format!("dm:{with}"),
    }
}

fn format_room_id(id: &RoomId) -> String {
    match id {
        RoomId::Channel {
            scope: RoomScope::Global,
            name,
        } => format!("#{name}"),
        RoomId::Channel {
            scope: RoomScope::Project { project },
            name,
        } => format!("#proj:{project}/{name}"),
        RoomId::Direct { pair } => {
            let (lo, hi): (Principal, Principal) = pair.clone().into();
            format!("dm:{lo}:{hi}")
        }
    }
}

fn extract_valid_labels(tags: &[String]) -> Vec<Label> {
    tags.iter()
        .filter_map(|t| Label::parse(t.as_str()).ok())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn merge_labels_into_tags(existing_tags: &[String], room_labels: &[Label]) -> Vec<String> {
    let current_valid = extract_valid_labels(existing_tags);
    let target_valid: Vec<Label> = room_labels
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if current_valid == target_valid {
        return existing_tags.to_vec();
    }
    let mut out: Vec<String> = existing_tags
        .iter()
        .filter(|t| Label::parse(t.as_str()).is_err())
        .cloned()
        .collect();
    for lbl in target_valid {
        out.push(lbl.as_str().to_owned());
    }
    out
}

fn upsert_managed_property(
    props: &mut Vec<(String, String)>,
    key: &str,
    value: Option<String>,
    insert_at: usize,
) {
    match value {
        Some(v) => {
            if let Some((_, existing)) = props.iter_mut().find(|(k, _)| k == key) {
                *existing = v;
            } else {
                let pos = insert_at.min(props.len());
                props.insert(pos, (key.to_owned(), v));
            }
        }
        None => {
            props.retain(|(k, _)| k != key);
        }
    }
}

fn managed_prop_count(props: &[(String, String)]) -> usize {
    props
        .iter()
        .take_while(|(k, _)| matches!(k.as_str(), PROP_REX_SEQ | PROP_REX_EPOCH | PROP_REX_LEASE))
        .count()
}

fn apply_task_view_to_card(
    card: &mut TaskCard,
    task: &TaskView,
    seq_to_org_id: &BTreeMap<Seq, OrgId>,
) {
    card.org_id = Some(task.org_id.clone());
    card.revision = Some(CardRevision(task.rev));
    let target_lane = task_status_to_lane(task.status);
    let preserve_idea = task.status == TaskStatus::Open && card.lane.as_str() == "idea";
    if !preserve_idea {
        card.lane = target_lane;
    }
    if card.effective_priority() != task.priority {
        card.priority = OrgPriority::from_priority(task.priority);
        card.blocking = task.priority == Priority::Blocking;
    }
    card.title = task.title.clone();
    card.due_ms = task.due_ms.map(|u| u.0);
    card.needs = task
        .needs
        .iter()
        .filter_map(|dep_seq| seq_to_org_id.get(dep_seq).cloned())
        .collect();
    card.tags = merge_labels_into_tags(&card.tags, &task.labels);

    upsert_managed_property(
        &mut card.properties,
        PROP_REX_SEQ,
        Some(task.seq.0.to_string()),
        0,
    );
    let after_seq = managed_prop_count(&card.properties).min(1);
    match &task.lease {
        Some(lease) if task.status == TaskStatus::Claimed => {
            upsert_managed_property(
                &mut card.properties,
                PROP_REX_EPOCH,
                Some(lease.epoch.0.to_string()),
                after_seq,
            );
            let after_epoch = managed_prop_count(&card.properties).min(2);
            // Omit `expires_at` from :REX_LEASE: so periodic worker heartbeats do not
            // churn the .org file on disk when lease holder and epoch have not changed.
            upsert_managed_property(
                &mut card.properties,
                PROP_REX_LEASE,
                Some(lease.holder.to_string()),
                after_epoch,
            );
        }
        _ => {
            upsert_managed_property(&mut card.properties, PROP_REX_EPOCH, None, after_seq);
            upsert_managed_property(&mut card.properties, PROP_REX_LEASE, None, after_seq);
        }
    }
}

fn card_from_task_view(task: &TaskView, seq_to_org_id: &BTreeMap<Seq, OrgId>) -> TaskCard {
    let mut card = TaskCard {
        org_id: Some(task.org_id.clone()),
        id: None,
        revision: Some(CardRevision(task.rev)),
        lane: task_status_to_lane(task.status),
        priority: OrgPriority::from_priority(task.priority),
        blocking: task.priority == Priority::Blocking,
        title: task.title.clone(),
        tags: Vec::new(),
        assignee: None,
        due_ms: task.due_ms.map(|u| u.0),
        links: Vec::new(),
        needs: Vec::new(),
        properties: Vec::new(),
        body: String::new(),
    };
    apply_task_view_to_card(&mut card, task, seq_to_org_id);
    card
}

/// Surgically project room tasks into an existing `OrgDocument` (preserving
/// preamble lines, unknown `:PROPERTIES:`, `**` subheadings, body prose, and
/// non-label tags) or construct a fresh canonical document when `existing` is `None`.
pub fn project_tasks_into_document(
    existing: Option<&OrgDocument>,
    room_label: &str,
    tasks: &[TaskView],
) -> OrgDocument {
    let seq_to_org_id: BTreeMap<Seq, OrgId> =
        tasks.iter().map(|t| (t.seq, t.org_id.clone())).collect();
    let by_org_id: BTreeMap<&OrgId, &TaskView> = tasks.iter().map(|t| (&t.org_id, t)).collect();

    let mut doc = match existing {
        Some(prev) => prev.clone(),
        None => OrgDocument {
            title: Some(room_label.to_owned()),
            preamble: vec![CANONICAL_TODO_HEADER.to_owned()],
            tasks: Vec::new(),
        },
    };

    let mut seen_org_ids = BTreeSet::new();
    for card in &mut doc.tasks {
        if let Some(oid) = card.org_id.clone()
            && let Some(task) = by_org_id.get(&oid)
        {
            apply_task_view_to_card(card, task, &seq_to_org_id);
            seen_org_ids.insert(oid);
        }
    }

    for task in tasks {
        if !seen_org_ids.contains(&task.org_id) {
            doc.tasks.push(card_from_task_view(task, &seq_to_org_id));
        }
    }

    doc
}

fn project_tasks_into_emacs_document(
    existing: Option<&EmacsOrgDocument>,
    room_label: &str,
    tasks: &[TaskView],
) -> EmacsOrgDocument {
    let doc = project_tasks_into_document(existing.map(|e| &e.doc), room_label, tasks);
    let mut planning = existing.map(|e| e.planning.clone()).unwrap_or_default();
    planning.resize_with(doc.tasks.len(), Vec::new);
    EmacsOrgDocument { doc, planning }
}

fn baseline_entry_from_view(task: &TaskView, seq_to_org_id: &BTreeMap<Seq, OrgId>) -> BaselineTask {
    let needs = task
        .needs
        .iter()
        .filter_map(|dep| seq_to_org_id.get(dep).cloned())
        .collect();
    let mut labels = task.labels.clone();
    labels.sort();
    labels.dedup();
    BaselineTask {
        org_id: task.org_id.clone(),
        seq: task.seq,
        rev: task.rev,
        title: task.title.clone(),
        status: task.status,
        priority: task.priority,
        due_ms: task.due_ms,
        needs,
        labels,
    }
}

fn baseline_from_tasks(tasks: &[TaskView]) -> BTreeMap<OrgId, BaselineTask> {
    let seq_to_org_id: BTreeMap<Seq, OrgId> =
        tasks.iter().map(|t| (t.seq, t.org_id.clone())).collect();
    tasks
        .iter()
        .map(|t| {
            (
                t.org_id.clone(),
                baseline_entry_from_view(t, &seq_to_org_id),
            )
        })
        .collect()
}

fn record_view_in_baseline(
    state: &mut OrgSyncState,
    view: &TaskView,
    seq_to_org_id: &BTreeMap<Seq, OrgId>,
) {
    let mut entry = baseline_entry_from_view(view, seq_to_org_id);
    if let Some(prev) = state.tasks.get(&view.org_id) {
        entry.labels = prev.labels.clone();
    }
    state.tasks.insert(view.org_id.clone(), entry);
}

fn record_claim_in_baseline(
    state: &mut OrgSyncState,
    room_task: &TaskView,
    new_rev: u64,
    seq_to_org_id: &BTreeMap<Seq, OrgId>,
) {
    let entry = state
        .tasks
        .entry(room_task.org_id.clone())
        .or_insert_with(|| baseline_entry_from_view(room_task, seq_to_org_id));
    entry.rev = new_rev;
    entry.status = TaskStatus::Claimed;
}

fn record_labels_in_baseline(
    state: &mut OrgSyncState,
    room_task: &TaskView,
    seq_to_org_id: &BTreeMap<Seq, OrgId>,
    disk_labels: &[Label],
) {
    let entry = state
        .tasks
        .entry(room_task.org_id.clone())
        .or_insert_with(|| baseline_entry_from_view(room_task, seq_to_org_id));
    entry.labels = disk_labels.to_vec();
}

fn write_atomic(target: &Path, bytes: &[u8]) -> Result<(), OrgSyncError> {
    rex_wave_fs::write_atomic_path(target, bytes, ORG_FILE_MODE, false).map_err(|source| {
        OrgSyncError::Io {
            path: target.to_path_buf(),
            source,
        }
    })
}

/// Read an org file (bounded, following user symlinks).
pub fn read_org_file(path: &Path) -> std::io::Result<Vec<u8>> {
    rex_wave_fs::read_input_bounded(path, MAX_ORG_FILE_BYTES)
}

fn write_conflict_sidecar(
    target: &Path,
    conflicts: &[OrgSyncConflict],
) -> Result<PathBuf, OrgSyncError> {
    let sidecar = conflict_sidecar_path(target);
    let mut text = String::new();
    let _ = writeln!(
        text,
        "# rex-wave org sync conflicts for {}",
        target.display()
    );
    for c in conflicts {
        let _ = writeln!(text, "- org_id: {}", c.org_id);
        if let Some(seq) = c.seq {
            let _ = writeln!(text, "  seq: {}", seq.0);
        }
        let _ = writeln!(text, "  kind: {:?}", c.kind);
        let _ = writeln!(text, "  detail: {}", c.detail);
    }
    write_atomic(&sidecar, text.as_bytes())?;
    Ok(sidecar)
}

async fn emit_conflict_notifications<C: Client>(
    client: &C,
    options: &OrgSyncOptions,
    conflicts: &[OrgSyncConflict],
) {
    let Some(to) = &options.notify_principal else {
        return;
    };
    for conflict in conflicts {
        let _ = client
            .call(InboxRaise {
                to: to.clone(),
                draft: conflict.to_notification_draft(),
            })
            .await;
    }
}

async fn fetch_room_tasks<C: Client>(
    client: &C,
    room: &RoomRef,
) -> Result<(Option<RoomId>, Vec<TaskView>), ApiError> {
    match client
        .call(RoomTasks {
            room: room.clone(),
            filter: None,
            limit: None,
        })
        .await
    {
        Ok(list) => Ok((Some(list.room), list.tasks)),
        Err(ApiError::NotFound { .. }) => Ok((None, Vec::new())),
        Err(err) => Err(err),
    }
}

fn map_api_conflict(
    path: &Path,
    org_id: OrgId,
    seq: Option<Seq>,
    err: ApiError,
) -> Result<OrgSyncConflict, OrgSyncError> {
    match err {
        ApiError::Conflict { detail, data } => {
            let kind = match data {
                Some(ConflictData::RevisionConflict { expected, actual }) => {
                    OrgConflictKind::RevisionConflict { expected, actual }
                }
                Some(ConflictData::LeaseFenced {
                    expected,
                    actual,
                    holder,
                }) => OrgConflictKind::LeaseFenced {
                    expected,
                    actual,
                    holder,
                },
                Some(ConflictData::UnmetNeeds { unmet }) => OrgConflictKind::UnmetNeeds { unmet },
                _ => OrgConflictKind::Other,
            };
            Ok(OrgSyncConflict {
                path: path.to_path_buf(),
                org_id,
                seq,
                kind,
                detail,
            })
        }
        ApiError::Invalid(inv) if inv.what == "needs" || inv.what == "needs_add" => {
            let kind = if inv.reason.contains("cycle") || inv.reason.contains("itself") {
                OrgConflictKind::DependencyCycle
            } else {
                OrgConflictKind::Other
            };
            Ok(OrgSyncConflict {
                path: path.to_path_buf(),
                org_id,
                seq,
                kind,
                detail: inv.to_string(),
            })
        }
        other => Err(OrgSyncError::Api(other)),
    }
}

fn synthetic_conflict_org_id(idx: usize, title: &Title) -> OrgId {
    OrgId::mint_deterministic(&format!("rex-wave:org-conflict:{idx}:{}", title.as_str()))
}

fn org_dep_has_cycle(start: &OrgId, graph: &BTreeMap<OrgId, Vec<OrgId>>) -> bool {
    let mut visited = BTreeSet::new();
    let mut stack: Vec<&OrgId> = graph
        .get(start)
        .map(|deps| deps.iter().collect())
        .unwrap_or_default();
    while let Some(node) = stack.pop() {
        if node == start {
            return true;
        }
        if !visited.insert(node) {
            continue;
        }
        if let Some(deps) = graph.get(node) {
            stack.extend(deps.iter());
        }
    }
    false
}

struct ParsedCardInfo {
    idx: usize,
    status: TaskStatus,
    priority: Priority,
    due_ms: Option<UnixMs>,
    labels: Vec<Label>,
}

fn parse_cards_metadata(doc: &OrgDocument) -> Result<Vec<ParsedCardInfo>, OrgSyncError> {
    let mut infos = Vec::with_capacity(doc.tasks.len());
    for (idx, card) in doc.tasks.iter().enumerate() {
        let Some(status) = task_status_from_lane(&card.lane) else {
            return Err(OrgSyncError::UnsupportedStatus {
                keyword: card.lane.as_str().to_owned(),
                title: card.title.as_str().to_owned(),
            });
        };
        infos.push(ParsedCardInfo {
            idx,
            status,
            priority: card.effective_priority(),
            due_ms: card.due_ms.map(UnixMs),
            labels: extract_valid_labels(&card.tags),
        });
    }
    Ok(infos)
}

fn card_differs_from_baseline(card: &TaskCard, info: &ParsedCardInfo, base: &BaselineTask) -> bool {
    card.title != base.title
        || info.status != base.status
        || info.priority != base.priority
        || info.due_ms != base.due_ms
        || card.needs != base.needs
        || info.labels != base.labels
}

fn card_differs_from_room(
    card: &TaskCard,
    info: &ParsedCardInfo,
    room_task: &TaskView,
    seq_to_org_id: &BTreeMap<Seq, OrgId>,
) -> bool {
    let room_needs: Vec<OrgId> = room_task
        .needs
        .iter()
        .filter_map(|s| seq_to_org_id.get(s).cloned())
        .collect();
    let mut room_labels = room_task.labels.clone();
    room_labels.sort();
    room_labels.dedup();
    card.title != room_task.title
        || info.status != room_task.status
        || info.priority != room_task.priority
        || info.due_ms != room_task.due_ms
        || card.needs != room_needs
        || info.labels != room_labels
}

fn expected_rev_for_card(card: &TaskCard, oid: &OrgId, state: &OrgSyncState) -> u64 {
    state
        .tasks
        .get(oid)
        .map(|b| b.rev)
        .or_else(|| card.revision.map(|r| r.0))
        .unwrap_or(0)
}

/// Reconcile local Emacs `.org` edits into the room's task threads.
///
/// - Ignores `.#*` lockfiles and `#*#` autosaves, and defers when `.#<file>` exists.
/// - Newly added headings mint room task-threads while preserving any Emacs-supplied `:ID:`.
/// - Modified headings submit optimistic-CAS commands carrying the expected revision
///   (tracked baseline rev when present, otherwise parsed `:REX_REV:` from the file).
/// - On any conflict (`RevisionConflict`, `LeaseFenced`, `UnmetNeeds`, cycle, missing dep),
///   leaves the `.org` file untouched and reports structured [`OrgSyncConflict`]s.
pub async fn sync_org_to_room<C: Client>(
    client: &C,
    room: &RoomRef,
    path: &Path,
    state: &mut OrgSyncState,
    options: &OrgSyncOptions,
) -> Result<OrgToRoomOutcome, OrgSyncError> {
    if is_emacs_lock_or_autosave(path) {
        return Ok(OrgToRoomOutcome::IgnoredEphemeral);
    }
    if let Some(lock_path) = emacs_lock_path(path)
        && rex_wave_fs::entry_exists(&lock_path)
    {
        return Ok(OrgToRoomOutcome::DeferredLock { lock_path });
    }

    let initial_bytes = read_org_file(path).map_err(|source| OrgSyncError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let text = String::from_utf8(initial_bytes.clone()).map_err(|source| OrgSyncError::Utf8 {
        path: path.to_path_buf(),
        source,
    })?;
    let mut edoc = parse_emacs_document(&text)?;
    let infos = parse_cards_metadata(&edoc.doc)?;

    let (resolved_room, room_tasks) = fetch_room_tasks(client, room).await?;
    let mut seq_to_org_id: BTreeMap<Seq, OrgId> = room_tasks
        .iter()
        .map(|t| (t.seq, t.org_id.clone()))
        .collect();
    let room_by_org_id: BTreeMap<OrgId, TaskView> = room_tasks
        .iter()
        .map(|t| (t.org_id.clone(), t.clone()))
        .collect();

    // Re-attach minted OrgIds from prior creations whose write-back was skipped
    // (due to a conflict or lock race) so retries never post duplicate tasks.
    let mut unid_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut unid_keys: BTreeMap<usize, (String, usize)> = BTreeMap::new();
    let mut matched_pending_slots: BTreeSet<usize> = BTreeSet::new();
    let mut unmatched_unid: Vec<(usize, (String, usize))> = Vec::new();
    for (idx, card) in edoc.doc.tasks.iter_mut().enumerate() {
        if card.org_id.is_none() {
            let title = card.title.as_str().to_owned();
            let occ = {
                let count = unid_counts.entry(title.clone()).or_insert(0);
                let cur = *count;
                *count += 1;
                cur
            };
            if let Some((slot, entry)) =
                state
                    .pending_created
                    .iter()
                    .enumerate()
                    .find(|(slot, entry)| {
                        !matched_pending_slots.contains(slot)
                            && entry.title == title
                            && entry.occurrence == occ
                            && room_by_org_id.contains_key(&entry.org_id)
                    })
            {
                card.org_id = Some(entry.org_id.clone());
                matched_pending_slots.insert(slot);
            } else {
                unmatched_unid.push((idx, (title, occ)));
            }
        }
    }
    let mut leftover_slots: Vec<usize> = state
        .pending_created
        .iter()
        .enumerate()
        .filter_map(|(slot, entry)| {
            (!matched_pending_slots.contains(&slot) && room_by_org_id.contains_key(&entry.org_id))
                .then_some(slot)
        })
        .collect();
    leftover_slots.sort_by_key(|&slot| state.pending_created[slot].ordinal);
    let mut leftover_iter = leftover_slots.into_iter();
    for (idx, (new_title, new_occ)) in unmatched_unid {
        if let Some(slot) = leftover_iter.next() {
            let entry = &mut state.pending_created[slot];
            edoc.doc.tasks[idx].org_id = Some(entry.org_id.clone());
            entry.ordinal = idx;
            entry.title = new_title;
            entry.occurrence = new_occ;
        } else {
            unid_keys.insert(idx, (new_title, new_occ));
        }
    }

    let mut new_indices = Vec::new();
    let mut new_org_id_to_idx: BTreeMap<OrgId, usize> = BTreeMap::new();
    let mut modified_indices = Vec::new();
    let mut conflicts = Vec::new();

    for info in &infos {
        let card = &edoc.doc.tasks[info.idx];
        match &card.org_id {
            Some(oid) if room_by_org_id.contains_key(oid) => {
                let room_task = &room_by_org_id[oid];
                let differs_room = card_differs_from_room(card, info, room_task, &seq_to_org_id);
                let edited = differs_room
                    && state
                        .tasks
                        .get(oid)
                        .is_none_or(|base| card_differs_from_baseline(card, info, base));
                if edited {
                    modified_indices.push(info.idx);
                }
            }
            Some(oid) if state.tasks.contains_key(oid) => {
                conflicts.push(OrgSyncConflict {
                    path: path.to_path_buf(),
                    org_id: oid.clone(),
                    seq: state.tasks.get(oid).map(|b| b.seq),
                    kind: OrgConflictKind::Other,
                    detail: format!("task `{oid}` exists in sync baseline but not in room"),
                });
            }
            Some(oid) => {
                new_org_id_to_idx.insert(oid.clone(), info.idx);
                new_indices.push(info.idx);
            }
            None => {
                new_indices.push(info.idx);
            }
        }
    }

    let mut in_degree: BTreeMap<usize, usize> = new_indices.iter().map(|&i| (i, 0)).collect();
    let mut dependents: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for &idx in &new_indices {
        let card = &edoc.doc.tasks[idx];
        let mut seen_new_deps = BTreeSet::new();
        for dep in &card.needs {
            if room_by_org_id.contains_key(dep) {
                continue;
            }
            if let Some(&dep_idx) = new_org_id_to_idx.get(dep) {
                if dep_idx == idx {
                    conflicts.push(OrgSyncConflict {
                        path: path.to_path_buf(),
                        org_id: card
                            .org_id
                            .clone()
                            .unwrap_or_else(|| synthetic_conflict_org_id(idx, &card.title)),
                        seq: None,
                        kind: OrgConflictKind::DependencyCycle,
                        detail: format!("task `{}` cannot depend on itself", card.title),
                    });
                } else if seen_new_deps.insert(dep_idx) {
                    *in_degree.get_mut(&idx).expect("indexed") += 1;
                    dependents.entry(dep_idx).or_default().push(idx);
                }
            } else {
                conflicts.push(OrgSyncConflict {
                    path: path.to_path_buf(),
                    org_id: card
                        .org_id
                        .clone()
                        .unwrap_or_else(|| synthetic_conflict_org_id(idx, &card.title)),
                    seq: None,
                    kind: OrgConflictKind::UnresolvedDependency {
                        missing: dep.clone(),
                    },
                    detail: format!("unknown :REX_NEEDS: dependency `{dep}`"),
                });
            }
        }
    }

    let mut ready_queue: BTreeSet<usize> = in_degree
        .iter()
        .filter_map(|(&idx, &deg)| (deg == 0).then_some(idx))
        .collect();
    let mut ordered_new = Vec::with_capacity(new_indices.len());
    while let Some(idx) = ready_queue.pop_first() {
        ordered_new.push(idx);
        if let Some(children) = dependents.get(&idx) {
            for &child in children {
                let deg = in_degree.get_mut(&child).expect("child indexed");
                *deg -= 1;
                if *deg == 0 {
                    ready_queue.insert(child);
                }
            }
        }
    }
    if ordered_new.len() < new_indices.len() {
        for &idx in &new_indices {
            if !ordered_new.contains(&idx) {
                let card = &edoc.doc.tasks[idx];
                conflicts.push(OrgSyncConflict {
                    path: path.to_path_buf(),
                    org_id: card
                        .org_id
                        .clone()
                        .unwrap_or_else(|| synthetic_conflict_org_id(idx, &card.title)),
                    seq: None,
                    kind: OrgConflictKind::DependencyCycle,
                    detail: format!(
                        "dependency cycle among new org tasks involving `{}`",
                        card.title
                    ),
                });
            }
        }
    }

    for &idx in &modified_indices {
        let card = &edoc.doc.tasks[idx];
        let oid = card.org_id.as_ref().expect("modified task has org_id");
        let room_task = &room_by_org_id[oid];
        let expected_rev = expected_rev_for_card(card, oid, state);
        if expected_rev != room_task.rev {
            conflicts.push(OrgSyncConflict {
                path: path.to_path_buf(),
                org_id: oid.clone(),
                seq: Some(room_task.seq),
                kind: OrgConflictKind::RevisionConflict {
                    expected: expected_rev,
                    actual: room_task.rev,
                },
                detail: format!(
                    "stale expected rev {expected_rev} for `{oid}` (room is at rev {})",
                    room_task.rev
                ),
            });
            continue;
        }
        for dep in &card.needs {
            if !room_by_org_id.contains_key(dep) && !new_org_id_to_idx.contains_key(dep) {
                conflicts.push(OrgSyncConflict {
                    path: path.to_path_buf(),
                    org_id: oid.clone(),
                    seq: Some(room_task.seq),
                    kind: OrgConflictKind::UnresolvedDependency {
                        missing: dep.clone(),
                    },
                    detail: format!("unknown :REX_NEEDS: dependency `{dep}`"),
                });
            }
        }
    }

    // Preflight cycle detection across the combined post-edit dependency graph
    // (unmodified room tasks + edited existing tasks + new tasks) so cycles never
    // leave half-applied mutations.
    if conflicts.is_empty() && !modified_indices.is_empty() {
        let mut combined_needs: BTreeMap<OrgId, Vec<OrgId>> = room_tasks
            .iter()
            .map(|t| {
                let deps = t
                    .needs
                    .iter()
                    .filter_map(|s| seq_to_org_id.get(s).cloned())
                    .collect();
                (t.org_id.clone(), deps)
            })
            .collect();
        for &idx in &new_indices {
            if let Some(oid) = &edoc.doc.tasks[idx].org_id {
                combined_needs.insert(oid.clone(), edoc.doc.tasks[idx].needs.clone());
            }
        }
        for &idx in &modified_indices {
            let oid = edoc.doc.tasks[idx]
                .org_id
                .as_ref()
                .expect("modified task has org_id");
            combined_needs.insert(oid.clone(), edoc.doc.tasks[idx].needs.clone());
        }
        for &idx in &modified_indices {
            let oid = edoc.doc.tasks[idx]
                .org_id
                .as_ref()
                .expect("modified task has org_id");
            if org_dep_has_cycle(oid, &combined_needs) {
                conflicts.push(OrgSyncConflict {
                    path: path.to_path_buf(),
                    org_id: oid.clone(),
                    seq: Some(room_by_org_id[oid].seq),
                    kind: OrgConflictKind::DependencyCycle,
                    detail: format!("dependency cycle detected involving `{oid}`"),
                });
            }
        }
    }

    if !conflicts.is_empty() {
        let sidecar_path = if options.write_conflict_sidecar {
            Some(write_conflict_sidecar(path, &conflicts)?)
        } else {
            None
        };
        emit_conflict_notifications(client, options, &conflicts).await;
        return Ok(OrgToRoomOutcome::Conflict {
            applied: Vec::new(),
            conflicts,
            sidecar_path,
        });
    }

    let mut org_id_to_seq: BTreeMap<OrgId, Seq> = room_tasks
        .iter()
        .map(|t| (t.org_id.clone(), t.seq))
        .collect();
    let mut created = Vec::new();
    let mut updated = Vec::new();
    let mut transitioned = Vec::new();
    let mut labeled = Vec::new();
    let mut applied = Vec::new();

    for idx in ordered_new {
        let info = &infos[idx];
        let card = &edoc.doc.tasks[idx];
        let needs: Vec<Seq> = card
            .needs
            .iter()
            .filter_map(|dep| org_id_to_seq.get(dep).copied())
            .collect();
        let body_text = if card.body.trim().is_empty() {
            card.title.as_str()
        } else {
            card.body.as_str()
        };
        let body = Body::parse(body_text).unwrap_or_else(|_| {
            Body::parse(card.title.as_str()).expect("non-empty title fits Body")
        });
        let create_idem = card
            .org_id
            .as_ref()
            .and_then(|oid| IdemKey::parse(format!("org-create:{oid}")).ok());
        let spec = TaskSpec {
            title: card.title.clone(),
            org_id: card.org_id.clone(),
            needs,
            priority: Some(info.priority),
            due_ms: info.due_ms,
        };
        match client
            .call(RoomPost {
                room: room.clone(),
                body,
                re: None,
                after: None,
                labels: info.labels.clone(),
                task: Some(spec),
                idem: create_idem,
            })
            .await
        {
            Ok(posted) => {
                let oid = posted
                    .org_id
                    .or_else(|| card.org_id.clone())
                    .expect("room task post returns org_id");
                org_id_to_seq.insert(oid.clone(), posted.seq);
                seq_to_org_id.insert(posted.seq, oid.clone());
                if let Some((title, occ)) = unid_keys.get(&idx) {
                    state.pending_created.push(PendingCreatedTask {
                        ordinal: idx,
                        title: title.clone(),
                        occurrence: *occ,
                        org_id: oid.clone(),
                    });
                }
                state.tasks.insert(
                    oid.clone(),
                    BaselineTask {
                        org_id: oid.clone(),
                        seq: posted.seq,
                        rev: 1,
                        title: card.title.clone(),
                        status: TaskStatus::Open,
                        priority: info.priority,
                        due_ms: info.due_ms,
                        needs: card.needs.clone(),
                        labels: info.labels.clone(),
                    },
                );
                edoc.doc.tasks[idx].org_id = Some(oid.clone());
                created.push(oid.clone());
                applied.push(oid.clone());

                if info.status != TaskStatus::Open {
                    let res = if info.status == TaskStatus::Claimed {
                        client
                            .call(TaskClaim {
                                room: room.clone(),
                                task: posted.seq,
                                ttl_ms: None,
                                expected_rev: Some(1),
                                force: false,
                                idem: None,
                            })
                            .await
                            .map(|r| (r.rev, TaskStatus::Claimed))
                    } else {
                        client
                            .call(TaskTransition {
                                room: room.clone(),
                                task: posted.seq,
                                status: info.status,
                                epoch: None,
                                expected_rev: Some(1),
                                note: None,
                                idem: None,
                            })
                            .await
                            .map(|v| (v.rev, v.status))
                    };
                    match res {
                        Ok((new_rev, new_status)) => {
                            if let Some(base) = state.tasks.get_mut(&oid) {
                                base.rev = new_rev;
                                base.status = new_status;
                            }
                            transitioned.push(oid);
                        }
                        Err(err) => {
                            conflicts.push(map_api_conflict(path, oid, Some(posted.seq), err)?);
                        }
                    }
                }
            }
            Err(err) => {
                let oid = card
                    .org_id
                    .clone()
                    .unwrap_or_else(|| synthetic_conflict_org_id(idx, &card.title));
                conflicts.push(map_api_conflict(path, oid, None, err)?);
            }
        }
    }

    for idx in modified_indices {
        let info = &infos[idx];
        let card = &edoc.doc.tasks[idx];
        let oid = card.org_id.clone().expect("modified task has org_id");
        let room_task = &room_by_org_id[&oid];
        let mut expected_rev = expected_rev_for_card(card, &oid, state);

        let target_needs: Vec<Seq> = card
            .needs
            .iter()
            .filter_map(|dep| org_id_to_seq.get(dep).copied())
            .collect();
        let needs_add: Vec<Seq> = target_needs
            .iter()
            .copied()
            .filter(|d| !room_task.needs.contains(d))
            .collect();
        let needs_remove: Vec<Seq> = room_task
            .needs
            .iter()
            .copied()
            .filter(|d| !target_needs.contains(d))
            .collect();
        let title_patch = (card.title != room_task.title).then(|| card.title.clone());
        let priority_patch = (info.priority != room_task.priority).then_some(info.priority);
        let clear_due = room_task.due_ms.is_some() && info.due_ms.is_none();
        let due_patch = (info.due_ms.is_some() && info.due_ms != room_task.due_ms)
            .then_some(info.due_ms)
            .flatten();

        let mut task_failed = false;
        if title_patch.is_some()
            || priority_patch.is_some()
            || clear_due
            || due_patch.is_some()
            || !needs_add.is_empty()
            || !needs_remove.is_empty()
        {
            match client
                .call(TaskUpdate {
                    room: room.clone(),
                    task: room_task.seq,
                    expected_rev: Some(expected_rev),
                    title: title_patch,
                    needs_add,
                    needs_remove,
                    priority: priority_patch,
                    due_ms: due_patch,
                    clear_due,
                    idem: None,
                })
                .await
            {
                Ok(view) => {
                    expected_rev = view.rev;
                    record_view_in_baseline(state, &view, &seq_to_org_id);
                    updated.push(oid.clone());
                    applied.push(oid.clone());
                }
                Err(err) => {
                    conflicts.push(map_api_conflict(
                        path,
                        oid.clone(),
                        Some(room_task.seq),
                        err,
                    )?);
                    task_failed = true;
                }
            }
        }

        if !task_failed && info.status != room_task.status {
            if info.status == TaskStatus::Claimed {
                match client
                    .call(TaskClaim {
                        room: room.clone(),
                        task: room_task.seq,
                        ttl_ms: None,
                        expected_rev: Some(expected_rev),
                        force: false,
                        idem: None,
                    })
                    .await
                {
                    Ok(receipt) => {
                        record_claim_in_baseline(state, room_task, receipt.rev, &seq_to_org_id);
                        transitioned.push(oid.clone());
                        if !applied.contains(&oid) {
                            applied.push(oid.clone());
                        }
                    }
                    Err(err) => {
                        conflicts.push(map_api_conflict(
                            path,
                            oid.clone(),
                            Some(room_task.seq),
                            err,
                        )?);
                        task_failed = true;
                    }
                }
            } else {
                match client
                    .call(TaskTransition {
                        room: room.clone(),
                        task: room_task.seq,
                        status: info.status,
                        epoch: None,
                        expected_rev: Some(expected_rev),
                        note: None,
                        idem: None,
                    })
                    .await
                {
                    Ok(view) => {
                        record_view_in_baseline(state, &view, &seq_to_org_id);
                        transitioned.push(oid.clone());
                        if !applied.contains(&oid) {
                            applied.push(oid.clone());
                        }
                    }
                    Err(err) => {
                        conflicts.push(map_api_conflict(
                            path,
                            oid.clone(),
                            Some(room_task.seq),
                            err,
                        )?);
                        task_failed = true;
                    }
                }
            }
        }

        if !task_failed {
            // Diff labels against the previous baseline rather than live room labels
            // so concurrent RoomLabel additions (which do not bump rev) are never wiped.
            let base_labels: BTreeSet<Label> = state
                .tasks
                .get(&oid)
                .map(|b| b.labels.iter().cloned().collect())
                .unwrap_or_else(|| room_task.labels.iter().cloned().collect());
            let org_labels: BTreeSet<Label> = info.labels.iter().cloned().collect();
            let add: Vec<Label> = org_labels.difference(&base_labels).cloned().collect();
            let remove: Vec<Label> = base_labels.difference(&org_labels).cloned().collect();
            if !add.is_empty() || !remove.is_empty() {
                client
                    .call(RoomLabel {
                        room: room.clone(),
                        seq: room_task.seq,
                        add,
                        remove,
                        idem: None,
                    })
                    .await?;
                record_labels_in_baseline(state, room_task, &seq_to_org_id, &info.labels);
                labeled.push(oid.clone());
                if !applied.contains(&oid) {
                    applied.push(oid);
                }
            }
        }
    }

    if !conflicts.is_empty() {
        let sidecar_path = if options.write_conflict_sidecar {
            Some(write_conflict_sidecar(path, &conflicts)?)
        } else {
            None
        };
        emit_conflict_notifications(client, options, &conflicts).await;
        return Ok(OrgToRoomOutcome::Conflict {
            applied,
            conflicts,
            sidecar_path,
        });
    }

    let (fresh_room_id, fresh_tasks) = fetch_room_tasks(client, room).await?;
    let label = room_title_label(room, fresh_room_id.as_ref().or(resolved_room.as_ref()));
    let projected = project_tasks_into_emacs_document(Some(&edoc), &label, &fresh_tasks);

    let wrote_back = if projected != edoc {
        let rendered = render_emacs_document(&projected).into_bytes();
        let still_clean = !has_emacs_lock(path)
            && read_org_file(path)
                .map(|cur| cur == initial_bytes)
                .unwrap_or(false);
        if still_clean {
            write_atomic(path, &rendered)?;
            state.last_synced_bytes = Some(rendered);
            state.tasks = baseline_from_tasks(&fresh_tasks);
            state.pending_created.clear();
            true
        } else {
            false
        }
    } else {
        state.last_synced_bytes = Some(initial_bytes);
        state.tasks = baseline_from_tasks(&fresh_tasks);
        state.pending_created.clear();
        false
    };

    Ok(OrgToRoomOutcome::Synced {
        created,
        updated,
        transitioned,
        labeled,
        wrote_back,
    })
}

/// Project room task-threads back to an Emacs `.org` file.
///
/// Only writes when on-disk bytes match `state.last_synced_bytes` (or the file
/// does not yet exist on an initial export) and no `.#<filename>` lock exists.
/// Preserves unknown drawers, preamble lines, planning lines, `**` subheadings,
/// and body prose.
pub async fn sync_room_to_org<C: Client>(
    client: &C,
    room: &RoomRef,
    path: &Path,
    state: &mut OrgSyncState,
) -> Result<RoomToOrgOutcome, OrgSyncError> {
    if is_emacs_lock_or_autosave(path) {
        return Ok(RoomToOrgOutcome::IgnoredEphemeral);
    }
    if let Some(lock_path) = emacs_lock_path(path)
        && rex_wave_fs::entry_exists(&lock_path)
    {
        return Ok(RoomToOrgOutcome::DeferredLock { lock_path });
    }

    let (existing_bytes, existing_doc) = match read_org_file(path) {
        Ok(bytes) => {
            if state.last_synced_bytes.as_deref() != Some(bytes.as_slice()) {
                return Ok(RoomToOrgOutcome::DeferredLocalEdits);
            }
            let text = String::from_utf8(bytes.clone()).map_err(|source| OrgSyncError::Utf8 {
                path: path.to_path_buf(),
                source,
            })?;
            let edoc = parse_emacs_document(&text)?;
            (Some(bytes), Some(edoc))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if state.last_synced_bytes.is_some() {
                return Ok(RoomToOrgOutcome::DeferredLocalEdits);
            }
            (None, None)
        }
        Err(source) => {
            return Err(OrgSyncError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    let (resolved_room, room_tasks) = fetch_room_tasks(client, room).await?;
    let label = room_title_label(room, resolved_room.as_ref());
    let projected = project_tasks_into_emacs_document(existing_doc.as_ref(), &label, &room_tasks);
    let rendered = render_emacs_document(&projected).into_bytes();

    if existing_bytes.as_deref() == Some(rendered.as_slice()) {
        state.tasks = baseline_from_tasks(&room_tasks);
        return Ok(RoomToOrgOutcome::Unchanged);
    }

    if let Some(lock_path) = emacs_lock_path(path)
        && rex_wave_fs::entry_exists(&lock_path)
    {
        return Ok(RoomToOrgOutcome::DeferredLock { lock_path });
    }
    if let Some(prev_bytes) = &existing_bytes {
        let current = read_org_file(path).map_err(|source| OrgSyncError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if current != *prev_bytes {
            return Ok(RoomToOrgOutcome::DeferredLocalEdits);
        }
    }

    write_atomic(path, &rendered)?;
    state.last_synced_bytes = Some(rendered);
    state.tasks = baseline_from_tasks(&room_tasks);
    state.pending_created.clear();
    Ok(RoomToOrgOutcome::Written {
        tasks_synced: room_tasks.len(),
    })
}
