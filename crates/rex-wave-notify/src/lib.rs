//! # rex-wave-notify
//!
//! Per-principal [`Inbox`] decider, pure [`classify`] and [`rank`] notification
//! policy, and the [`Herald`] reactor.

use rex_wave_api::notify::*;
use rex_wave_api::*;
use rex_wave_runtime::{DecideCtx, Decider, EntryMeta, LocalClient, Reactor};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;

const DECAY_STEP_MS: u64 = 60_000;
const MAX_DECAY: u16 = 900;

/// Reconnect grace window (`15 s`) before raising `MailUnrung` after a
/// reconnectable seat binding drops (`0 s` when the seat was never bound).
pub const RECONNECT_GRACE_MS: u64 = 15_000;

/// Stable lowercase transport slug used in `TransportDown` cards and dedupe keys.
pub fn transport_kind_slug(kind: presence::TransportKind) -> &'static str {
    match kind {
        presence::TransportKind::UdsSession => "uds_session",
        presence::TransportKind::ClaudeSession => "claude_session",
        presence::TransportKind::BbThread => "bb_thread",
        presence::TransportKind::T3Thread => "t3_thread",
        presence::TransportKind::SelfWatcher => "self_watcher",
        presence::TransportKind::OperatorToast => "operator_toast",
    }
}

/// Returns `true` when `binding` uses a reconnectable socket or bridge transport
/// that qualifies for the [`RECONNECT_GRACE_MS`] grace window after a drop.
pub fn is_reconnectable_binding(binding: &presence::SeatBinding) -> bool {
    matches!(binding.liveness, presence::LivenessMode::SocketLifetime)
        || matches!(
            binding.endpoint,
            presence::TransportEndpoint::UdsSession { .. }
                | presence::TransportEndpoint::BbThread { .. }
                | presence::TransportEndpoint::T3Thread { .. }
        )
}

/// Builds the single per-seat informational `MailUnrung` Operator status card.
/// Phrased strictly as a status notice with zero decision prompts or buttons.
pub fn mail_unrung_draft(
    seat: &SeatName,
    unread_count: u32,
    oldest_at: UnixMs,
) -> NotificationDraft {
    let dedupe = format!("mail-unrung:{seat}");
    if unread_count == 0 {
        return mail_unrung_resolved_draft(seat);
    }
    let noun = if unread_count == 1 {
        "letter"
    } else {
        "letters"
    };
    let oldest_iso = oldest_at.to_rfc3339_sec();
    NotificationDraft {
        kind: NotifyKind::MailUnrung {
            seat: seat.clone(),
            unread_count,
            oldest_at,
        },
        title: format!("@{seat} has {unread_count} unrung {noun}"),
        preview: format!(
            "Banked with no live binding for @{seat}; will ring on rebind (oldest banked at {oldest_iso})."
        ),
        actions: Vec::new(),
        dedupe,
    }
}

/// Builds the resolution draft that auto-marks an existing `mail-unrung:<seat>`
/// card `Done` once the seat rebinds and ACKs pending digests (or becomes non-Active).
pub fn mail_unrung_resolved_draft(seat: &SeatName) -> NotificationDraft {
    NotificationDraft {
        kind: NotifyKind::MailUnrung {
            seat: seat.clone(),
            unread_count: 0,
            oldest_at: UnixMs(0),
        },
        title: format!("@{seat} unrung mail cleared"),
        preview: format!("Pending mail for @{seat} has been acknowledged."),
        actions: Vec::new(),
        dedupe: format!("mail-unrung:{seat}"),
    }
}

/// Builds the deduped informational `TransportDown` Operator status card.
pub fn transport_down_draft(
    transport: presence::TransportKind,
    detail: Option<&str>,
) -> NotificationDraft {
    let slug = transport_kind_slug(transport);
    let trimmed = detail
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("provider health check failed");
    NotificationDraft {
        kind: NotifyKind::TransportDown {
            transport: slug.to_owned(),
            detail: trimmed.to_owned(),
        },
        title: format!("Transport {slug} offline"),
        preview: format!(
            "Status notice: {slug} bindings are paused until recovery ({}).",
            snippet(trimmed, 120)
        ),
        actions: Vec::new(),
        dedupe: format!("transport-down:{slug}"),
    }
}

/// Builds the recovery draft that auto-marks an existing `transport-down:<kind>` card `Done`.
pub fn transport_recovered_draft(transport: presence::TransportKind) -> NotificationDraft {
    let slug = transport_kind_slug(transport);
    NotificationDraft {
        kind: NotifyKind::TransportDown {
            transport: slug.to_owned(),
            detail: String::new(),
        },
        title: format!("Transport {slug} recovered"),
        preview: format!("Status notice: {slug} transport health is restored."),
        actions: Vec::new(),
        dedupe: format!("transport-down:{slug}"),
    }
}

/// Builds a deduped informational `OrgConflict` Operator status card.
pub fn org_conflict_draft(
    path: impl Into<String>,
    org_id: OrgId,
    detail: impl Into<String>,
) -> NotificationDraft {
    let path = path.into();
    let raw_detail = detail.into();
    let trimmed = raw_detail.trim();
    let effective_detail = if trimmed.is_empty() {
        "stale :REX_REV: revision during two-way .org sync".to_owned()
    } else {
        trimmed.to_owned()
    };
    let dedupe = format!("org-conflict:{path}:{org_id}");
    NotificationDraft {
        kind: NotifyKind::OrgConflict {
            path: path.clone(),
            org_id: org_id.clone(),
            detail: effective_detail.clone(),
        },
        title: format!("Org revision conflict on {org_id}"),
        preview: format!(
            "Status notice: write-back deferred for {path} ({}).",
            snippet(&effective_detail, 120)
        ),
        actions: Vec::new(),
        dedupe,
    }
}

/// Builds the resolution draft that auto-marks an existing `org-conflict:<path>:<org_id>` card `Done`.
pub fn org_conflict_resolved_draft(path: impl Into<String>, org_id: OrgId) -> NotificationDraft {
    let path = path.into();
    let dedupe = format!("org-conflict:{path}:{org_id}");
    NotificationDraft {
        kind: NotifyKind::OrgConflict {
            path: path.clone(),
            org_id: org_id.clone(),
            detail: String::new(),
        },
        title: format!("Org revision conflict cleared on {org_id}"),
        preview: format!("Status notice: {path} reconciled cleanly."),
        actions: Vec::new(),
        dedupe,
    }
}

fn snippet(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    let total_chars = trimmed.chars().count();
    if total_chars <= max_chars {
        return trimmed
            .chars()
            .map(|ch| if ch == '\n' || ch == '\r' { ' ' } else { ch })
            .collect();
    }
    let mut out: String = trimmed
        .chars()
        .take(max_chars)
        .map(|ch| if ch == '\n' || ch == '\r' { ' ' } else { ch })
        .collect();
    out.push_str(&format!("… [{max_chars} of {total_chars} chars shown]"));
    out
}

fn room_slug(room: &room::RoomId) -> String {
    match room {
        room::RoomId::Channel {
            scope: room::RoomScope::Global,
            name,
        } => format!("global:{name}"),
        room::RoomId::Channel {
            scope: room::RoomScope::Project { project },
            name,
        } => format!("project:{project}:{name}"),
        room::RoomId::Direct { pair } => {
            let (a, b): (Principal, Principal) = pair.clone().into();
            format!("dm:{a}:{b}")
        }
    }
}

fn room_label(room: &room::RoomId) -> String {
    match room {
        room::RoomId::Channel {
            scope: room::RoomScope::Global,
            name,
        } => format!("#{name}"),
        room::RoomId::Channel {
            scope: room::RoomScope::Project { project },
            name,
        } => format!("#{project}/{name}"),
        room::RoomId::Direct { pair } => {
            let (a, b): (Principal, Principal) = pair.clone().into();
            format!("DM({a},{b})")
        }
    }
}

fn mark_read_action(room: &room::RoomId, upto: Seq) -> NotifyAction {
    NotifyAction {
        label: "Mark read".into(),
        key: Some('r'),
        run: Box::new(WireCommand::InboxMarkRead(InboxMarkRead {
            room: room.clone(),
            upto,
        })),
    }
}

fn interaction_actions(
    wave: &WaveSlug,
    role: &RoleName,
    interaction: &wave::PendingInteraction,
) -> Vec<NotifyAction> {
    let deny = NotifyAction {
        label: "Deny".into(),
        key: Some('d'),
        run: Box::new(WireCommand::WaveResolve(wave::WaveResolve {
            wave: wave.clone(),
            role: role.clone(),
            interaction: interaction.id.clone(),
            resolution: wave::InteractionResolution::Deny,
        })),
    };
    match &interaction.kind {
        wave::InteractionKind::Permission => vec![
            NotifyAction {
                label: "Allow once".into(),
                key: Some('a'),
                run: Box::new(WireCommand::WaveResolve(wave::WaveResolve {
                    wave: wave.clone(),
                    role: role.clone(),
                    interaction: interaction.id.clone(),
                    resolution: wave::InteractionResolution::AllowOnce {
                        granted_permissions: None,
                    },
                })),
            },
            NotifyAction {
                label: "Allow for session".into(),
                key: Some('s'),
                run: Box::new(WireCommand::WaveResolve(wave::WaveResolve {
                    wave: wave.clone(),
                    role: role.clone(),
                    interaction: interaction.id.clone(),
                    resolution: wave::InteractionResolution::AllowForSession {
                        granted_permissions: None,
                    },
                })),
            },
            deny,
        ],
        wave::InteractionKind::Question { .. } | wave::InteractionKind::Extension => vec![deny],
    }
}

fn is_blocking_mail(item: &mail::MailboxItem) -> bool {
    matches!(item.kind.as_str(), "blocker" | "approval")
        || item
            .label
            .as_deref()
            .is_some_and(|l| l.starts_with("[blocking]"))
}

pub fn rank(kind: &NotifyKind, age_ms: u64) -> Rank {
    let base: u16 = match kind {
        NotifyKind::Interaction { .. } => 9_000,
        NotifyKind::WorkerRefused { .. } => 8_500,
        NotifyKind::Mail { item } if is_blocking_mail(item) => 8_000,
        NotifyKind::LetterBanked { letter } if is_blocking_mail(&letter.item) => 8_000,
        NotifyKind::DirectMessage { .. } => 7_000,
        NotifyKind::Mention { .. } => 6_000,
        NotifyKind::WorkerStuck { .. } => 5_000,
        NotifyKind::TransportDown { .. } => 5_500,
        NotifyKind::MailDeliveryFailed { .. } => 5_200,
        NotifyKind::OrgConflict { .. } => 4_800,
        NotifyKind::LetterBanked { .. } | NotifyKind::Mail { .. } => 4_500,
        NotifyKind::MailUnrung { .. } => 4_200,
        NotifyKind::Reminder { .. } => 4_000,
        NotifyKind::CardAssigned { .. } => 3_500,
        NotifyKind::WaveFinished { .. } => 3_000,
    };
    let decay = (age_ms / DECAY_STEP_MS).min(u64::from(MAX_DECAY)) as u16;
    Rank(base.saturating_sub(decay))
}

#[derive(Clone, Debug)]
struct PendingLetter {
    path: PathBuf,
    at: UnixMs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DropRecord {
    dropped_at: UnixMs,
    reconnectable: bool,
}

/// Pure verdict for whether an Operator `MailUnrung` card should be raised,
/// held inside the 15 s reconnect grace window, suppressed, or auto-resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MailUnrungEval {
    Suppress,
    WithinGrace {
        ready_at: UnixMs,
        unread_count: u32,
        oldest_at: UnixMs,
    },
    Raise {
        unread_count: u32,
        oldest_at: UnixMs,
    },
    Resolve,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RaisedMailUnrungState {
    unread_count: u32,
    oldest_at: UnixMs,
    operators: BTreeSet<Principal>,
}

/// Reconnect grace window a [`HeraldView`] applies; defaults to
/// [`RECONNECT_GRACE_MS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GraceMs(u64);

impl Default for GraceMs {
    fn default() -> Self {
        GraceMs(RECONNECT_GRACE_MS)
    }
}

#[derive(Default)]
pub struct HeraldView {
    grace: GraceMs,
    /// The house's operator surface seat; `None` means
    /// [`presence::DEFAULT_OPERATOR_SURFACE_SEAT`].
    operator_surface_seat: Option<SeatName>,
    latest_at: UnixMs,
    members: BTreeMap<room::RoomId, BTreeSet<Principal>>,
    wave_owners: BTreeMap<WaveSlug, Principal>,
    operators: BTreeSet<Principal>,
    operator_seats: BTreeSet<SeatName>,
    resident_seats: BTreeSet<SeatName>,
    blocked: BTreeMap<(WaveSlug, RoleName), (BbInteractionId, String)>,
    cleared_interactions: Vec<(WaveSlug, RoleName, BbInteractionId, String)>,
    seat_status: BTreeMap<SeatName, presence::SeatStatus>,
    seat_mail_horizon: BTreeMap<SeatName, UnixMs>,
    live_bindings: BTreeMap<SeatName, BTreeMap<IncarnationId, presence::SeatBinding>>,
    pending_canary_bindings: BTreeMap<SeatName, BTreeMap<IncarnationId, presence::SeatBinding>>,
    ever_bound: BTreeSet<SeatName>,
    last_bound_at: BTreeMap<SeatName, UnixMs>,
    last_drop: BTreeMap<SeatName, DropRecord>,
    down_transports: BTreeSet<presence::TransportKind>,
    pending_mail: BTreeMap<SeatName, BTreeMap<Sha256Hex, PendingLetter>>,
    unrung_mail: BTreeMap<SeatName, BTreeMap<Sha256Hex, PendingLetter>>,
    acked_mail: BTreeSet<(SeatName, Sha256Hex)>,
    mail_meta_by_sha: BTreeMap<Sha256Hex, (String, PathBuf, bool)>,
    failed_mail: BTreeSet<(SeatName, Sha256Hex)>,
    raised_mail_unrung: BTreeMap<SeatName, RaisedMailUnrungState>,
    persisted_inbox_by_id: BTreeMap<(Principal, NotificationId), String>,
    persisted_inbox_dedupes: BTreeSet<(Principal, String)>,
    mail_unrung_emits: Vec<(SeatName, u32, UnixMs)>,
    transport_emits: Vec<(presence::TransportKind, bool, Option<String>)>,
}

fn box_seat_from_path(path: &std::path::Path) -> Option<SeatName> {
    let raw = path.to_string_lossy();
    let trimmed = raw
        .trim_start_matches("./")
        .trim_start_matches('/')
        .trim_start_matches("interchange/");
    let first = trimmed.split('/').next()?;
    let seat_raw = first.strip_prefix("to-")?;
    SeatName::parse(seat_raw).ok()
}

impl HeraldView {
    pub fn observe(&mut self, entry: &Envelope) {
        self.observe_inner(entry, true);
    }

    fn observe_replay(&mut self, entry: &Envelope) {
        self.observe_inner(entry, false);
    }

    fn rearm_failed_mail_on_live_rebind(&mut self, seat: &SeatName, rebound_at: UnixMs) {
        let mut rearmed: Vec<Sha256Hex> = Vec::new();
        self.failed_mail.retain(|(s, sha)| {
            if s == seat {
                rearmed.push(sha.clone());
                false
            } else {
                true
            }
        });
        if rearmed.is_empty() {
            return;
        }
        let horizon = self.seat_mail_horizon.get(seat).copied();
        for sha in rearmed {
            if self.acked_mail.contains(&(seat.clone(), sha.clone())) {
                continue;
            }
            if let Some((sender_norm, path, non_ringable)) =
                self.mail_meta_by_sha.get(&sha).cloned()
                && !non_ringable
                && seat.as_str() != sender_norm
                && horizon.is_none_or(|h| rebound_at >= h)
            {
                self.pending_mail
                    .entry(seat.clone())
                    .or_default()
                    .entry(sha)
                    .or_insert(PendingLetter {
                        path,
                        at: rebound_at,
                    });
            }
        }
    }

    fn observe_inner(&mut self, entry: &Envelope, emit_mail_unrung: bool) {
        self.latest_at = self.latest_at.max(entry.at);
        self.cleared_interactions.clear();
        self.mail_unrung_emits.clear();
        self.transport_emits.clear();
        self.record_principal(&entry.by);

        let mut check_seats: BTreeSet<SeatName> = BTreeSet::new();

        match (&entry.stream, &entry.event) {
            (StreamId::Room { room }, Event::Room(ev)) => {
                if let room::RoomId::Direct { pair } = room {
                    let (a, b): (Principal, Principal) = pair.clone().into();
                    self.record_principal(&a);
                    self.record_principal(&b);
                    let set = self.members.entry(room.clone()).or_default();
                    set.insert(a);
                    set.insert(b);
                }
                match ev {
                    room::RoomEvent::MemberJoined { member, .. } => {
                        self.record_principal(member);
                        self.members
                            .entry(room.clone())
                            .or_default()
                            .insert(member.clone());
                    }
                    room::RoomEvent::MemberLeft { member } => {
                        if let Some(set) = self.members.get_mut(room) {
                            set.remove(member);
                        }
                    }
                    room::RoomEvent::Opened { .. }
                    | room::RoomEvent::Posted { .. }
                    | room::RoomEvent::TopicSet { .. }
                    | room::RoomEvent::Labeled { .. }
                    | room::RoomEvent::TaskClaimed { .. }
                    | room::RoomEvent::TaskHeartbeat { .. }
                    | room::RoomEvent::TaskTransitioned { .. }
                    | room::RoomEvent::TaskUpdated { .. } => {}
                }
            }
            (StreamId::Wave { wave }, Event::Wave(ev)) => match ev {
                wave::WaveEvent::Started { .. } => {
                    if entry.by.is_operator() {
                        self.wave_owners.insert(wave.clone(), entry.by.clone());
                    }
                }
                wave::WaveEvent::WorkerObserved { role, observation } => match observation {
                    wave::Observation::Activity {
                        activity: wave::Activity::Blocked { interaction },
                        ..
                    } => {
                        let key = (wave.clone(), role.clone());
                        if let Some((old_id, old_summary)) = self.blocked.remove(&key)
                            && old_id != interaction.id
                        {
                            self.cleared_interactions.push((
                                wave.clone(),
                                role.clone(),
                                old_id,
                                old_summary,
                            ));
                        }
                        self.blocked
                            .insert(key, (interaction.id.clone(), interaction.summary.clone()));
                    }
                    wave::Observation::RuntimeMeta { .. } => {}
                    wave::Observation::TurnsSuppressed { keys, .. } => {
                        if wave::failure_activity_from_suppression_keys(keys).is_some()
                            && let Some((old_id, old_summary)) =
                                self.blocked.remove(&(wave.clone(), role.clone()))
                        {
                            self.cleared_interactions.push((
                                wave.clone(),
                                role.clone(),
                                old_id,
                                old_summary,
                            ));
                        }
                    }
                    _ => {
                        if let Some((old_id, old_summary)) =
                            self.blocked.remove(&(wave.clone(), role.clone()))
                        {
                            self.cleared_interactions.push((
                                wave.clone(),
                                role.clone(),
                                old_id,
                                old_summary,
                            ));
                        }
                    }
                },
                wave::WaveEvent::InteractionResolved {
                    role, interaction, ..
                } => {
                    let key = (wave.clone(), role.clone());
                    let summary = match self.blocked.get(&key) {
                        Some((id, s)) if id == interaction => {
                            let s = s.clone();
                            self.blocked.remove(&key);
                            s
                        }
                        _ => "Resolved".into(),
                    };
                    self.cleared_interactions.push((
                        wave.clone(),
                        role.clone(),
                        interaction.clone(),
                        summary,
                    ));
                }
                wave::WaveEvent::WorkerRetiring { role, .. } => {
                    if let Some((old_id, old_summary)) =
                        self.blocked.remove(&(wave.clone(), role.clone()))
                    {
                        self.cleared_interactions.push((
                            wave.clone(),
                            role.clone(),
                            old_id,
                            old_summary,
                        ));
                    }
                }
                wave::WaveEvent::Finished { .. } => {
                    let keys: Vec<(WaveSlug, RoleName)> = self
                        .blocked
                        .keys()
                        .filter(|(w, _)| w == wave)
                        .cloned()
                        .collect();
                    for key in keys {
                        if let Some((old_id, old_summary)) = self.blocked.remove(&key) {
                            self.cleared_interactions
                                .push((key.0, key.1, old_id, old_summary));
                        }
                    }
                }
                wave::WaveEvent::WorkerRequested { .. } | wave::WaveEvent::Told { .. } => {}
            },
            (StreamId::Presence, Event::Presence(ev)) => match ev {
                presence::PresenceEvent::SeatClaimed { record } => {
                    self.seat_status.insert(record.seat.clone(), record.status);
                    if let Some(horizon) = record.mail_horizon_at {
                        self.seat_mail_horizon.insert(record.seat.clone(), horizon);
                        if let Some(map) = self.pending_mail.get_mut(&record.seat) {
                            map.retain(|_, letter| letter.at >= horizon);
                        }
                        if let Some(map) = self.unrung_mail.get_mut(&record.seat) {
                            map.retain(|_, letter| letter.at >= horizon);
                        }
                    } else {
                        self.seat_mail_horizon.remove(&record.seat);
                    }
                    check_seats.insert(record.seat.clone());
                }
                presence::PresenceEvent::SeatStatusSet { seat, status, .. } => {
                    self.seat_status.insert(seat.clone(), *status);
                    check_seats.insert(seat.clone());
                }
                presence::PresenceEvent::Bound {
                    binding,
                    superseded,
                } => {
                    let seat = binding.seat.clone();
                    let seat_bindings = self.live_bindings.entry(seat.clone()).or_default();
                    let pending_canaries = self
                        .pending_canary_bindings
                        .entry(seat.clone())
                        .or_default();
                    for old_id in superseded {
                        seat_bindings.remove(old_id);
                        pending_canaries.remove(old_id);
                    }
                    self.ever_bound.insert(seat.clone());
                    if matches!(
                        binding.status,
                        presence::BindingStatus::Live | presence::BindingStatus::TransportDown
                    ) {
                        pending_canaries.remove(&binding.incarnation_id);
                        seat_bindings.insert(binding.incarnation_id.clone(), binding.clone());
                        if binding.status == presence::BindingStatus::Live
                            && !self.down_transports.contains(&binding.endpoint.kind())
                        {
                            self.last_drop.remove(&seat);
                            self.last_bound_at
                                .entry(seat.clone())
                                .and_modify(|prev| *prev = (*prev).max(entry.at))
                                .or_insert(entry.at);
                            self.rearm_failed_mail_on_live_rebind(&seat, entry.at);
                        }
                    } else if binding.status == presence::BindingStatus::PendingCanary {
                        seat_bindings.remove(&binding.incarnation_id);
                        pending_canaries.insert(binding.incarnation_id.clone(), binding.clone());
                    } else {
                        seat_bindings.remove(&binding.incarnation_id);
                        pending_canaries.remove(&binding.incarnation_id);
                    }
                    check_seats.insert(seat);
                }
                presence::PresenceEvent::CanaryConfirmed {
                    seat,
                    incarnation_id,
                    epoch,
                } => {
                    if let Some(mut binding) = self
                        .pending_canary_bindings
                        .get_mut(seat)
                        .and_then(|m| m.remove(incarnation_id))
                        && binding.epoch == *epoch
                    {
                        binding.canary_token = None;
                        binding.status = if self.down_transports.contains(&binding.endpoint.kind())
                        {
                            presence::BindingStatus::TransportDown
                        } else {
                            presence::BindingStatus::Live
                        };
                        let became_live = binding.status == presence::BindingStatus::Live;
                        self.live_bindings
                            .entry(seat.clone())
                            .or_default()
                            .insert(incarnation_id.clone(), binding);
                        if became_live {
                            self.last_drop.remove(seat);
                            self.last_bound_at
                                .entry(seat.clone())
                                .and_modify(|prev| *prev = (*prev).max(entry.at))
                                .or_insert(entry.at);
                            self.rearm_failed_mail_on_live_rebind(seat, entry.at);
                        }
                    }
                    check_seats.insert(seat.clone());
                }
                presence::PresenceEvent::Heartbeated {
                    seat,
                    incarnation_id,
                    expires_at,
                } => {
                    if let Some(binding) = self
                        .live_bindings
                        .get_mut(seat)
                        .and_then(|m| m.get_mut(incarnation_id))
                    {
                        binding.expires_at = *expires_at;
                    }
                    if let Some(binding) = self
                        .pending_canary_bindings
                        .get_mut(seat)
                        .and_then(|m| m.get_mut(incarnation_id))
                    {
                        binding.expires_at = *expires_at;
                    }
                }
                presence::PresenceEvent::Unbound {
                    seat,
                    incarnation_id,
                    status,
                    ..
                } => {
                    if let Some(map) = self.pending_canary_bindings.get_mut(seat) {
                        map.remove(incarnation_id);
                    }
                    let dropped = self
                        .live_bindings
                        .get_mut(seat)
                        .and_then(|m| m.remove(incarnation_id));
                    let reconnectable = dropped.as_ref().is_some_and(|b| {
                        *status == presence::BindingStatus::Unbound && is_reconnectable_binding(b)
                    });
                    if dropped.is_some() && !self.has_live_binding(seat, entry.at) {
                        self.last_drop.insert(
                            seat.clone(),
                            DropRecord {
                                dropped_at: entry.at,
                                reconnectable,
                            },
                        );
                        if let Some(pending) = self.pending_mail.get(seat) {
                            let unrung = self.unrung_mail.entry(seat.clone()).or_default();
                            for (sha, letter) in pending {
                                unrung.entry(sha.clone()).or_insert_with(|| letter.clone());
                            }
                        }
                    }
                    check_seats.insert(seat.clone());
                }
                presence::PresenceEvent::TransportHealthChanged {
                    transport,
                    down,
                    detail,
                } => {
                    if *down {
                        self.down_transports.insert(*transport);
                        for (seat, map) in &mut self.live_bindings {
                            let mut affected = false;
                            for b in map.values_mut() {
                                if b.endpoint.kind() == *transport
                                    && b.status == presence::BindingStatus::Live
                                {
                                    b.status = presence::BindingStatus::TransportDown;
                                    affected = true;
                                }
                            }
                            if affected {
                                check_seats.insert(seat.clone());
                            }
                        }
                    } else {
                        self.down_transports.remove(transport);
                        for (seat, map) in &mut self.live_bindings {
                            let mut restored = false;
                            for b in map.values_mut() {
                                if b.endpoint.kind() == *transport
                                    && b.status == presence::BindingStatus::TransportDown
                                {
                                    b.status = presence::BindingStatus::Live;
                                    restored = true;
                                }
                            }
                            if restored {
                                self.last_drop.remove(seat);
                                self.last_bound_at
                                    .entry(seat.clone())
                                    .and_modify(|prev| *prev = (*prev).max(entry.at))
                                    .or_insert(entry.at);
                                check_seats.insert(seat.clone());
                            }
                        }
                    }
                    self.transport_emits
                        .push((*transport, *down, detail.clone()));
                }
            },
            (StreamId::Mail, Event::Mail(ev)) => match ev {
                mail::MailEvent::Banked { letter, .. } => {
                    if let Ok(seat) = SeatName::parse(&letter.item.to) {
                        self.record_incoming_mail(
                            seat.clone(),
                            letter.sha256.clone(),
                            letter.path.clone(),
                            entry.at,
                            emit_mail_unrung,
                        );
                        check_seats.insert(seat);
                    }
                }
                mail::MailEvent::Projected { item } => {
                    if item.state != "acked"
                        && let Ok(seat) = SeatName::parse(&item.to)
                    {
                        self.record_incoming_mail(
                            seat.clone(),
                            item.reference.sha256.clone(),
                            item.reference.path.clone(),
                            entry.at,
                            emit_mail_unrung,
                        );
                        check_seats.insert(seat);
                    }
                }
                mail::MailEvent::MessageCommitted {
                    message,
                    mutated_after_ack,
                    ..
                } => {
                    let is_system_courier_notice = entry.by
                        == (Principal::System {
                            part: SystemPart::CourierHub,
                        })
                        && message.from == "system/courier_hub";
                    let non_ringable =
                        *mutated_after_ack || message.receipt_only || is_system_courier_notice;
                    let sender_norm = message
                        .from
                        .trim()
                        .trim_start_matches('@')
                        .to_ascii_lowercase();
                    self.mail_meta_by_sha.insert(
                        message.sha256.clone(),
                        (sender_norm.clone(), message.path.clone(), non_ringable),
                    );
                    // MailStore.tla EditNeverRingable: post-ack edits, receipt-only messages,
                    // sender self-copies, and system courier-hub failure notices never trigger
                    // Operator MailUnrung alerts.
                    if !non_ringable {
                        for seat in message.to.iter().chain(message.cc.iter()) {
                            if seat.as_str() == sender_norm {
                                continue;
                            }
                            self.record_incoming_mail(
                                seat.clone(),
                                message.sha256.clone(),
                                message.path.clone(),
                                entry.at,
                                emit_mail_unrung,
                            );
                            check_seats.insert(seat.clone());
                        }
                    }
                }
                mail::MailEvent::RecipientsAdded { sha256, to } => {
                    if let Some((sender_norm, path, non_ringable)) =
                        self.mail_meta_by_sha.get(sha256).cloned()
                        && !non_ringable
                    {
                        for seat in to {
                            if seat.as_str() == sender_norm {
                                continue;
                            }
                            self.record_incoming_mail(
                                seat.clone(),
                                sha256.clone(),
                                path.clone(),
                                entry.at,
                                emit_mail_unrung,
                            );
                            check_seats.insert(seat.clone());
                        }
                    }
                }
                mail::MailEvent::PathHeadUpdated { .. } => {}
                mail::MailEvent::Acked {
                    path,
                    sha256,
                    seat: explicit_seat,
                    ..
                } => {
                    let seats_to_ack = match explicit_seat {
                        Some(s) => vec![s.clone()],
                        None => self.seats_for_ack(&entry.by, path, sha256),
                    };
                    for seat in seats_to_ack {
                        self.acked_mail.insert((seat.clone(), sha256.clone()));
                        if let Some(map) = self.pending_mail.get_mut(&seat) {
                            map.remove(sha256);
                        }
                        if let Some(map) = self.unrung_mail.get_mut(&seat) {
                            map.remove(sha256);
                        }
                        check_seats.insert(seat);
                    }
                }
                mail::MailEvent::DeliveryFailed {
                    kind,
                    seat,
                    sha256s,
                    ..
                } => {
                    // Loud courier failures escalate to hub_owner_seat + sender and appear
                    // passively in the TUI, never raising Operator MailUnrung cards.
                    if matches!(
                        kind,
                        mail::DeliveryFailureKind::Refused | mail::DeliveryFailureKind::GiveUp
                    ) {
                        for sha in sha256s {
                            self.failed_mail.insert((seat.clone(), sha.clone()));
                            if let Some(map) = self.pending_mail.get_mut(seat) {
                                map.remove(sha);
                            }
                            if let Some(map) = self.unrung_mail.get_mut(seat) {
                                map.remove(sha);
                            }
                        }
                        check_seats.insert(seat.clone());
                    }
                }
                mail::MailEvent::RingDispatched { .. }
                | mail::MailEvent::RingAttempted { .. }
                | mail::MailEvent::BellRang { .. } => {}
            },
            (StreamId::Inbox { owner }, Event::Inbox(ev)) => match ev {
                InboxEvent::Raised { id, draft, .. } | InboxEvent::Updated { id, draft, .. } => {
                    if let Some(old_dedupe) = self
                        .persisted_inbox_by_id
                        .insert((owner.clone(), *id), draft.dedupe.clone())
                        && old_dedupe != draft.dedupe
                    {
                        self.persisted_inbox_dedupes
                            .remove(&(owner.clone(), old_dedupe));
                    }
                    self.persisted_inbox_dedupes
                        .insert((owner.clone(), draft.dedupe.clone()));
                    if let NotifyKind::MailUnrung {
                        seat,
                        unread_count,
                        oldest_at,
                    } = &draft.kind
                    {
                        if *unread_count > 0 {
                            let state =
                                self.raised_mail_unrung
                                    .entry(seat.clone())
                                    .or_insert_with(|| RaisedMailUnrungState {
                                        unread_count: *unread_count,
                                        oldest_at: *oldest_at,
                                        operators: BTreeSet::new(),
                                    });
                            state.unread_count = *unread_count;
                            state.oldest_at = *oldest_at;
                            state.operators.insert(owner.clone());
                        } else if let Some(state) = self.raised_mail_unrung.get_mut(seat) {
                            state.operators.remove(owner);
                            if state.operators.is_empty() {
                                self.raised_mail_unrung.remove(seat);
                            }
                        }
                    }
                }
                InboxEvent::StateSet { .. }
                | InboxEvent::CursorAdvanced { .. }
                | InboxEvent::ArchivedSet { .. } => {}
            },
            _ => {}
        }

        if !emit_mail_unrung {
            return;
        }

        // Also sweep any seats waiting on reconnect grace whose deadline has now elapsed.
        for seat in self.unrung_mail.keys() {
            check_seats.insert(seat.clone());
        }
        for seat in self.raised_mail_unrung.keys() {
            check_seats.insert(seat.clone());
        }

        let operator_set: BTreeSet<Principal> = self.all_operators().into_iter().collect();
        for seat in check_seats {
            match self.evaluate_mail_unrung_with_grace(&seat, entry.at, self.grace.0) {
                MailUnrungEval::Raise {
                    unread_count,
                    oldest_at,
                } => {
                    let next = RaisedMailUnrungState {
                        unread_count,
                        oldest_at,
                        operators: operator_set.clone(),
                    };
                    if self.raised_mail_unrung.get(&seat) != Some(&next) {
                        self.raised_mail_unrung.insert(seat.clone(), next);
                        self.mail_unrung_emits.push((seat, unread_count, oldest_at));
                    }
                }
                MailUnrungEval::Resolve => {
                    if self.raised_mail_unrung.remove(&seat).is_some() {
                        self.mail_unrung_emits.push((seat, 0, UnixMs(0)));
                    }
                }
                MailUnrungEval::Suppress | MailUnrungEval::WithinGrace { .. } => {}
            }
        }
    }

    fn reconcile_unrung_after_replay(&mut self) {
        let now = self.latest_at;
        let seats: Vec<SeatName> = self.pending_mail.keys().cloned().collect();
        for seat in seats {
            let Some(pending) = self.pending_mail.get(&seat) else {
                continue;
            };
            if pending.is_empty() {
                continue;
            }
            if !self.has_live_binding(&seat, now) {
                let unrung = self.unrung_mail.entry(seat).or_default();
                for (sha, letter) in pending {
                    unrung.entry(sha.clone()).or_insert_with(|| letter.clone());
                }
            } else if self.raised_mail_unrung.contains_key(&seat) {
                // Seat already had an open MailUnrung card before restart and has
                // since rebound: only count letters that arrived before the live bind
                // so letters received while live never inflate a raised/dismissed card.
                let bound_at = self.last_bound_at.get(&seat).copied().unwrap_or(UnixMs(0));
                let unrung = self.unrung_mail.entry(seat).or_default();
                for (sha, letter) in pending {
                    if letter.at <= bound_at {
                        unrung.entry(sha.clone()).or_insert_with(|| letter.clone());
                    }
                }
            }
        }
    }

    fn record_incoming_mail(
        &mut self,
        seat: SeatName,
        sha256: Sha256Hex,
        path: PathBuf,
        at: UnixMs,
        track_unrung: bool,
    ) {
        if self.acked_mail.contains(&(seat.clone(), sha256.clone()))
            || self.failed_mail.contains(&(seat.clone(), sha256.clone()))
        {
            return;
        }
        if self
            .seat_mail_horizon
            .get(&seat)
            .is_some_and(|&horizon| at < horizon)
        {
            return;
        }
        let letter = PendingLetter { path, at };
        self.pending_mail
            .entry(seat.clone())
            .or_default()
            .entry(sha256.clone())
            .or_insert_with(|| letter.clone());
        if track_unrung && !self.has_live_binding(&seat, at) {
            self.unrung_mail
                .entry(seat)
                .or_default()
                .entry(sha256)
                .or_insert(letter);
        }
    }

    fn seats_for_ack(
        &self,
        by: &Principal,
        path: &std::path::Path,
        sha256: &Sha256Hex,
    ) -> Vec<SeatName> {
        if let Some(box_seat) = box_seat_from_path(path) {
            return vec![box_seat];
        }
        match by {
            Principal::Seat { seat } | Principal::Operator { seat } => vec![seat.clone()],
            Principal::System { .. } | Principal::Worker { .. } => self
                .pending_mail
                .iter()
                .filter(|(_, map)| {
                    map.get(sha256)
                        .is_some_and(|pending| path.as_os_str().is_empty() || pending.path == path)
                })
                .map(|(seat, _)| seat.clone())
                .collect(),
        }
    }

    fn is_operator_seat(&self, seat: &SeatName) -> bool {
        self.operator_seats.contains(seat)
            || (self.operator_seats.is_empty() && seat.as_str() == "owner")
    }

    fn is_operator_surface_seat(&self, seat: &SeatName) -> bool {
        match &self.operator_surface_seat {
            Some(surface) => surface == seat,
            None => presence::is_default_operator_surface_seat(seat.as_str()),
        }
    }

    /// The reconnect grace window (ms) this view applies.
    pub fn grace_ms(&self) -> u64 {
        self.grace.0
    }

    fn has_live_binding(&self, seat: &SeatName, now: UnixMs) -> bool {
        self.live_bindings.get(seat).is_some_and(|m| {
            m.values().any(|b| {
                b.status == presence::BindingStatus::Live
                    && !self.down_transports.contains(&b.endpoint.kind())
                    && b.expires_at.is_none_or(|exp| exp > now)
            })
        })
    }

    fn has_only_transport_down_bindings(&self, seat: &SeatName, now: UnixMs) -> bool {
        self.live_bindings.get(seat).is_some_and(|m| {
            let mut saw_unexpired = false;
            for b in m.values() {
                if b.expires_at.is_some_and(|exp| exp <= now) {
                    continue;
                }
                saw_unexpired = true;
                if !self.down_transports.contains(&b.endpoint.kind())
                    && b.status == presence::BindingStatus::Live
                {
                    return false;
                }
            }
            saw_unexpired
        })
    }

    /// Evaluates whether `seat` should raise, hold in reconnect grace, suppress,
    /// or resolve a `MailUnrung` Operator card at `now` using this view's grace
    /// window ([`RECONNECT_GRACE_MS`] unless the [`Herald`] was built with
    /// [`Herald::with_grace`]).
    pub fn evaluate_mail_unrung(&self, seat: &SeatName, now: UnixMs) -> MailUnrungEval {
        self.evaluate_mail_unrung_with_grace(seat, now, self.grace.0)
    }

    /// Evaluates `MailUnrung` policy for `seat` at `now` with an explicit grace duration.
    pub fn evaluate_mail_unrung_with_grace(
        &self,
        seat: &SeatName,
        now: UnixMs,
        grace_ms: u64,
    ) -> MailUnrungEval {
        if self.is_operator_seat(seat)
            || self.is_operator_surface_seat(seat)
            || presence::is_non_seat_target(seat.as_str())
        {
            return if self.raised_mail_unrung.contains_key(seat) {
                MailUnrungEval::Resolve
            } else {
                MailUnrungEval::Suppress
            };
        }
        let status = self
            .seat_status
            .get(seat)
            .copied()
            .unwrap_or(presence::SeatStatus::Active);
        if matches!(
            status,
            presence::SeatStatus::Dormant | presence::SeatStatus::Retired
        ) {
            return if self.raised_mail_unrung.contains_key(seat) {
                MailUnrungEval::Resolve
            } else {
                MailUnrungEval::Suppress
            };
        }

        let Some(unrung) = self.unrung_mail.get(seat).filter(|m| !m.is_empty()) else {
            return if self.raised_mail_unrung.contains_key(seat) {
                MailUnrungEval::Resolve
            } else {
                MailUnrungEval::Suppress
            };
        };

        let unread_count = unrung.len() as u32;
        let oldest_at = unrung.values().map(|l| l.at).min().unwrap_or(now);

        if self.has_live_binding(seat, now) {
            // Rebinding alone does not clear an already-raised card until pending
            // digests are ACKed; a fast reconnect before any card was raised
            // suppresses alerting cleanly.
            return if self.raised_mail_unrung.contains_key(seat) {
                MailUnrungEval::Raise {
                    unread_count,
                    oldest_at,
                }
            } else {
                MailUnrungEval::Suppress
            };
        }

        // During a provider-level TransportDown outage, the deduped TransportDown
        // card owns alerting instead of raising per-seat MailUnrung cards.
        if self.has_only_transport_down_bindings(seat, now)
            && !self.raised_mail_unrung.contains_key(seat)
        {
            return MailUnrungEval::Suppress;
        }

        if !self.ever_bound.contains(seat) {
            return MailUnrungEval::Raise {
                unread_count,
                oldest_at,
            };
        }

        if let Some(drop_rec) = self.last_drop.get(seat)
            && drop_rec.reconnectable
        {
            let ready_at = UnixMs(drop_rec.dropped_at.0.saturating_add(grace_ms));
            if now < ready_at {
                return MailUnrungEval::WithinGrace {
                    ready_at,
                    unread_count,
                    oldest_at,
                };
            }
        }

        MailUnrungEval::Raise {
            unread_count,
            oldest_at,
        }
    }

    /// Returns the earliest reconnect-grace expiry timestamp across all seats
    /// currently held inside the 15 s grace window.
    pub fn next_grace_deadline(&self, now: UnixMs) -> Option<UnixMs> {
        let mut earliest: Option<UnixMs> = None;
        for seat in self.unrung_mail.keys() {
            if let MailUnrungEval::WithinGrace { ready_at, .. } =
                self.evaluate_mail_unrung(seat, now)
            {
                earliest = Some(match earliest {
                    Some(prev) => prev.min(ready_at),
                    None => ready_at,
                });
            }
        }
        earliest
    }

    fn pending_mail_unrung_transitions(
        &self,
        now: UnixMs,
    ) -> Vec<(
        SeatName,
        Option<RaisedMailUnrungState>,
        Vec<(Principal, NotificationDraft)>,
    )> {
        let seats: Vec<SeatName> = self
            .unrung_mail
            .keys()
            .chain(self.raised_mail_unrung.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let operators = self.all_operators();
        let operator_set: BTreeSet<Principal> = operators.iter().cloned().collect();
        let mut out = Vec::new();
        for seat in seats {
            match self.evaluate_mail_unrung(&seat, now) {
                MailUnrungEval::Raise {
                    unread_count,
                    oldest_at,
                } => {
                    let next = RaisedMailUnrungState {
                        unread_count,
                        oldest_at,
                        operators: operator_set.clone(),
                    };
                    if self.raised_mail_unrung.get(&seat) != Some(&next) {
                        let draft = mail_unrung_draft(&seat, unread_count, oldest_at);
                        let raises = operators
                            .iter()
                            .map(|op| (op.clone(), draft.clone()))
                            .collect();
                        out.push((seat, Some(next), raises));
                    }
                }
                MailUnrungEval::Resolve => {
                    if self.raised_mail_unrung.contains_key(&seat) {
                        let draft = mail_unrung_resolved_draft(&seat);
                        let raises = operators
                            .iter()
                            .map(|op| (op.clone(), draft.clone()))
                            .collect();
                        out.push((seat, None, raises));
                    }
                }
                MailUnrungEval::Suppress | MailUnrungEval::WithinGrace { .. } => {}
            }
        }
        out
    }

    /// Flushes any `MailUnrung` alerts whose 15 s reconnect grace window has
    /// expired by `now`, returning `(operator, draft)` pairs ready for `InboxRaise`.
    pub fn due_mail_unrung(&mut self, now: UnixMs) -> Vec<(Principal, NotificationDraft)> {
        let transitions = self.pending_mail_unrung_transitions(now);
        let mut out = Vec::new();
        for (seat, next_state, raises) in transitions {
            match next_state {
                Some(state) => {
                    self.raised_mail_unrung.insert(seat, state);
                }
                None => {
                    self.raised_mail_unrung.remove(&seat);
                }
            }
            out.extend(raises);
        }
        out
    }

    fn all_operators(&self) -> Vec<Principal> {
        if !self.operators.is_empty() {
            return self.operators.iter().cloned().collect();
        }
        vec![Principal::Operator {
            seat: SeatName::parse("owner").expect("default operator seat is valid"),
        }]
    }

    fn record_principal(&mut self, p: &Principal) {
        match p {
            Principal::Operator { seat } => {
                self.operator_seats.insert(seat.clone());
                self.operators.insert(p.clone());
            }
            Principal::Seat { seat } => {
                self.resident_seats.insert(seat.clone());
            }
            Principal::Worker { .. } | Principal::System { .. } => {}
        }
    }

    fn members_of(&self, room: &room::RoomId) -> Option<&BTreeSet<Principal>> {
        self.members.get(room)
    }

    fn operators_for_wave(&self, wave: &WaveSlug) -> Vec<Principal> {
        if let Some(owner) = self.wave_owners.get(wave) {
            return vec![owner.clone()];
        }
        self.all_operators()
    }

    fn principals_for_seat(&self, seat: &SeatName) -> Vec<Principal> {
        let mut out = Vec::new();
        if self.operator_seats.contains(seat) {
            out.push(Principal::Operator { seat: seat.clone() });
        }
        if self.resident_seats.contains(seat) {
            out.push(Principal::Seat { seat: seat.clone() });
        }
        if out.is_empty() {
            if seat.as_str() == "owner" {
                out.push(Principal::Operator { seat: seat.clone() });
            } else {
                out.push(Principal::Seat { seat: seat.clone() });
            }
        }
        out
    }

    fn principal_for_assignee(&self, raw: &str) -> Option<Principal> {
        let trimmed = raw.trim().trim_start_matches('@');
        if let Some((w, r)) = trimmed.split_once('/')
            && let (Ok(wave), Ok(role)) = (WaveSlug::parse(w), RoleName::parse(r))
        {
            return Some(Principal::Worker { wave, role });
        }
        let seat = SeatName::parse(trimmed).ok()?;
        self.principals_for_seat(&seat).into_iter().next()
    }

    fn has_persisted_inbox_card(&self, owner: &Principal, dedupe: &str) -> bool {
        self.persisted_inbox_dedupes
            .contains(&(owner.clone(), dedupe.to_owned()))
    }
}

pub fn classify(entry: &Envelope, view: &HeraldView) -> Vec<(Principal, NotificationDraft)> {
    let mut out = Vec::new();

    for (wave, role, interaction_id, summary) in &view.cleared_interactions {
        for op in view.operators_for_wave(wave) {
            out.push((
                op,
                NotificationDraft {
                    kind: NotifyKind::Interaction {
                        wave: wave.clone(),
                        role: role.clone(),
                        interaction: interaction_id.clone(),
                    },
                    title: format!("@{wave}/{role} interaction resolved"),
                    preview: snippet(summary, 160),
                    actions: Vec::new(),
                    dedupe: format!("interaction:{wave}:{role}:{interaction_id}"),
                },
            ));
        }
    }

    for (transport, down, detail) in &view.transport_emits {
        let draft = if *down {
            transport_down_draft(*transport, detail.as_deref())
        } else {
            transport_recovered_draft(*transport)
        };
        for op in view.all_operators() {
            out.push((op, draft.clone()));
        }
    }

    for (seat, unread_count, oldest_at) in &view.mail_unrung_emits {
        let draft = if *unread_count == 0 {
            mail_unrung_resolved_draft(seat)
        } else {
            mail_unrung_draft(seat, *unread_count, *oldest_at)
        };
        for op in view.all_operators() {
            out.push((op, draft.clone()));
        }
    }

    match (&entry.stream, &entry.event) {
        (StreamId::Room { room }, Event::Room(room::RoomEvent::Posted { body, mentions, .. })) => {
            match room {
                room::RoomId::Direct { pair } => {
                    let (a, b): (Principal, Principal) = pair.clone().into();
                    let mut recipients = BTreeSet::new();
                    for side in [a, b] {
                        match side {
                            Principal::Seat { seat } => {
                                recipients.extend(view.principals_for_seat(&seat));
                            }
                            other => {
                                recipients.insert(other);
                            }
                        }
                    }
                    for recipient in recipients {
                        if !recipient.same_actor(&entry.by)
                            && !matches!(recipient, Principal::System { .. })
                        {
                            out.push((
                                recipient,
                                NotificationDraft {
                                    kind: NotifyKind::DirectMessage {
                                        room: room.clone(),
                                        seq: entry.seq,
                                    },
                                    title: format!("Direct message from {}", entry.by),
                                    preview: snippet(body.as_str(), 160),
                                    actions: vec![mark_read_action(room, entry.seq)],
                                    dedupe: format!("dm:{}:{}", room_slug(room), entry.seq.0),
                                },
                            ));
                        }
                    }
                }
                room::RoomId::Channel { .. } => {
                    let is_system_post = matches!(entry.by, Principal::System { .. });
                    let mut recipients: BTreeSet<Principal> =
                        mentions.principals.iter().cloned().collect();
                    if mentions.all
                        && let Some(room_members) = view.members_of(room)
                    {
                        recipients.extend(room_members.iter().cloned());
                    }
                    recipients.retain(|p| {
                        p != &entry.by
                            && !matches!(p, Principal::System { .. })
                            && !(is_system_post && p.is_operator())
                    });
                    for recipient in recipients {
                        out.push((
                            recipient,
                            NotificationDraft {
                                kind: NotifyKind::Mention {
                                    room: room.clone(),
                                    seq: entry.seq,
                                },
                                title: format!("Mention from {} in {}", entry.by, room_label(room)),
                                preview: snippet(body.as_str(), 160),
                                actions: vec![mark_read_action(room, entry.seq)],
                                dedupe: format!("mention:{}:{}", room_slug(room), entry.seq.0),
                            },
                        ));
                    }
                }
            }
        }

        (StreamId::Wave { wave }, Event::Wave(ev)) => match ev {
            wave::WaveEvent::WorkerObserved { role, observation } => match observation {
                wave::Observation::Activity {
                    activity: wave::Activity::Blocked { interaction },
                    ..
                } => {
                    let actions = interaction_actions(wave, role, interaction);
                    for op in view.operators_for_wave(wave) {
                        out.push((
                            op,
                            NotificationDraft {
                                kind: NotifyKind::Interaction {
                                    wave: wave.clone(),
                                    role: role.clone(),
                                    interaction: interaction.id.clone(),
                                },
                                title: format!("@{wave}/{role} waiting on interaction"),
                                preview: snippet(&interaction.summary, 160),
                                actions: actions.clone(),
                                dedupe: format!("interaction:{wave}:{role}:{}", interaction.id),
                            },
                        ));
                    }
                }
                wave::Observation::Activity {
                    activity: wave::Activity::Refused { reason },
                    ..
                } => {
                    for op in view.operators_for_wave(wave) {
                        out.push((
                            op,
                            NotificationDraft {
                                kind: NotifyKind::WorkerRefused {
                                    wave: wave.clone(),
                                    role: role.clone(),
                                    reason: reason.clone(),
                                    seq: entry.seq,
                                },
                                title: format!("@{wave}/{role} refused"),
                                preview: snippet(reason, 160),
                                actions: Vec::new(),
                                dedupe: format!("worker-refused:{wave}:{role}"),
                            },
                        ));
                    }
                }
                wave::Observation::Activity {
                    activity: wave::Activity::Errored { error },
                    ..
                }
                | wave::Observation::SpawnFailed { error, .. } => {
                    for op in view.operators_for_wave(wave) {
                        out.push((
                            op,
                            NotificationDraft {
                                kind: NotifyKind::WorkerStuck {
                                    wave: wave.clone(),
                                    role: role.clone(),
                                    why: error.clone(),
                                    seq: entry.seq,
                                },
                                title: format!("@{wave}/{role} stuck"),
                                preview: snippet(error, 160),
                                actions: Vec::new(),
                                dedupe: format!("worker-stuck:{wave}:{role}"),
                            },
                        ));
                    }
                }
                wave::Observation::TurnsSuppressed { keys, .. } => {
                    match wave::failure_activity_from_suppression_keys(keys) {
                        Some(wave::Activity::Refused { reason }) => {
                            for op in view.operators_for_wave(wave) {
                                out.push((
                                    op,
                                    NotificationDraft {
                                        kind: NotifyKind::WorkerRefused {
                                            wave: wave.clone(),
                                            role: role.clone(),
                                            reason: reason.clone(),
                                            seq: entry.seq,
                                        },
                                        title: format!("@{wave}/{role} refused"),
                                        preview: snippet(&reason, 160),
                                        actions: Vec::new(),
                                        dedupe: format!("worker-refused:{wave}:{role}"),
                                    },
                                ));
                            }
                        }
                        Some(wave::Activity::Errored { error }) => {
                            for op in view.operators_for_wave(wave) {
                                out.push((
                                    op,
                                    NotificationDraft {
                                        kind: NotifyKind::WorkerStuck {
                                            wave: wave.clone(),
                                            role: role.clone(),
                                            why: error.clone(),
                                            seq: entry.seq,
                                        },
                                        title: format!("@{wave}/{role} stuck"),
                                        preview: snippet(&error, 160),
                                        actions: Vec::new(),
                                        dedupe: format!("worker-stuck:{wave}:{role}"),
                                    },
                                ));
                            }
                        }
                        _ => {}
                    }
                }
                wave::Observation::Spawned { .. }
                | wave::Observation::Activity { .. }
                | wave::Observation::RuntimeMeta { .. }
                | wave::Observation::Archived { .. } => {}
            },
            wave::WaveEvent::Finished { outcome } => {
                let detail = match outcome {
                    wave::WaveOutcome::Completed { summary } => summary.as_str(),
                    wave::WaveOutcome::Cancelled { reason }
                    | wave::WaveOutcome::Failed { reason } => reason.as_str(),
                };
                for op in view.operators_for_wave(wave) {
                    out.push((
                        op,
                        NotificationDraft {
                            kind: NotifyKind::WaveFinished { wave: wave.clone() },
                            title: format!("Wave {wave} finished"),
                            preview: snippet(detail, 160),
                            actions: Vec::new(),
                            dedupe: format!("wave-finished:{wave}"),
                        },
                    ));
                }
            }
            wave::WaveEvent::Started { .. }
            | wave::WaveEvent::WorkerRequested { .. }
            | wave::WaveEvent::Told { .. }
            | wave::WaveEvent::InteractionResolved { .. }
            | wave::WaveEvent::WorkerRetiring { .. } => {}
        },

        (StreamId::Board { board }, Event::Board(ev)) => match ev {
            board::BoardEvent::CardCreated { card } => {
                if let Some(assignee) = &card.assignee
                    && let Some(to) = view.principal_for_assignee(assignee.as_str())
                    && to != entry.by
                {
                    out.push((
                        to,
                        NotificationDraft {
                            kind: NotifyKind::CardAssigned {
                                card: board::CardRef {
                                    board: board.clone(),
                                    no: card.no,
                                    revision: card.revision,
                                },
                            },
                            title: format!("Card #{} assigned in {board}", card.no.0),
                            preview: snippet(card.title.as_str(), 160),
                            actions: Vec::new(),
                            dedupe: format!(
                                "card-assigned:{board}:{}:{}",
                                card.no.0, card.revision.0
                            ),
                        },
                    ));
                }
            }
            board::BoardEvent::CardPatched {
                no,
                revision,
                patch,
            } => {
                if let Some(board::Assign::To { who }) = &patch.assignee
                    && let Some(to) = view.principal_for_assignee(who.as_str())
                    && to != entry.by
                {
                    let preview = patch
                        .title
                        .as_ref()
                        .map_or_else(|| format!("Card #{}", no.0), |t| snippet(t.as_str(), 160));
                    out.push((
                        to,
                        NotificationDraft {
                            kind: NotifyKind::CardAssigned {
                                card: board::CardRef {
                                    board: board.clone(),
                                    no: *no,
                                    revision: *revision,
                                },
                            },
                            title: format!("Card #{} assigned in {board}", no.0),
                            preview,
                            actions: Vec::new(),
                            dedupe: format!("card-assigned:{board}:{}:{}", no.0, revision.0),
                        },
                    ));
                }
            }
            board::BoardEvent::Opened { .. } | board::BoardEvent::CardArchived { .. } => {}
        },

        (StreamId::Mail, Event::Mail(ev)) => match ev {
            mail::MailEvent::Banked { letter, .. } => {
                if let Ok(seat) = SeatName::parse(&letter.item.to) {
                    let ack_action = NotifyAction {
                        label: "Acknowledge letter".into(),
                        key: Some('k'),
                        run: Box::new(WireCommand::MailAck(mail::MailAck::for_path_and_sha(
                            letter.path.clone(),
                            letter.sha256.clone(),
                        ))),
                    };
                    let preview = letter
                        .item
                        .label
                        .as_deref()
                        .map_or_else(|| letter.path.display().to_string(), |l| snippet(l, 160));
                    for to in view.principals_for_seat(&seat) {
                        out.push((
                            to,
                            NotificationDraft {
                                kind: NotifyKind::LetterBanked {
                                    letter: letter.clone(),
                                },
                                title: format!("Letter banked from {}", letter.item.from),
                                preview: preview.clone(),
                                actions: vec![ack_action.clone()],
                                dedupe: format!("letter-banked:{}", letter.wake_id.0),
                            },
                        ));
                    }
                }
            }
            mail::MailEvent::Projected { item } => {
                if item.state != "acked"
                    && let Ok(seat) = SeatName::parse(&item.to)
                {
                    let ack_action = NotifyAction {
                        label: "Acknowledge letter".into(),
                        key: Some('k'),
                        run: Box::new(WireCommand::MailAck(mail::MailAck::for_path_and_sha(
                            item.reference.path.clone(),
                            item.reference.sha256.clone(),
                        ))),
                    };
                    let preview = item
                        .label
                        .as_deref()
                        .map_or_else(|| item.id.clone(), |l| snippet(l, 160));
                    for to in view.principals_for_seat(&seat) {
                        out.push((
                            to,
                            NotificationDraft {
                                kind: NotifyKind::Mail { item: item.clone() },
                                title: format!("Mail from {}", item.from),
                                preview: preview.clone(),
                                actions: vec![ack_action.clone()],
                                dedupe: format!("mail:{}:{}", item.id, item.reference.sha256),
                            },
                        ));
                    }
                }
            }
            mail::MailEvent::BellRang { .. }
            | mail::MailEvent::Acked { .. }
            | mail::MailEvent::MessageCommitted { .. }
            | mail::MailEvent::RecipientsAdded { .. }
            | mail::MailEvent::PathHeadUpdated { .. }
            | mail::MailEvent::RingDispatched { .. }
            | mail::MailEvent::RingAttempted { .. }
            | mail::MailEvent::DeliveryFailed { .. } => {}
        },

        _ => {}
    }

    out
}

fn kinds_equal(a: &NotifyKind, b: &NotifyKind) -> bool {
    match (a, b) {
        (NotifyKind::Mention { room: r1, seq: s1 }, NotifyKind::Mention { room: r2, seq: s2 }) => {
            r1 == r2 && s1 == s2
        }
        (
            NotifyKind::DirectMessage { room: r1, seq: s1 },
            NotifyKind::DirectMessage { room: r2, seq: s2 },
        ) => r1 == r2 && s1 == s2,
        (
            NotifyKind::Interaction {
                wave: w1,
                role: r1,
                interaction: i1,
            },
            NotifyKind::Interaction {
                wave: w2,
                role: r2,
                interaction: i2,
            },
        ) => w1 == w2 && r1 == r2 && i1 == i2,
        (
            NotifyKind::WorkerStuck {
                wave: w1,
                role: r1,
                why: y1,
                seq: s1,
            },
            NotifyKind::WorkerStuck {
                wave: w2,
                role: r2,
                why: y2,
                seq: s2,
            },
        ) => w1 == w2 && r1 == r2 && y1 == y2 && s1 == s2,
        (
            NotifyKind::WorkerRefused {
                wave: w1,
                role: r1,
                reason: y1,
                seq: s1,
            },
            NotifyKind::WorkerRefused {
                wave: w2,
                role: r2,
                reason: y2,
                seq: s2,
            },
        ) => w1 == w2 && r1 == r2 && y1 == y2 && s1 == s2,
        (NotifyKind::WaveFinished { wave: w1 }, NotifyKind::WaveFinished { wave: w2 }) => w1 == w2,
        (NotifyKind::CardAssigned { card: c1 }, NotifyKind::CardAssigned { card: c2 }) => {
            c1.board == c2.board && c1.no == c2.no && c1.revision == c2.revision
        }
        (NotifyKind::Mail { item: m1 }, NotifyKind::Mail { item: m2 }) => m1 == m2,
        (NotifyKind::LetterBanked { letter: l1 }, NotifyKind::LetterBanked { letter: l2 }) => {
            l1 == l2
        }
        (NotifyKind::Reminder { bell_id: b1 }, NotifyKind::Reminder { bell_id: b2 }) => b1 == b2,
        (
            NotifyKind::MailUnrung {
                seat: s1,
                unread_count: u1,
                oldest_at: o1,
            },
            NotifyKind::MailUnrung {
                seat: s2,
                unread_count: u2,
                oldest_at: o2,
            },
        ) => s1 == s2 && u1 == u2 && o1 == o2,
        (
            NotifyKind::TransportDown {
                transport: t1,
                detail: d1,
            },
            NotifyKind::TransportDown {
                transport: t2,
                detail: d2,
            },
        ) => t1 == t2 && d1 == d2,
        (
            NotifyKind::OrgConflict {
                path: p1,
                org_id: o1,
                detail: d1,
            },
            NotifyKind::OrgConflict {
                path: p2,
                org_id: o2,
                detail: d2,
            },
        ) => p1 == p2 && o1 == o2 && d1 == d2,
        (
            NotifyKind::MailDeliveryFailed {
                failure_kind: k1,
                courier: c1,
                seat: s1,
                reason: r1,
            },
            NotifyKind::MailDeliveryFailed {
                failure_kind: k2,
                courier: c2,
                seat: s2,
                reason: r2,
            },
        ) => k1 == k2 && c1 == c2 && s1 == s2 && r1 == r2,
        _ => false,
    }
}

fn actions_equal(a: &[NotifyAction], b: &[NotifyAction]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(x, y)| {
            x.label == y.label && x.key == y.key && format!("{:?}", x.run) == format!("{:?}", y.run)
        })
}

fn drafts_equal(a: &NotificationDraft, b: &NotificationDraft) -> bool {
    a.dedupe == b.dedupe
        && a.title == b.title
        && a.preview == b.preview
        && kinds_equal(&a.kind, &b.kind)
        && actions_equal(&a.actions, &b.actions)
}

pub struct Inbox;

#[derive(Default)]
pub struct InboxState {
    items: BTreeMap<NotificationId, Notification>,
    by_dedupe: BTreeMap<String, NotificationId>,
    cursors: BTreeMap<room::RoomId, Seq>,
    archived: BTreeSet<ArchiveTarget>,
    next_id: u64,
    latest_at: UnixMs,
    head: Seq,
}

impl Decider for Inbox {
    type State = InboxState;
    type Cmd = InboxCommand;
    type Event = InboxEvent;

    fn address(cmd: &InboxCommand, ctx: &DecideCtx) -> Result<StreamId, ApiError> {
        let owner = match cmd {
            InboxCommand::InboxRaise(c) => c.to.clone(),
            _ => ctx.by().clone(),
        };
        Ok(StreamId::Inbox { owner })
    }

    fn idem(_cmd: &InboxCommand) -> Option<IdemKey> {
        None
    }

    fn decide(
        state: &InboxState,
        cmd: &InboxCommand,
        _ctx: &DecideCtx,
    ) -> Result<Vec<InboxEvent>, ApiError> {
        match cmd {
            InboxCommand::InboxRaise(c) => {
                // Structural Operator attention guard: mail delivery failures and
                // courier degradations escalate to hub_owner_seat/sender and surface
                // passively in TUI Settings, never raising interrupting Operator cards.
                if matches!(c.draft.kind, NotifyKind::MailDeliveryFailed { .. })
                    && c.to.is_operator()
                {
                    return Ok(vec![]);
                }
                let is_resolved_interaction =
                    matches!(c.draft.kind, NotifyKind::Interaction { .. })
                        && c.draft.actions.is_empty();
                let is_resolved_status = matches!(
                    &c.draft.kind,
                    NotifyKind::MailUnrung {
                        unread_count: 0,
                        ..
                    }
                ) || matches!(
                    &c.draft.kind,
                    NotifyKind::TransportDown { detail, .. }
                        | NotifyKind::OrgConflict { detail, .. }
                        if detail.is_empty()
                );
                let is_resolvable_status_kind = matches!(
                    c.draft.kind,
                    NotifyKind::MailUnrung { .. }
                        | NotifyKind::TransportDown { .. }
                        | NotifyKind::OrgConflict { .. }
                );
                let fresh_rank = rank(&c.draft.kind, 0);
                if let Some(&existing_id) = state.by_dedupe.get(&c.draft.dedupe)
                    && let Some(existing) = state.items.get(&existing_id)
                {
                    if let (
                        NotifyKind::WorkerStuck { seq: prev_seq, .. },
                        NotifyKind::WorkerStuck { seq: new_seq, .. },
                    )
                    | (
                        NotifyKind::WorkerRefused { seq: prev_seq, .. },
                        NotifyKind::WorkerRefused { seq: new_seq, .. },
                    ) = (&existing.draft.kind, &c.draft.kind)
                    {
                        if new_seq <= prev_seq {
                            return Ok(vec![]);
                        }
                        let mut events = vec![InboxEvent::Updated {
                            id: existing_id,
                            rank: fresh_rank,
                            draft: c.draft.clone(),
                        }];
                        if existing.state != NotifyState::Unread {
                            events.push(InboxEvent::StateSet {
                                ids: vec![existing_id],
                                state: NotifyState::Unread,
                            });
                        }
                        return Ok(events);
                    }
                    if matches!(c.draft.kind, NotifyKind::Interaction { .. })
                        && matches!(existing.state, NotifyState::Done | NotifyState::Dismissed)
                    {
                        return Ok(vec![]);
                    }
                    if is_resolved_status {
                        if matches!(existing.state, NotifyState::Done | NotifyState::Dismissed)
                            && drafts_equal(&existing.draft, &c.draft)
                        {
                            return Ok(vec![]);
                        }
                        let mut events = Vec::new();
                        if !drafts_equal(&existing.draft, &c.draft) {
                            events.push(InboxEvent::Updated {
                                id: existing_id,
                                rank: fresh_rank,
                                draft: c.draft.clone(),
                            });
                        }
                        if existing.state != NotifyState::Done {
                            events.push(InboxEvent::StateSet {
                                ids: vec![existing_id],
                                state: NotifyState::Done,
                            });
                        }
                        return Ok(events);
                    }
                    if is_resolvable_status_kind {
                        if drafts_equal(&existing.draft, &c.draft) {
                            return Ok(vec![]);
                        }
                        let should_reopen = match existing.state {
                            NotifyState::Done => true,
                            NotifyState::Dismissed => match (&existing.draft.kind, &c.draft.kind) {
                                (
                                    NotifyKind::MailUnrung {
                                        unread_count: prev_n,
                                        oldest_at: prev_at,
                                        ..
                                    },
                                    NotifyKind::MailUnrung {
                                        unread_count: new_n,
                                        oldest_at: new_at,
                                        ..
                                    },
                                ) => new_n > prev_n || new_at > prev_at,
                                (
                                    NotifyKind::TransportDown { detail: prev_d, .. },
                                    NotifyKind::TransportDown { detail: new_d, .. },
                                )
                                | (
                                    NotifyKind::OrgConflict { detail: prev_d, .. },
                                    NotifyKind::OrgConflict { detail: new_d, .. },
                                ) => prev_d != new_d,
                                _ => false,
                            },
                            NotifyState::Unread | NotifyState::Seen => false,
                        };
                        let mut events = vec![InboxEvent::Updated {
                            id: existing_id,
                            rank: fresh_rank,
                            draft: c.draft.clone(),
                        }];
                        if should_reopen {
                            events.push(InboxEvent::StateSet {
                                ids: vec![existing_id],
                                state: NotifyState::Unread,
                            });
                        }
                        return Ok(events);
                    }
                    let mut events = Vec::new();
                    if !drafts_equal(&existing.draft, &c.draft) {
                        events.push(InboxEvent::Updated {
                            id: existing_id,
                            rank: fresh_rank,
                            draft: c.draft.clone(),
                        });
                    }
                    if is_resolved_interaction && existing.state != NotifyState::Done {
                        events.push(InboxEvent::StateSet {
                            ids: vec![existing_id],
                            state: NotifyState::Done,
                        });
                    }
                    return Ok(events);
                }
                if is_resolved_interaction || is_resolved_status {
                    return Ok(vec![]);
                }
                if let NotifyKind::WorkerStuck { seq: new_seq, .. } = &c.draft.kind {
                    let prefix = format!("{}:", c.draft.dedupe);
                    let mut best_legacy: Option<(Seq, NotificationId)> = None;
                    for (k, &id) in state.by_dedupe.range(prefix.clone()..) {
                        let Some(rest) = k.strip_prefix(&prefix) else {
                            break;
                        };
                        if let Ok(parsed) = rest.parse::<u64>() {
                            let seq = Seq(parsed);
                            if best_legacy.is_none_or(|(prev, _)| seq > prev) {
                                best_legacy = Some((seq, id));
                            }
                        }
                    }
                    if let Some((legacy_seq, legacy_id)) = best_legacy
                        && let Some(existing) = state.items.get(&legacy_id)
                    {
                        if *new_seq <= legacy_seq {
                            return Ok(vec![]);
                        }
                        let mut events = vec![InboxEvent::Updated {
                            id: legacy_id,
                            rank: fresh_rank,
                            draft: c.draft.clone(),
                        }];
                        if existing.state != NotifyState::Unread {
                            events.push(InboxEvent::StateSet {
                                ids: vec![legacy_id],
                                state: NotifyState::Unread,
                            });
                        }
                        return Ok(events);
                    }
                }
                let id = NotificationId(state.next_id + 1);
                Ok(vec![InboxEvent::Raised {
                    id,
                    rank: fresh_rank,
                    draft: c.draft.clone(),
                }])
            }

            InboxCommand::InboxAck(c) => {
                let mut seen = BTreeSet::new();
                let mut changed = Vec::new();
                for id in &c.ids {
                    if seen.insert(*id)
                        && let Some(existing) = state.items.get(id)
                        && existing.state != c.state
                    {
                        changed.push(*id);
                    }
                }
                if changed.is_empty() {
                    Ok(vec![])
                } else {
                    Ok(vec![InboxEvent::StateSet {
                        ids: changed,
                        state: c.state,
                    }])
                }
            }

            InboxCommand::InboxMarkRead(c) => {
                let mut events = Vec::new();
                let current = state.cursors.get(&c.room).copied().unwrap_or(Seq::ORIGIN);
                if c.upto > current {
                    events.push(InboxEvent::CursorAdvanced {
                        room: c.room.clone(),
                        upto: c.upto,
                    });
                }
                let seen_ids: Vec<NotificationId> = state
                    .items
                    .values()
                    .filter(|n| {
                        n.state == NotifyState::Unread
                            && matches!(
                                &n.draft.kind,
                                NotifyKind::Mention { room, seq }
                                    | NotifyKind::DirectMessage { room, seq }
                                    if room == &c.room && *seq <= c.upto
                            )
                    })
                    .map(|n| n.id)
                    .collect();
                if !seen_ids.is_empty() {
                    events.push(InboxEvent::StateSet {
                        ids: seen_ids,
                        state: NotifyState::Seen,
                    });
                }
                Ok(events)
            }

            InboxCommand::SetArchived(c) => {
                if state.archived.contains(&c.target) == c.archived {
                    Ok(vec![])
                } else {
                    Ok(vec![InboxEvent::ArchivedSet {
                        target: c.target.clone(),
                        archived: c.archived,
                    }])
                }
            }

            InboxCommand::InboxList(_) => Ok(vec![]),
        }
    }

    fn evolve(state: &mut InboxState, event: &InboxEvent, meta: &EntryMeta) {
        state.head = meta.seq;
        if meta.at > state.latest_at {
            state.latest_at = meta.at;
        }
        match event {
            InboxEvent::Raised { id, rank, draft } => {
                state.next_id = state.next_id.max(id.0);
                state.by_dedupe.insert(draft.dedupe.clone(), *id);
                state.items.insert(
                    *id,
                    Notification {
                        id: *id,
                        at: meta.at,
                        rank: *rank,
                        state: NotifyState::Unread,
                        draft: draft.clone(),
                    },
                );
            }
            InboxEvent::Updated { id, rank, draft } => {
                let (prev_state, prev_at) = state
                    .items
                    .get(id)
                    .map_or((NotifyState::Unread, meta.at), |n| (n.state, n.at));
                let next_at =
                    if matches!(
                        draft.kind,
                        NotifyKind::WorkerStuck { .. } | NotifyKind::WorkerRefused { .. }
                    ) || (matches!(
                        draft.kind,
                        NotifyKind::MailUnrung { .. }
                            | NotifyKind::TransportDown { .. }
                            | NotifyKind::OrgConflict { .. }
                    ) && matches!(prev_state, NotifyState::Done | NotifyState::Dismissed))
                    {
                        meta.at
                    } else {
                        prev_at
                    };
                if let Some(old) = state.items.get(id)
                    && old.draft.dedupe != draft.dedupe
                {
                    state.by_dedupe.remove(&old.draft.dedupe);
                }
                state.by_dedupe.insert(draft.dedupe.clone(), *id);
                state.items.insert(
                    *id,
                    Notification {
                        id: *id,
                        at: next_at,
                        rank: *rank,
                        state: prev_state,
                        draft: draft.clone(),
                    },
                );
            }
            InboxEvent::StateSet {
                ids,
                state: next_state,
            } => {
                for id in ids {
                    if let Some(item) = state.items.get_mut(id) {
                        item.state = *next_state;
                    }
                }
            }
            InboxEvent::CursorAdvanced { room, upto } => {
                let cur = state.cursors.entry(room.clone()).or_default();
                if *cur < *upto {
                    *cur = *upto;
                }
            }
            InboxEvent::ArchivedSet { target, archived } => {
                if *archived {
                    state.archived.insert(target.clone());
                } else {
                    state.archived.remove(target);
                }
            }
        }
    }

    fn reply(state: &InboxState, cmd: InboxCommand, _committed: &[Envelope]) -> WireReply {
        match cmd {
            InboxCommand::InboxList(q) => {
                let unread = state
                    .items
                    .values()
                    .filter(|n| n.state == NotifyState::Unread)
                    .count() as u32;
                let limit = q.limit.unwrap_or(200).clamp(1, 1000) as usize;
                let mut items: Vec<Notification> = state
                    .items
                    .values()
                    .filter(|n| {
                        q.include_done
                            || !matches!(n.state, NotifyState::Done | NotifyState::Dismissed)
                    })
                    .map(|n| {
                        let age_ms = state.latest_at.0.saturating_sub(n.at.0);
                        let mut projected = n.clone();
                        projected.rank = rank(&n.draft.kind, age_ms);
                        projected
                    })
                    .collect();
                items.sort_by(|a, b| (b.rank, b.at, b.id).cmp(&(a.rank, a.at, a.id)));
                let total_count = items.len();
                items.truncate(limit);
                let truncated = items.len() < total_count;
                let cursors = state.cursors.iter().map(|(r, s)| (r.clone(), *s)).collect();
                WireReply::InboxList(InboxPage {
                    head: state.head,
                    items,
                    unread,
                    cursors,
                    total_count,
                    truncated,
                })
            }
            InboxCommand::InboxAck(_) => WireReply::InboxAck(Ack),
            InboxCommand::InboxMarkRead(_) => WireReply::InboxMarkRead(Ack),
            InboxCommand::SetArchived(_) => WireReply::SetArchived(Ack),
            InboxCommand::InboxRaise(_) => WireReply::InboxRaise(Ack),
        }
    }

    fn wrap(event: InboxEvent) -> Event {
        Event::Inbox(event)
    }

    fn unwrap(event: &Event) -> Option<&InboxEvent> {
        match event {
            Event::Inbox(e) => Some(e),
            _ => None,
        }
    }
}

fn is_resolved_only_draft(draft: &NotificationDraft) -> bool {
    (matches!(draft.kind, NotifyKind::Interaction { .. }) && draft.actions.is_empty())
        || matches!(
            &draft.kind,
            NotifyKind::MailUnrung {
                unread_count: 0,
                ..
            }
        )
        || matches!(
            &draft.kind,
            NotifyKind::TransportDown { detail, .. }
                | NotifyKind::OrgConflict { detail, .. }
                if detail.is_empty()
        )
}

#[derive(Default)]
struct ReplayBuffer {
    items: std::collections::VecDeque<(Principal, NotificationDraft, Option<NotificationDraft>)>,
    index: BTreeMap<(Principal, String), usize>,
    popped: usize,
}

impl ReplayBuffer {
    fn upsert(&mut self, to: Principal, draft: NotificationDraft, has_persisted_card: bool) {
        let key = (to.clone(), draft.dedupe.clone());
        if let Some(&abs_idx) = self.index.get(&key)
            && let Some(rel_idx) = abs_idx.checked_sub(self.popped)
            && let Some(slot) = self.items.get_mut(rel_idx)
        {
            if matches!(draft.kind, NotifyKind::Interaction { .. }) {
                if is_resolved_only_draft(&slot.1) || slot.2.is_some() {
                    return;
                }
                if is_resolved_only_draft(&draft) {
                    slot.2 = Some(draft);
                } else {
                    slot.1 = draft;
                }
                return;
            }
            let is_status_kind = matches!(
                draft.kind,
                NotifyKind::MailUnrung { .. }
                    | NotifyKind::TransportDown { .. }
                    | NotifyKind::OrgConflict { .. }
            );
            if is_status_kind {
                if is_resolved_only_draft(&draft) {
                    if has_persisted_card || is_resolved_only_draft(&slot.1) {
                        slot.1 = draft;
                        slot.2 = None;
                    } else {
                        slot.2 = Some(draft);
                    }
                } else {
                    slot.1 = draft;
                    slot.2 = None;
                }
                return;
            }
            slot.1 = draft;
            slot.2 = None;
            return;
        }
        if !has_persisted_card && is_resolved_only_draft(&draft) {
            return;
        }
        let abs_idx = self.popped + self.items.len();
        self.index.insert(key, abs_idx);
        self.items.push_back((to, draft, None));
    }

    fn peek(&self) -> Option<(Principal, NotificationDraft)> {
        self.items
            .front()
            .map(|(to, draft, _)| (to.clone(), draft.clone()))
    }

    fn pop_committed(&mut self) {
        if let Some(front) = self.items.front_mut()
            && let Some(res) = front.2.take()
        {
            front.1 = res;
            return;
        }
        if let Some((to, draft, _)) = self.items.pop_front() {
            let key = (to, draft.dedupe);
            if self.index.get(&key) == Some(&self.popped) {
                self.index.remove(&key);
            }
            self.popped += 1;
        }
        if self.items.is_empty() {
            self.index.clear();
            self.popped = 0;
        }
    }
}

#[derive(Default)]
struct GraceSignal {
    notified: std::sync::atomic::AtomicBool,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
}

impl GraceSignal {
    fn notify(&self) {
        self.notified
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(w) = self.waker.lock().expect("grace waker lock").take() {
            w.wake();
        }
    }

    fn notified(&self) -> impl Future<Output = ()> + Send + '_ {
        std::future::poll_fn(|cx| {
            if self
                .notified
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return std::task::Poll::Ready(());
            }
            let mut slot = self.waker.lock().expect("grace waker lock");
            if self
                .notified
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return std::task::Poll::Ready(());
            }
            *slot = Some(cx.waker().clone());
            std::task::Poll::Pending
        })
    }
}

/// Cloneable handle to a running [`Herald`] reactor's view, allowing a daemon
/// timer task to query [`HeraldHandle::next_grace_deadline`] and call
/// [`HeraldHandle::flush_due_at`] after `spawn_reactor` takes ownership of `Herald`.
#[derive(Clone)]
pub struct HeraldHandle {
    view: std::sync::Arc<std::sync::Mutex<HeraldView>>,
    grace_signal: std::sync::Arc<GraceSignal>,
}

impl HeraldHandle {
    pub fn next_grace_deadline(&self, now: UnixMs) -> Option<UnixMs> {
        self.view
            .lock()
            .expect("herald view lock")
            .next_grace_deadline(now)
    }

    /// Resolves when [`Herald`] updates its grace view after a committed event or replay catch-up.
    pub fn notified(&self) -> impl Future<Output = ()> + Send + '_ {
        self.grace_signal.notified()
    }

    pub fn flush_due_at(
        &self,
        now: UnixMs,
        client: &LocalClient,
    ) -> impl Future<Output = Result<(), ApiError>> + Send + use<> {
        let pending = self
            .view
            .lock()
            .expect("herald view lock")
            .pending_mail_unrung_transitions(now);
        let view = self.view.clone();
        let client = client.clone();
        async move {
            for (seat, next_state, raises) in pending {
                for (to, draft) in raises {
                    client.call(InboxRaise { to, draft }).await?;
                }
                let mut guard = view.lock().expect("herald view lock");
                match next_state {
                    Some(state) => {
                        guard.raised_mail_unrung.insert(seat, state);
                    }
                    None => {
                        guard.raised_mail_unrung.remove(&seat);
                    }
                }
            }
            Ok(())
        }
    }
}

pub struct Herald {
    view: std::sync::Arc<std::sync::Mutex<HeraldView>>,
    grace_signal: std::sync::Arc<GraceSignal>,
    caught_up: bool,
    replay: std::sync::Arc<std::sync::Mutex<ReplayBuffer>>,
}

impl Herald {
    pub fn new() -> Self {
        Self::with_grace(std::time::Duration::from_millis(RECONNECT_GRACE_MS))
    }

    /// A [`Herald`] whose `MailUnrung` reconnect grace window is `grace`
    /// instead of [`RECONNECT_GRACE_MS`].
    pub fn with_grace(grace: std::time::Duration) -> Self {
        let grace_ms = u64::try_from(grace.as_millis()).expect("grace window fits in u64 ms");
        assert!(grace_ms > 0, "reconnect grace window must be positive");
        let view = HeraldView {
            grace: GraceMs(grace_ms),
            ..HeraldView::default()
        };
        Herald {
            view: std::sync::Arc::new(std::sync::Mutex::new(view)),
            grace_signal: std::sync::Arc::new(GraceSignal::default()),
            caught_up: false,
            replay: std::sync::Arc::new(std::sync::Mutex::new(ReplayBuffer::default())),
        }
    }

    /// Names the house's operator surface seat: letters to it never raise
    /// `MailUnrung` cards. Defaults to [`presence::DEFAULT_OPERATOR_SURFACE_SEAT`].
    pub fn with_operator_surface_seat(self, seat: SeatName) -> Self {
        self.view
            .lock()
            .expect("herald view lock")
            .operator_surface_seat = Some(seat);
        self
    }

    /// Constructs a [`Herald`] paired with a [`HeraldHandle`] so a timer task
    /// can flush reconnect grace deadlines while `spawn_reactor` owns `Herald`.
    pub fn with_handle() -> (Self, HeraldHandle) {
        let herald = Self::new();
        let handle = herald.handle();
        (herald, handle)
    }

    pub fn handle(&self) -> HeraldHandle {
        HeraldHandle {
            view: self.view.clone(),
            grace_signal: self.grace_signal.clone(),
        }
    }

    /// Evaluates pending `MailUnrung` reconnect grace windows at `now` and raises
    /// any newly due informational cards through `client`.
    pub fn flush_due_at(
        &mut self,
        now: UnixMs,
        client: &LocalClient,
    ) -> impl Future<Output = Result<(), ApiError>> + Send + use<> {
        self.handle().flush_due_at(now, client)
    }

    pub fn next_grace_deadline(&self, now: UnixMs) -> Option<UnixMs> {
        self.handle().next_grace_deadline(now)
    }
}

impl Default for Herald {
    fn default() -> Self {
        Self::new()
    }
}

impl Reactor for Herald {
    const NAME: &'static str = "herald";

    fn interest(&self) -> Interest {
        Interest::Everything
    }

    fn react(
        &mut self,
        frame: Frame,
        client: &LocalClient,
    ) -> impl Future<Output = Result<(), ApiError>> + Send {
        let mut live_raises = Vec::new();
        let mut flush_replay = false;
        match frame {
            Frame::Committed(env) => {
                if self.caught_up {
                    {
                        let mut view = self.view.lock().expect("herald view lock");
                        view.observe(&env);
                        live_raises = classify(&env, &view);
                    }
                    self.grace_signal.notify();
                    flush_replay = true;
                } else {
                    let drafts: Vec<(Principal, NotificationDraft, bool)> = {
                        let mut view = self.view.lock().expect("herald view lock");
                        view.observe_replay(&env);
                        classify(&env, &view)
                            .into_iter()
                            .map(|(to, draft)| {
                                let has_persisted =
                                    view.has_persisted_inbox_card(&to, &draft.dedupe);
                                (to, draft, has_persisted)
                            })
                            .collect()
                    };
                    let mut buf = self.replay.lock().expect("herald replay buffer");
                    for (to, draft, has_persisted) in drafts {
                        buf.upsert(to, draft, has_persisted);
                    }
                }
            }
            Frame::CaughtUp { .. } => {
                self.caught_up = true;
                let due: Vec<(Principal, NotificationDraft, bool)> = {
                    let mut view = self.view.lock().expect("herald view lock");
                    view.reconcile_unrung_after_replay();
                    let now = view.latest_at;
                    view.due_mail_unrung(now)
                        .into_iter()
                        .map(|(to, draft)| {
                            let has_persisted = view.has_persisted_inbox_card(&to, &draft.dedupe);
                            (to, draft, has_persisted)
                        })
                        .collect()
                };
                self.grace_signal.notify();
                if !due.is_empty() {
                    let mut buf = self.replay.lock().expect("herald replay buffer");
                    for (to, draft, has_persisted) in due {
                        buf.upsert(to, draft, has_persisted);
                    }
                }
                flush_replay = true;
            }
            _ => {}
        }
        let view = self.view.clone();
        let replay = self.replay.clone();
        let client = client.clone();
        async move {
            if flush_replay {
                loop {
                    let next = replay.lock().expect("herald replay buffer").peek();
                    let Some((to, draft)) = next else { break };
                    client.call(InboxRaise { to, draft }).await?;
                    replay.lock().expect("herald replay buffer").pop_committed();
                }
            }
            for (to, draft) in live_raises {
                let mail_unrung_seat = match &draft.kind {
                    NotifyKind::MailUnrung { seat, .. } => Some(seat.clone()),
                    _ => None,
                };
                if let Err(err) = client.call(InboxRaise { to, draft }).await {
                    if let Some(seat) = mail_unrung_seat {
                        view.lock()
                            .expect("herald view lock")
                            .raised_mail_unrung
                            .remove(&seat);
                    }
                    return Err(err);
                }
            }
            Ok(())
        }
    }
}
