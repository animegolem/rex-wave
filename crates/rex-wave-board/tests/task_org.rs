use rex_kanban::Priority;
use rex_wave_api::notify::{InboxRaise, NotifyKind};
use rex_wave_api::room::*;
use rex_wave_api::*;
use rex_wave_board::task_org::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct FakeRoomState {
    next_seq: u64,
    tasks: BTreeMap<Seq, TaskView>,
    org_ids: BTreeMap<OrgId, Seq>,
    post_idems: BTreeMap<IdemKey, Posted>,
    raised_notifications: Vec<InboxRaise>,
    bump_rev_before_next_update: Option<Seq>,
    create_lock_on_next_room_tasks: Option<PathBuf>,
}

#[derive(Clone)]
struct FakeTaskRoomClient {
    me: Principal,
    state: Arc<Mutex<FakeRoomState>>,
}

impl FakeTaskRoomClient {
    fn new(me: Principal) -> Self {
        Self {
            me,
            state: Arc::new(Mutex::new(FakeRoomState::default())),
        }
    }

    fn bump_task_remotely(
        &self,
        seq: Seq,
        new_title: Option<&str>,
        new_status: Option<TaskStatus>,
    ) {
        let mut g = self.state.lock().unwrap();
        let task = g.tasks.get_mut(&seq).expect("task exists");
        task.rev += 1;
        if let Some(t) = new_title {
            task.title = rex_wave_api::board::Title::parse(t).unwrap();
        }
        if let Some(s) = new_status {
            task.status = s;
            if s != TaskStatus::Claimed {
                task.lease = None;
            }
        }
    }

    fn add_label_remotely(&self, seq: Seq, label: &str) {
        let mut g = self.state.lock().unwrap();
        let task = g.tasks.get_mut(&seq).expect("task exists");
        let lbl = Label::parse(label).unwrap();
        if !task.labels.contains(&lbl) {
            task.labels.push(lbl);
            task.labels.sort();
        }
    }

    fn claim_remotely(&self, seq: Seq, holder: Principal, epoch: u64, expires_at: u64) {
        let mut g = self.state.lock().unwrap();
        let task = g.tasks.get_mut(&seq).expect("task exists");
        task.rev += 1;
        task.status = TaskStatus::Claimed;
        task.lease = Some(TaskLease {
            holder,
            epoch: LeaseEpoch(epoch),
            claimed_at: UnixMs(1_000),
            expires_at: UnixMs(expires_at),
        });
        task.ready = false;
    }

    fn heartbeat_remotely(&self, seq: Seq, new_expires_at: u64) {
        let mut g = self.state.lock().unwrap();
        let task = g.tasks.get_mut(&seq).expect("task exists");
        let lease = task.lease.as_mut().expect("active lease");
        lease.expires_at = UnixMs(new_expires_at);
    }

    fn set_race_bump_on_next_update(&self, seq: Seq) {
        self.state.lock().unwrap().bump_rev_before_next_update = Some(seq);
    }

    fn create_lock_on_next_room_tasks(&self, lock_path: PathBuf) {
        self.state.lock().unwrap().create_lock_on_next_room_tasks = Some(lock_path);
    }

    fn task_count(&self) -> usize {
        self.state.lock().unwrap().tasks.len()
    }

    fn task(&self, seq: Seq) -> TaskView {
        self.state.lock().unwrap().tasks[&seq].clone()
    }

    fn notifications(&self) -> Vec<InboxRaise> {
        self.state.lock().unwrap().raised_notifications.clone()
    }
}

impl Client for FakeTaskRoomClient {
    fn me(&self) -> &Principal {
        &self.me
    }

    async fn call_wire(&self, cmd: WireCommand) -> Result<WireReply, ApiError> {
        let mut g = self.state.lock().unwrap();
        match cmd {
            WireCommand::RoomTasks(req) => {
                if let Some(lock_path) = g.create_lock_on_next_room_tasks.take() {
                    let _ = fs::write(&lock_path, "mid-sync-lock");
                }
                let room = req.room.resolve(&self.me)?;
                let tasks: Vec<_> = g.tasks.values().cloned().collect();
                let total_count = tasks.len();
                Ok(WireReply::RoomTasks(TaskListView {
                    room,
                    head: Seq(g.next_seq),
                    tasks,
                    total_count,
                    truncated: false,
                }))
            }
            WireCommand::RoomPost(req) => {
                if let Some(idem) = &req.idem
                    && let Some(prior) = g.post_idems.get(idem)
                {
                    return Ok(WireReply::RoomPost(prior.clone()));
                }
                let room = req.room.resolve(&self.me)?;
                g.next_seq += 1;
                let seq = Seq(g.next_seq);
                let org_id = if let Some(spec) = req.task {
                    let oid = match spec.org_id {
                        Some(oid) => {
                            if g.org_ids.contains_key(&oid) {
                                return Err(ApiError::conflict(format!("duplicate org_id {oid}")));
                            }
                            oid
                        }
                        None => OrgId::mint_deterministic(&format!("fake:room:task:{}", seq.0)),
                    };
                    g.org_ids.insert(oid.clone(), seq);
                    let mut labels = req.labels;
                    labels.sort();
                    labels.dedup();
                    let ready = spec.needs.iter().all(|dep| {
                        g.tasks
                            .get(dep)
                            .is_some_and(|t| t.status == TaskStatus::Done)
                    });
                    g.tasks.insert(
                        seq,
                        TaskView {
                            room: room.clone(),
                            seq,
                            org_id: oid.clone(),
                            rev: 1,
                            title: spec.title,
                            status: TaskStatus::Open,
                            priority: spec.priority.unwrap_or(Priority::Normal),
                            due_ms: spec.due_ms,
                            needs: spec.needs,
                            labels,
                            lease: None,
                            ready,
                            lease_expired: false,
                            created_by: self.me.clone(),
                            created_at: UnixMs(1_000),
                            updated_at: UnixMs(1_000),
                        },
                    );
                    Some(oid)
                } else {
                    None
                };
                let posted = Posted {
                    room,
                    seq,
                    mentions: Mentions::default(),
                    re: req.re,
                    org_id,
                    delivery_notice: None,
                };
                if let Some(idem) = req.idem {
                    g.post_idems.insert(idem, posted.clone());
                }
                Ok(WireReply::RoomPost(posted))
            }
            WireCommand::TaskUpdate(req) => {
                if g.bump_rev_before_next_update == Some(req.task) {
                    g.bump_rev_before_next_update = None;
                    if let Some(t) = g.tasks.get_mut(&req.task) {
                        t.rev += 1;
                    }
                }
                let Some(existing) = g.tasks.get(&req.task).cloned() else {
                    return Err(ApiError::NotFound {
                        what: format!("task {}", req.task.0),
                    });
                };
                if let Some(expected) = req.expected_rev
                    && expected != existing.rev
                {
                    return Err(ApiError::conflict_with(
                        format!("rev conflict: expected {expected}, actual {}", existing.rev),
                        ConflictData::RevisionConflict {
                            expected,
                            actual: existing.rev,
                        },
                    ));
                }
                let task = g.tasks.get_mut(&req.task).unwrap();
                task.rev += 1;
                if let Some(t) = req.title {
                    task.title = t;
                }
                if !req.needs_remove.is_empty() {
                    task.needs.retain(|d| !req.needs_remove.contains(d));
                }
                for dep in req.needs_add {
                    if !task.needs.contains(&dep) {
                        task.needs.push(dep);
                    }
                }
                if let Some(p) = req.priority {
                    task.priority = p;
                }
                if req.clear_due {
                    task.due_ms = None;
                } else if let Some(d) = req.due_ms {
                    task.due_ms = Some(d);
                }
                Ok(WireReply::TaskUpdate(task.clone()))
            }
            WireCommand::TaskClaim(req) => {
                let room = req.room.resolve(&self.me)?;
                let Some(existing) = g.tasks.get(&req.task).cloned() else {
                    return Err(ApiError::NotFound {
                        what: format!("task {}", req.task.0),
                    });
                };
                if let Some(expected) = req.expected_rev
                    && expected != existing.rev
                {
                    return Err(ApiError::conflict_with(
                        "rev conflict",
                        ConflictData::RevisionConflict {
                            expected,
                            actual: existing.rev,
                        },
                    ));
                }
                if existing.status == TaskStatus::Claimed
                    && let Some(lease) = &existing.lease
                {
                    return Err(ApiError::conflict_with(
                        "actively claimed",
                        ConflictData::LeaseFenced {
                            expected: LeaseEpoch(0),
                            actual: lease.epoch,
                            holder: Some(lease.holder.clone()),
                        },
                    ));
                }
                let unmet: Vec<Seq> = existing
                    .needs
                    .iter()
                    .copied()
                    .filter(|dep| {
                        g.tasks
                            .get(dep)
                            .is_none_or(|t| t.status != TaskStatus::Done)
                    })
                    .collect();
                if !unmet.is_empty() && !req.force {
                    return Err(ApiError::conflict_with(
                        format!("unmet needs: {unmet:?}"),
                        ConflictData::UnmetNeeds { unmet },
                    ));
                }
                let task = g.tasks.get_mut(&req.task).unwrap();
                task.rev += 1;
                task.status = TaskStatus::Claimed;
                let epoch = task
                    .lease
                    .as_ref()
                    .map_or(LeaseEpoch(1), |l| l.epoch.next());
                let expires_at = UnixMs(100_000);
                task.lease = Some(TaskLease {
                    holder: self.me.clone(),
                    epoch,
                    claimed_at: UnixMs(2_000),
                    expires_at,
                });
                Ok(WireReply::TaskClaim(TaskLeaseReceipt {
                    room,
                    task: req.task,
                    org_id: task.org_id.clone(),
                    rev: task.rev,
                    epoch,
                    expires_at,
                }))
            }
            WireCommand::TaskTransition(req) => {
                let Some(task) = g.tasks.get_mut(&req.task) else {
                    return Err(ApiError::NotFound {
                        what: format!("task {}", req.task.0),
                    });
                };
                if let Some(expected) = req.expected_rev
                    && expected != task.rev
                {
                    return Err(ApiError::conflict_with(
                        "rev conflict",
                        ConflictData::RevisionConflict {
                            expected,
                            actual: task.rev,
                        },
                    ));
                }
                task.rev += 1;
                task.status = req.status;
                if req.status != TaskStatus::Claimed {
                    task.lease = None;
                }
                Ok(WireReply::TaskTransition(task.clone()))
            }
            WireCommand::RoomLabel(req) => {
                let room = req.room.resolve(&self.me)?;
                let Some(task) = g.tasks.get_mut(&req.seq) else {
                    return Err(ApiError::NotFound {
                        what: format!("task {}", req.seq.0),
                    });
                };
                let mut set: BTreeSet<Label> = task.labels.iter().cloned().collect();
                for r in &req.remove {
                    set.remove(r);
                }
                for a in &req.add {
                    set.insert(a.clone());
                }
                task.labels = set.into_iter().collect();
                Ok(WireReply::RoomLabel(Labeled {
                    room,
                    seq: req.seq,
                    labels: task.labels.clone(),
                }))
            }
            WireCommand::InboxRaise(req) => {
                g.raised_notifications.push(req);
                Ok(WireReply::InboxRaise(Ack))
            }
            other => Err(ApiError::Internal(format!("unexpected cmd {other:?}"))),
        }
    }

    async fn watch(&self, _interest: Interest, _from: Cursor) -> Result<FrameStream, ApiError> {
        Err(ApiError::Unavailable {
            family: Family::Read,
        })
    }
}

fn operator(name: &str) -> Principal {
    Principal::Operator {
        seat: SeatName::parse(name).unwrap(),
    }
}

fn tasks_room() -> RoomRef {
    RoomRef::Channel {
        scope: RoomScope::Global,
        name: ChannelName::parse("sprint").unwrap(),
    }
}

#[test]
fn emacs_clause_b4_lock_and_autosave_detection() {
    assert!(is_emacs_lock_or_autosave(Path::new("/tmp/.#tasks.org")));
    assert!(is_emacs_lock_or_autosave(Path::new("/tmp/#tasks.org#")));
    assert!(!is_emacs_lock_or_autosave(Path::new("/tmp/tasks.org")));
    assert!(!is_emacs_lock_or_autosave(Path::new("/tmp/.gitignore")));
    assert_eq!(
        emacs_lock_path(Path::new("/tmp/notes/tasks.org")),
        Some(Path::new("/tmp/notes/.#tasks.org").to_path_buf())
    );
}

#[tokio::test]
async fn two_way_org_sync_preserves_emacs_ids_drawers_prose_and_honors_dangling_lock_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let org_path = dir.path().join("sprint.org");
    let client = FakeTaskRoomClient::new(operator("owner"));
    let room = tasks_room();
    let mut state = OrgSyncState::default();

    let initial_emacs = "\
#+TITLE: #sprint
#+STARTUP: overview
#+CATEGORY: house-ops

* TODO [#A] Ship Org reconciler :backend:area%3Dcore:
:PROPERTIES:
:ID: src-bush1951
:EFFORT: 2h
:OWNER_NOTE: keep this exact custom value
:END:
Initial design notes from Emacs.

** Subheading checklist
- [ ] verify Clause B-4 symlink

* TODO Follow-up docs
:PROPERTIES:
:REX_NEEDS: [[id:src-bush1951][Bush 1951]]
:END:
Body prose on newly added heading without an :ID:.
";
    fs::write(&org_path, initial_emacs).unwrap();

    assert_eq!(
        sync_org_to_room(
            &client,
            &room,
            &dir.path().join(".#sprint.org"),
            &mut state,
            &OrgSyncOptions::default(),
        )
        .await
        .unwrap(),
        OrgToRoomOutcome::IgnoredEphemeral
    );
    assert_eq!(
        sync_room_to_org(&client, &room, &dir.path().join("#sprint.org#"), &mut state)
            .await
            .unwrap(),
        RoomToOrgOutcome::IgnoredEphemeral
    );

    let outcome = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    let OrgToRoomOutcome::Synced {
        created,
        wrote_back,
        ..
    } = outcome
    else {
        panic!("expected Synced, got {outcome:?}");
    };
    assert!(wrote_back);
    assert_eq!(created.len(), 2);
    assert_eq!(created[0].as_str(), "src-bush1951");
    assert!(
        created[1].is_rex_minted(),
        "heading without :ID: adopts room-minted rw-<hex> OrgId"
    );

    let after_push = fs::read_to_string(&org_path).unwrap();
    assert!(after_push.contains("#+STARTUP: overview\n#+CATEGORY: house-ops\n"));
    assert!(after_push.contains(":ID: src-bush1951\n:REX_REV: 1\n:REX_SEQ: 1\n"));
    assert!(after_push.contains(":EFFORT: 2h\n:OWNER_NOTE: keep this exact custom value\n"));
    assert!(after_push.contains("** Subheading checklist\n- [ ] verify Clause B-4 symlink\n"));
    assert!(after_push.contains(":backend:area%3Dcore:"));
    assert!(after_push.contains(&format!(
        ":ID: {}\n:REX_REV: 1\n:REX_NEEDS: src-bush1951\n:REX_SEQ: 2\n",
        created[1]
    )));

    let worker_p = Principal::Worker {
        wave: WaveSlug::parse("w3").unwrap(),
        role: RoleName::parse("author").unwrap(),
    };
    client.claim_remotely(Seq(1), worker_p, 1, 999_000);

    let lock_path = emacs_lock_path(&org_path).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("golem@mac.99999:1700000000", &lock_path).unwrap();
    #[cfg(not(unix))]
    fs::write(&lock_path, "lock").unwrap();

    assert!(
        has_emacs_lock(&org_path),
        "has_emacs_lock must detect dangling Emacs lock symlinks via lstat"
    );
    let deferred_write = sync_room_to_org(&client, &room, &org_path, &mut state)
        .await
        .unwrap();
    assert_eq!(
        deferred_write,
        RoomToOrgOutcome::DeferredLock {
            lock_path: lock_path.clone()
        }
    );
    assert_eq!(
        fs::read_to_string(&org_path).unwrap(),
        after_push,
        "locked .org file is never modified while .#<file> exists"
    );
    let deferred_read = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        deferred_read,
        OrgToRoomOutcome::DeferredLock {
            lock_path: lock_path.clone()
        }
    );

    fs::remove_file(&lock_path).unwrap();
    let written = sync_room_to_org(&client, &room, &org_path, &mut state)
        .await
        .unwrap();
    assert_eq!(written, RoomToOrgOutcome::Written { tasks_synced: 2 });

    let after_pull = fs::read_to_string(&org_path).unwrap();
    assert!(after_pull.contains("* STRT [#A] Ship Org reconciler :backend:area%3Dcore:"));
    assert!(after_pull.contains(":REX_REV: 2\n"));
    assert!(after_pull.contains(":REX_EPOCH: 1\n"));
    assert!(after_pull.contains(":REX_LEASE: @w3/author\n"));
    assert!(after_pull.contains(":EFFORT: 2h\n:OWNER_NOTE: keep this exact custom value\n"));
    assert!(after_pull.contains("** Subheading checklist\n- [ ] verify Clause B-4 symlink\n"));

    client.heartbeat_remotely(Seq(1), 1_500_000);
    let after_hb = sync_room_to_org(&client, &room, &org_path, &mut state)
        .await
        .unwrap();
    assert_eq!(
        after_hb,
        RoomToOrgOutcome::Unchanged,
        "worker heartbeat extending expires_at must not churn the .org file"
    );

    let locally_edited = after_pull.replace(
        "* STRT [#A] Ship Org reconciler",
        "* DONE [#A] Ship Org reconciler cleanly",
    );
    fs::write(&org_path, &locally_edited).unwrap();
    let dirty_defer = sync_room_to_org(&client, &room, &org_path, &mut state)
        .await
        .unwrap();
    assert_eq!(dirty_defer, RoomToOrgOutcome::DeferredLocalEdits);
    assert_eq!(fs::read_to_string(&org_path).unwrap(), locally_edited);

    let pushed_edit = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    let OrgToRoomOutcome::Synced {
        updated,
        transitioned,
        wrote_back,
        ..
    } = pushed_edit
    else {
        panic!("expected Synced after clean local edit, got {pushed_edit:?}");
    };
    assert_eq!(updated, vec![OrgId::parse("src-bush1951").unwrap()]);
    assert_eq!(transitioned, vec![OrgId::parse("src-bush1951").unwrap()]);
    assert!(wrote_back);

    let final_disk = fs::read_to_string(&org_path).unwrap();
    assert!(final_disk.contains("* DONE [#A] Ship Org reconciler cleanly"));
    assert!(final_disk.contains(":REX_REV: 4\n"));
    assert!(!final_disk.contains(":REX_EPOCH:"));
    assert!(!final_disk.contains(":REX_LEASE:"));
    assert!(final_disk.contains("** Subheading checklist\n- [ ] verify Clause B-4 symlink\n"));
}

#[tokio::test]
async fn loud_revision_conflict_refuses_overwrite_writes_sidecar_and_notifies() {
    let dir = tempfile::tempdir().unwrap();
    let org_path = dir.path().join("conflict.org");
    let client = FakeTaskRoomClient::new(operator("owner"));
    let room = tasks_room();
    let mut state = OrgSyncState::default();

    fs::write(
        &org_path,
        "\
#+TITLE: #sprint

* TODO Task one
:PROPERTIES:
:ID: task-one
:END:

* TODO Task two
:PROPERTIES:
:ID: task-two
:END:
",
    )
    .unwrap();
    sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();

    client.bump_task_remotely(Seq(1), Some("Task one remote"), Some(TaskStatus::Review));

    let disk_v1 = fs::read_to_string(&org_path).unwrap();
    let edited_task_two = disk_v1.replace("* TODO Task two", "* TODO Task two edited");
    fs::write(&org_path, &edited_task_two).unwrap();
    let non_conflicting = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert!(matches!(non_conflicting, OrgToRoomOutcome::Synced { .. }));
    let disk_v2 = fs::read_to_string(&org_path).unwrap();
    assert!(disk_v2.contains("* WAIT Task one remote"));
    assert!(disk_v2.contains("* TODO Task two edited"));

    client.bump_task_remotely(Seq(1), Some("Task one rev3"), None);
    let stale_emacs_edit = disk_v2.replace("* WAIT Task one remote", "* DONE Task one local edit");
    fs::write(&org_path, &stale_emacs_edit).unwrap();

    let options = OrgSyncOptions {
        write_conflict_sidecar: true,
        notify_principal: Some(operator("owner")),
    };
    let conflict_res = sync_org_to_room(&client, &room, &org_path, &mut state, &options)
        .await
        .unwrap();
    let OrgToRoomOutcome::Conflict {
        applied,
        conflicts,
        sidecar_path,
    } = conflict_res
    else {
        panic!("expected Conflict, got {conflict_res:?}");
    };
    assert!(applied.is_empty());
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].org_id.as_str(), "task-one");
    assert_eq!(
        conflicts[0].kind,
        OrgConflictKind::RevisionConflict {
            expected: 2,
            actual: 3,
        }
    );
    assert_eq!(
        fs::read_to_string(&org_path).unwrap(),
        stale_emacs_edit,
        "on RevisionConflict, .org file must NEVER be overwritten"
    );

    let sidecar = sidecar_path.expect("conflict sidecar written");
    assert_eq!(sidecar, conflict_sidecar_path(&org_path));
    let sidecar_text = fs::read_to_string(&sidecar).unwrap();
    assert!(sidecar_text.contains("task-one"));
    assert!(sidecar_text.contains("RevisionConflict"));

    let raised = client.notifications();
    assert_eq!(raised.len(), 1);
    assert!(matches!(
        &raised[0].draft.kind,
        NotifyKind::OrgConflict { org_id, .. } if org_id.as_str() == "task-one"
    ));

    let race_path = dir.path().join("race.org");
    let mut race_state = OrgSyncState::default();
    fs::write(
        &race_path,
        "* TODO Race task\n:PROPERTIES:\n:ID: race-1\n:END:\n",
    )
    .unwrap();
    sync_org_to_room(
        &client,
        &room,
        &race_path,
        &mut race_state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    let race_before = fs::read_to_string(&race_path).unwrap();
    let race_edited = race_before.replace("Race task", "Race task local");
    fs::write(&race_path, &race_edited).unwrap();
    client.set_race_bump_on_next_update(Seq(3));

    let race_res = sync_org_to_room(
        &client,
        &room,
        &race_path,
        &mut race_state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        race_res,
        OrgToRoomOutcome::Conflict { ref conflicts, .. }
            if conflicts.len() == 1
                && matches!(
                    conflicts[0].kind,
                    OrgConflictKind::RevisionConflict {
                        expected: 1,
                        actual: 2
                    }
                )
    ));
    assert_eq!(
        fs::read_to_string(&race_path).unwrap(),
        race_edited,
        "mid-flight server RevisionConflict also refuses to overwrite .org"
    );
}

#[tokio::test]
async fn topological_new_task_creation_and_cycle_rejection_in_org_sync() {
    let dir = tempfile::tempdir().unwrap();
    let org_path = dir.path().join("dag.org");
    let client = FakeTaskRoomClient::new(operator("owner"));
    let room = tasks_room();
    let mut state = OrgSyncState::default();

    fs::write(
        &org_path,
        "\
#+TITLE: #sprint

* TODO Child task
:PROPERTIES:
:ID: child-1
:REX_NEEDS: parent-1
:END:

* TODO Parent task
:PROPERTIES:
:ID: parent-1
:END:
",
    )
    .unwrap();

    let synced = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    let OrgToRoomOutcome::Synced { created, .. } = synced else {
        panic!("expected topological creation to succeed, got {synced:?}");
    };
    assert_eq!(
        created.iter().map(OrgId::as_str).collect::<Vec<_>>(),
        vec!["parent-1", "child-1"]
    );

    let current = fs::read_to_string(&org_path).unwrap();
    let cyclic = current.replace(
        ":ID: parent-1\n:REX_REV: 1\n",
        ":ID: parent-1\n:REX_REV: 1\n:REX_NEEDS: child-1\n",
    );
    fs::write(&org_path, &cyclic).unwrap();
    let cycle_res = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        cycle_res,
        OrgToRoomOutcome::Conflict { ref conflicts, .. }
            if conflicts.iter().any(|c| c.kind == OrgConflictKind::DependencyCycle)
    ));
    assert_eq!(fs::read_to_string(&org_path).unwrap(), cyclic);
}

#[tokio::test]
async fn partial_apply_and_unid_unmet_needs_conflict_recover_without_duplicates_or_stuck_rev() {
    let dir = tempfile::tempdir().unwrap();
    let org_path = dir.path().join("recover.org");
    let client = FakeTaskRoomClient::new(operator("owner"));
    let room = tasks_room();
    let mut state = OrgSyncState::default();

    fs::write(
        &org_path,
        "\
#+TITLE: #sprint

* TODO Open dependency
:PROPERTIES:
:ID: dep-open
:END:

* TODO Card A
:PROPERTIES:
:ID: card-a
:END:

* TODO Card B
:PROPERTIES:
:ID: card-b
:REX_NEEDS: dep-open
:END:
",
    )
    .unwrap();
    sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(client.task_count(), 3);

    let synced_v1 = fs::read_to_string(&org_path).unwrap();
    let edited = format!(
        "{}\n* STRT New unid task\n:PROPERTIES:\n:REX_NEEDS: dep-open\n:END:\n",
        synced_v1
            .replace("* TODO Card A", "* TODO Card A renamed")
            .replace("* TODO Card B", "* STRT Card B renamed")
    );
    fs::write(&org_path, &edited).unwrap();

    let first_attempt = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    let OrgToRoomOutcome::Conflict {
        applied, conflicts, ..
    } = first_attempt
    else {
        panic!("expected UnmetNeeds conflict, got {first_attempt:?}");
    };
    assert_eq!(client.task_count(), 4);
    assert!(applied.contains(&OrgId::parse("card-a").unwrap()));
    assert!(applied.contains(&OrgId::parse("card-b").unwrap()));
    assert_eq!(client.task(Seq(3)).rev, 2);
    assert_eq!(client.task(Seq(3)).title.as_str(), "Card B renamed");
    assert_eq!(client.task(Seq(3)).status, TaskStatus::Open);
    assert!(
        conflicts
            .iter()
            .all(|c| matches!(c.kind, OrgConflictKind::UnmetNeeds { .. })),
        "both unid STRT task and Card B hit UnmetNeeds: {conflicts:?}"
    );
    assert_eq!(fs::read_to_string(&org_path).unwrap(), edited);

    let retry_while_still_blocked = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        client.task_count(),
        4,
        "retrying after conflict must never post a duplicate of the un-ID'd heading"
    );
    assert!(
        matches!(
            retry_while_still_blocked,
            OrgToRoomOutcome::Conflict { ref conflicts, .. }
                if conflicts.iter().all(|c| matches!(c.kind, OrgConflictKind::UnmetNeeds { .. }))
        ),
        "single-card rename+STRT partial apply must not turn into RevisionConflict: {retry_while_still_blocked:?}"
    );

    client.bump_task_remotely(Seq(1), None, Some(TaskStatus::Done));
    let recovered = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    let OrgToRoomOutcome::Synced {
        created,
        transitioned,
        wrote_back,
        ..
    } = recovered
    else {
        panic!("expected clean recovery after dep-open finished, got {recovered:?}");
    };
    assert!(created.is_empty(), "zero duplicate creations on recovery");
    assert_eq!(client.task_count(), 4);
    assert_eq!(transitioned.len(), 2);
    assert!(wrote_back);

    let final_org = fs::read_to_string(&org_path).unwrap();
    assert!(final_org.contains("* TODO Card A renamed\n:PROPERTIES:\n:ID: card-a\n:REX_REV: 2\n"));
    assert!(final_org.contains("* STRT Card B renamed"));
    assert!(final_org.contains(":ID: card-b\n:REX_REV: 3\n"));
    assert!(final_org.contains("* STRT New unid task"));
    assert!(state.pending_created.is_empty());

    let lock_path = emacs_lock_path(&org_path).unwrap();
    let with_two_unid = format!(
        "{}\n* TODO Zebra lgoin\n\n* TODO Alpha tset\n",
        final_org.replace("* TODO Card A renamed", "* TODO Card A v3")
    );
    fs::write(&org_path, &with_two_unid).unwrap();
    client.create_lock_on_next_room_tasks(lock_path.clone());
    let r_locked = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            r_locked,
            OrgToRoomOutcome::Synced {
                wrote_back: false,
                ..
            }
        ),
        "lock created during sync skips write-back safely: {r_locked:?}"
    );
    assert_eq!(client.task_count(), 6);
    assert_eq!(client.task(Seq(5)).title.as_str(), "Zebra lgoin");
    assert_eq!(client.task(Seq(6)).title.as_str(), "Alpha tset");
    fs::remove_file(&lock_path).unwrap();

    let edited_again = with_two_unid
        .replace("* TODO Card A v3", "* TODO Card A v4")
        .replace("* TODO Zebra lgoin", "* TODO Zebra login")
        .replace("* TODO Alpha tset", "* TODO Alpha test");
    fs::write(&org_path, &edited_again).unwrap();

    let r_converged = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            r_converged,
            OrgToRoomOutcome::Synced {
                ref created,
                ref updated,
                wrote_back: true,
                ..
            } if created.is_empty() && updated.len() == 3
        ),
        "second edit after skipped write-back and multi-heading typo fixes converge cleanly: {r_converged:?}"
    );
    assert_eq!(client.task_count(), 6);
    assert_eq!(client.task(Seq(2)).title.as_str(), "Card A v4");
    assert_eq!(
        client.task(Seq(5)).title.as_str(),
        "Zebra login",
        "positional ordinal pairing keeps first un-ID'd heading on Seq(5) instead of swapping alphabetically"
    );
    assert_eq!(client.task(Seq(6)).title.as_str(), "Alpha test");
}

#[tokio::test]
async fn real_emacs_aligned_id_and_planning_lines_round_trip_without_duplication() {
    let dir = tempfile::tempdir().unwrap();
    let org_path = dir.path().join("emacs-real.org");
    let client = FakeTaskRoomClient::new(operator("owner"));
    let room = tasks_room();
    let mut state = OrgSyncState::default();

    let emacs_initial = "\
#+TITLE: #sprint

* TODO Test task
SCHEDULED: <2026-11-22 Sun> DEADLINE: <2026-11-25 Wed>
:PROPERTIES:
:ID:       E4AFCAD8-9429-42C7-9036-B01F182D9C84
:END:
Body line from Emacs.
";
    fs::write(&org_path, emacs_initial).unwrap();

    let created = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    let OrgToRoomOutcome::Synced {
        created: ids,
        wrote_back,
        ..
    } = created
    else {
        panic!("expected Synced on real Emacs fixture, got {created:?}");
    };
    assert!(wrote_back);
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].as_str(), "E4AFCAD8-9429-42C7-9036-B01F182D9C84");
    assert_eq!(client.task_count(), 1);

    let after_create = fs::read_to_string(&org_path).unwrap();
    assert!(
        after_create.contains(
            "* TODO Test task\nSCHEDULED: <2026-11-22 Sun> DEADLINE: <2026-11-25 Wed>\n:PROPERTIES:\n:ID: E4AFCAD8-9429-42C7-9036-B01F182D9C84\n:REX_REV: 1\n:REX_SEQ: 1\n:END:\nBody line from Emacs.\n"
        ),
        "planning line stays between heading and :PROPERTIES: drawer:\n{after_create}"
    );

    let emacs_closed = "\
#+TITLE: #sprint

* DONE Test task
CLOSED: [2026-11-21 Sat 09:53] DEADLINE: <2026-11-25 Wed> SCHEDULED: <2026-11-22 Sun>
:PROPERTIES:
:ID:       E4AFCAD8-9429-42C7-9036-B01F182D9C84
:REX_REV:  1
:REX_SEQ:  1
:END:
Body line from Emacs.
";
    fs::write(&org_path, emacs_closed).unwrap();

    let closed = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    let OrgToRoomOutcome::Synced {
        created: new_ids,
        transitioned,
        wrote_back,
        ..
    } = closed
    else {
        panic!("expected Synced when closing in Emacs with CLOSED planning line, got {closed:?}");
    };
    assert!(
        new_ids.is_empty(),
        "closing a task with CLOSED planning line must not create a duplicate task"
    );
    assert_eq!(
        transitioned,
        vec![OrgId::parse("E4AFCAD8-9429-42C7-9036-B01F182D9C84").unwrap()]
    );
    assert!(wrote_back);
    assert_eq!(client.task_count(), 1);
    assert_eq!(client.task(Seq(1)).status, TaskStatus::Done);

    let after_close = fs::read_to_string(&org_path).unwrap();
    assert!(after_close.contains(
        "* DONE Test task\nCLOSED: [2026-11-21 Sat 09:53] DEADLINE: <2026-11-25 Wed> SCHEDULED: <2026-11-22 Sun>\n:PROPERTIES:\n:ID: E4AFCAD8-9429-42C7-9036-B01F182D9C84\n:REX_REV: 2\n:REX_SEQ: 1\n:END:\nBody line from Emacs.\n"
    ));

    client.bump_task_remotely(Seq(1), Some("Test task retitled remotely"), None);
    let pulled = sync_room_to_org(&client, &room, &org_path, &mut state)
        .await
        .unwrap();
    assert_eq!(pulled, RoomToOrgOutcome::Written { tasks_synced: 1 });
    let after_pull = fs::read_to_string(&org_path).unwrap();
    assert!(after_pull.contains(
        "* DONE Test task retitled remotely\nCLOSED: [2026-11-21 Sat 09:53] DEADLINE: <2026-11-25 Wed> SCHEDULED: <2026-11-22 Sun>\n:PROPERTIES:\n:ID: E4AFCAD8-9429-42C7-9036-B01F182D9C84\n:REX_REV: 3\n:REX_SEQ: 1\n:END:\nBody line from Emacs.\n"
    ));
}

#[tokio::test]
async fn emacs_title_edit_does_not_remove_concurrent_room_labels() {
    let dir = tempfile::tempdir().unwrap();
    let org_path = dir.path().join("labels.org");
    let client = FakeTaskRoomClient::new(operator("owner"));
    let room = tasks_room();
    let mut state = OrgSyncState::default();

    fs::write(
        &org_path,
        "\
#+TITLE: #sprint

* TODO Labeled task :area%3Dcore:
:PROPERTIES:
:ID: lbl-1
:END:
",
    )
    .unwrap();
    sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();

    client.add_label_remotely(Seq(1), "x=1");

    let cur = fs::read_to_string(&org_path).unwrap();
    let renamed = cur.replace("Labeled task", "Labeled task renamed");
    fs::write(&org_path, &renamed).unwrap();

    let res = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert!(matches!(res, OrgToRoomOutcome::Synced { .. }));
    assert_eq!(
        client.task(Seq(1)).labels,
        vec![
            Label::parse("area=core").unwrap(),
            Label::parse("x=1").unwrap()
        ],
        "remote RoomLabel addition must not be wiped by an unrelated Emacs title edit"
    );
    let final_disk = fs::read_to_string(&org_path).unwrap();
    assert!(final_disk.contains(":area%3Dcore:x%3D1:"));

    let mut fresh_state = OrgSyncState::default();
    let cold_edit = final_disk.replace(
        "* TODO Labeled task renamed :area%3Dcore:x%3D1:",
        "* TODO Labeled task cold edit :area%3Dcore:x%3D1:tier%3Dp0:",
    );
    fs::write(&org_path, &cold_edit).unwrap();
    let cold_res = sync_org_to_room(
        &client,
        &room,
        &org_path,
        &mut fresh_state,
        &OrgSyncOptions::default(),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            cold_res,
            OrgToRoomOutcome::Synced {
                ref updated,
                ref labeled,
                wrote_back: true,
                ..
            } if updated.len() == 1 && labeled.len() == 1
        ),
        "cold-start title + label edit pushes both TaskUpdate and RoomLabel: {cold_res:?}"
    );
    assert_eq!(client.task(Seq(1)).title.as_str(), "Labeled task cold edit");
    assert_eq!(
        client.task(Seq(1)).labels,
        vec![
            Label::parse("area=core").unwrap(),
            Label::parse("tier=p0").unwrap(),
            Label::parse("x=1").unwrap(),
        ]
    );
    assert!(
        fs::read_to_string(&org_path)
            .unwrap()
            .contains(":area%3Dcore:x%3D1:tier%3Dp0:")
    );
}
