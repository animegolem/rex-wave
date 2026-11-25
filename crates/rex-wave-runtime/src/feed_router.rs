use crate::BoxFut;
use crate::read::AgentFeed;
use rex_wave_api::read::ThreadPageView;
use rex_wave_api::{
    ApiError, Family, FrameStream, HostSeq, HostThreadId, ProjectId, RoleName, RunnerKind, WaveSlug,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;

pub type ThreadIdResolver =
    Arc<dyn Fn(&WaveSlug, &RoleName) -> BoxFut<'static, Option<HostThreadId>> + Send + Sync>;

/// Multiplexes [`AgentFeed`] across arbitrary registered runner backends, routing
/// by resolved [`HostThreadId::runner_kind`] and merging `project_names` watches.
pub struct FeedRouter {
    feeds: BTreeMap<RunnerKind, Arc<dyn AgentFeed>>,
    fallback_order: Vec<RunnerKind>,
    resolver: Option<ThreadIdResolver>,
    merged_project_names: Option<watch::Receiver<Arc<BTreeMap<ProjectId, String>>>>,
}

impl FeedRouter {
    pub fn new(entries: impl IntoIterator<Item = (RunnerKind, Arc<dyn AgentFeed>)>) -> Self {
        let mut feeds = BTreeMap::new();
        let mut fallback_order = Vec::new();
        for (kind, feed) in entries {
            if !feeds.contains_key(&kind) {
                fallback_order.push(kind);
            }
            feeds.insert(kind, feed);
        }
        let watches: Vec<(
            RunnerKind,
            watch::Receiver<Arc<BTreeMap<ProjectId, String>>>,
        )> = feeds
            .iter()
            .filter_map(|(&k, f)| f.project_names().map(|rx| (k, rx)))
            .collect();
        let merged_project_names = merge_project_name_watches(watches);
        FeedRouter {
            feeds,
            fallback_order,
            resolver: None,
            merged_project_names,
        }
    }

    pub fn with_thread_resolver(mut self, resolver: ThreadIdResolver) -> Self {
        self.resolver = Some(resolver);
        self
    }

    async fn resolve_target(&self, wave: &WaveSlug, role: &RoleName) -> Option<RunnerKind> {
        let resolver = self.resolver.as_ref()?;
        let thread = resolver(wave, role).await?;
        Some(thread.runner_kind())
    }
}

/// Merge priority: non-BB runners populate first in `RunnerKind` order, then `Bb`
/// overwrites on collision so local BB project display names win when both exist.
fn merge_maps(
    sources: &[(
        RunnerKind,
        watch::Receiver<Arc<BTreeMap<ProjectId, String>>>,
    )],
) -> Arc<BTreeMap<ProjectId, String>> {
    let mut out = BTreeMap::new();
    for (kind, rx) in sources {
        if *kind != RunnerKind::Bb {
            for (k, v) in rx.borrow().iter() {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    for (kind, rx) in sources {
        if *kind == RunnerKind::Bb {
            for (k, v) in rx.borrow().iter() {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    Arc::new(out)
}

fn merge_project_name_watches(
    mut sources: Vec<(
        RunnerKind,
        watch::Receiver<Arc<BTreeMap<ProjectId, String>>>,
    )>,
) -> Option<watch::Receiver<Arc<BTreeMap<ProjectId, String>>>> {
    match sources.len() {
        0 => None,
        1 => Some(sources.remove(0).1),
        _ => {
            for (_, rx) in &mut sources {
                let _ = rx.borrow_and_update();
            }
            let initial = merge_maps(&sources);
            let (tx, rx) = watch::channel(initial);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let notify = Arc::new(Notify::new());
                    let mut watchers = JoinSet::new();
                    for (_, mut sub_rx) in sources.iter().cloned() {
                        let notify = notify.clone();
                        watchers.spawn(async move {
                            while sub_rx.changed().await.is_ok() {
                                notify.notify_one();
                            }
                        });
                    }
                    loop {
                        tokio::select! {
                            biased;
                            () = tx.closed() => break,
                            () = notify.notified() => {
                                for (_, sub_rx) in &mut sources {
                                    let _ = sub_rx.borrow_and_update();
                                }
                                let next = merge_maps(&sources);
                                tx.send_if_modified(|cur| {
                                    if **cur != *next {
                                        *cur = next;
                                        true
                                    } else {
                                        false
                                    }
                                });
                            }
                            joined = watchers.join_next() => {
                                if joined.is_none() {
                                    break;
                                }
                            }
                        }
                    }
                    watchers.abort_all();
                });
            }
            Some(rx)
        }
    }
}

impl AgentFeed for FeedRouter {
    fn page(
        &self,
        wave: &WaveSlug,
        role: &RoleName,
        after: Option<HostSeq>,
        limit: u32,
    ) -> BoxFut<'_, Result<ThreadPageView, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            if let Some(target) = self.resolve_target(&wave, &role).await {
                return match self.feeds.get(&target) {
                    Some(f) => f.page(&wave, &role, after, limit).await,
                    None => Err(ApiError::Unavailable {
                        family: Family::Wave,
                    }),
                };
            }

            let len = self.fallback_order.len();
            for (idx, kind) in self.fallback_order.iter().enumerate() {
                let Some(feed) = self.feeds.get(kind) else {
                    continue;
                };
                match feed.page(&wave, &role, after, limit).await {
                    Ok(page) => return Ok(page),
                    Err(ApiError::NotFound { .. }) if idx + 1 < len => {}
                    Err(e) => return Err(e),
                }
            }
            Err(ApiError::Unavailable {
                family: Family::Wave,
            })
        })
    }

    fn live(&self, wave: &WaveSlug, role: &RoleName) -> BoxFut<'_, Result<FrameStream, ApiError>> {
        let wave = wave.clone();
        let role = role.clone();
        Box::pin(async move {
            if let Some(target) = self.resolve_target(&wave, &role).await {
                return match self.feeds.get(&target) {
                    Some(f) => f.live(&wave, &role).await,
                    None => Err(ApiError::Unavailable {
                        family: Family::Wave,
                    }),
                };
            }

            let len = self.fallback_order.len();
            for (idx, kind) in self.fallback_order.iter().enumerate() {
                let Some(feed) = self.feeds.get(kind) else {
                    continue;
                };
                match feed.live(&wave, &role).await {
                    Ok(stream) => return Ok(stream),
                    Err(ApiError::NotFound { .. }) if idx + 1 < len => {}
                    Err(e) => return Err(e),
                }
            }
            Err(ApiError::Unavailable {
                family: Family::Wave,
            })
        })
    }

    fn project_names(&self) -> Option<watch::Receiver<Arc<BTreeMap<ProjectId, String>>>> {
        self.merged_project_names.clone()
    }
}

/// Pure fallback computation of `(room, since)` for custom [`crate::BridgeBackend`]s
/// that do not route through [`ReadSide`].
pub fn fallback_parked_room_and_since(
    worker: &rex_wave_api::Principal,
    w: &rex_wave_api::read::Wait,
) -> Option<(Option<rex_wave_api::room::RoomId>, rex_wave_api::Seq)> {
    use crate::read::can_read;
    use rex_wave_api::{Interest, Seq, StreamId};

    let effective = w.effective_cursor_for(worker).ok()?;
    match &w.interest {
        Interest::Everything if !worker.is_operator_or_system() => None,
        Interest::Room { room } | Interest::RoomFiltered { room, .. } => {
            let id = room.resolve(worker).ok()?;
            let s = StreamId::Room { room: id.clone() };
            can_read(worker, &s).ok()?;
            Some((Some(id), effective.at(&s)))
        }
        Interest::Streams { streams } => {
            for s in streams {
                can_read(worker, s).ok()?;
            }
            if streams.len() == 1
                && let StreamId::Room { room } = &streams[0]
            {
                Some((Some(room.clone()), effective.at(&streams[0])))
            } else {
                Some((None, Seq::ORIGIN))
            }
        }
        _ => Some((None, Seq::ORIGIN)),
    }
}
