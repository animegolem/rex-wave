use rex_wave_api::Envelope;
use std::sync::Arc;
use tokio::sync::broadcast;

/// In-process doorbell, published to only after a journal commit. Lossy by
/// design (bounded broadcast): a lagging watcher re-reads the journal, which
/// is the source of truth.
#[derive(Clone)]
pub struct Hub {
    tx: broadcast::Sender<Arc<Envelope>>,
}

impl Hub {
    pub fn new(capacity: usize) -> Self {
        Hub { tx: broadcast::channel(capacity).0 }
    }

    pub fn publish(&self, committed: &[Envelope]) {
        for e in committed {
            let _ = self.tx.send(Arc::new(e.clone()));
        }
    }

    pub fn tap(&self) -> broadcast::Receiver<Arc<Envelope>> {
        self.tx.subscribe()
    }
}
