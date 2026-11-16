use crate::router::LocalClient;
use futures_util::StreamExt;
use rex_wave_api::{ApiError, Client, Cursor, Frame, Interest};
use std::future::Future;
use std::time::Duration;

/// An effectful process manager. Watches frames via the public API and reports
/// results back as commands. Must be idempotent per frame.
pub trait Reactor: Send + 'static {
    const NAME: &'static str;
    fn interest(&self) -> Interest;
    fn react(&mut self, frame: Frame, client: &LocalClient) -> impl Future<Output = Result<(), ApiError>> + Send;
}

/// Run a reactor from `Cursor::default()` (full replay = level-triggered
/// reconcile), resuming from its cursor if the watch stream ends or errors.
pub fn spawn_reactor<R: Reactor>(mut reactor: R, client: LocalClient) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut cursor = Cursor::default();
        loop {
            let mut stream = match client.watch(reactor.interest(), cursor.clone()).await {
                Ok(s) => s,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            while let Some(item) = stream.next().await {
                match item {
                    Ok(frame) => {
                        let next_cursor = match &frame {
                            Frame::Committed(env) => Some((env.stream.clone(), env.seq)),
                            Frame::CaughtUp { cursor: c } => {
                                cursor = c.clone();
                                None
                            }
                            _ => None,
                        };
                        if reactor.react(frame, &client).await.is_ok()
                            && let Some((s, seq)) = next_cursor
                        {
                            cursor.advance(&s, seq);
                        }
                    }
                    Err(_) => break,
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
}
