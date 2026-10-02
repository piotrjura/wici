//! Wici relay server: authenticates devices, pairs them, stores and pushes
//! sealed messages, and forwards live updates. It never sees plaintext.

mod config;
mod delivery;
mod handlers;
mod hub;
mod rate;
mod session;
pub mod store;

use std::future::Future;
use std::io;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::response::Response;
use axum::routing::get;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub use config::{Config, ConfigError, Limits, Timeouts};

use crate::hub::Hub;
use crate::store::Store;

/// Shared server state.
#[derive(Debug)]
pub(crate) struct App {
    store: Store,
    hub: Hub,
    config: Config,
    shutdown: CancellationToken,
}

/// Serves `/v1/ws` and `/health` on `listener` until `shutdown` completes.
///
/// # Errors
///
/// Returns the listener's I/O error.
pub async fn serve(
    listener: TcpListener,
    store: Store,
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    let app = Arc::new(App {
        store,
        hub: Hub::default(),
        config,
        shutdown: CancellationToken::new(),
    });
    let sweeper = tokio::spawn(sweep(Arc::clone(&app)));
    let router = Router::new()
        .route("/v1/ws", get(upgrade))
        .route("/health", get(|| async { "ok" }))
        .with_state(Arc::clone(&app));
    let stopping = Arc::clone(&app);
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.await;
            stopping.shutdown.cancel();
            stopping.hub.stop_all();
        })
        .await;
    app.shutdown.cancel();
    let _ = sweeper.await;
    result
}

async fn upgrade(State(app): State<Arc<App>>, upgrade: WebSocketUpgrade) -> Response {
    let limit = app.config.limits.max_frame_bytes;
    upgrade
        .max_message_size(limit)
        .max_frame_size(limit)
        .on_upgrade(move |socket| session::run(socket, app))
}

/// Expires overdue invitations and claims and tells their members.
async fn sweep(app: Arc<App>) {
    let mut ticker = tokio::time::interval(app.config.timeouts.sweep);
    loop {
        tokio::select! {
            () = app.shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        match app.store.expire_due(100).await {
            Ok(expired) => expired.iter().for_each(|record| app.notify_pair(record)),
            Err(error) => tracing::warn!(%error, "expiry sweep failed"),
        }
    }
}
