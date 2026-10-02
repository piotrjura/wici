//! Test helpers: temporary databases and servers.
//!
//! Needs `WICI_TEST_DATABASE_URL`; `scripts/with-postgres.sh` sets it.
//! Helpers panic on setup failure, which fails the calling test.

use std::net::SocketAddr;
use std::time::Duration;

use sqlx::Connection;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use wici_server::store::{Store, StoreLimits};
use wici_server::{Config, Limits, Timeouts};

/// How long helpers wait before failing a test.
pub const WAIT: Duration = Duration::from_secs(5);

#[expect(clippy::panic, reason = "test setup failure must fail the test")]
fn fail(what: &str, error: &dyn std::fmt::Display) -> ! {
    panic!("{what}: {error}")
}

/// Creates an empty database and returns its URL.
pub async fn database_url() -> String {
    let admin = std::env::var("WICI_TEST_DATABASE_URL").unwrap_or_else(|error| {
        fail(
            "WICI_TEST_DATABASE_URL (run through scripts/with-postgres.sh)",
            &error,
        )
    });
    let name = format!("wici_{}", uuid::Uuid::now_v7().simple());
    let mut conn = sqlx::PgConnection::connect(&admin)
        .await
        .unwrap_or_else(|error| fail("connect to test PostgreSQL", &error));
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&mut conn)
        .await
        .unwrap_or_else(|error| fail("create test database", &error));
    let base = admin
        .rsplit_once('/')
        .map_or(admin.as_str(), |(base, _)| base);
    format!("{base}/{name}")
}

/// A store on a fresh database.
pub async fn store(limits: StoreLimits) -> Store {
    Store::connect(&database_url().await, 8, limits)
        .await
        .unwrap_or_else(|error| fail("connect store", &error))
}

/// Default test configuration.
#[must_use]
pub fn config() -> Config {
    Config {
        listen: SocketAddr::from(([127, 0, 0, 1], 0)),
        database_url: String::new(),
        database_connections: 8,
        limits: Limits::default(),
        timeouts: Timeouts::default(),
    }
}

/// Stop signal and task of a running server.
type Running = (oneshot::Sender<()>, JoinHandle<std::io::Result<()>>);

/// A running server on its own database.
#[derive(Debug)]
pub struct TestServer {
    /// WebSocket URL.
    pub url: String,
    address: SocketAddr,
    database: String,
    config: Config,
    running: Option<Running>,
}

impl TestServer {
    /// Starts a server with the default configuration.
    pub async fn start() -> Self {
        Self::with(|_| {}).await
    }

    /// Starts a server after `change` edits the configuration.
    pub async fn with(change: impl FnOnce(&mut Config)) -> Self {
        let mut config = config();
        change(&mut config);
        let database = database_url().await;
        let listener = bind(config.listen).await;
        let address = listener
            .local_addr()
            .unwrap_or_else(|error| fail("local address", &error));
        let mut server = Self {
            url: format!("ws://{address}/v1/ws"),
            address,
            database,
            config,
            running: None,
        };
        server.run(listener).await;
        server
    }

    async fn run(&mut self, listener: TcpListener) {
        let store = Store::connect(&self.database, 8, self.config.limits.store)
            .await
            .unwrap_or_else(|error| fail("connect store", &error));
        let (stop, stopped) = oneshot::channel();
        let shutdown = async {
            let _ = stopped.await;
        };
        let task = tokio::spawn(wici_server::serve(
            listener,
            store,
            self.config.clone(),
            shutdown,
        ));
        self.running = Some((stop, task));
    }

    /// `host:port` for plain HTTP requests.
    #[must_use]
    pub fn http_address(&self) -> String {
        self.address.to_string()
    }

    /// Stops the server and waits for it. Data stays for [`TestServer::restart`].
    pub async fn stop(&mut self) {
        if let Some((stop, task)) = self.running.take() {
            let _ = stop.send(());
            match tokio::time::timeout(WAIT, task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => fail("server stopped with", &error),
                Ok(Err(error)) => fail("server task", &error),
                Err(error) => fail("server stop", &error),
            }
        }
    }

    /// Starts the server again on the same address and database.
    pub async fn restart(&mut self) {
        self.stop().await;
        let listener = bind(self.address).await;
        self.run(listener).await;
    }
}

async fn bind(address: SocketAddr) -> TcpListener {
    TcpListener::bind(address)
        .await
        .unwrap_or_else(|error| fail("bind test server", &error))
}
