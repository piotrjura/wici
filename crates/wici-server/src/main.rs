//! `wici-server` binary. Configured by environment variables; see
//! [`wici_server::Config::from_env`].

use std::error::Error;
use std::process::ExitCode;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;
use wici_server::Config;
use wici_server::store::Store;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "server stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env(|key| std::env::var(key).ok())?;
    let store = Store::connect(
        &config.database_url,
        config.database_connections,
        config.limits.store,
    )
    .await?;
    let listener = TcpListener::bind(config.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "listening");
    wici_server::serve(listener, store, config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
