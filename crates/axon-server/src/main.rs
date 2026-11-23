use std::{sync::Arc, time::Duration};

use anyhow::Context;
use axon_core::store::{EventStore, MemoryStore};
use axon_server::{
    AppState, Options,
    config::{AccessConfig, Cli, DEV_TOKENS},
    pg::PgStore,
    router,
};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .init();
    let cli = Cli::parse();

    let access = match &cli.access_file {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading access file {}", path.display()))?;
            AccessConfig::from_toml(&text)?
        }
        None => {
            anyhow::ensure!(
                cli.bind.ip().is_loopback(),
                "refusing to serve the built-in development credentials on {}; \
                 pass --access-file or bind to a loopback address",
                cli.bind
            );
            for (principal, token) in DEV_TOKENS {
                tracing::warn!(principal, token, "development credential enabled");
            }
            AccessConfig::dev()
        }
    };

    let store: Arc<dyn EventStore> = match &cli.database_url {
        Some(url) => {
            let store = PgStore::connect(url)
                .await
                .context("connecting to PostgreSQL")?;
            tracing::info!("using the PostgreSQL event store");
            Arc::new(store)
        }
        None => {
            tracing::warn!("no database configured: jobs are kept in memory and lost on restart");
            Arc::new(MemoryStore::default())
        }
    };

    let options = Options {
        max_concurrent_jobs: cli.max_concurrent_jobs,
        tool_timeout: Duration::from_secs(cli.tool_timeout_secs),
    };
    let state = AppState::new(store, &access, options)?;

    let resumed = state
        .runner
        .recover()
        .await
        .context("recovering unfinished jobs")?;
    if resumed > 0 {
        tracing::info!(resumed, "resumed unfinished jobs");
    }

    let listener = tokio::net::TcpListener::bind(cli.bind)
        .await
        .with_context(|| format!("binding {}", cli.bind))?;
    tracing::info!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    tracing::info!("shutting down");
}
