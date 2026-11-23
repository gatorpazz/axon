//! HTTP front end for axon.
//!
//! | Surface                     | Purpose                                         |
//! |-----------------------------|-------------------------------------------------|
//! | `POST /v1/jobs` and friends | submit, inspect and cancel jobs                 |
//! | `GET /v1/jobs/{id}/events`  | Server-Sent Events: replay, then live tail      |
//! | `/v1/tools`                 | discover and directly invoke tools              |
//! | `POST /mcp`                 | the same tools over the Model Context Protocol  |
//! | `/healthz`, `/metrics`      | operations                                      |

pub mod auth;
pub mod config;
pub mod error;
pub mod pg;
mod routes;

use std::{sync::Arc, time::Duration};

use axon_core::{
    authz::Authorizer, gateway::Gateway, metrics::Metrics, runner::JobRunner, store::EventStore,
    tools,
};
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::{
    auth::Authenticator,
    config::{AccessConfig, ConfigError},
};

#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub max_concurrent_jobs: usize,
    pub tool_timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: 8,
            tool_timeout: Duration::from_secs(30),
        }
    }
}

/// Shared application state. Cheap to clone: everything is behind an `Arc`.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn EventStore>,
    pub gateway: Arc<Gateway>,
    pub runner: JobRunner,
    pub auth: Arc<Authenticator>,
    pub metrics: Arc<Metrics>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("gateway", &self.gateway)
            .field("runner", &self.runner)
            .finish_non_exhaustive()
    }
}

impl AppState {
    /// Wires the object graph: authorizer -> gateway -> runner over `store`.
    pub fn new(
        store: Arc<dyn EventStore>,
        access: &AccessConfig,
        options: Options,
    ) -> Result<Self, ConfigError> {
        let auth = Arc::new(Authenticator::new(&access.principals)?);
        let authz = Arc::new(Authorizer::parse(access.tuples.iter().map(String::as_str))?);
        let metrics = Arc::new(Metrics::default());
        let gateway = Arc::new(
            tools::register_builtin(Gateway::builder(authz, metrics.clone()))
                .timeout(options.tool_timeout)
                .build(),
        );
        let runner = JobRunner::new(
            store.clone(),
            gateway.clone(),
            metrics.clone(),
            options.max_concurrent_jobs,
        );
        Ok(Self {
            store,
            gateway,
            runner,
            auth,
            metrics,
        })
    }
}

pub fn router(state: AppState) -> Router {
    routes::router()
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
