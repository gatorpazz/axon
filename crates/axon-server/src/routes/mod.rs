mod jobs;
mod mcp;
mod tools;

use axum::{
    Router,
    extract::State,
    http::header::CONTENT_TYPE,
    response::{Html, IntoResponse},
    routing::{get, post},
};

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics))
        .route("/v1/tools", get(tools::list))
        .route("/v1/tools/{name}/invoke", post(tools::invoke))
        .route("/v1/jobs", post(jobs::submit).get(jobs::list))
        .route("/v1/jobs/{id}", get(jobs::get))
        .route("/v1/jobs/{id}/cancel", post(jobs::cancel))
        .route("/v1/jobs/{id}/events", get(jobs::events))
        .route("/mcp", post(mcp::handle))
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../../assets/index.html"))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}
