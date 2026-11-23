use std::convert::Infallible;

use axon_core::{
    JobId,
    job::{EventKind, JobSpec, JobState},
    store::{Actor, JobSummary, StoreError, follow},
};
use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::{HeaderMap, StatusCode, header::LOCATION},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_stream::{Stream, StreamExt};

use crate::{AppState, auth::Caller, error::ApiError};

type JobPath = Result<Path<JobId>, PathRejection>;

pub async fn submit(
    State(state): State<AppState>,
    Caller(caller): Caller,
    spec: Result<Json<JobSpec>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let Json(spec) = spec?;
    spec.validate()?;
    // Fail fast on tools the caller cannot use. The gateway checks again when
    // each step runs, so this is a courtesy, not the enforcement point.
    for step in &spec.steps {
        state.gateway.authorize(&caller, &step.tool)?;
    }

    let job_id = JobId::new();
    let submitted = EventKind::Submitted {
        owner: caller.clone(),
        spec,
    };
    let actor = Actor::Principal(caller);
    state.store.append(&actor, job_id, 0, submitted).await?;
    state.metrics.inc("axon_jobs_submitted_total", &[]);
    state.runner.spawn(job_id);

    let summary = state.store.job(&actor, job_id).await?;
    Ok((
        StatusCode::CREATED,
        [(LOCATION, format!("/v1/jobs/{job_id}"))],
        Json(summary),
    ))
}

pub async fn list(
    State(state): State<AppState>,
    Caller(caller): Caller,
) -> Result<Json<Value>, ApiError> {
    let jobs: Vec<JobSummary> = state.store.jobs(&Actor::Principal(caller)).await?;
    Ok(Json(json!({ "jobs": jobs })))
}

pub async fn get(
    State(state): State<AppState>,
    Caller(caller): Caller,
    id: JobPath,
) -> Result<Json<JobState>, ApiError> {
    let Path(id) = id?;
    let events = state
        .store
        .events_after(&Actor::Principal(caller), id, 0)
        .await?;
    Ok(Json(JobState::replay(&events)?))
}

/// Cancels a job by appending `cancelled` on top of its latest event.
///
/// The append is optimistic: if the runner writes first we re-read and try
/// again, and if the job finished in the meantime the caller gets a 409.
pub async fn cancel(
    State(state): State<AppState>,
    Caller(caller): Caller,
    id: JobPath,
) -> Result<Json<JobState>, ApiError> {
    const ATTEMPTS: usize = 8;
    let Path(id) = id?;
    let actor = Actor::Principal(caller.clone());

    for _ in 0..ATTEMPTS {
        let events = state.store.events_after(&actor, id, 0).await?;
        let mut job = JobState::replay(&events)?;
        if job.status.is_terminal() {
            return Err(ApiError::conflict(format!("job already {}", job.status)));
        }
        let cancelled = EventKind::Cancelled { by: caller.clone() };
        match state
            .store
            .append(&actor, id, job.last_seq, cancelled)
            .await
        {
            Ok(event) => {
                job.apply(&event)?;
                state
                    .metrics
                    .inc("axon_jobs_finished_total", &[("status", "cancelled")]);
                return Ok(Json(job));
            }
            Err(StoreError::Conflict { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(ApiError::conflict(
        "job is changing too quickly to cancel; retry",
    ))
}

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    /// Resume after this sequence number. `Last-Event-ID` takes precedence.
    #[serde(default)]
    after: u64,
}

/// Server-Sent Events for one job: everything after the requested position is
/// replayed, then new events are pushed as they are appended. The stream ends
/// after the terminal event. Each SSE `id` is the event's `seq`, so a
/// reconnecting client resumes exactly where it stopped via `Last-Event-ID`.
pub async fn events(
    State(state): State<AppState>,
    Caller(caller): Caller,
    id: JobPath,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let Path(id) = id?;
    let actor = Actor::Principal(caller);
    // Resolve 404s before committing to a 200 event stream.
    state.store.job(&actor, id).await?;

    let after = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(query.after);

    let stream = follow(state.store.clone(), actor, id, after).map(move |item| {
        Ok(match item {
            Ok(event) => Event::default()
                .id(event.seq.to_string())
                .event(event.kind.name())
                .json_data(&event)
                .unwrap_or_else(|_| Event::default().event("error").data("unserializable event")),
            Err(error) => {
                tracing::warn!(job_id = %id, %error, "event stream aborted");
                Event::default().event("error").data("stream aborted")
            }
        })
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
