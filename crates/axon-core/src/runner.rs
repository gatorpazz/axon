//! The durable job runner.
//!
//! A job is driven purely from its event log: load, fold, do the next thing,
//! append the result. There is no in-memory job table to lose, so recovery
//! after a crash is the same code path as a fresh start ([`JobRunner::recover`]
//! simply re-spawns every unfinished job).
//!
//! Guarantees:
//!
//! * A step that recorded `step_succeeded` is never executed again.
//! * A step interrupted mid-flight is re-executed (at-least-once), up to
//!   `max_attempts` starts, so a step that reliably kills the process cannot
//!   crash-loop forever. Tools are therefore expected to be idempotent.
//! * Any event the runner did not write (in practice: a cancellation) wins.
//!   The runner notices either through a failed optimistic append or through a
//!   store notice, which also aborts the tool call in flight.

use std::sync::Arc;

use tokio::{
    sync::{Semaphore, broadcast},
    task::JoinHandle,
};

use crate::{
    gateway::Gateway,
    ids::JobId,
    job::{EventKind, JobState, ReplayError, resolve_refs},
    metrics::Metrics,
    store::{Actor, EventStore, Notice, StoreError},
};

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("corrupt event log: {0}")]
    Replay(#[from] ReplayError),
}

#[derive(Clone)]
pub struct JobRunner {
    store: Arc<dyn EventStore>,
    gateway: Arc<Gateway>,
    metrics: Arc<Metrics>,
    permits: Arc<Semaphore>,
    max_attempts: u32,
}

impl std::fmt::Debug for JobRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobRunner")
            .field("available_permits", &self.permits.available_permits())
            .field("max_attempts", &self.max_attempts)
            .finish_non_exhaustive()
    }
}

impl JobRunner {
    pub fn new(
        store: Arc<dyn EventStore>,
        gateway: Arc<Gateway>,
        metrics: Arc<Metrics>,
        max_concurrent_jobs: usize,
    ) -> Self {
        Self {
            store,
            gateway,
            metrics,
            permits: Arc::new(Semaphore::new(max_concurrent_jobs.max(1))),
            max_attempts: 3,
        }
    }

    /// Runs a job in the background. The job stays `queued` until one of the
    /// `max_concurrent_jobs` slots frees up.
    pub fn spawn(&self, job_id: JobId) -> JoinHandle<()> {
        let runner = self.clone();
        tokio::spawn(async move {
            // The semaphore is never closed, so this only fails if it is.
            let Ok(_permit) = runner.permits.acquire().await else {
                return;
            };
            if let Err(error) = runner.drive(job_id).await {
                tracing::error!(%job_id, %error, "job runner stopped");
            }
        })
    }

    /// Resumes every unfinished job. Call once at startup.
    pub async fn recover(&self) -> Result<usize, StoreError> {
        let unfinished = self.store.unfinished().await?;
        for &job_id in &unfinished {
            self.spawn(job_id);
        }
        Ok(unfinished.len())
    }

    /// Drives one job to a terminal status.
    pub async fn drive(&self, job_id: JobId) -> Result<(), RunnerError> {
        // Subscribe before loading so no foreign event can slip between the two.
        let mut notices = self.store.subscribe();
        let mut state = self.load(job_id).await?;

        while !state.status.is_terminal() {
            let Some(index) = state.next_runnable() else {
                self.record(&mut state, EventKind::Succeeded).await?;
                continue;
            };
            let step = state.spec.steps[index].clone();
            let attempt = state.steps[index].attempts + 1;
            if attempt > self.max_attempts {
                let error = format!(
                    "step `{}` was interrupted {} times without completing",
                    step.id, self.max_attempts
                );
                self.record(&mut state, EventKind::Failed { error }).await?;
                continue;
            }
            if !self
                .record(
                    &mut state,
                    EventKind::StepStarted {
                        step: step.id.clone(),
                        attempt,
                    },
                )
                .await?
            {
                continue;
            }

            let outcome = match resolve_refs(&step.input, &|id| state.output_of(id)) {
                Err(error) => Err(error.to_string()),
                Ok(input) => {
                    // Losing the race drops the invoke future, which aborts
                    // the tool call in flight.
                    let invoked = tokio::select! {
                        result = self.gateway.invoke(&state.owner, &step.tool, input) => Some(result),
                        () = superseded(&mut notices, job_id, state.last_seq) => None,
                    };
                    match invoked {
                        Some(result) => result.map_err(|e| e.to_string()),
                        None => {
                            state = self.load(job_id).await?;
                            continue;
                        }
                    }
                }
            };

            match outcome {
                Ok(invocation) => {
                    let event = EventKind::StepSucceeded {
                        step: step.id,
                        output: invocation.output,
                        provenance: invocation.provenance,
                    };
                    self.record(&mut state, event).await?;
                }
                Err(error) => {
                    let failed = EventKind::StepFailed {
                        step: step.id.clone(),
                        error: error.clone(),
                    };
                    if self.record(&mut state, failed).await? {
                        let error = format!("step `{}` failed: {error}", step.id);
                        self.record(&mut state, EventKind::Failed { error }).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn load(&self, job_id: JobId) -> Result<JobState, RunnerError> {
        let events = self.store.events_after(&Actor::System, job_id, 0).await?;
        Ok(JobState::replay(&events)?)
    }

    /// Appends `kind` after the state's last event and folds it in.
    ///
    /// Returns `false` if someone else appended first; `state` is then
    /// reloaded and the caller should re-evaluate what to do.
    async fn record(&self, state: &mut JobState, kind: EventKind) -> Result<bool, RunnerError> {
        match self
            .store
            .append(&Actor::System, state.id, state.last_seq, kind)
            .await
        {
            Ok(event) => {
                state.apply(&event)?;
                if state.status.is_terminal() {
                    self.metrics.inc(
                        "axon_jobs_finished_total",
                        &[("status", state.status.as_str())],
                    );
                    tracing::info!(job_id = %state.id, status = %state.status, "job finished");
                }
                Ok(true)
            }
            Err(StoreError::Conflict { .. }) => {
                *state = self.load(state.id).await?;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Resolves once the job has an event past `last_seq`, i.e. one this runner
/// did not write.
async fn superseded(notices: &mut broadcast::Receiver<Notice>, job_id: JobId, last_seq: u64) {
    loop {
        match notices.recv().await {
            Ok(notice) if notice.job_id == job_id && notice.seq > last_seq => return,
            // A lagged receiver may have missed the notice. That is safe: the
            // runner's next append will conflict and it will find out then.
            Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => std::future::pending().await,
        }
    }
}
