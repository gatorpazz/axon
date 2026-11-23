//! The append-only event log.
//!
//! [`EventStore`] is the only persistence interface in the system. Two rules
//! carry most of the design:
//!
//! 1. **Optimistic concurrency.** Every append names the sequence number it
//!    expects to follow. Two writers racing on a job cannot both win, which is
//!    also how cancellation pre-empts the runner without any locking.
//! 2. **Isolation lives in the store.** Every call carries the [`Actor`] it is
//!    made on behalf of, and a job that actor may not see is reported as
//!    [`StoreError::NotFound`], indistinguishable from one that does not exist.
//!    The PostgreSQL backend enforces this with row-level security; the
//!    in-memory one mirrors it. [`contract`] holds both to the same behaviour.

pub mod contract;
mod memory;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

pub use self::memory::MemoryStore;
use crate::{
    ids::{JobId, PrincipalId},
    job::{EventKind, JobEvent, JobStatus},
};

/// On whose behalf a store operation runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Actor {
    /// The platform itself (the job runner, recovery). Sees every job.
    System,
    /// An authenticated caller. Sees only jobs it owns.
    Principal(PrincipalId),
}

impl Actor {
    pub fn can_access(&self, owner: &PrincipalId) -> bool {
        match self {
            Self::System => true,
            Self::Principal(p) => p == owner,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobSummary {
    pub id: JobId,
    pub owner: PrincipalId,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_seq: u64,
}

/// "Job `job_id` now has an event at `seq`." Deliberately not the event
/// itself: subscribers re-read from the store, which stays the single source
/// of truth and applies access control on the way out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Notice {
    pub job_id: JobId,
    pub seq: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("job not found")]
    NotFound,
    #[error("job is at seq {actual}, append expected {expected}")]
    Conflict { expected: u64, actual: u64 },
    #[error("invalid append: {0}")]
    InvalidAppend(&'static str),
    #[error("storage backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl StoreError {
    pub fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Backend(Box::new(error))
    }
}

#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    /// Appends an event at `expected_seq + 1`.
    ///
    /// `expected_seq == 0` creates the job and requires a `Submitted` event
    /// whose owner the actor can act as; any later append must not be
    /// `Submitted`. Fails with [`StoreError::Conflict`] if the log has moved.
    async fn append(
        &self,
        actor: &Actor,
        job_id: JobId,
        expected_seq: u64,
        kind: EventKind,
    ) -> Result<JobEvent, StoreError>;

    /// Events with `seq > after_seq`, in order.
    async fn events_after(
        &self,
        actor: &Actor,
        job_id: JobId,
        after_seq: u64,
    ) -> Result<Vec<JobEvent>, StoreError>;

    async fn job(&self, actor: &Actor, job_id: JobId) -> Result<JobSummary, StoreError>;

    /// Jobs visible to the actor, newest first.
    async fn jobs(&self, actor: &Actor) -> Result<Vec<JobSummary>, StoreError>;

    /// Ids of all non-terminal jobs, oldest first. Used for crash recovery.
    async fn unfinished(&self) -> Result<Vec<JobId>, StoreError>;

    /// Wake-up signals for appended events. Lossy by design (slow receivers
    /// lag); consumers must treat it as a hint and re-read the log.
    fn subscribe(&self) -> broadcast::Receiver<Notice>;
}

const FOLLOW_POLL_FALLBACK: std::time::Duration = std::time::Duration::from_secs(10);

/// Validation shared by every backend.
pub fn check_append(expected_seq: u64, kind: &EventKind, actor: &Actor) -> Result<(), StoreError> {
    match (expected_seq, kind) {
        (0, EventKind::Submitted { owner, .. }) if actor.can_access(owner) => Ok(()),
        (0, EventKind::Submitted { .. }) => Err(StoreError::InvalidAppend(
            "cannot submit a job owned by another principal",
        )),
        (0, _) => Err(StoreError::InvalidAppend(
            "a job's first event must be `submitted`",
        )),
        (_, EventKind::Submitted { .. }) => Err(StoreError::InvalidAppend(
            "`submitted` may only be a job's first event",
        )),
        _ => Ok(()),
    }
}

/// Streams a job's events from `after_seq` onward and completes after the
/// terminal event.
///
/// Gap-free without holding any lock across the replay/live boundary: the
/// subscription is opened *before* the first read, and a notice (or a lagged
/// receiver, or the fallback timer) only ever triggers another read from the
/// last delivered `seq`.
pub fn follow(
    store: Arc<dyn EventStore>,
    actor: Actor,
    job_id: JobId,
    after_seq: u64,
) -> ReceiverStream<Result<JobEvent, StoreError>> {
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        let mut notices = store.subscribe();
        let mut last = after_seq;
        loop {
            let events = match store.events_after(&actor, job_id, last).await {
                Ok(events) => events,
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            };
            for event in events {
                last = event.seq;
                let terminal = event.kind.status().is_terminal();
                // A send error means the consumer hung up; stop following.
                if tx.send(Ok(event)).await.is_err() || terminal {
                    return;
                }
            }
            loop {
                tokio::select! {
                    () = tx.closed() => return,
                    // Notices are hints and a backend may drop one (e.g. a
                    // reconnecting LISTEN connection), so re-read periodically.
                    () = tokio::time::sleep(FOLLOW_POLL_FALLBACK) => break,
                    notice = notices.recv() => match notice {
                        Ok(n) if n.job_id == job_id && n.seq > last => break,
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => break,
                        Err(broadcast::error::RecvError::Closed) => return,
                    },
                }
            }
        }
    });
    ReceiverStream::new(rx)
}
