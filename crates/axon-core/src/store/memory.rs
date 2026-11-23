//! In-memory [`EventStore`] for tests and zero-dependency local runs.

use std::{collections::HashMap, sync::Mutex};

use async_trait::async_trait;
use tokio::sync::broadcast;

use super::{Actor, EventStore, JobSummary, Notice, StoreError, check_append};
use crate::{
    ids::JobId,
    job::{EventKind, JobEvent},
    provenance,
};

struct Record {
    summary: JobSummary,
    events: Vec<JobEvent>,
}

pub struct MemoryStore {
    jobs: Mutex<HashMap<JobId, Record>>,
    notices: broadcast::Sender<Notice>,
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore").finish_non_exhaustive()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            jobs: Mutex::default(),
            notices: broadcast::channel(1024).0,
        }
    }
}

impl MemoryStore {
    /// Runs `f` on a job's record, if the actor may see it.
    fn with_record<T>(
        &self,
        actor: &Actor,
        job_id: JobId,
        f: impl FnOnce(&Record) -> T,
    ) -> Result<T, StoreError> {
        let jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.get(&job_id)
            .filter(|record| actor.can_access(&record.summary.owner))
            .map(f)
            .ok_or(StoreError::NotFound)
    }
}

#[async_trait]
impl EventStore for MemoryStore {
    async fn append(
        &self,
        actor: &Actor,
        job_id: JobId,
        expected_seq: u64,
        kind: EventKind,
    ) -> Result<JobEvent, StoreError> {
        check_append(expected_seq, &kind, actor)?;
        let at = provenance::now();
        let seq = expected_seq + 1;
        let event = {
            let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
            match (jobs.get_mut(&job_id), &kind) {
                (None, EventKind::Submitted { owner, .. }) => {
                    let summary = JobSummary {
                        id: job_id,
                        owner: owner.clone(),
                        status: kind.status(),
                        created_at: at,
                        updated_at: at,
                        last_seq: seq,
                    };
                    let event = JobEvent {
                        job_id,
                        seq,
                        at,
                        kind,
                    };
                    jobs.insert(
                        job_id,
                        Record {
                            summary,
                            events: vec![event.clone()],
                        },
                    );
                    event
                }
                (Some(record), _) if actor.can_access(&record.summary.owner) => {
                    let actual = record.summary.last_seq;
                    if actual != expected_seq {
                        return Err(StoreError::Conflict {
                            expected: expected_seq,
                            actual,
                        });
                    }
                    record.summary.status = kind.status();
                    record.summary.updated_at = at;
                    record.summary.last_seq = seq;
                    let event = JobEvent {
                        job_id,
                        seq,
                        at,
                        kind,
                    };
                    record.events.push(event.clone());
                    event
                }
                _ => return Err(StoreError::NotFound),
            }
        };
        // No receivers is fine: nobody is following right now.
        let _ = self.notices.send(Notice { job_id, seq });
        Ok(event)
    }

    async fn events_after(
        &self,
        actor: &Actor,
        job_id: JobId,
        after_seq: u64,
    ) -> Result<Vec<JobEvent>, StoreError> {
        self.with_record(actor, job_id, |record| {
            // seq is 1-based and gapless, so seq N lives at index N - 1.
            let start = usize::try_from(after_seq).unwrap_or(usize::MAX);
            record.events.get(start..).unwrap_or_default().to_vec()
        })
    }

    async fn job(&self, actor: &Actor, job_id: JobId) -> Result<JobSummary, StoreError> {
        self.with_record(actor, job_id, |record| record.summary.clone())
    }

    async fn jobs(&self, actor: &Actor) -> Result<Vec<JobSummary>, StoreError> {
        let jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        let mut visible: Vec<_> = jobs
            .values()
            .filter(|record| actor.can_access(&record.summary.owner))
            .map(|record| record.summary.clone())
            .collect();
        visible.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(visible)
    }

    async fn unfinished(&self) -> Result<Vec<JobId>, StoreError> {
        let jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        let mut ids: Vec<_> = jobs
            .values()
            .filter(|record| !record.summary.status.is_terminal())
            .map(|record| record.summary.id)
            .collect();
        ids.sort();
        Ok(ids)
    }

    fn subscribe(&self) -> broadcast::Receiver<Notice> {
        self.notices.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::store::contract;

    #[tokio::test]
    async fn satisfies_the_store_contract() {
        contract::run_all(Arc::new(MemoryStore::default())).await;
    }
}
