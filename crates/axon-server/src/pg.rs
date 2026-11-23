//! PostgreSQL [`EventStore`].
//!
//! * **Isolation** is delegated to row-level security: every transaction
//!   starts by declaring its actor (`set_config('axon.actor', ..., true)`,
//!   transaction-local so pooled connections cannot leak it), and the policies
//!   in `migrations/0001_init.sql` do the rest. No query here filters on
//!   `owner`.
//! * **Optimistic concurrency** is a compare-and-set on `jobs.last_seq`. The
//!   row lock taken by the `UPDATE` serializes racing writers; the loser
//!   matches zero rows.
//! * **Fan-out** uses `LISTEN/NOTIFY`: the notification is sent inside the
//!   appending transaction, so it is delivered only if the event committed,
//!   and reaches every server replica.

use std::time::Duration;

use async_trait::async_trait;
use axon_core::{
    JobId,
    job::{EventKind, JobEvent},
    provenance,
    store::{Actor, EventStore, JobSummary, Notice, StoreError, check_append},
};
use chrono::{DateTime, Utc};
use sqlx::{
    PgPool, Postgres, Transaction,
    postgres::{PgListener, PgPoolOptions},
    types::Json,
};
use tokio::sync::broadcast;
use uuid::Uuid;

const CHANNEL: &str = "axon_events";

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug)]
pub struct PgStore {
    pool: PgPool,
    notices: broadcast::Sender<Notice>,
}

type SummaryRow = (Uuid, String, String, DateTime<Utc>, DateTime<Utc>, i64);

fn seq_to_db(seq: u64) -> i64 {
    i64::try_from(seq).unwrap_or(i64::MAX)
}

fn seq_from_db(seq: i64) -> u64 {
    // The schema constrains seq to be >= 1.
    u64::try_from(seq).unwrap_or(0)
}

fn summary_from_row(row: SummaryRow) -> Result<JobSummary, StoreError> {
    let (id, owner, status, created_at, updated_at, last_seq) = row;
    Ok(JobSummary {
        id: id.into(),
        owner: owner.parse().map_err(StoreError::backend)?,
        status: status.parse().map_err(StoreError::backend)?,
        created_at,
        updated_at,
        last_seq: seq_from_db(last_seq),
    })
}

impl PgStore {
    /// Connects, applies pending migrations and starts the notification
    /// listener.
    pub async fn connect(database_url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await
            .map_err(StoreError::backend)?;
        MIGRATOR.run(&pool).await.map_err(StoreError::backend)?;

        let mut listener = PgListener::connect_with(&pool)
            .await
            .map_err(StoreError::backend)?;
        listener
            .listen(CHANNEL)
            .await
            .map_err(StoreError::backend)?;
        let (notices, _) = broadcast::channel(1024);
        tokio::spawn(forward_notifications(listener, notices.clone()));

        Ok(Self { pool, notices })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Opens a transaction acting as `actor`.
    async fn begin(&self, actor: &Actor) -> Result<Transaction<'static, Postgres>, StoreError> {
        let actor = match actor {
            Actor::System => "system",
            Actor::Principal(principal) => principal.as_str(),
        };
        let mut tx = self.pool.begin().await.map_err(StoreError::backend)?;
        sqlx::query("SELECT set_config('axon.actor', $1, true)")
            .bind(actor)
            .execute(&mut *tx)
            .await
            .map_err(StoreError::backend)?;
        Ok(tx)
    }

    /// `last_seq` of a job as seen by the transaction's actor.
    async fn visible_seq(
        tx: &mut Transaction<'static, Postgres>,
        job_id: JobId,
    ) -> Result<Option<u64>, StoreError> {
        let seq: Option<i64> = sqlx::query_scalar("SELECT last_seq FROM jobs WHERE id = $1")
            .bind(job_id.as_uuid())
            .fetch_optional(&mut **tx)
            .await
            .map_err(StoreError::backend)?;
        Ok(seq.map(seq_from_db))
    }
}

/// Relays `NOTIFY` payloads (`<job uuid>:<seq>`) to in-process subscribers.
async fn forward_notifications(mut listener: PgListener, notices: broadcast::Sender<Notice>) {
    loop {
        match listener.recv().await {
            Ok(notification) => {
                let parsed = notification
                    .payload()
                    .split_once(':')
                    .and_then(|(job_id, seq)| {
                        Some(Notice {
                            job_id: job_id.parse().ok()?,
                            seq: seq.parse().ok()?,
                        })
                    });
                match parsed {
                    // An error only means nobody is subscribed right now.
                    Some(notice) => drop(notices.send(notice)),
                    None => {
                        tracing::warn!(payload = notification.payload(), "malformed notification")
                    }
                }
            }
            // `PgListener` reconnects on the next `recv`; notices sent in the
            // gap are lost, which subscribers tolerate by design.
            Err(error) => {
                tracing::warn!(%error, "notification listener error; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[async_trait]
impl EventStore for PgStore {
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
        let status = kind.status().as_str();
        let mut tx = self.begin(actor).await?;

        let advanced = if let EventKind::Submitted { owner, .. } = &kind {
            sqlx::query(
                "INSERT INTO jobs (id, owner, status, created_at, updated_at, last_seq)
                 VALUES ($1, $2, $3, $4, $4, 1)
                 ON CONFLICT (id) DO NOTHING",
            )
            .bind(job_id.as_uuid())
            .bind(owner.as_str())
            .bind(status)
            .bind(at)
        } else {
            sqlx::query(
                "UPDATE jobs SET last_seq = $2, status = $3, updated_at = $4
                 WHERE id = $1 AND last_seq = $5",
            )
            .bind(job_id.as_uuid())
            .bind(seq_to_db(seq))
            .bind(status)
            .bind(at)
            .bind(seq_to_db(expected_seq))
        }
        .execute(&mut *tx)
        .await
        .map_err(StoreError::backend)?
        .rows_affected();

        if advanced == 0 {
            // Either the log moved, or the job is not visible to this actor.
            return Err(match Self::visible_seq(&mut tx, job_id).await? {
                Some(actual) => StoreError::Conflict {
                    expected: expected_seq,
                    actual,
                },
                None if expected_seq == 0 => StoreError::Conflict {
                    expected: 0,
                    actual: 0,
                },
                None => StoreError::NotFound,
            });
        }

        sqlx::query("INSERT INTO job_events (job_id, seq, at, kind) VALUES ($1, $2, $3, $4)")
            .bind(job_id.as_uuid())
            .bind(seq_to_db(seq))
            .bind(at)
            .bind(Json(&kind))
            .execute(&mut *tx)
            .await
            .map_err(StoreError::backend)?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(CHANNEL)
            .bind(format!("{job_id}:{seq}"))
            .execute(&mut *tx)
            .await
            .map_err(StoreError::backend)?;
        tx.commit().await.map_err(StoreError::backend)?;

        Ok(JobEvent {
            job_id,
            seq,
            at,
            kind,
        })
    }

    async fn events_after(
        &self,
        actor: &Actor,
        job_id: JobId,
        after_seq: u64,
    ) -> Result<Vec<JobEvent>, StoreError> {
        let mut tx = self.begin(actor).await?;
        let rows: Vec<(i64, DateTime<Utc>, Json<EventKind>)> = sqlx::query_as(
            "SELECT seq, at, kind FROM job_events WHERE job_id = $1 AND seq > $2 ORDER BY seq",
        )
        .bind(job_id.as_uuid())
        .bind(seq_to_db(after_seq))
        .fetch_all(&mut *tx)
        .await
        .map_err(StoreError::backend)?;

        // No rows is ambiguous: caught up, or no such (visible) job.
        if rows.is_empty() && Self::visible_seq(&mut tx, job_id).await?.is_none() {
            return Err(StoreError::NotFound);
        }
        Ok(rows
            .into_iter()
            .map(|(seq, at, Json(kind))| JobEvent {
                job_id,
                seq: seq_from_db(seq),
                at,
                kind,
            })
            .collect())
    }

    async fn job(&self, actor: &Actor, job_id: JobId) -> Result<JobSummary, StoreError> {
        let mut tx = self.begin(actor).await?;
        let row: Option<SummaryRow> = sqlx::query_as(
            "SELECT id, owner, status, created_at, updated_at, last_seq FROM jobs WHERE id = $1",
        )
        .bind(job_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(StoreError::backend)?;
        row.map(summary_from_row)
            .unwrap_or(Err(StoreError::NotFound))
    }

    async fn jobs(&self, actor: &Actor) -> Result<Vec<JobSummary>, StoreError> {
        let mut tx = self.begin(actor).await?;
        // No owner filter: row-level security already restricts the result.
        let rows: Vec<SummaryRow> = sqlx::query_as(
            "SELECT id, owner, status, created_at, updated_at, last_seq FROM jobs ORDER BY id DESC",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(StoreError::backend)?;
        rows.into_iter().map(summary_from_row).collect()
    }

    async fn unfinished(&self) -> Result<Vec<JobId>, StoreError> {
        let mut tx = self.begin(&Actor::System).await?;
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM jobs WHERE status IN ('queued', 'running') ORDER BY id",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(StoreError::backend)?;
        Ok(ids.into_iter().map(JobId::from).collect())
    }

    fn subscribe(&self) -> broadcast::Receiver<Notice> {
        self.notices.subscribe()
    }
}
