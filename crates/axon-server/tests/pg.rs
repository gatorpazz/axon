//! PostgreSQL store tests. They need a database and are skipped unless
//! `AXON_TEST_DATABASE_URL` is set. The role must NOT be a superuser (or have
//! BYPASSRLS), otherwise PostgreSQL skips row-level security entirely; the
//! tests check for that and fail loudly rather than pass vacuously.

use std::{sync::Arc, time::Duration};

use axon_core::{
    JobId, PrincipalId,
    authz::Authorizer,
    gateway::Gateway,
    job::{EventKind, JobSpec, JobState, JobStatus},
    metrics::Metrics,
    runner::JobRunner,
    store::{Actor, EventStore, contract, follow},
    tools,
};
use axon_server::pg::PgStore;
use serde_json::json;
use tokio_stream::StreamExt;

async fn store() -> Option<Arc<PgStore>> {
    let Ok(url) = std::env::var("AXON_TEST_DATABASE_URL") else {
        eprintln!("AXON_TEST_DATABASE_URL is not set; skipping PostgreSQL test");
        return None;
    };
    let store = PgStore::connect(&url).await.expect("connects and migrates");
    let (bypasses_rls,): (bool,) = sqlx::query_as(
        "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(store.pool())
    .await
    .expect("role lookup");
    assert!(
        !bypasses_rls,
        "test role bypasses row-level security; use an unprivileged role"
    );
    Some(Arc::new(store))
}

fn principal(name: &str) -> PrincipalId {
    format!("user:{name}-{}", JobId::new().as_uuid().simple())
        .parse()
        .expect("valid")
}

fn spec() -> JobSpec {
    serde_json::from_value(json!({
        "steps": [
            {"id": "select", "tool": "cohort.query",
             "input": {"biomarker": "amyloid_centiloid", "group_by": "diagnosis"}},
            {"id": "test", "tool": "stats.welch_t_test",
             "input": {"a": {"$ref": "select#/groups/ad"}, "b": {"$ref": "select#/groups/cu"}}},
        ]
    }))
    .expect("valid spec")
}

#[tokio::test]
async fn satisfies_the_store_contract() {
    let Some(store) = store().await else { return };
    contract::run_all(store).await;
}

#[tokio::test]
async fn row_level_security_holds_for_raw_sql() {
    let Some(store) = store().await else { return };
    let alice = principal("alice");
    let id = JobId::new();
    store
        .append(
            &Actor::Principal(alice.clone()),
            id,
            0,
            EventKind::Submitted {
                owner: alice.clone(),
                spec: spec(),
            },
        )
        .await
        .expect("submitted");

    // Bypass the store entirely and ask the database directly.
    let visible_as = |actor: Option<String>| {
        let pool = store.pool().clone();
        async move {
            let mut tx = pool.begin().await.expect("begin");
            if let Some(actor) = actor {
                sqlx::query("SELECT set_config('axon.actor', $1, true)")
                    .bind(actor)
                    .execute(&mut *tx)
                    .await
                    .expect("set actor");
            }
            let (jobs,): (i64,) = sqlx::query_as("SELECT count(*) FROM jobs WHERE id = $1")
                .bind(id.as_uuid())
                .fetch_one(&mut *tx)
                .await
                .expect("count jobs");
            let (events,): (i64,) =
                sqlx::query_as("SELECT count(*) FROM job_events WHERE job_id = $1")
                    .bind(id.as_uuid())
                    .fetch_one(&mut *tx)
                    .await
                    .expect("count events");
            (jobs, events)
        }
    };

    assert_eq!(visible_as(Some(alice.to_string())).await, (1, 1));
    assert_eq!(visible_as(Some("system".into())).await, (1, 1));
    assert_eq!(visible_as(Some(principal("bob").to_string())).await, (0, 0));
    // A connection that never declares an actor sees nothing at all.
    assert_eq!(visible_as(None).await, (0, 0));

    // Writes are blocked too: another principal cannot forge an event...
    let mut tx = store.pool().begin().await.expect("begin");
    sqlx::query("SELECT set_config('axon.actor', $1, true)")
        .bind(principal("mallory").to_string())
        .execute(&mut *tx)
        .await
        .expect("set actor");
    let forged =
        sqlx::query("INSERT INTO job_events (job_id, seq, at, kind) VALUES ($1, 2, now(), '{}')")
            .bind(id.as_uuid())
            .execute(&mut *tx)
            .await;
    let error = forged.expect_err("insert must be rejected").to_string();
    assert!(error.contains("row-level security"), "{error}");
    drop(tx);

    // ...nor create a job in someone else's name.
    let mut tx = store.pool().begin().await.expect("begin");
    sqlx::query("SELECT set_config('axon.actor', $1, true)")
        .bind(principal("mallory").to_string())
        .execute(&mut *tx)
        .await
        .expect("set actor");
    let forged = sqlx::query(
        "INSERT INTO jobs (id, owner, status, created_at, updated_at, last_seq) VALUES ($1, $2, 'queued', now(), now(), 1)",
    )
    .bind(JobId::new().as_uuid())
    .bind(alice.as_str())
    .execute(&mut *tx)
    .await;
    assert!(
        forged
            .expect_err("insert must be rejected")
            .to_string()
            .contains("row-level security")
    );
}

#[tokio::test]
async fn runs_a_job_end_to_end_and_survives_a_restart() {
    let Some(store) = store().await else { return };
    let alice = principal("alice");
    let authz = Authorizer::parse([format!("tool:*#invoker@{alice}").as_str()]).expect("tuples");
    let metrics = Arc::new(Metrics::default());
    let gateway = Arc::new(
        tools::register_builtin(Gateway::builder(Arc::new(authz), metrics.clone())).build(),
    );

    let id = JobId::new();
    store
        .append(
            &Actor::Principal(alice.clone()),
            id,
            0,
            EventKind::Submitted {
                owner: alice.clone(),
                spec: spec(),
            },
        )
        .await
        .expect("submitted");

    // Nothing ran the job yet, as if the process died right after accepting
    // it. A second connection (a "restarted" server) drives it by id; calling
    // `recover()` here would also pick up other tests' unfinished jobs.
    let url = std::env::var("AXON_TEST_DATABASE_URL").expect("checked above");
    let restarted: Arc<dyn EventStore> =
        Arc::new(PgStore::connect(&url).await.expect("reconnects"));
    assert!(
        restarted
            .unfinished()
            .await
            .expect("unfinished")
            .contains(&id)
    );
    JobRunner::new(restarted, gateway, metrics, 2).spawn(id);

    // Follow on the ORIGINAL connection: events written through the other
    // pool arrive via LISTEN/NOTIFY.
    let stream = follow(store.clone(), Actor::Principal(alice), id, 0);
    let events: Vec<_> = tokio::time::timeout(Duration::from_secs(15), stream.collect())
        .await
        .expect("job completes");
    let events: Vec<_> = events.into_iter().map(|e| e.expect("event")).collect();

    let state = JobState::replay(&events).expect("log replays");
    assert_eq!(state.status, JobStatus::Succeeded);
    assert!(
        state.output_of("test").expect("output")["p_value"]
            .as_f64()
            .expect("p")
            < 0.001
    );
    assert!(!store.unfinished().await.expect("unfinished").contains(&id));
}
