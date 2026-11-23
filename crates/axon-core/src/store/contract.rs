//! Behavioural contract for [`EventStore`] backends.
//!
//! Every backend's test suite calls [`run_all`]; the in-memory and PostgreSQL
//! stores must be observably identical. The checks panic on violation, which
//! is the desired behaviour inside a `#[test]`.
//!
//! Each check uses fresh job ids and distinct principals, so the suite is safe
//! to run against a shared database.

use std::{sync::Arc, time::Duration};

use serde_json::json;
use tokio_stream::StreamExt;

use super::{Actor, EventStore, Notice, StoreError, follow};
use crate::{
    ids::{JobId, PrincipalId},
    job::{EventKind, JobSpec, JobStatus},
};

pub async fn run_all(store: Arc<dyn EventStore>) {
    appends_and_reads_back(&*store).await;
    enforces_optimistic_concurrency(&*store).await;
    validates_first_and_later_events(&*store).await;
    isolates_principals(&*store).await;
    tracks_unfinished_jobs(&*store).await;
    notifies_subscribers(&*store).await;
    follow_replays_then_tails(store).await;
}

fn principal(name: &str) -> PrincipalId {
    // Unique per run so repeated runs against one database do not interfere.
    format!("user:{name}-{}", JobId::new().as_uuid().simple())
        .parse()
        .expect("valid principal id")
}

fn submitted(owner: &PrincipalId) -> EventKind {
    let spec: JobSpec = serde_json::from_value(json!({
        "name": "contract",
        "steps": [{"id": "only", "tool": "noop", "input": {"x": 1.5, "nested": [null, "s"]}}],
    }))
    .expect("valid spec");
    EventKind::Submitted {
        owner: owner.clone(),
        spec,
    }
}

fn started() -> EventKind {
    EventKind::StepStarted {
        step: "only".into(),
        attempt: 1,
    }
}

async fn create(store: &dyn EventStore, owner: &PrincipalId) -> JobId {
    let id = JobId::new();
    store
        .append(&Actor::Principal(owner.clone()), id, 0, submitted(owner))
        .await
        .expect("job is created");
    id
}

async fn appends_and_reads_back(store: &dyn EventStore) {
    let owner = principal("alice");
    let actor = Actor::Principal(owner.clone());
    let id = JobId::new();

    let first = store
        .append(&actor, id, 0, submitted(&owner))
        .await
        .expect("create");
    let second = store
        .append(&Actor::System, id, 1, started())
        .await
        .expect("append");
    assert_eq!((first.seq, second.seq), (1, 2));

    // Events survive a round trip bit-for-bit, including timestamps.
    let all = store.events_after(&actor, id, 0).await.expect("read");
    assert_eq!(all, vec![first, second.clone()]);
    assert_eq!(
        store.events_after(&actor, id, 1).await.expect("read"),
        vec![second.clone()]
    );
    assert!(
        store
            .events_after(&actor, id, 2)
            .await
            .expect("read")
            .is_empty()
    );
    assert!(
        store
            .events_after(&actor, id, 99)
            .await
            .expect("read")
            .is_empty()
    );

    let summary = store.job(&actor, id).await.expect("summary");
    assert_eq!(summary.owner, owner);
    assert_eq!(summary.status, JobStatus::Running);
    assert_eq!(summary.last_seq, 2);
    assert_eq!(summary.updated_at, second.at);

    assert!(matches!(
        store.events_after(&actor, JobId::new(), 0).await,
        Err(StoreError::NotFound)
    ));
}

async fn enforces_optimistic_concurrency(store: &dyn EventStore) {
    let owner = principal("alice");
    let id = create(store, &owner).await;

    assert!(matches!(
        store.append(&Actor::System, id, 0, submitted(&owner)).await,
        Err(StoreError::Conflict {
            expected: 0,
            actual: 1
        })
    ));
    assert!(matches!(
        store.append(&Actor::System, id, 5, started()).await,
        Err(StoreError::Conflict {
            expected: 5,
            actual: 1
        })
    ));

    // Of two writers racing from the same position, exactly one wins.
    let (a, b) = tokio::join!(
        store.append(&Actor::System, id, 1, started()),
        store.append(
            &Actor::System,
            id,
            1,
            EventKind::Cancelled { by: owner.clone() }
        ),
    );
    assert!(
        matches!(
            (&a, &b),
            (Ok(_), Err(StoreError::Conflict { .. })) | (Err(StoreError::Conflict { .. }), Ok(_))
        ),
        "expected one winner, got {a:?} / {b:?}"
    );
    assert_eq!(
        store
            .events_after(&Actor::System, id, 0)
            .await
            .expect("read")
            .len(),
        2
    );
}

async fn validates_first_and_later_events(store: &dyn EventStore) {
    let owner = principal("alice");
    let other = principal("mallory");

    assert!(matches!(
        store
            .append(&Actor::System, JobId::new(), 0, started())
            .await,
        Err(StoreError::InvalidAppend(_))
    ));
    assert!(matches!(
        store
            .append(&Actor::Principal(other), JobId::new(), 0, submitted(&owner))
            .await,
        Err(StoreError::InvalidAppend(_))
    ));
    let id = create(store, &owner).await;
    assert!(matches!(
        store.append(&Actor::System, id, 1, submitted(&owner)).await,
        Err(StoreError::InvalidAppend(_))
    ));
    assert!(matches!(
        store
            .append(&Actor::System, JobId::new(), 3, started())
            .await,
        Err(StoreError::NotFound)
    ));
}

async fn isolates_principals(store: &dyn EventStore) {
    let alice = principal("alice");
    let bob = Actor::Principal(principal("bob"));
    let id = create(store, &alice).await;

    // To bob, alice's job does not exist: same error as a random id.
    assert!(matches!(
        store.job(&bob, id).await,
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        store.events_after(&bob, id, 0).await,
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        store.append(&bob, id, 1, started()).await,
        Err(StoreError::NotFound)
    ));
    assert!(store.jobs(&bob).await.expect("list").is_empty());

    // The failed append left no trace.
    let alice = Actor::Principal(alice);
    assert_eq!(store.job(&alice, id).await.expect("summary").last_seq, 1);
    let mine = store.jobs(&alice).await.expect("list");
    assert_eq!(mine.iter().map(|j| j.id).collect::<Vec<_>>(), vec![id]);
    assert!(
        store
            .jobs(&Actor::System)
            .await
            .expect("list")
            .iter()
            .any(|j| j.id == id)
    );
}

async fn tracks_unfinished_jobs(store: &dyn EventStore) {
    let owner = principal("alice");
    let running = create(store, &owner).await;
    let done = create(store, &owner).await;
    store
        .append(
            &Actor::System,
            done,
            1,
            EventKind::Failed {
                error: "boom".into(),
            },
        )
        .await
        .expect("append");

    let unfinished = store.unfinished().await.expect("unfinished");
    assert!(unfinished.contains(&running));
    assert!(!unfinished.contains(&done));

    // Listing is newest first.
    let ids: Vec<_> = store
        .jobs(&Actor::Principal(owner))
        .await
        .expect("list")
        .iter()
        .map(|j| j.id)
        .collect();
    assert_eq!(ids, vec![done, running]);
}

async fn notifies_subscribers(store: &dyn EventStore) {
    let mut notices = store.subscribe();
    let id = create(store, &principal("alice")).await;

    let wait = async {
        loop {
            match notices.recv().await {
                Ok(notice) if notice.job_id == id => return notice,
                Ok(_) => {}
                Err(error) => panic!("subscription failed: {error}"),
            }
        }
    };
    let notice = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("a notice arrives");
    assert_eq!(notice, Notice { job_id: id, seq: 1 });
}

async fn follow_replays_then_tails(store: Arc<dyn EventStore>) {
    let owner = principal("alice");
    let actor = Actor::Principal(owner.clone());
    let id = create(&*store, &owner).await;
    store
        .append(&Actor::System, id, 1, started())
        .await
        .expect("append");

    // Start mid-log: seq 2 is replayed, 3 and 4 arrive live.
    let mut stream = follow(store.clone(), actor, id, 1);
    let writer = {
        let store = store.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let failed = EventKind::StepFailed {
                step: "only".into(),
                error: "x".into(),
            };
            store
                .append(&Actor::System, id, 2, failed)
                .await
                .expect("append");
            let failed = EventKind::Failed { error: "x".into() };
            store
                .append(&Actor::System, id, 3, failed)
                .await
                .expect("append");
        }
    };
    let reader = async {
        let mut seqs = Vec::new();
        while let Some(event) = stream.next().await {
            seqs.push(event.expect("event").seq);
        }
        seqs
    };
    let (seqs, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(reader, writer)
    })
    .await
    .expect("stream completes after the terminal event");
    assert_eq!(seqs, vec![2, 3, 4]);

    // Following someone else's job yields an error, not their events.
    let mut stream = follow(store, Actor::Principal(principal("bob")), id, 0);
    assert!(matches!(
        stream.next().await,
        Some(Err(StoreError::NotFound))
    ));
}
