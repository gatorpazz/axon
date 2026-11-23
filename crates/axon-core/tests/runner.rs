//! End-to-end tests of the job runner against the in-memory store.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axon_core::{
    JobId, PrincipalId,
    authz::Authorizer,
    gateway::Gateway,
    job::{EventKind, JobEvent, JobSpec, JobState, JobStatus, StepStatus},
    metrics::Metrics,
    runner::JobRunner,
    store::{Actor, EventStore, MemoryStore, follow},
    tool::{Tool, ToolContext, ToolError},
    tools,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_stream::StreamExt;

/// Counts its calls, optionally sleeps, optionally fails.
#[derive(Clone, Default)]
struct Probe {
    calls: Arc<AtomicUsize>,
}

#[derive(Deserialize, JsonSchema)]
struct ProbeInput {
    #[serde(default)]
    sleep_ms: u64,
    #[serde(default)]
    fail: bool,
    #[serde(default)]
    echo: Value,
}

impl Tool for Probe {
    const NAME: &'static str = "test.probe";
    const VERSION: &'static str = "0.0.1";
    const DESCRIPTION: &'static str = "Test helper.";
    type Input = ProbeInput;
    type Output = Value;

    async fn call(&self, _: &ToolContext, input: ProbeInput) -> Result<Value, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(input.sleep_ms)).await;
        if input.fail {
            return Err(ToolError::Failed("probe was told to fail".into()));
        }
        Ok(json!({ "echo": input.echo }))
    }
}

struct Harness {
    store: Arc<MemoryStore>,
    runner: JobRunner,
    metrics: Arc<Metrics>,
    probe: Probe,
}

fn alice() -> PrincipalId {
    "user:alice".parse().expect("valid")
}

fn harness() -> Harness {
    let authz = Authorizer::parse([
        "tool:*#invoker@user:alice",
        "tool:stats.describe#invoker@user:bob",
    ])
    .expect("tuples");
    let metrics = Arc::new(Metrics::default());
    let probe = Probe::default();
    let gateway = tools::register_builtin(Gateway::builder(Arc::new(authz), metrics.clone()))
        .register(probe.clone())
        .build();
    let store = Arc::new(MemoryStore::default());
    let runner = JobRunner::new(store.clone(), Arc::new(gateway), metrics.clone(), 4);
    Harness {
        store,
        runner,
        metrics,
        probe,
    }
}

impl Harness {
    async fn submit(&self, owner: &PrincipalId, spec: Value) -> JobId {
        let spec: JobSpec = serde_json::from_value(spec).expect("spec deserializes");
        spec.validate().expect("spec is valid");
        let id = JobId::new();
        self.store
            .append(
                &Actor::Principal(owner.clone()),
                id,
                0,
                EventKind::Submitted {
                    owner: owner.clone(),
                    spec,
                },
            )
            .await
            .expect("submitted");
        id
    }

    /// Follows the job to its terminal event and returns the full log.
    async fn finish(&self, id: JobId) -> Vec<JobEvent> {
        let stream = follow(self.store.clone(), Actor::System, id, 0);
        let events: Vec<_> = tokio::time::timeout(Duration::from_secs(10), stream.collect())
            .await
            .expect("job reaches a terminal status");
        events.into_iter().map(|e| e.expect("event")).collect()
    }
}

fn names(events: &[JobEvent]) -> Vec<&'static str> {
    events.iter().map(|e| e.kind.name()).collect()
}

#[tokio::test]
async fn runs_a_pipeline_and_threads_outputs_between_steps() {
    let h = harness();
    let id = h
        .submit(
            &alice(),
            json!({
                "name": "apoe4 vs p-tau217",
                "steps": [
                    {"id": "select", "tool": "cohort.query",
                     "input": {"biomarker": "plasma_ptau217", "group_by": "apoe4_carrier"}},
                    {"id": "test", "tool": "stats.welch_t_test",
                     "input": {"a": {"$ref": "select#/groups/carrier"},
                               "b": {"$ref": "select#/groups/non_carrier"}}},
                    {"id": "carriers", "tool": "stats.describe",
                     "input": {"values": {"$ref": "select#/groups/carrier"}}},
                ],
            }),
        )
        .await;
    h.runner.spawn(id);

    let events = h.finish(id).await;
    assert_eq!(
        names(&events),
        [
            "submitted",
            "step_started",
            "step_succeeded",
            "step_started",
            "step_succeeded",
            "step_started",
            "step_succeeded",
            "succeeded",
        ]
    );

    let state = JobState::replay(&events).expect("log replays");
    assert_eq!(state.status, JobStatus::Succeeded);

    // The t-test really received the two groups selected by the first step.
    let groups = &state.output_of("select").expect("select output")["groups"];
    let test = state.output_of("test").expect("test output");
    assert_eq!(
        test["n_a"],
        groups["carrier"].as_array().expect("array").len()
    );
    assert_eq!(
        test["n_b"],
        groups["non_carrier"].as_array().expect("array").len()
    );
    // The synthetic cohort bakes in a carrier effect; the pipeline finds it.
    assert!(test["mean_difference"].as_f64().expect("number") > 0.0);
    assert!(test["p_value"].as_f64().expect("number") < 0.001);

    // Every step result carries provenance tying it to tool, version and caller.
    for step in &state.steps {
        let provenance = step.provenance.as_ref().expect("provenance");
        assert_eq!(provenance.tool, step.tool);
        assert_eq!(provenance.principal, alice());
        assert_eq!(
            provenance.output_sha256,
            axon_core::provenance::digest(step.output.as_ref().expect("output"))
        );
    }
    assert_eq!(
        h.metrics
            .get("axon_jobs_finished_total", &[("status", "succeeded")]),
        1
    );
}

#[tokio::test]
async fn a_failing_step_fails_the_job_and_skips_the_rest() {
    let h = harness();
    let id = h
        .submit(
            &alice(),
            json!({"steps": [
                {"id": "ok", "tool": "test.probe"},
                {"id": "bad", "tool": "test.probe", "input": {"fail": true}},
                {"id": "never", "tool": "test.probe"},
            ]}),
        )
        .await;
    h.runner.spawn(id);

    let events = h.finish(id).await;
    assert_eq!(
        names(&events),
        [
            "submitted",
            "step_started",
            "step_succeeded",
            "step_started",
            "step_failed",
            "failed"
        ]
    );
    let state = JobState::replay(&events).expect("log replays");
    assert_eq!(
        state.error.as_deref(),
        Some("step `bad` failed: probe was told to fail")
    );
    assert_eq!(state.steps[2].status, StepStatus::Pending);
    assert_eq!(h.probe.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn authorization_is_enforced_at_execution_time() {
    // bob may only use stats.describe; even if such a job gets into the log,
    // the gateway refuses the step when it runs.
    let h = harness();
    let bob: PrincipalId = "user:bob".parse().expect("valid");
    let id = h
        .submit(
            &bob,
            json!({"steps": [{"id": "q", "tool": "cohort.query", "input": {"biomarker": "age"}}]}),
        )
        .await;
    h.runner.spawn(id);

    let state = JobState::replay(&h.finish(id).await).expect("log replays");
    assert_eq!(state.status, JobStatus::Failed);
    assert_eq!(
        state.steps[0].error.as_deref(),
        Some("`user:bob` may not invoke `cohort.query`")
    );
}

#[tokio::test]
async fn a_dangling_reference_fails_the_step_without_calling_the_tool() {
    let h = harness();
    let id = h
        .submit(
            &alice(),
            json!({"steps": [
                {"id": "a", "tool": "test.probe"},
                {"id": "b", "tool": "test.probe", "input": {"echo": {"$ref": "a#/no/such/path"}}},
            ]}),
        )
        .await;
    h.runner.spawn(id);

    let state = JobState::replay(&h.finish(id).await).expect("log replays");
    assert_eq!(state.status, JobStatus::Failed);
    assert_eq!(h.probe.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_preempts_a_running_step() {
    let h = harness();
    let id = h
        .submit(
            &alice(),
            json!({"steps": [
                {"id": "slow", "tool": "test.probe", "input": {"sleep_ms": 20_000}},
                {"id": "after", "tool": "test.probe"},
            ]}),
        )
        .await;
    let handle = h.runner.spawn(id);

    // Wait until the step is in flight, then cancel the way the API does:
    // an optimistic append on top of the latest event.
    let mut stream = follow(h.store.clone(), Actor::System, id, 0);
    let mut last_seq = 0;
    while let Some(event) = stream.next().await {
        let event = event.expect("event");
        last_seq = event.seq;
        if matches!(event.kind, EventKind::StepStarted { .. }) {
            break;
        }
    }
    h.store
        .append(
            &Actor::Principal(alice()),
            id,
            last_seq,
            EventKind::Cancelled { by: alice() },
        )
        .await
        .expect("cancel is appended");

    // The runner stops promptly instead of sleeping out the 20 s tool call.
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("runner notices the cancellation")
        .expect("runner task does not panic");

    let events = h.finish(id).await;
    assert_eq!(names(&events), ["submitted", "step_started", "cancelled"]);
    assert_eq!(h.probe.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_resumes_from_the_log_without_repeating_finished_steps() {
    let h = harness();
    let id = h
        .submit(
            &alice(),
            json!({"steps": [
                {"id": "a", "tool": "test.probe", "input": {"echo": 1}},
                {"id": "b", "tool": "test.probe",
                 "input": {"echo": {"$ref": "a#/echo"}, "sleep_ms": 300}},
            ]}),
        )
        .await;

    // Simulate a process that finished step `a`, started `b`, then died: run
    // the real runner and kill its task while `b` is still in flight.
    let doomed = h.runner.spawn(id);
    let mut stream = follow(h.store.clone(), Actor::System, id, 0);
    while let Some(event) = stream.next().await {
        if matches!(event.expect("event").kind, EventKind::StepStarted { ref step, .. } if step == "b")
        {
            break;
        }
    }
    doomed.abort();
    let _ = doomed.await;

    // "Restart": recovery finds the unfinished job in the store and resumes it.
    assert_eq!(h.runner.recover().await.expect("recover"), 1);

    let events = h.finish(id).await;
    let state = JobState::replay(&events).expect("log replays");
    assert_eq!(state.status, JobStatus::Succeeded);
    assert_eq!(state.output_of("b"), Some(&json!({"echo": 1})));

    // `a` was not executed again; `b` was retried as attempt 2.
    assert_eq!((state.steps[0].attempts, state.steps[1].attempts), (1, 2));
    let a_successes = events
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::StepSucceeded { step, .. } if step == "a"))
        .count();
    assert_eq!(a_successes, 1);
    assert!(
        h.probe.calls.load(Ordering::SeqCst) <= 3,
        "a once, b at most twice"
    );
}

#[tokio::test]
async fn a_step_that_keeps_getting_interrupted_is_given_up_on() {
    let h = harness();
    let id = h
        .submit(
            &alice(),
            json!({"steps": [{"id": "a", "tool": "test.probe"}]}),
        )
        .await;
    // Three starts with no completion: three crashes mid-step.
    for attempt in 1..=3 {
        h.store
            .append(
                &Actor::System,
                id,
                u64::from(attempt),
                EventKind::StepStarted {
                    step: "a".into(),
                    attempt,
                },
            )
            .await
            .expect("append");
    }
    h.runner.recover().await.expect("recover");

    let state = JobState::replay(&h.finish(id).await).expect("log replays");
    assert_eq!(state.status, JobStatus::Failed);
    assert_eq!(h.probe.calls.load(Ordering::SeqCst), 0);
}
