//! Job state as a pure left fold over events.
//!
//! Nothing here performs I/O. The runner, the API and the tests all derive
//! state the same way, so "what the log says" and "what the job is" cannot
//! drift apart.

use serde::Serialize;
use serde_json::Value;

use super::{
    event::{EventKind, JobEvent, JobStatus},
    spec::JobSpec,
};
use crate::{
    ids::{JobId, PrincipalId},
    provenance::Provenance,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StepState {
    pub id: String,
    pub tool: String,
    pub status: StepStatus,
    /// How many times the step was started. More than one means the runner was
    /// interrupted mid-step and resumed.
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JobState {
    pub id: JobId,
    pub owner: PrincipalId,
    pub status: JobStatus,
    pub last_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub steps: Vec<StepState>,
    #[serde(skip)]
    pub spec: JobSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReplayError {
    #[error("a job's log must start with a `submitted` event at seq 1")]
    NotSubmitted,
    #[error("expected seq {expected}, got {got}")]
    Gap { expected: u64, got: u64 },
    #[error("`{0}` event after the job reached a terminal status")]
    AfterTerminal(&'static str),
    #[error("`submitted` may only be the first event")]
    Resubmitted,
    #[error("event refers to unknown step `{0}`")]
    UnknownStep(String),
}

impl JobState {
    /// Rebuilds state from a complete log.
    pub fn replay<'a>(events: impl IntoIterator<Item = &'a JobEvent>) -> Result<Self, ReplayError> {
        let mut events = events.into_iter();
        let first = events.next().ok_or(ReplayError::NotSubmitted)?;
        let EventKind::Submitted { owner, spec } = &first.kind else {
            return Err(ReplayError::NotSubmitted);
        };
        if first.seq != 1 {
            return Err(ReplayError::NotSubmitted);
        }
        let mut state = Self {
            id: first.job_id,
            owner: owner.clone(),
            status: JobStatus::Queued,
            last_seq: 1,
            error: None,
            steps: spec
                .steps
                .iter()
                .map(|step| StepState {
                    id: step.id.clone(),
                    tool: step.tool.clone(),
                    status: StepStatus::Pending,
                    attempts: 0,
                    output: None,
                    provenance: None,
                    error: None,
                })
                .collect(),
            spec: spec.clone(),
        };
        events.try_for_each(|event| state.apply(event))?;
        Ok(state)
    }

    /// Applies the next event. On error the state is left untouched.
    pub fn apply(&mut self, event: &JobEvent) -> Result<(), ReplayError> {
        if event.seq != self.last_seq + 1 {
            return Err(ReplayError::Gap {
                expected: self.last_seq + 1,
                got: event.seq,
            });
        }
        if self.status.is_terminal() {
            return Err(ReplayError::AfterTerminal(event.kind.name()));
        }
        match &event.kind {
            EventKind::Submitted { .. } => return Err(ReplayError::Resubmitted),
            EventKind::StepStarted { step, attempt } => {
                let step = self.step_mut(step)?;
                step.status = StepStatus::Running;
                step.attempts = *attempt;
            }
            EventKind::StepSucceeded {
                step,
                output,
                provenance,
            } => {
                let step = self.step_mut(step)?;
                step.status = StepStatus::Succeeded;
                step.output = Some(output.clone());
                step.provenance = Some(provenance.clone());
            }
            EventKind::StepFailed { step, error } => {
                let step = self.step_mut(step)?;
                step.status = StepStatus::Failed;
                step.error = Some(error.clone());
            }
            EventKind::Failed { error } => self.error = Some(error.clone()),
            EventKind::Succeeded | EventKind::Cancelled { .. } => {}
        }
        self.status = event.kind.status();
        self.last_seq = event.seq;
        Ok(())
    }

    /// Index of the first step that has not succeeded yet.
    pub fn next_runnable(&self) -> Option<usize> {
        self.steps
            .iter()
            .position(|s| s.status != StepStatus::Succeeded)
    }

    pub fn output_of(&self, step: &str) -> Option<&Value> {
        self.steps.iter().find(|s| s.id == step)?.output.as_ref()
    }

    fn step_mut(&mut self, id: &str) -> Result<&mut StepState, ReplayError> {
        self.steps
            .iter_mut()
            .find(|s| s.id == id)
            .ok_or_else(|| ReplayError::UnknownStep(id.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::provenance;

    fn log(kinds: Vec<EventKind>) -> Vec<JobEvent> {
        let job_id = JobId::new();
        kinds
            .into_iter()
            .zip(1..)
            .map(|(kind, seq)| JobEvent {
                job_id,
                seq,
                at: provenance::now(),
                kind,
            })
            .collect()
    }

    fn submitted() -> EventKind {
        EventKind::Submitted {
            owner: "user:alice".parse().expect("valid"),
            spec: serde_json::from_value(json!({
                "steps": [{"id": "a", "tool": "t"}, {"id": "b", "tool": "t"}]
            }))
            .expect("spec"),
        }
    }

    fn succeeded(step: &str) -> EventKind {
        EventKind::StepSucceeded {
            step: step.into(),
            output: json!({"step": step}),
            provenance: Provenance {
                invocation_id: uuid::Uuid::nil(),
                tool: "t".into(),
                tool_version: "1".into(),
                principal: "user:alice".parse().expect("valid"),
                input_sha256: String::new(),
                output_sha256: String::new(),
                started_at: provenance::now(),
                duration_ms: 0,
            },
        }
    }

    fn started(step: &str, attempt: u32) -> EventKind {
        EventKind::StepStarted {
            step: step.into(),
            attempt,
        }
    }

    #[test]
    fn folds_a_successful_run() {
        let events = log(vec![
            submitted(),
            started("a", 1),
            succeeded("a"),
            started("b", 1),
            succeeded("b"),
            EventKind::Succeeded,
        ]);
        let state = JobState::replay(&events).expect("replays");
        assert_eq!(state.status, JobStatus::Succeeded);
        assert_eq!(state.last_seq, 6);
        assert_eq!(state.next_runnable(), None);
        assert_eq!(state.output_of("b"), Some(&json!({"step": "b"})));
    }

    #[test]
    fn an_interrupted_step_is_runnable_again() {
        let events = log(vec![
            submitted(),
            started("a", 1),
            succeeded("a"),
            started("b", 1),
        ]);
        let state = JobState::replay(&events).expect("replays");
        assert_eq!(state.status, JobStatus::Running);
        assert_eq!(state.next_runnable(), Some(1));
        assert_eq!(state.steps[1].attempts, 1);
    }

    #[test]
    fn rejects_malformed_logs() {
        assert_eq!(JobState::replay(&[]), Err(ReplayError::NotSubmitted));
        assert_eq!(
            JobState::replay(&log(vec![started("a", 1)])),
            Err(ReplayError::NotSubmitted)
        );
        assert_eq!(
            JobState::replay(&log(vec![submitted(), submitted()])),
            Err(ReplayError::Resubmitted)
        );
        assert_eq!(
            JobState::replay(&log(vec![submitted(), started("zzz", 1)])),
            Err(ReplayError::UnknownStep("zzz".into()))
        );
        assert_eq!(
            JobState::replay(&log(vec![
                submitted(),
                EventKind::Succeeded,
                started("a", 1)
            ])),
            Err(ReplayError::AfterTerminal("step_started"))
        );

        let mut gappy = log(vec![submitted(), started("a", 1)]);
        gappy[1].seq = 5;
        assert_eq!(
            JobState::replay(&gappy),
            Err(ReplayError::Gap {
                expected: 2,
                got: 5
            })
        );
    }

    #[test]
    fn a_rejected_event_leaves_state_untouched() {
        let events = log(vec![submitted(), started("a", 1)]);
        let mut state = JobState::replay(&events).expect("replays");
        let before = state.clone();
        let mut bad = events[1].clone();
        bad.seq = 3;
        bad.kind = started("nope", 1);
        assert!(state.apply(&bad).is_err());
        assert_eq!(state, before);
    }
}
