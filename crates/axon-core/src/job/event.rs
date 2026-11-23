//! The event vocabulary. A job *is* its ordered list of events.

use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::spec::JobSpec;
use crate::{
    ids::{JobId, PrincipalId},
    provenance::Provenance,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobEvent {
    pub job_id: JobId,
    /// 1-based, gapless position in the job's log.
    pub seq: u64,
    pub at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    /// Always the first event of a job, and only ever the first.
    Submitted {
        owner: PrincipalId,
        spec: JobSpec,
    },
    StepStarted {
        step: String,
        attempt: u32,
    },
    StepSucceeded {
        step: String,
        output: Value,
        provenance: Provenance,
    },
    StepFailed {
        step: String,
        error: String,
    },
    Succeeded,
    Failed {
        error: String,
    },
    Cancelled {
        by: PrincipalId,
    },
}

impl EventKind {
    /// Wire name, used as the SSE event type.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Submitted { .. } => "submitted",
            Self::StepStarted { .. } => "step_started",
            Self::StepSucceeded { .. } => "step_succeeded",
            Self::StepFailed { .. } => "step_failed",
            Self::Succeeded => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
        }
    }

    /// The job status once this event has been applied.
    pub fn status(&self) -> JobStatus {
        match self {
            Self::Submitted { .. } => JobStatus::Queued,
            Self::StepStarted { .. } | Self::StepSucceeded { .. } | Self::StepFailed { .. } => {
                JobStatus::Running
            }
            Self::Succeeded => JobStatus::Succeeded,
            Self::Failed { .. } => JobStatus::Failed,
            Self::Cancelled { .. } => JobStatus::Cancelled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown job status `{0}`")]
pub struct UnknownStatus(String);

impl FromStr for JobStatus {
    type Err = UnknownStatus;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        [
            Self::Queued,
            Self::Running,
            Self::Succeeded,
            Self::Failed,
            Self::Cancelled,
        ]
        .into_iter()
        .find(|status| status.as_str() == s)
        .ok_or_else(|| UnknownStatus(s.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn events_have_a_flat_tagged_wire_format() {
        let job_id = JobId::new();
        let event = JobEvent {
            job_id,
            seq: 2,
            at: DateTime::UNIX_EPOCH,
            kind: EventKind::StepStarted {
                step: "select".into(),
                attempt: 1,
            },
        };
        let wire = serde_json::to_value(&event).expect("serializes");
        assert_eq!(
            wire,
            json!({
                "job_id": job_id,
                "seq": 2,
                "at": "1970-01-01T00:00:00Z",
                "type": "step_started",
                "step": "select",
                "attempt": 1,
            })
        );
        assert_eq!(
            serde_json::from_value::<JobEvent>(wire).expect("deserializes"),
            event
        );
    }

    #[test]
    fn status_round_trips_through_text() {
        for status in [JobStatus::Queued, JobStatus::Running, JobStatus::Cancelled] {
            assert_eq!(status.to_string().parse(), Ok(status));
        }
        assert!("done".parse::<JobStatus>().is_err());
    }
}
