//! Jobs: what callers submit ([`JobSpec`]), what happens to them
//! ([`JobEvent`]) and what that adds up to ([`JobState`]).

mod event;
mod spec;
mod state;

pub use event::{EventKind, JobEvent, JobStatus};
pub use spec::{JobSpec, MAX_STEPS, RefError, SpecError, StepSpec, resolve_refs};
pub use state::{JobState, ReplayError, StepState, StepStatus};
