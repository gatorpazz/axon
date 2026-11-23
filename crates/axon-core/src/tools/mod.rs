//! Built-in tools.
//!
//! These stand in for real scientific tools so the platform can be exercised
//! end to end. The cohort is **synthetic** (see [`cohort`]); the statistics
//! are real implementations with tests against reference values.

pub mod cohort;
pub mod stats;

use crate::gateway::GatewayBuilder;

/// Registers every built-in tool.
pub fn register_builtin(builder: GatewayBuilder) -> GatewayBuilder {
    builder
        .register(cohort::CohortQuery::default())
        .register(stats::Describe)
        .register(stats::WelchTTest)
}
