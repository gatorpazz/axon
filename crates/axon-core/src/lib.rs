//! Domain core for **axon**: a small platform slice for running agent jobs
//! against scientific tools.
//!
//! The crate is transport-agnostic and has no I/O of its own beyond the
//! [`store::EventStore`] trait:
//!
//! * [`authz`] – relationship-based authorization (Zanzibar-style tuples).
//! * [`tool`] / [`gateway`] – typed tools, type-erased behind one gateway that
//!   authorizes, bounds, measures and stamps [`provenance`] on every call.
//! * [`job`] – job specs, the event vocabulary and the state fold.
//! * [`store`] – the append-only event log abstraction plus an in-memory
//!   implementation and a contract test suite every backend must pass.
//! * [`runner`] – the durable executor that drives jobs from their event log.

pub mod authz;
pub mod gateway;
pub mod ids;
pub mod job;
pub mod metrics;
pub mod provenance;
pub mod runner;
pub mod store;
pub mod tool;
pub mod tools;

pub use ids::{JobId, PrincipalId};
