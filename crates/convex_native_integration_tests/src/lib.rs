//! End-to-end integration tests for `convex_native`.
//!
//! This crate exists to prove that every feature exposed by the
//! framework (schema + derives, queries, mutations, actions,
//! scheduler, storage, HTTP actions, crons, error handling,
//! logging, drain, timeouts, circuit breakers, introspection)
//! works correctly in both topologies:
//!
//! - **Standalone** — the monolith shape from `STANDALONE.md`, i.e. a single
//!   process running the framework + a real `Database<ProdRuntime>` in memory.
//!   Tests drive the `NativeFunctionRunner` against the fixture app.
//! - **Distributed** — the split shape from `DISTRIBUTED_PLAN.md`, i.e. a
//!   backend process (owning OCC + subscriptions) talking to a worker process
//!   (owning the handler registry) over tonic. Tests drive a real
//!   `FunctionExecutionServer` over an ephemeral TCP port + a
//!   `DistributedFunctionRunner` / `PoolFunctionRunner`
//!   + a `BackendCallbackServer` for action sub-calls.
//!
//! The shared fixture app (`fixture_app`) is the single source of
//! truth for what the test suite exercises. Whenever the framework
//! grows a new feature, the fixture grows an example that uses it
//! and both topology files gain an assertion.

pub mod db_fixture;
pub mod fixture_app;
