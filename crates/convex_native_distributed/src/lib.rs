//! Distributed execution for `convex_native`.
//!
//! Per `convex-native/IMPLEMENTATION_PLAN.md` Phase 3.2+:
//! this crate implements the worker-side gRPC server and
//! conductor-side client generated from
//! `crates/pb/protos/function_execution.proto` (Phase 3.1).
//!
//! Currently shipped (this file): **conversions** between the
//! generated `pb::function_execution::*` proto messages and the
//! tonic-free Rust shapes in `convex_native::distributed`. Server
//! and client scaffolding land in follow-up commits.
//!
//! Why conversions first? They're the stable contract between the
//! wire format and the in-process types — the `FunctionExecutor`
//! trait in `convex_native::distributed` already takes
//! `ExecuteRequest`/`ExecuteResponse`, so the server can be
//! implemented as a thin decoder that maps proto → native → runner
//! → native → proto. Landing the conversions independently lets us
//! test them without pulling in a real gRPC transport.

pub mod conversions;
pub mod server;

pub use server::FunctionExecutionServer;
