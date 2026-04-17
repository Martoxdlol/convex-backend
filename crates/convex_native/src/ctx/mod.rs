//! Typed context wrappers exposed to native function bodies.
//!
//! `QueryCtx` is the read-only wrapper handed to `#[convex::query]`
//! functions. `MutationCtx` extends it with write operations. Both types
//! wrap a borrowed `database::Transaction<RT>` without taking ownership —
//! the `NativeFunctionRunner` owns the transaction and passes a `&mut` to
//! the user's function for the duration of the call.
//!
//! These types are intentionally minimal: they route typed calls into the
//! existing `UserFacingModel` and `Transaction` APIs. See
//! `convex-native/USAGE.md` for the full developer surface.

pub mod action;
pub mod mutation;
pub mod query;
pub mod query_builder;
pub mod scheduler;
pub mod storage;

pub use action::{
    ActionCtx,
    ActionDb,
};
pub use mutation::{
    MutationCtx,
    MutationDb,
};
pub use query::{
    QueryCtx,
    QueryDb,
};
pub use query_builder::TypedQueryBuilder;
pub use scheduler::{
    MutationScheduler,
    Scheduler,
};
pub use storage::{
    FileMetadata,
    StorageCtx,
    StorageId,
};
