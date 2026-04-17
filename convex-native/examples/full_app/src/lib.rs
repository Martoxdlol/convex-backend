//! Library crate for `convex_full_app_example`.
//!
//! Declaring every module here is load-bearing: the
//! `#[convex::query/mutation/action]` attribute macros emit
//! `inventory::submit!(NativeFunctionRegistration { .. })` calls
//! at item scope. Those calls only end up in the linker's
//! collection sections if the module is actually compiled into
//! the target binary. `main.rs` `use`s this crate with `as _`,
//! which forces the linkage.
//!
//! The split between `lib.rs` + `main.rs` is deliberate — it
//! lets tests (`tests/full_app_introspection.rs`) exercise the
//! registration surface without booting a worker.

pub mod actions;
pub mod crons;
pub mod http;
pub mod mutations;
pub mod queries;
pub mod schema;

// Re-exports so downstream code (tests, binary) can `use
// convex_full_app_example::{User, CreateUserArgs, ...}` without
// drilling into the module tree.
pub use mutations::{
    CreateUser,
    CreateUserArgs,
};
pub use queries::{
    GetByEmail,
    GetByEmailArgs,
};
pub use schema::{
    Message,
    MessageField,
    MessageIndex,
    User,
    UserField,
    UserIndex,
};
