//! # convex_native
//!
//! Framework crate for writing Convex server functions (queries, mutations,
//! actions) in native Rust. See `convex-native/native-rust-functions.md` in
//! the repository root for the design document.
//!
//! Developer code is expected to bring the common types into scope with
//! `use convex_native::prelude::*;`.

pub mod convert;
pub mod document;
pub mod id;
pub mod prelude;
pub mod registry;
pub mod schema;

pub use convert::{
    FromConvex,
    ToConvex,
};
pub use document::{
    ConvexDocument,
    ConvexPatch,
    FieldReference,
    IndexReference,
};
pub use id::Id;
pub use registry::{
    HandlerFn,
    NativeFunctionRegistration,
    NativeFunctionRegistry,
};
pub use schema::{
    NativeSchema,
    TableRegistration,
};
