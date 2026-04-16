//! # convex_native
//!
//! Framework crate for writing Convex server functions (queries, mutations,
//! actions) in native Rust. See `convex-native/native-rust-functions.md` in
//! the repository root for the design document.
//!
//! Developer code is expected to bring the common types into scope with
//! `use convex_native::prelude::*;`.

// Re-export derive macros so developers only need `convex_native` as a dep.
pub use convex_macro::ConvexDocument;
#[doc(hidden)]
pub use inventory;

/// Pseudo-namespace so developers can write `#[convex::query]` and
/// `#[convex::mutation]` by importing `convex_native::convex`.
pub mod convex {
    pub use convex_macro::{
        mutation,
        query,
    };
}

/// Items re-exported for use by generated proc-macro code. Not part of the
/// public API.
#[doc(hidden)]
pub mod __private {
    pub use common::{
        bootstrap_model::index::database_index::IndexedFields,
        paths::FieldPath,
        schemas::{
            IndexSchema,
            TableDefinition,
        },
        types::IndexDescriptor,
    };
    pub use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
        TableName,
    };
}

pub mod convert;
pub mod ctx;
pub mod document;
pub mod id;
pub mod prelude;
pub mod registry;
pub mod runner;
pub mod schema;

pub use convert::{
    FromConvex,
    ToConvex,
};
pub use ctx::{
    query_builder::Order,
    MutationCtx,
    MutationDb,
    QueryCtx,
    QueryDb,
    TypedQueryBuilder,
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
    MutationHandlerFn,
    NativeFunctionRegistration,
    NativeFunctionRegistry,
    QueryHandlerFn,
    Rt,
};
pub use runner::NativeFunctionRunner;
pub use schema::{
    NativeSchema,
    TableRegistration,
};
