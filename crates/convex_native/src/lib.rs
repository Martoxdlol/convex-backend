//! # convex_native
//!
//! Framework crate for writing Convex server functions (queries, mutations,
//! actions) in native Rust. See `convex-native/USAGE.md` for the developer
//! surface and `convex-native/DISTRIBUTED_PLAN.md` for the target topology.
//!
//! Developer code is expected to bring the common types into scope with
//! `use convex_native::prelude::*;`.

// Re-export derive macros so developers only need `convex_native` as a dep.
pub use convex_macro::{
    ConvexDocument,
    ConvexEnum,
    ConvexNested,
    ConvexUnion,
};
#[doc(hidden)]
pub use inventory;

/// Crate version — derived at build time from `CARGO_PKG_VERSION`.
/// Useful for introspection and deployment traceability: surfaced in
/// the `introspect::describe_json` envelope (`convex_native_version`
/// field) and in the worker's `Health` response (`registry_version`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Pseudo-namespace so developers can write `#[convex::query]`,
/// `#[convex::mutation]`, and `#[convex::action]` by importing
/// `convex_native::convex`.
pub mod convex {
    pub use convex_macro::{
        action,
        cron,
        http_action,
        mutation,
        query,
    };
}

/// Items re-exported for use by generated proc-macro code. Not part of the
/// public API.
#[doc(hidden)]
pub mod __private {
    pub use common::{
        bootstrap_model::index::{
            database_index::IndexedFields,
            vector_index::VectorDimensions,
        },
        paths::FieldPath,
        schemas::{
            validator::{
                FieldValidator,
                LiteralValidator,
                ObjectValidator,
                Validator,
            },
            DocumentSchema,
            IndexSchema,
            TableDefinition,
            TextIndexSchema,
            VectorIndexSchema,
        },
        types::IndexDescriptor,
    };
    pub use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
        IdentifierFieldName,
        TableName,
    };

    pub use crate::schema_type::{
        build_object_validator,
        field_validator_for,
        identifier_field,
        string_literal_validator,
    };
}

pub mod auth;
pub mod backend;
pub mod callbacks;
pub mod circuit_breaker;
pub mod convert;
pub mod cron;
pub mod ctx;
pub mod distributed;
pub mod document;
pub mod errors;
pub mod function_ref;
pub mod http;
pub mod id;
pub mod introspect;
pub mod logging;
pub mod metrics;
pub mod prelude;
pub mod registry;
pub mod runner;
pub mod schema;
pub mod schema_diff;
pub mod schema_type;
pub mod testing;
pub mod warmup;

pub use auth::AuthInfo;
pub use backend::{
    BuiltBackend,
    ConvexBackend,
};
pub use callbacks::{
    NativeActionCallbacks,
    NoopCallbacks,
};
pub use circuit_breaker::{
    CircuitBreaker,
    CircuitBreakerConfig,
};
pub use common::query::Cursor;
pub use convert::{
    FromConvex,
    ToConvex,
};
pub use cron::{
    CronRegistration,
    CronRegistry,
};
pub use ctx::{
    query_builder::{
        Order,
        TypedPage,
    },
    ActionCtx,
    ActionDb,
    FileMetadata,
    MutationCtx,
    MutationDb,
    MutationScheduler,
    QueryCtx,
    QueryDb,
    Scheduler,
    StorageCtx,
    StorageId,
    TypedQueryBuilder,
};
pub use distributed::{
    ConvexMode,
    ExecuteRequest,
    ExecuteResponse,
    FunctionExecutor,
};
pub use document::{
    ConvexDocument,
    ConvexPatch,
    DocumentWithMeta,
    FieldReference,
    IndexReference,
};
pub use function_ref::{
    ConvexActionFunction,
    ConvexMutationFunction,
    ConvexQueryFunction,
};
pub use http::{
    HttpActionCtx,
    HttpRequest,
    HttpResponse,
    HttpRouteRegistration,
    HttpRouter,
};
pub use id::Id;
pub use logging::{
    LogBuffer,
    LogLevel,
    Logger,
    NativeLogLine,
};
pub use metrics::{
    CountingMetrics,
    NativeMetricsSink,
    NoopMetrics,
    Outcome,
};
pub use registry::{
    ActionHandlerFn,
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
pub use schema_diff::{
    diff as diff_schemas,
    SchemaChange,
};
pub use schema_type::ConvexSchema;
pub use warmup::{
    plan_warmup,
    WarmupEntry,
};
