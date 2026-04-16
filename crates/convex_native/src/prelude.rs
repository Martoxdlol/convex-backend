//! Glob-import target for developer code.
//!
//! `use convex_native::prelude::*;` brings in every type and trait a
//! developer is expected to reference in their schema and function code.
//! The set is intentionally tight; add to it only when the symbol shows up
//! in example code from the design doc.

pub use anyhow::{
    anyhow,
    Result,
};

pub use crate::{
    convert::{
        FromConvex,
        ToConvex,
    },
    document::{
        ConvexDocument,
        ConvexPatch,
        FieldReference,
        IndexReference,
    },
    id::Id,
};
// Derive macros are re-exported from the crate root; bring them into
// scope via the prelude too so `use convex_native::prelude::*;` is
// sufficient for schema & function code alike.
pub use crate::{
    ConvexEnum,
    ConvexNested,
    ConvexUnion,
};
