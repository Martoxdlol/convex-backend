//! Typed query builder.
//!
//! Today this is a compile-time scaffold: it accepts `T::Index` and
//! `T::Field` values so incorrect uses (e.g. `query::<User>()
//! .with_index(MessageIndex::X)`) are rejected at compile time. Runtime
//! execution (`.collect()`, `.first()`, `.page()`) is not wired up yet —
//! it will land alongside the full `NativeFunctionRunner`
//! (Phase 1.4). Calling the terminal methods returns `unimplemented!()`
//! for now, which keeps the builder usable in type-check-only code paths
//! during subsequent development.

use std::marker::PhantomData;

use common::runtime::Runtime;
use database::Transaction;
use value::{
    ConvexValue,
    TableNamespace,
};

use super::query::QueryDb;
use crate::{
    convert::ToConvex,
    document::{
        ConvexDocument,
        FieldReference,
        IndexReference,
    },
};

/// Ordering used for index traversal.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

pub struct TypedQueryBuilder<'db, 'tx, RT: Runtime, T: ConvexDocument> {
    // We hold onto the raw transaction + namespace directly rather than a
    // `&mut QueryDb` so the builder can be constructed from both
    // `QueryDb::query()` and `MutationDb::query()` without a second layer
    // of lifetime gymnastics.
    _tx: &'db mut Transaction<RT>,
    _namespace: TableNamespace,
    _borrow: PhantomData<&'tx ()>,
    index: Option<&'static str>,
    index_fields: &'static [&'static str],
    filters: Vec<EqFilter>,
    order: Order,
    limit: Option<usize>,
    _marker: PhantomData<fn() -> T>,
}

#[allow(dead_code)] // consumed when Phase 1.4 wires the runner through.
struct EqFilter {
    field: &'static str,
    value: ConvexValue,
}

impl<'db, 'tx, RT: Runtime, T: ConvexDocument> TypedQueryBuilder<'db, 'tx, RT, T> {
    pub(crate) fn new(db: &'db mut QueryDb<'tx, RT>) -> Self {
        // Rebind lifetimes — `db` contains a `&'tx mut Transaction<RT>`
        // but we only need access for `'db`.
        Self::new_from_parts(db.tx, db.namespace)
    }

    pub(crate) fn new_from_parts(tx: &'db mut Transaction<RT>, namespace: TableNamespace) -> Self {
        Self {
            _tx: tx,
            _namespace: namespace,
            _borrow: PhantomData,
            index: None,
            index_fields: &[],
            filters: Vec::new(),
            order: Order::Asc,
            limit: None,
            _marker: PhantomData,
        }
    }

    /// Use a specific index. The index must belong to table `T`; mixing
    /// indexes across tables is a compile-time error.
    pub fn with_index(mut self, index: T::Index) -> Self {
        self.index = Some(index.as_str());
        self.index_fields = index.fields();
        self
    }

    /// Equality filter on a field of `T`. `T::Field` ensures compile-time
    /// field validation.
    pub fn eq<V: ToConvex>(mut self, field: T::Field, value: V) -> anyhow::Result<Self> {
        let v = value.to_convex()?;
        self.filters.push(EqFilter {
            field: field.as_str(),
            value: v,
        });
        Ok(self)
    }

    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Terminal: collect all matching documents into a `Vec`.
    ///
    /// Not yet wired through to the database — see module doc comment.
    pub async fn collect(self) -> anyhow::Result<Vec<T>> {
        let _ = (
            self.index,
            self.index_fields,
            self.filters,
            self.order,
            self.limit,
        );
        anyhow::bail!("TypedQueryBuilder::collect() is not yet implemented (Phase 1.4 work)")
    }

    /// Terminal: return the first matching document, if any.
    pub async fn first(self) -> anyhow::Result<Option<T>> {
        let _ = (
            self.index,
            self.index_fields,
            self.filters,
            self.order,
            self.limit,
        );
        anyhow::bail!("TypedQueryBuilder::first() is not yet implemented (Phase 1.4 work)")
    }
}
