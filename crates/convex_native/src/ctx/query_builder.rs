//! Typed query builder.
//!
//! Given a derived `ConvexDocument` type `T`, this builder turns typed
//! `T::Index` / `T::Field` choices into the internal `Query` /
//! `IndexRange` representation and drives the resulting
//! `DeveloperQuery<RT>` to completion.
//!
//! Compile-time guarantees:
//! - `with_index(T::Index)` only accepts indexes declared on `T`.
//! - `eq(T::Field, value)` only accepts fields declared on `T`.
//!
//! Runtime guarantees:
//! - Documents fetched from the database are parsed through
//!   `T::from_convex_object`, so type mismatches surface as errors from the
//!   same conversion layer as `#[derive(ConvexDocument)]`.

use std::marker::PhantomData;

use common::{
    query::{
        FullTableScan,
        IndexRange,
        IndexRangeExpression,
        Order as ConvexOrder,
        Query,
        QuerySource,
    },
    runtime::Runtime,
    types::{
        IndexDescriptor,
        IndexName,
        MaybeValue,
    },
};
use database::{
    query::TableFilter,
    DeveloperQuery,
    Transaction,
};
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

impl From<Order> for ConvexOrder {
    fn from(o: Order) -> Self {
        match o {
            Order::Asc => ConvexOrder::Asc,
            Order::Desc => ConvexOrder::Desc,
        }
    }
}

pub struct TypedQueryBuilder<'db, 'tx, RT: Runtime, T: ConvexDocument> {
    // We hold onto the raw transaction + namespace directly rather than a
    // `&mut QueryDb` so the builder can be constructed from both
    // `QueryDb::query()` and `MutationDb::query()` without a second layer
    // of lifetime gymnastics.
    tx: &'db mut Transaction<RT>,
    namespace: TableNamespace,
    _borrow: PhantomData<&'tx ()>,
    index: Option<&'static str>,
    index_fields: &'static [&'static str],
    filters: Vec<EqFilter>,
    order: Order,
    limit: Option<usize>,
    _marker: PhantomData<fn() -> T>,
}

#[allow(dead_code)] // fields are read when we lower to IndexRangeExpression.
struct RangeFilter {
    field: &'static str,
    op: RangeOp,
    value: ConvexValue,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum RangeOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl RangeOp {
    fn to_expr(self, field: common::paths::FieldPath, value: ConvexValue) -> IndexRangeExpression {
        match self {
            RangeOp::Eq => IndexRangeExpression::Eq(field, MaybeValue(Some(value))),
            RangeOp::Gt => IndexRangeExpression::Gt(field, MaybeValue(Some(value))),
            RangeOp::Gte => IndexRangeExpression::Gte(field, MaybeValue(Some(value))),
            RangeOp::Lt => IndexRangeExpression::Lt(field, MaybeValue(Some(value))),
            RangeOp::Lte => IndexRangeExpression::Lte(field, MaybeValue(Some(value))),
        }
    }
}

// Legacy alias kept to preserve the existing public API shape.
type EqFilter = RangeFilter;

impl<'db, 'tx, RT: Runtime, T: ConvexDocument> TypedQueryBuilder<'db, 'tx, RT, T> {
    pub(crate) fn new(db: &'db mut QueryDb<'tx, RT>) -> Self {
        Self::new_from_parts(db.tx, db.namespace)
    }

    pub(crate) fn new_from_parts(tx: &'db mut Transaction<RT>, namespace: TableNamespace) -> Self {
        Self {
            tx,
            namespace,
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
        self.filters.push(RangeFilter {
            field: field.as_str(),
            op: RangeOp::Eq,
            value: value.to_convex()?,
        });
        Ok(self)
    }

    /// Strict greater-than comparator: index field must be `>` `value`.
    pub fn gt<V: ToConvex>(mut self, field: T::Field, value: V) -> anyhow::Result<Self> {
        self.filters.push(RangeFilter {
            field: field.as_str(),
            op: RangeOp::Gt,
            value: value.to_convex()?,
        });
        Ok(self)
    }

    /// Greater-than-or-equal comparator.
    pub fn gte<V: ToConvex>(mut self, field: T::Field, value: V) -> anyhow::Result<Self> {
        self.filters.push(RangeFilter {
            field: field.as_str(),
            op: RangeOp::Gte,
            value: value.to_convex()?,
        });
        Ok(self)
    }

    /// Strict less-than comparator.
    pub fn lt<V: ToConvex>(mut self, field: T::Field, value: V) -> anyhow::Result<Self> {
        self.filters.push(RangeFilter {
            field: field.as_str(),
            op: RangeOp::Lt,
            value: value.to_convex()?,
        });
        Ok(self)
    }

    /// Less-than-or-equal comparator.
    pub fn lte<V: ToConvex>(mut self, field: T::Field, value: V) -> anyhow::Result<Self> {
        self.filters.push(RangeFilter {
            field: field.as_str(),
            op: RangeOp::Lte,
            value: value.to_convex()?,
        });
        Ok(self)
    }

    /// Terminal: count matching documents without materializing them.
    /// Runs the query under the hood, iterating to completion; future
    /// work can replace this with a proper count optimization.
    pub async fn count(self) -> anyhow::Result<usize> {
        Ok(self.collect().await?.len())
    }

    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Terminal: expect exactly zero or one matching document.
    /// Errors if more than one document matches the filter. Handy for
    /// looking up by a unique index.
    pub async fn unique(self) -> anyhow::Result<Option<T>> {
        let mut results = self.limit(2).collect().await?;
        if results.len() > 1 {
            anyhow::bail!("TypedQueryBuilder::unique() matched more than one document");
        }
        Ok(results.pop())
    }

    /// Terminal: take up to `n` matching documents. Equivalent to
    /// `.limit(n).collect()`.
    pub async fn take(self, n: usize) -> anyhow::Result<Vec<T>> {
        self.limit(n).collect().await
    }

    /// Terminal: collect all matching documents into a `Vec`.
    pub async fn collect(self) -> anyhow::Result<Vec<T>> {
        let TypedQueryBuilder {
            tx,
            namespace,
            index,
            index_fields,
            filters,
            order,
            limit,
            ..
        } = self;

        let mut query = make_query::<T>(index, index_fields, filters, order, limit)?;
        let mut dq = DeveloperQuery::<RT>::new(
            tx,
            namespace,
            query.take(),
            TableFilter::ExcludePrivateSystemTables,
        )?;
        let mut out = Vec::new();
        while let Some(doc) = dq.next(tx, None).await? {
            let (_, value) = (doc.id(), doc.into_value());
            let parsed = T::from_convex_object(value.0)?;
            out.push(parsed);
        }
        Ok(out)
    }

    /// Terminal: return the first matching document, if any.
    pub async fn first(self) -> anyhow::Result<Option<T>> {
        let TypedQueryBuilder {
            tx,
            namespace,
            index,
            index_fields,
            filters,
            order,
            ..
        } = self;
        // Force a limit of 1 for efficiency regardless of any upstream
        // `.limit(...)` — first() is strictly "one-or-none".
        let mut query = make_query::<T>(index, index_fields, filters, order, Some(1))?;
        let mut dq = DeveloperQuery::<RT>::new(
            tx,
            namespace,
            query.take(),
            TableFilter::ExcludePrivateSystemTables,
        )?;
        match dq.next(tx, None).await? {
            None => Ok(None),
            Some(doc) => {
                let value = doc.into_value();
                Ok(Some(T::from_convex_object(value.0)?))
            },
        }
    }
}

// Small helper newtype so we can `.take()` the Query even after passing
// the builder's fields through pattern destructuring.
struct QueryHolder(Option<Query>);
impl QueryHolder {
    fn take(&mut self) -> Query {
        self.0.take().expect("QueryHolder consumed twice")
    }
}

fn make_query<T: ConvexDocument>(
    index: Option<&'static str>,
    index_fields: &'static [&'static str],
    filters: Vec<RangeFilter>,
    order: Order,
    limit: Option<usize>,
) -> anyhow::Result<QueryHolder> {
    let internal_order: ConvexOrder = order.into();
    let source = match index {
        Some(idx) => {
            let mut range: Vec<IndexRangeExpression> = Vec::new();
            for RangeFilter { field, op, value } in filters {
                anyhow::ensure!(
                    index_fields.contains(&field),
                    "filter field {field:?} is not part of index {idx:?}",
                );
                let field_path: common::paths::FieldPath = field.parse()?;
                range.push(op.to_expr(field_path, value));
            }
            let descriptor = IndexDescriptor::new(idx.to_string())?;
            let name = IndexName::new(T::table_name(), descriptor)?;
            QuerySource::IndexRange(IndexRange {
                index_name: name,
                range,
                order: internal_order,
            })
        },
        None => {
            anyhow::ensure!(
                filters.is_empty(),
                "TypedQueryBuilder: filters require .with_index(...) in phase 1",
            );
            QuerySource::FullTableScan(FullTableScan {
                table_name: T::table_name(),
                order: internal_order,
            })
        },
    };
    let mut query = Query {
        source,
        operators: vec![],
    };
    if let Some(lim) = limit {
        query = query.limit(lim);
    }
    Ok(QueryHolder(Some(query)))
}
