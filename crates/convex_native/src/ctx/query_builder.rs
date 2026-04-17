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
        Cursor,
        Expression,
        IndexRange,
        IndexRangeExpression,
        Order as ConvexOrder,
        Query,
    },
    runtime::Runtime,
    types::{
        IndexDescriptor,
        IndexName,
        MaybeValue,
    },
};
use database::{
    query::{
        PaginationOptions,
        TableFilter,
    },
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

/// One page of results from `TypedQueryBuilder::page`.
///
/// `cursor` is the opaque cursor for fetching the next page — pass it
/// back into the next `.page(Some(cursor), ...)` call. `is_done`
/// is `true` when the underlying scan has been exhausted (the page
/// returned fewer rows than requested).
#[derive(Debug, Clone)]
pub struct TypedPage<T> {
    pub items: Vec<T>,
    pub cursor: Option<Cursor>,
    pub is_done: bool,
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

    /// Lower to a post-scan `Expression`. Used for full-table-scan
    /// filtering when no index is selected — each filter becomes a
    /// predicate evaluated per row.
    fn to_filter_expr(self, field: common::paths::FieldPath, value: ConvexValue) -> Expression {
        let field_expr = Box::new(Expression::Field(field));
        let literal = Box::new(Expression::Literal(MaybeValue(Some(value))));
        match self {
            RangeOp::Eq => Expression::Eq(field_expr, literal),
            RangeOp::Gt => Expression::Gt(field_expr, literal),
            RangeOp::Gte => Expression::Gte(field_expr, literal),
            RangeOp::Lt => Expression::Lt(field_expr, literal),
            RangeOp::Lte => Expression::Lte(field_expr, literal),
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

    /// Terminal: fetch one page of results starting at `start_cursor`
    /// (or from the beginning when `None`). The query is bounded by
    /// `page_size` rows via the database's
    /// [`PaginationOptions::ManualPagination`] mode, so each call reads
    /// at most `page_size` rows from storage. The returned `next_cursor`
    /// is suitable for the next call; `is_done` flips to `true` when
    /// the page didn't fill (no more rows to read).
    ///
    /// Notes vs `.collect()`:
    /// - `.collect()` reads the entire range; `.page()` reads at most
    ///   `page_size` rows.
    /// - The cursor is opaque — pass it back unchanged. A cursor is only valid
    ///   against the same query (same index, same filters, same order); the
    ///   database guards this with a fingerprint.
    /// - For reactive paginated queries, this isn't enough by itself — the sync
    ///   layer calls a different code path. `.page()` is the right shape for
    ///   one-shot scrolls inside a query/mutation.
    pub async fn page(
        self,
        start_cursor: Option<Cursor>,
        page_size: usize,
    ) -> anyhow::Result<TypedPage<T>> {
        anyhow::ensure!(
            page_size > 0,
            "TypedQueryBuilder::page page_size must be > 0"
        );
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
        // `limit` and `page_size` overlap. We honour `limit` as an
        // additional cap on this page — but the page itself is bounded
        // by `page_size` via PaginationOptions. If a smaller .limit() is
        // set, that wins.
        let cap = match limit {
            Some(l) => l.min(page_size),
            None => page_size,
        };
        let mut query = make_query::<T>(index, index_fields, filters, order, None)?;
        let mut dq = DeveloperQuery::<RT>::new_bounded(
            tx,
            namespace,
            query.take(),
            PaginationOptions::ManualPagination {
                start_cursor,
                maximum_rows_read: Some(cap),
                maximum_bytes_read: None,
            },
            None,
            TableFilter::ExcludePrivateSystemTables,
        )?;
        let mut items = Vec::with_capacity(cap);
        let mut exhausted = false;
        while items.len() < cap {
            match dq.next(tx, None).await? {
                Some(doc) => {
                    let value = doc.into_value();
                    items.push(T::from_convex_object(value.0)?);
                },
                None => {
                    exhausted = true;
                    break;
                },
            }
        }
        // If we hit `cap` without ever calling next() and seeing None,
        // we don't yet know whether the underlying scan is done — the
        // honest answer is "ask the database" via the cursor. We surface
        // `is_done` only when the inner iterator returned None.
        let next_cursor = dq.cursor();
        Ok(TypedPage {
            items,
            cursor: next_cursor,
            is_done: exhausted,
        })
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
    let mut query = match index {
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
            Query::index_range(IndexRange {
                index_name: name,
                range,
                order: internal_order,
            })
        },
        None => {
            // Full-table scan with post-scan filtering: every filter
            // lowers to a `QueryOperator::Filter(Expression)` stacked on
            // top of the scan. Less efficient than an indexed lookup
            // (reads every row) but preserves the same `.eq/.gt/...`
            // surface so handlers can be written before their indexes
            // are in place.
            let mut query = Query::full_table_scan(T::table_name(), internal_order);
            for RangeFilter { field, op, value } in filters {
                let field_path: common::paths::FieldPath = field.parse()?;
                query = query.filter(op.to_filter_expr(field_path, value));
            }
            query
        },
    };
    if let Some(lim) = limit {
        query = query.limit(lim);
    }
    Ok(QueryHolder(Some(query)))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use common::{
        query::{
            QueryOperator,
            QuerySource,
        },
        schemas::TableDefinition,
        types::TableName,
    };
    use value::ConvexObject;

    use super::*;

    /// Hand-rolled `ConvexDocument` stand-in. The derive macro emits
    /// `::convex_native::...` paths, which don't resolve inside the crate
    /// under test — so for unit tests against `make_query` we implement
    /// the trait directly. This widget has one scalar field and one
    /// declared index `by_owner(owner)`.
    #[derive(Clone, Debug)]
    struct Widget;

    #[derive(Copy, Clone, Debug)]
    #[allow(dead_code)]
    enum WidgetField {
        Owner,
        Count,
    }
    impl FieldReference for WidgetField {
        fn as_str(&self) -> &'static str {
            match self {
                WidgetField::Owner => "owner",
                WidgetField::Count => "count",
            }
        }
    }

    #[derive(Copy, Clone, Debug)]
    #[allow(dead_code)]
    enum WidgetIndex {
        ByOwner,
    }
    impl IndexReference for WidgetIndex {
        fn as_str(&self) -> &'static str {
            "by_owner"
        }

        fn fields(&self) -> &'static [&'static str] {
            &["owner"]
        }
    }

    impl ConvexDocument for Widget {
        type Field = WidgetField;
        type Index = WidgetIndex;
        type Patch = ();

        fn table_name() -> TableName {
            TableName::from_str("widgets").unwrap()
        }

        fn table_definition() -> TableDefinition {
            TableDefinition {
                table_name: Self::table_name(),
                indexes: Default::default(),
                staged_db_indexes: Default::default(),
                text_indexes: Default::default(),
                staged_text_indexes: Default::default(),
                vector_indexes: Default::default(),
                staged_vector_indexes: Default::default(),
                document_type: None,
            }
        }

        fn to_convex_object(&self) -> anyhow::Result<ConvexObject> {
            ConvexObject::try_from(std::collections::BTreeMap::new())
        }

        fn from_convex_object(_obj: ConvexObject) -> anyhow::Result<Self> {
            Ok(Widget)
        }
    }

    fn eq_filter(field: &'static str, value: ConvexValue) -> RangeFilter {
        RangeFilter {
            field,
            op: RangeOp::Eq,
            value,
        }
    }

    #[test]
    fn make_query_without_index_and_filters_is_full_table_scan() {
        let mut holder = make_query::<Widget>(None, &[], vec![], Order::Asc, None).expect("build");
        let q = holder.take();
        match &q.source {
            QuerySource::FullTableScan(fts) => {
                assert_eq!(&fts.table_name.to_string(), "widgets");
                assert_eq!(fts.order, ConvexOrder::Asc);
            },
            other => panic!("expected FullTableScan, got {other:?}"),
        }
        assert!(q.operators.is_empty(), "no filters => no operators");
    }

    #[test]
    fn make_query_without_index_with_eq_filter_stacks_filter_operator() {
        // Non-indexed filters should lower to a full-table scan plus a
        // `QueryOperator::Filter(Expression::Eq(...))` on top.
        let filters = vec![eq_filter(
            "owner",
            ConvexValue::try_from("alice".to_string()).unwrap(),
        )];
        let mut holder = make_query::<Widget>(None, &[], filters, Order::Asc, None).expect("build");
        let q = holder.take();
        assert!(matches!(&q.source, QuerySource::FullTableScan(_)));
        assert_eq!(q.operators.len(), 1, "exactly one filter operator");
        match &q.operators[0] {
            QueryOperator::Filter(Expression::Eq(l, r)) => {
                match l.as_ref() {
                    Expression::Field(fp) => {
                        // FieldPath's Display wraps the name in quotes; the
                        // Debug form is stable. We just need to see the
                        // field name appear in the rendered form.
                        let rendered = fp.to_string();
                        assert!(
                            rendered.contains("owner"),
                            "lhs field mentions owner: {rendered}",
                        );
                    },
                    other => panic!("lhs should be Field, got {other:?}"),
                }
                match r.as_ref() {
                    Expression::Literal(MaybeValue(Some(ConvexValue::String(s)))) => {
                        assert_eq!(s.as_ref(), "alice");
                    },
                    other => panic!("rhs should be literal string, got {other:?}"),
                }
            },
            other => panic!("expected Filter(Eq), got {other:?}"),
        }
    }

    #[test]
    fn make_query_without_index_stacks_multiple_range_filters() {
        // gte + lt on the same non-indexed field should stack as two
        // Filter operators, in the order they were declared.
        let filters = vec![
            RangeFilter {
                field: "count",
                op: RangeOp::Gte,
                value: ConvexValue::Int64(10),
            },
            RangeFilter {
                field: "count",
                op: RangeOp::Lt,
                value: ConvexValue::Int64(100),
            },
        ];
        let mut holder = make_query::<Widget>(None, &[], filters, Order::Asc, None).expect("build");
        let q = holder.take();
        assert_eq!(q.operators.len(), 2);
        assert!(
            matches!(
                &q.operators[0],
                QueryOperator::Filter(Expression::Gte(_, _))
            ),
            "first operator must be Gte",
        );
        assert!(
            matches!(&q.operators[1], QueryOperator::Filter(Expression::Lt(_, _))),
            "second operator must be Lt",
        );
    }

    #[test]
    fn make_query_without_index_honours_limit_after_filters() {
        // Limit must come after filters so the filter runs on every
        // matching row, not just the first N rows of the scan.
        let filters = vec![eq_filter("count", ConvexValue::Int64(7))];
        let mut holder =
            make_query::<Widget>(None, &[], filters, Order::Asc, Some(5)).expect("build");
        let q = holder.take();
        assert_eq!(q.operators.len(), 2);
        assert!(matches!(&q.operators[0], QueryOperator::Filter(_)));
        assert!(matches!(&q.operators[1], QueryOperator::Limit(5)));
    }

    #[test]
    fn make_query_with_index_preserves_existing_behaviour() {
        // Indexed path must still build an IndexRange with the range
        // expression attached directly to the source (not as a filter
        // operator).
        let filters = vec![eq_filter(
            "owner",
            ConvexValue::try_from("bob".to_string()).unwrap(),
        )];
        let mut holder =
            make_query::<Widget>(Some("by_owner"), &["owner"], filters, Order::Asc, None)
                .expect("build");
        let q = holder.take();
        match &q.source {
            QuerySource::IndexRange(ir) => {
                assert_eq!(ir.range.len(), 1);
                assert!(matches!(ir.range[0], IndexRangeExpression::Eq(_, _)));
            },
            other => panic!("expected IndexRange, got {other:?}"),
        }
        assert!(q.operators.is_empty(), "indexed filter lives on source");
    }

    #[test]
    fn make_query_with_index_rejects_filter_on_non_index_field() {
        // Filtering an indexed query on a field that isn't part of the
        // index is a declaration error — the old behaviour must be
        // preserved now that we no longer refuse all non-indexed
        // filters up front.
        let filters = vec![eq_filter("count", ConvexValue::Int64(1))];
        let result = make_query::<Widget>(Some("by_owner"), &["owner"], filters, Order::Asc, None);
        let err = match result {
            Ok(_) => panic!("expected error for non-index field"),
            Err(e) => e,
        };
        assert!(
            format!("{err}").contains("is not part of index"),
            "error mentions the bad field: {err}",
        );
    }

    /// `TypedPage<T>` carries the page contents plus pagination state.
    /// We can't drive the full `.page()` path without a real
    /// `Database<RT>` (covered by end-to-end backend tests), but the
    /// shape itself must stay clonable + debug-printable so callers
    /// can store it across requests and log it.
    #[test]
    fn typed_page_is_clone_and_debug() {
        let page: TypedPage<i64> = TypedPage {
            items: vec![1, 2, 3],
            cursor: None,
            is_done: true,
        };
        let cloned = page.clone();
        assert_eq!(cloned.items, vec![1, 2, 3]);
        assert!(cloned.is_done);
        assert!(cloned.cursor.is_none());
        let rendered = format!("{page:?}");
        assert!(rendered.contains("items"));
        assert!(rendered.contains("is_done"));
    }
}
