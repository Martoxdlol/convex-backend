//! Read-only context for `#[convex::query]` functions.

use std::sync::atomic::{
    AtomicBool,
    Ordering,
};

use common::runtime::Runtime;
use database::Transaction;
use value::TableNamespace;

use super::query_builder::TypedQueryBuilder;
use crate::{
    convert::FromConvex,
    document::ConvexDocument,
    id::Id,
};

/// Determinism-observation bits the runner drains into
/// `UdfOutcome::observed_*`. Shared between the ctx and the outer
/// dispatcher via `Arc<Observed>`. Uses `AtomicBool` so the ctx
/// stays `Send`-able across the handler's `await` points.
#[derive(Default, Debug)]
pub struct Observed {
    identity: AtomicBool,
    unix_timestamp: AtomicBool,
}

impl Observed {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn note_identity(&self) {
        self.identity.store(true, Ordering::Relaxed);
    }

    pub(crate) fn note_unix_timestamp(&self) {
        self.unix_timestamp.store(true, Ordering::Relaxed);
    }

    pub fn identity(&self) -> bool {
        self.identity.load(Ordering::Relaxed)
    }

    pub fn unix_timestamp(&self) -> bool {
        self.unix_timestamp.load(Ordering::Relaxed)
    }
}

/// Top-level context passed to native queries.
///
/// Wraps a borrowed `Transaction<RT>` plus the table namespace the function
/// is executing in. Owned by the `NativeFunctionRunner`; the function body
/// only sees it as `&mut QueryCtx`.
pub struct QueryCtx<'tx, RT: Runtime> {
    pub(crate) tx: &'tx mut Transaction<RT>,
    pub(crate) namespace: TableNamespace,
    pub(crate) log_buffer: crate::logging::LogBuffer,
    pub(crate) observed: std::sync::Arc<Observed>,
}

impl<'tx, RT: Runtime> QueryCtx<'tx, RT> {
    /// Construct from a raw transaction. Used by the runner.
    pub fn new(tx: &'tx mut Transaction<RT>, namespace: TableNamespace) -> Self {
        Self {
            tx,
            namespace,
            log_buffer: crate::logging::LogBuffer::new(),
            observed: std::sync::Arc::new(Observed::new()),
        }
    }

    /// Construct with an externally-owned log buffer. Useful when the
    /// caller (e.g. the runner) wants to read the captured log lines
    /// after the handler returns.
    pub fn with_log_buffer(
        tx: &'tx mut Transaction<RT>,
        namespace: TableNamespace,
        log_buffer: crate::logging::LogBuffer,
    ) -> Self {
        Self {
            tx,
            namespace,
            log_buffer,
            observed: std::sync::Arc::new(Observed::new()),
        }
    }

    /// Construct with both the log buffer AND a caller-owned
    /// `Observed` handle. The runner uses this so it can drain the
    /// observed flags after the handler returns and populate
    /// `UdfOutcome::observed_identity` / `observed_time`.
    pub fn with_log_buffer_and_observed(
        tx: &'tx mut Transaction<RT>,
        namespace: TableNamespace,
        log_buffer: crate::logging::LogBuffer,
        observed: std::sync::Arc<Observed>,
    ) -> Self {
        Self {
            tx,
            namespace,
            log_buffer,
            observed,
        }
    }

    /// Borrow the determinism-observation flags.
    #[doc(hidden)]
    pub fn observed(&self) -> &std::sync::Arc<Observed> {
        &self.observed
    }

    /// Borrow a logger that writes into the ctx's log buffer.
    pub fn log(&self) -> crate::logging::Logger<'_> {
        crate::logging::Logger {
            buffer: &self.log_buffer,
        }
    }

    /// Borrow the read-only database handle.
    pub fn db(&mut self) -> QueryDb<'_, RT> {
        QueryDb {
            tx: self.tx,
            namespace: self.namespace,
        }
    }

    /// Access the underlying transaction — used internally; not part of
    /// the public developer API.
    #[doc(hidden)]
    pub fn tx(&mut self) -> &mut Transaction<RT> {
        self.tx
    }

    /// Identity of the caller that initiated the request.
    ///
    /// Calling this marks the ctx as having observed identity; the
    /// runner drains that bit into `UdfOutcome::observed_identity`
    /// so the sync layer knows the read set depends on who asked.
    pub fn auth(&self) -> crate::auth::AuthInfo<'_> {
        self.observed.note_identity();
        crate::auth::AuthInfo::new(self.tx.identity())
    }

    /// Current wall-clock time as a `UnixTimestamp`. Sourced from the
    /// transaction's runtime so tests that drive a mock clock see the
    /// mocked value. Calling this marks the ctx as having observed
    /// time, which the runner drains into
    /// `UdfOutcome::observed_time`.
    pub fn unix_timestamp(&self) -> common::runtime::UnixTimestamp {
        self.observed.note_unix_timestamp();
        self.tx.runtime().unix_timestamp()
    }
}

/// Read-only typed database handle. Created via `QueryCtx::db()`.
pub struct QueryDb<'tx, RT: Runtime> {
    pub(crate) tx: &'tx mut Transaction<RT>,
    pub(crate) namespace: TableNamespace,
}

impl<'tx, RT: Runtime> QueryDb<'tx, RT> {
    /// Fetch a document by id. Returns `None` if the document doesn't exist.
    ///
    /// The generic parameter `T` determines both the table and the Rust
    /// type the returned document is parsed into.
    pub async fn get<T: ConvexDocument>(&mut self, id: Id<T>) -> anyhow::Result<Option<T>> {
        Ok(self.get_with_meta(id).await?.map(|m| m.doc))
    }

    /// Fetch a document by id, returning an error if it doesn't exist.
    /// Equivalent to `get(id).await?.ok_or_else(...)` but with a
    /// consistent error message.
    pub async fn try_get<T: ConvexDocument>(&mut self, id: Id<T>) -> anyhow::Result<T> {
        self.get(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("document {} not found in {}", id, T::table_name()))
    }

    /// Check whether a document exists by id, without materializing it.
    pub async fn exists<T: ConvexDocument>(&mut self, id: Id<T>) -> anyhow::Result<bool> {
        Ok(self.get(id).await?.is_some())
    }

    /// Fetch a document by id along with its metadata (id, creation time).
    /// Useful when you need to pass the id through further operations
    /// or sort by creation time.
    pub async fn get_with_meta<T: ConvexDocument>(
        &mut self,
        id: Id<T>,
    ) -> anyhow::Result<Option<crate::document::DocumentWithMeta<T>>> {
        use database::UserFacingModel;
        let dev_id = id.into_developer_id();
        let maybe_doc = UserFacingModel::new(self.tx, self.namespace)
            .get_with_ts(dev_id, None)
            .await?;
        match maybe_doc {
            None => Ok(None),
            Some((doc, _ts)) => {
                let creation_time = doc.creation_time();
                let value: common::pii::PII<value::ConvexObject> = doc.into_value();
                let parsed = T::from_convex_object(value.0)?;
                Ok(Some(crate::document::DocumentWithMeta {
                    id,
                    creation_time,
                    doc: parsed,
                }))
            },
        }
    }

    /// Start a typed query against table `T`. See [`TypedQueryBuilder`] for
    /// the chainable builder interface.
    pub fn query<T: ConvexDocument>(&mut self) -> TypedQueryBuilder<'_, 'tx, RT, T> {
        TypedQueryBuilder::new(self)
    }

    /// Bulk-fetch documents by id. Returns `None` for ids that don't
    /// resolve. Useful for following foreign keys across a batch — e.g.
    /// fetching the `author: Id<User>` for every `Message` in a list.
    pub async fn get_many<T: ConvexDocument>(
        &mut self,
        ids: impl IntoIterator<Item = Id<T>>,
    ) -> anyhow::Result<Vec<Option<T>>> {
        let mut out = Vec::new();
        for id in ids {
            out.push(self.get(id).await?);
        }
        Ok(out)
    }

    /// Count every document in table `T`. Runs a full scan under the
    /// hood — prefer `.query::<T>().with_index(...)...count()` when
    /// you only need a partial count.
    pub async fn count_all<T: ConvexDocument>(&mut self) -> anyhow::Result<usize> {
        self.query::<T>().count().await
    }

    /// Validate an id string received from an untrusted source (e.g.
    /// query-string arg, webhook body) against a known table `T`.
    /// Returns `Some(Id<T>)` when the id parses AND embeds `T`'s
    /// table number; returns `None` when the string is malformed, or
    /// when it addresses a different table.
    ///
    /// This is the typed analogue of the `1.0/db/normalizeId`
    /// syscall (`ctx.db.normalizeId("users", rawString)` in JS).
    /// Typical use: sanitise `Id<T>` values handed over from an HTTP
    /// action body before acting on them.
    pub fn normalize_id<T: ConvexDocument>(&mut self, id_str: &str) -> Option<Id<T>> {
        let table_name = T::table_name();
        let mapping = self.tx.table_mapping().namespace(self.namespace);
        let expected_number = mapping.id_and_number_if_exists(&table_name)?.table_number;
        let id_v6 = value::DeveloperDocumentId::decode(id_str).ok()?;
        if id_v6.table() == expected_number {
            Some(Id::new(id_v6))
        } else {
            None
        }
    }
}

/// Blanket helper that lets generated code recover a typed document from a
/// developer-facing document object without touching the conversion traits
/// directly.
#[doc(hidden)]
pub fn document_from_convex<T: ConvexDocument>(obj: value::ConvexObject) -> anyhow::Result<T> {
    T::from_convex_object(obj)
}

// Re-export so generated code can access it via a single path.
#[doc(hidden)]
pub use crate::convert::FromConvex as _FromConvex;
#[doc(hidden)]
pub fn _assert_from_convex<T: FromConvex>() {}

#[cfg(test)]
mod observed_tests {
    use super::*;

    #[test]
    fn observed_starts_false_and_flips_on_explicit_notes() {
        // Unit-level probe: the bits start clear and each accessor
        // trips only its own flag. The integration between this and
        // `ctx.auth()` / `ctx.unix_timestamp()` lives in the tests
        // that exercise real handlers — here we just pin the
        // single-purpose contract of `Observed` itself.
        let o = Observed::new();
        assert!(!o.identity());
        assert!(!o.unix_timestamp());
        o.note_identity();
        assert!(o.identity());
        assert!(!o.unix_timestamp(), "note_identity must not flip time");
        o.note_unix_timestamp();
        assert!(o.identity());
        assert!(o.unix_timestamp());
    }

    #[test]
    fn observed_is_idempotent_across_repeated_notes() {
        // The sync layer only cares whether identity / time were
        // observed at all — not how many times. Repeated calls from
        // inside a handler must not toggle the bit off.
        let o = Observed::new();
        o.note_identity();
        o.note_identity();
        o.note_identity();
        assert!(o.identity());
    }
}
