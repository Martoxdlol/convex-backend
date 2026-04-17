//! Utilities for unit-testing native functions.
//!
//! ## `args!` helper
//!
//! Constructing a `ConvexObject` by hand in tests is verbose; the
//! `args!` macro keeps the call sites readable:
//!
//! ```ignore
//! use convex_native::testing::args;
//!
//! let obj = args! {
//!     "email" => "alice@example.com".to_string(),
//!     "count" => 42_i64,
//! };
//! ```
//!
//! Writing a custom `NativeActionCallbacks` impl for every test is
//! tedious; this module provides a builder that wires up the common
//! patterns:
//!
//! ```ignore
//! use convex_native::testing::TestCallbacks;
//!
//! let callbacks = TestCallbacks::new()
//!     .on_query("get_user_count", |args| {
//!         // Inspect args, return a typed value.
//!         ConvexValue::Int64(7)
//!     })
//!     .on_mutation("create_user", |_args| ConvexValue::Null)
//!     .build();
//!
//! let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
//! runner
//!     .run_action_with_callbacks("notify_user", ns, obj, callbacks)
//!     .await?;
//! ```
//!
//! The builder records every call so tests can assert which paths
//! ran via `history()`.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    TableNamespace,
};

use crate::{
    callbacks::NativeActionCallbacks,
    ctx::storage::StorageId,
};

type QueryFn = Box<dyn Fn(ConvexObject) -> anyhow::Result<ConvexValue> + Send + Sync>;
type MutationFn = Box<dyn Fn(ConvexObject) -> anyhow::Result<ConvexValue> + Send + Sync>;

/// One entry captured in [`TestCallbacks::history`].
#[derive(Debug, Clone)]
pub enum CallRecord {
    Query { name: String, args: ConvexObject },
    Mutation { name: String, args: ConvexObject },
    Schedule { name: String, delay: Duration },
    StorageStore { content_type: String, bytes: usize },
    StorageGetUrl { id: StorageId },
    StorageDelete { id: StorageId },
}

/// Builder for a fake [`NativeActionCallbacks`]. Once [`build`] is
/// called you get back an `Arc<dyn NativeActionCallbacks>` plus a
/// history handle you can query in tests.
pub struct TestCallbacksBuilder {
    queries: BTreeMap<String, QueryFn>,
    mutations: BTreeMap<String, MutationFn>,
    default_storage_url: Option<String>,
}

impl Default for TestCallbacksBuilder {
    fn default() -> Self {
        Self {
            queries: BTreeMap::new(),
            mutations: BTreeMap::new(),
            default_storage_url: Some("https://test/url".into()),
        }
    }
}

impl TestCallbacksBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler for `name` — invoked when an action calls
    /// `ctx.run_query(marker, args)` with a marker whose `name()`
    /// matches.
    pub fn on_query<F>(mut self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(ConvexObject) -> anyhow::Result<ConvexValue> + Send + Sync + 'static,
    {
        self.queries.insert(name.into(), Box::new(f));
        self
    }

    pub fn on_mutation<F>(mut self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(ConvexObject) -> anyhow::Result<ConvexValue> + Send + Sync + 'static,
    {
        self.mutations.insert(name.into(), Box::new(f));
        self
    }

    /// Change the URL returned by `storage_get_url` (default
    /// `https://test/url`). Pass `None` to return `None`.
    pub fn with_storage_url(mut self, url: Option<&str>) -> Self {
        self.default_storage_url = url.map(|s| s.to_string());
        self
    }

    /// Finalize. Returns `(callbacks, history)` where `callbacks` can
    /// be passed to `run_action_with_callbacks` and `history` is a
    /// shared handle that captures every call.
    pub fn build(self) -> (Arc<dyn NativeActionCallbacks>, TestHistory) {
        let history = TestHistory(Arc::new(Mutex::new(Vec::new())));
        let inner = TestCallbacksImpl {
            queries: self.queries,
            mutations: self.mutations,
            default_storage_url: self.default_storage_url,
            history: history.clone(),
        };
        (Arc::new(inner), history)
    }
}

/// Shared handle over the captured call record list. Cloneable +
/// `Send + Sync` so tests can move copies into async tasks.
#[derive(Clone)]
pub struct TestHistory(Arc<Mutex<Vec<CallRecord>>>);

impl TestHistory {
    pub fn snapshot(&self) -> Vec<CallRecord> {
        self.0.lock().unwrap().clone()
    }

    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Count entries that match `pred`. Useful in assertions.
    pub fn count(&self, pred: impl Fn(&CallRecord) -> bool) -> usize {
        self.0.lock().unwrap().iter().filter(|r| pred(r)).count()
    }
}

/// Convenience alias used by tests — the most common entry point.
pub type TestCallbacks = TestCallbacksBuilder;

struct TestCallbacksImpl {
    queries: BTreeMap<String, QueryFn>,
    mutations: BTreeMap<String, MutationFn>,
    default_storage_url: Option<String>,
    history: TestHistory,
}

#[async_trait]
impl NativeActionCallbacks for TestCallbacksImpl {
    async fn run_query_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.history.0.lock().unwrap().push(CallRecord::Query {
            name: name.to_string(),
            args: args.clone(),
        });
        match self.queries.get(name) {
            Some(f) => f(args),
            None => anyhow::bail!("TestCallbacks: no query handler registered for {name:?}"),
        }
    }

    async fn run_mutation_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.history.0.lock().unwrap().push(CallRecord::Mutation {
            name: name.to_string(),
            args: args.clone(),
        });
        match self.mutations.get(name) {
            Some(f) => f(args),
            None => anyhow::bail!("TestCallbacks: no mutation handler registered for {name:?}"),
        }
    }

    async fn schedule(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
        delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        self.history.0.lock().unwrap().push(CallRecord::Schedule {
            name: name.to_string(),
            delay,
        });
        Ok(DeveloperDocumentId::MIN)
    }

    async fn storage_store(
        &self,
        _ns: TableNamespace,
        body: Bytes,
        content_type: &str,
    ) -> anyhow::Result<StorageId> {
        self.history
            .0
            .lock()
            .unwrap()
            .push(CallRecord::StorageStore {
                content_type: content_type.to_string(),
                bytes: body.len(),
            });
        Ok(StorageId("test-storage".into()))
    }

    async fn storage_get_url(
        &self,
        _ns: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<Option<String>> {
        self.history
            .0
            .lock()
            .unwrap()
            .push(CallRecord::StorageGetUrl { id });
        Ok(self.default_storage_url.clone())
    }

    async fn storage_delete(&self, _ns: TableNamespace, id: StorageId) -> anyhow::Result<bool> {
        self.history
            .0
            .lock()
            .unwrap()
            .push(CallRecord::StorageDelete { id });
        Ok(true)
    }
}

/// Ergonomic `ConvexObject` construction for tests.
///
/// ```ignore
/// let obj = args! {
///     "email" => "a@b".to_string(),
///     "count" => 7_i64,
/// };
/// ```
#[macro_export]
macro_rules! __convex_native_args {
    ( $( $key:expr => $value:expr ),* $(,)? ) => {{
        use ::std::collections::BTreeMap;
        // `mut` is only actually needed when at least one pair is
        // supplied; for the zero-pair form (`args! {}`) the local
        // never gets written to. Suppressing the lint keeps both
        // call shapes valid without the macro having to branch on
        // "did the user pass any pairs?".
        #[allow(unused_mut)]
        let mut map: BTreeMap<
            $crate::__private::FieldName,
            $crate::__private::ConvexValue,
        > = BTreeMap::new();
        $(
            let __name: $crate::__private::FieldName =
                $key.parse().expect("invalid field name");
            map.insert(__name, $crate::ToConvex::to_convex($value).expect("to_convex"));
        )*
        <$crate::__private::ConvexObject as ::std::convert::TryFrom<_>>::try_from(map)
            .expect("building ConvexObject")
    }};
}

pub use __convex_native_args as args;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_macro_builds_expected_object() {
        let obj = crate::testing::args! {
            "email" => "a@b".to_string(),
            "count" => 7_i64,
        };
        let map: std::collections::BTreeMap<_, _> = obj.into();
        assert_eq!(map.len(), 2);
        let count_key: value::FieldName = "count".parse().unwrap();
        assert_eq!(map.get(&count_key), Some(&ConvexValue::Int64(7)));
    }

    #[tokio::test]
    async fn query_handler_returns_registered_value() {
        let (cb, _history) = TestCallbacksBuilder::new()
            .on_query("whoami", |_| Ok(ConvexValue::Int64(42)))
            .build();
        let obj = ConvexObject::try_from(
            std::collections::BTreeMap::<value::FieldName, ConvexValue>::new(),
        )
        .unwrap();
        let v = cb
            .run_query_by_name(TableNamespace::Global, "whoami", obj)
            .await
            .unwrap();
        assert_eq!(v, ConvexValue::Int64(42));
    }

    #[tokio::test]
    async fn history_captures_every_call() {
        let (cb, history) = TestCallbacksBuilder::new()
            .on_query("get", |_| Ok(ConvexValue::Null))
            .on_mutation("set", |_| Ok(ConvexValue::Null))
            .build();
        let obj = ConvexObject::try_from(
            std::collections::BTreeMap::<value::FieldName, ConvexValue>::new(),
        )
        .unwrap();
        cb.run_query_by_name(TableNamespace::Global, "get", obj.clone())
            .await
            .unwrap();
        cb.run_mutation_by_name(TableNamespace::Global, "set", obj)
            .await
            .unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(
            history.count(|r| matches!(r, CallRecord::Query { name, .. } if name == "get")),
            1,
        );
        assert_eq!(
            history.count(|r| matches!(r, CallRecord::Mutation { name, .. } if name == "set")),
            1,
        );
    }

    fn empty_obj() -> ConvexObject {
        ConvexObject::try_from(std::collections::BTreeMap::<value::FieldName, ConvexValue>::new())
            .unwrap()
    }

    #[tokio::test]
    async fn unregistered_query_name_bails_with_helpful_message() {
        // If a handler calls an unregistered name, the stub must
        // bail with a message naming the missing registration so
        // the test author knows to add an `.on_query(...)` clause.
        let (cb, _h) = TestCallbacksBuilder::new().build();
        let err = cb
            .run_query_by_name(TableNamespace::Global, "missing", empty_obj())
            .await
            .expect_err("unregistered");
        let msg = format!("{err}");
        assert!(msg.contains("no query handler registered"));
        assert!(msg.contains("missing"), "names the missing handler: {msg}");
    }

    #[tokio::test]
    async fn unregistered_mutation_name_bails_with_helpful_message() {
        let (cb, _h) = TestCallbacksBuilder::new().build();
        let err = cb
            .run_mutation_by_name(TableNamespace::Global, "no_handler", empty_obj())
            .await
            .expect_err("unregistered");
        assert!(format!("{err}").contains("no mutation handler registered"));
    }

    #[tokio::test]
    async fn schedule_records_delay_and_returns_a_stub_id() {
        // The stub's scheduler path must log the call (so tests
        // assert on delays) and return a non-panicking stub id.
        let (cb, history) = TestCallbacksBuilder::new().build();
        let id = cb
            .schedule(
                TableNamespace::Global,
                "bg_job",
                empty_obj(),
                Duration::from_secs(7),
            )
            .await
            .expect("ok");
        assert_eq!(id, DeveloperDocumentId::MIN);
        let records = history.snapshot();
        assert_eq!(records.len(), 1);
        assert!(
            matches!(&records[0], CallRecord::Schedule { name, delay }
                if name == "bg_job" && *delay == Duration::from_secs(7)),
            "delay survives the round-trip: {:?}",
            records[0],
        );
    }

    #[tokio::test]
    async fn storage_store_records_content_type_and_byte_count() {
        // `storage_store` returns a fixed stub id; the valuable
        // piece is that the content-type string and body length
        // land in the history so tests can assert on them.
        let (cb, history) = TestCallbacksBuilder::new().build();
        let payload = Bytes::from_static(b"hello world");
        let id = cb
            .storage_store(TableNamespace::Global, payload.clone(), "text/plain")
            .await
            .expect("ok");
        assert_eq!(id, StorageId("test-storage".into()));
        let records = history.snapshot();
        assert!(matches!(
            &records[0],
            CallRecord::StorageStore { content_type, bytes }
                if content_type == "text/plain" && *bytes == payload.len()
        ));
    }

    #[tokio::test]
    async fn storage_get_url_returns_the_configured_default() {
        // `with_storage_url(None)` overrides to return None; otherwise
        // the stub returns the configured default URL.
        let (cb, _) = TestCallbacksBuilder::new().build();
        let url = cb
            .storage_get_url(TableNamespace::Global, StorageId("any".into()))
            .await
            .unwrap();
        assert_eq!(url, Some("https://test/url".into()));

        let (cb_none, _) = TestCallbacksBuilder::new().with_storage_url(None).build();
        let url = cb_none
            .storage_get_url(TableNamespace::Global, StorageId("any".into()))
            .await
            .unwrap();
        assert_eq!(url, None);

        let (cb_custom, _) = TestCallbacksBuilder::new()
            .with_storage_url(Some("https://override"))
            .build();
        let url = cb_custom
            .storage_get_url(TableNamespace::Global, StorageId("any".into()))
            .await
            .unwrap();
        assert_eq!(url, Some("https://override".into()));
    }

    #[tokio::test]
    async fn storage_delete_returns_true_and_records() {
        // The stub always returns `true` (file "was deleted"). Pin
        // that + record capture so tests can dedupe delete
        // invocations.
        let (cb, history) = TestCallbacksBuilder::new().build();
        let removed = cb
            .storage_delete(TableNamespace::Global, StorageId("a".into()))
            .await
            .expect("ok");
        assert!(removed);
        assert_eq!(history.len(), 1);
        assert!(matches!(
            &history.snapshot()[0],
            CallRecord::StorageDelete { id } if id == &StorageId("a".into()),
        ));
    }

    #[test]
    fn history_len_and_is_empty_track_insertions_across_clones() {
        // `TestHistory::clone()` shares the underlying Vec via Arc.
        // Tests that move a clone into an async task rely on both
        // handles observing the same writes.
        let (_cb, history) = TestCallbacksBuilder::new().build();
        assert!(history.is_empty());
        assert_eq!(history.len(), 0);
        let clone = history.clone();
        // Inject a record directly through the internal Arc to
        // avoid awaiting — we're just probing the shared-state
        // contract, not the forwarders themselves.
        history.0.lock().unwrap().push(CallRecord::StorageGetUrl {
            id: StorageId("x".into()),
        });
        assert_eq!(clone.len(), 1);
        assert!(!clone.is_empty());
    }

    #[test]
    fn args_macro_with_zero_pairs_yields_empty_object() {
        // The `args!` macro accepts a trailing comma and zero pairs.
        // That's the "no args" form handler tests use — must produce
        // a real empty ConvexObject, not a parse error.
        let obj = crate::testing::args! {};
        let map: std::collections::BTreeMap<_, _> = obj.into();
        assert!(map.is_empty());
    }
}
