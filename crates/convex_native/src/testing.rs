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
}
