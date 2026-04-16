//! Read+write context for `#[convex::mutation]` functions.

use common::runtime::Runtime;
use database::{
    PatchValue,
    Transaction,
    UserFacingModel,
};
use value::{
    ConvexObject,
    TableNamespace,
};

use super::{
    query::QueryDb,
    query_builder::TypedQueryBuilder,
};
use crate::{
    document::{
        ConvexDocument,
        ConvexPatch,
    },
    id::Id,
};

/// Top-level context passed to native mutations. Extends `QueryCtx` with
/// write operations (which go through `MutationDb`).
pub struct MutationCtx<'tx, RT: Runtime> {
    pub(crate) tx: &'tx mut Transaction<RT>,
    pub(crate) namespace: TableNamespace,
}

impl<'tx, RT: Runtime> MutationCtx<'tx, RT> {
    /// Construct from a raw transaction. Used by the runner.
    pub fn new(tx: &'tx mut Transaction<RT>, namespace: TableNamespace) -> Self {
        Self { tx, namespace }
    }

    /// Borrow the read+write database handle.
    pub fn db(&mut self) -> MutationDb<'_, RT> {
        MutationDb {
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

    /// Scheduler handle — see [`super::scheduler::Scheduler`].
    ///
    /// Today mutations don't have a real scheduler wired up (the
    /// equivalent backend integration is the `VirtualSchedulerModel`
    /// path). We return a scheduler bound to [`NoopCallbacks`] so the
    /// API is callable but returns a clear error until the backend
    /// integration lands.
    pub fn scheduler(&mut self) -> super::scheduler::Scheduler<'_> {
        use std::sync::Arc;
        super::scheduler::Scheduler::new_with_callbacks(
            super::scheduler::SchedulerScope::Mutation,
            self.namespace,
            Arc::new(crate::callbacks::NoopCallbacks),
        )
    }
}

/// Read+write typed database handle. Created via `MutationCtx::db()`.
pub struct MutationDb<'tx, RT: Runtime> {
    pub(crate) tx: &'tx mut Transaction<RT>,
    pub(crate) namespace: TableNamespace,
}

impl<'tx, RT: Runtime> MutationDb<'tx, RT> {
    /// Temporarily downgrade to a read-only handle. Makes it easy to share
    /// code between queries and mutations: both can call the same helper
    /// that takes a `&mut QueryDb`.
    pub fn as_query_db(&mut self) -> QueryDb<'_, RT> {
        QueryDb {
            tx: self.tx,
            namespace: self.namespace,
        }
    }

    pub async fn get<T: ConvexDocument>(&mut self, id: Id<T>) -> anyhow::Result<Option<T>> {
        self.as_query_db().get(id).await
    }

    /// Bulk-fetch — mirrors [`QueryDb::get_many`].
    pub async fn get_many<T: ConvexDocument>(
        &mut self,
        ids: impl IntoIterator<Item = Id<T>>,
    ) -> anyhow::Result<Vec<Option<T>>> {
        self.as_query_db().get_many(ids).await
    }

    pub fn query<T: ConvexDocument>(&mut self) -> TypedQueryBuilder<'_, 'tx, RT, T> {
        // Safety: `TypedQueryBuilder` only needs a `&mut QueryDb`. We
        // cannot hand out an intermediate `QueryDb` and store it inside
        // the builder without lifetime issues, so we construct a fresh
        // builder keyed directly to the underlying transaction.
        //
        // This shortcut is fine because no write state is observable on
        // the builder until a terminal method runs; terminal methods go
        // through the transaction anyway.
        TypedQueryBuilder::new_from_parts(self.tx, self.namespace)
    }

    /// Insert a new document of type `T`. Returns the generated id.
    pub async fn insert<T: ConvexDocument>(&mut self, doc: T) -> anyhow::Result<Id<T>> {
        let obj: ConvexObject = doc.to_convex_object()?;
        let id = UserFacingModel::new(self.tx, self.namespace)
            .insert(T::table_name(), obj)
            .await?;
        Ok(Id::new(id))
    }

    /// Merge the given patch into the existing document.
    pub async fn patch<P: ConvexPatch>(
        &mut self,
        id: Id<P::Document>,
        patch: P,
    ) -> anyhow::Result<P::Document> {
        let obj = patch.to_convex_object()?;
        // Treat every field in the patch object as a "set". `UserFacingModel`
        // distinguishes "unset" from "set to null" via explicit `MaybeValue`
        // wrappers, but our `XxxPatch` type only carries set fields, so a
        // plain `ConvexObject` -> `PatchValue` conversion is always correct.
        let patch_value = PatchValue::from(obj);
        let doc = UserFacingModel::new(self.tx, self.namespace)
            .patch(id.into_developer_id(), patch_value)
            .await?;
        let parsed = <P::Document as ConvexDocument>::from_convex_object(doc.into_value().0)?;
        Ok(parsed)
    }

    /// Replace the existing document wholesale.
    pub async fn replace<T: ConvexDocument>(&mut self, id: Id<T>, doc: T) -> anyhow::Result<T> {
        let obj = doc.to_convex_object()?;
        let replaced = UserFacingModel::new(self.tx, self.namespace)
            .replace(id.into_developer_id(), obj)
            .await?;
        let parsed = T::from_convex_object(replaced.into_value().0)?;
        Ok(parsed)
    }

    /// Delete the document. Returns the deleted document.
    pub async fn delete<T: ConvexDocument>(&mut self, id: Id<T>) -> anyhow::Result<T> {
        let doc = UserFacingModel::new(self.tx, self.namespace)
            .delete(id.into_developer_id())
            .await?;
        let parsed = T::from_convex_object(doc.into_value().0)?;
        Ok(parsed)
    }
}
