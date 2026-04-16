//! Native function dispatch.
//!
//! `NativeFunctionRunner` is the entry point the backend calls to execute
//! a native query or mutation. It does **not** yet implement the full
//! `function_runner::FunctionRunner` trait — that trait has six
//! JS-specific methods (`analyze`, `evaluate_app_definitions`,
//! `evaluate_component_initializer`, `evaluate_schema`,
//! `evaluate_auth_config`, plus HTTP-action dispatch inside
//! `run_function`) whose full implementation requires V8 integration.
//! Phase 1.5 wraps this runner behind a "composite" trait impl that
//! delegates those methods to the existing V8 runner and intercepts
//! only the native names.
//!
//! What this module does provide:
//!
//! - [`NativeFunctionRunner::run_query`] / [`run_mutation`] — drive a handler
//!   from the registry against a borrowed `Transaction<RT>` and return the
//!   handler's `ConvexValue`.
//! - [`NativeFunctionRunner::has_function`] — name-based check so the composite
//!   runner knows whether to dispatch natively or fall through to V8.

use std::sync::Arc;

use common::types::UdfType;
use database::Transaction;
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

use crate::{
    ctx::{
        action::ActionCtx,
        mutation::MutationCtx,
        query::QueryCtx,
    },
    registry::{
        HandlerFn,
        NativeFunctionRegistration,
        NativeFunctionRegistry,
        Rt,
    },
};

/// Runtime-installable handle over the collected native function registry.
///
/// Cheap to `Clone` — internally `Arc`'d so multiple subsystems (backend
/// schema bootstrap, composite runner, etc.) share the same table.
#[derive(Clone)]
pub struct NativeFunctionRunner {
    inner: Arc<NativeFunctionRegistry>,
}

impl NativeFunctionRunner {
    /// Collect every `inventory::submit!`ed registration in the binary.
    pub fn from_inventory() -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(NativeFunctionRegistry::collect()?),
        })
    }

    /// Number of registered functions.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// `true` if a native function with this name is registered.
    pub fn has_function(&self, name: &str) -> bool {
        self.inner.get(name).is_some()
    }

    /// `true` if the named function is registered and of the given kind.
    pub fn has_function_of_type(&self, name: &str, udf_type: UdfType) -> bool {
        match self.inner.get(name) {
            Some(r) => r.udf_type() == udf_type,
            None => false,
        }
    }

    /// Look up the full registration. Returns `None` if unknown.
    pub fn get(&self, name: &str) -> Option<&'static NativeFunctionRegistration> {
        self.inner.get(name)
    }

    /// Execute a named native query. Errors if the name isn't registered
    /// or isn't a query.
    pub async fn run_query(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        let registration = self
            .inner
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no native function registered with name {name:?}"))?;
        let HandlerFn::Query(handler) = registration.handler else {
            anyhow::bail!(
                "native function {name:?} is not a query (got {:?})",
                registration.udf_type(),
            );
        };
        let mut ctx = QueryCtx::new(tx, namespace);
        handler(&mut ctx, args).await
    }

    /// Execute a named native mutation. Errors if the name isn't
    /// registered or isn't a mutation.
    pub async fn run_mutation(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        let registration = self
            .inner
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no native function registered with name {name:?}"))?;
        let HandlerFn::Mutation(handler) = registration.handler else {
            anyhow::bail!(
                "native function {name:?} is not a mutation (got {:?})",
                registration.udf_type(),
            );
        };
        let mut ctx = MutationCtx::new(tx, namespace);
        handler(&mut ctx, args).await
    }

    /// Execute a named native action with `NoopCallbacks`. Actions that
    /// need to sub-call queries/mutations or touch storage/scheduler
    /// should go through [`run_action_with_callbacks`] instead.
    pub async fn run_action(
        self: &Arc<Self>,
        name: &str,
        namespace: TableNamespace,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.run_action_with_callbacks(
            name,
            namespace,
            args,
            Arc::new(crate::callbacks::NoopCallbacks),
        )
        .await
    }

    /// Execute a named native action with explicit callbacks. The
    /// action's `ActionCtx` gets those callbacks, so `run_query`,
    /// `run_mutation`, `scheduler()`, and `storage()` all route through
    /// them.
    pub async fn run_action_with_callbacks(
        self: &Arc<Self>,
        name: &str,
        namespace: TableNamespace,
        args: ConvexObject,
        callbacks: Arc<dyn crate::callbacks::NativeActionCallbacks>,
    ) -> anyhow::Result<ConvexValue> {
        let registration = self
            .inner
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no native function registered with name {name:?}"))?;
        let HandlerFn::Action(handler) = registration.handler else {
            anyhow::bail!(
                "native function {name:?} is not an action (got {:?})",
                registration.udf_type(),
            );
        };
        let mut ctx = ActionCtx::<Rt>::with_callbacks(Some(self.clone()), callbacks, namespace);
        handler(&mut ctx, args).await
    }

    /// Iterate the registered functions (metadata only).
    pub fn iter(&self) -> impl Iterator<Item = &'static NativeFunctionRegistration> + '_ {
        self.inner.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_from_inventory() {
        let runner = NativeFunctionRunner::from_inventory().expect("from_inventory");
        // No functions are registered at crate-unit-test scope.
        assert!(runner.is_empty());
        assert!(!runner.has_function("anything"));
    }
}
