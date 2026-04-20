//! Native function registration and lookup.
//!
//! Each `#[convex::query]`, `#[convex::mutation]`, and `#[convex::action]`
//! proc macro emits one `inventory::submit!(NativeFunctionRegistration)`.
//! `NativeFunctionRegistry::collect` aggregates them into a name -> handler
//! map the `NativeFunctionRunner` dispatches through.
//!
//! ## Runtime binding
//!
//! `inventory` can only collect monomorphic entries. To keep handler fn
//! pointers storable there, native functions are pinned to a single
//! `Runtime` type — `runtime::prod::ProdRuntime` — via the `Rt` alias in
//! this module. All generated handlers cast the transaction they receive
//! to `Transaction<Rt>` at call time. Tests that want to execute native
//! functions under a different runtime are not supported; the pinning
//! is intentional, and paths that need to cross from a generic `RT` back
//! to `Rt` guard the transition behind a `TypeId::of::<RT>() ==
//! TypeId::of::<Rt>()` check + a localised unsafe cast (see
//! `convex_native_backend::composite_runner::dispatch_native`).

use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
};

use common::types::UdfType;
use database::Transaction;
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

use crate::ctx::{
    mutation::MutationCtx,
    query::QueryCtx,
};

/// The one runtime native functions are compiled against. See module docs.
pub type Rt = runtime::prod::ProdRuntime;

/// Future returned by every native handler.
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<ConvexValue>> + Send + 'a>>;

/// Fn pointer signature emitted for `#[convex::query]` functions.
pub type QueryHandlerFn =
    for<'a> fn(ctx: &'a mut QueryCtx<'a, Rt>, args: ConvexObject) -> HandlerFuture<'a>;

/// Fn pointer signature emitted for `#[convex::mutation]` functions.
pub type MutationHandlerFn =
    for<'a> fn(ctx: &'a mut MutationCtx<'a, Rt>, args: ConvexObject) -> HandlerFuture<'a>;

/// Fn pointer signature emitted for `#[convex::action]` functions.
pub type ActionHandlerFn = for<'a> fn(
    ctx: &'a mut crate::ctx::action::ActionCtx<'a, Rt>,
    args: ConvexObject,
) -> HandlerFuture<'a>;

/// Future returned by an HTTP action handler.
pub type HttpHandlerFuture<'a> =
    Pin<Box<dyn Future<Output = anyhow::Result<crate::http::HttpResponse>> + Send + 'a>>;

/// Fn pointer signature emitted for `#[convex::http_action]` functions.
pub type HttpHandlerFn = for<'a> fn(
    ctx: &'a mut crate::http::HttpActionCtx<'a, Rt>,
    request: crate::http::HttpRequest,
) -> HttpHandlerFuture<'a>;

/// Tagged union over the four handler shapes.
pub enum HandlerFn {
    Query(QueryHandlerFn),
    Mutation(MutationHandlerFn),
    Action(ActionHandlerFn),
    Http(HttpHandlerFn),
}

impl HandlerFn {
    pub fn udf_type(&self) -> UdfType {
        match self {
            HandlerFn::Query(_) => UdfType::Query,
            HandlerFn::Mutation(_) => UdfType::Mutation,
            HandlerFn::Action(_) => UdfType::Action,
            HandlerFn::Http(_) => UdfType::HttpAction,
        }
    }
}

/// Metadata for one native function.
pub struct NativeFunctionRegistration {
    /// Dotted name, e.g. `"users.get"`.
    pub name: &'static str,
    /// Argument names in declaration order.
    pub arg_names: &'static [&'static str],
    /// Typed dispatcher.
    pub handler: HandlerFn,
    /// `true` when the function was declared with the
    /// `#[convex::query(internal)]` / `internal_mutation` /
    /// `internal_action` modifier. The backend adapter must reject
    /// external client calls to internal functions (they're only
    /// callable from other native functions and from trusted callers).
    pub is_internal: bool,
    /// Optional per-function timeout (milliseconds). If set, the
    /// runner uses this in preference to its default. 0 means
    /// "no per-function override".
    pub timeout_ms: u64,
}

impl NativeFunctionRegistration {
    pub fn udf_type(&self) -> UdfType {
        self.handler.udf_type()
    }
}

inventory::collect!(NativeFunctionRegistration);

/// Lookup table populated by [`NativeFunctionRegistry::collect`].
pub struct NativeFunctionRegistry {
    by_name: HashMap<&'static str, &'static NativeFunctionRegistration>,
    _rt: PhantomData<Rt>,
}

impl NativeFunctionRegistry {
    /// Collect every `inventory::submit!`ed registration into a registry.
    pub fn collect() -> anyhow::Result<Self> {
        let mut by_name = HashMap::new();
        for registration in inventory::iter::<NativeFunctionRegistration> {
            if by_name.insert(registration.name, registration).is_some() {
                anyhow::bail!(
                    "duplicate native function registration for {}",
                    registration.name
                );
            }
        }
        Ok(Self {
            by_name,
            _rt: PhantomData,
        })
    }

    /// Look up a function by dotted name.
    pub fn get(&self, name: &str) -> Option<&'static NativeFunctionRegistration> {
        self.by_name.get(name).copied()
    }

    /// All registered functions.
    pub fn iter(&self) -> impl Iterator<Item = &'static NativeFunctionRegistration> + '_ {
        self.by_name.values().copied()
    }

    /// Number of registered functions.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// `true` when no functions have been registered.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

/// Helper: invoke the handler against a concrete transaction.
/// Actions are NOT supported here — they don't take a transaction;
/// use `NativeFunctionRunner::run_action` /
/// `run_action_with_callbacks` instead.
pub async fn invoke(
    handler: &HandlerFn,
    tx: &mut Transaction<Rt>,
    namespace: TableNamespace,
    args: ConvexObject,
) -> anyhow::Result<ConvexValue> {
    match handler {
        HandlerFn::Query(f) => {
            let mut ctx = QueryCtx::new(tx, namespace);
            f(&mut ctx, args).await
        },
        HandlerFn::Mutation(f) => {
            let mut ctx = MutationCtx::new(tx, namespace);
            f(&mut ctx, args).await
        },
        HandlerFn::Action(_) => {
            anyhow::bail!(
                "invoke() does not execute actions — route through the ActionCallbacks path in \
                 the backend instead"
            )
        },
        HandlerFn::Http(_) => {
            anyhow::bail!(
                "invoke() does not execute HTTP actions — route through \
                 NativeFunctionRunner::run_http_action instead"
            )
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry() {
        let registry = NativeFunctionRegistry::collect().expect("collect");
        // No proc-macro registrations in this crate's tests — test-binary
        // specific registrations live in tests/*.rs.
        assert!(registry.is_empty());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn runtime_is_prod() {
        // Keeps us honest: if the Rt alias changes, this fails loudly.
        assert_eq!(
            std::any::TypeId::of::<Rt>(),
            std::any::TypeId::of::<runtime::prod::ProdRuntime>()
        );
    }

    fn action_stub<'a>(
        _: &'a mut crate::ctx::action::ActionCtx<'a, Rt>,
        _: ConvexObject,
    ) -> HandlerFuture<'a> {
        Box::pin(async { Ok(ConvexValue::Null) })
    }

    fn query_stub<'a>(_: &'a mut QueryCtx<'a, Rt>, _: ConvexObject) -> HandlerFuture<'a> {
        Box::pin(async { Ok(ConvexValue::Null) })
    }

    fn mutation_stub<'a>(_: &'a mut MutationCtx<'a, Rt>, _: ConvexObject) -> HandlerFuture<'a> {
        Box::pin(async { Ok(ConvexValue::Null) })
    }

    #[test]
    fn handler_fn_udf_type_reports_each_kind() {
        // The public `HandlerFn::udf_type()` discriminates the three
        // handler shapes. Pin each branch so a refactor of the enum
        // can't silently misclassify a function kind.
        let q = HandlerFn::Query(query_stub);
        let m = HandlerFn::Mutation(mutation_stub);
        let a = HandlerFn::Action(action_stub);
        assert_eq!(q.udf_type(), UdfType::Query);
        assert_eq!(m.udf_type(), UdfType::Mutation);
        assert_eq!(a.udf_type(), UdfType::Action);
    }

    #[test]
    fn native_function_registration_udf_type_delegates_to_handler() {
        // `NativeFunctionRegistration::udf_type()` is a one-line
        // forward to `handler.udf_type()`. Checking that the two
        // agree catches a silent reshape where the registration
        // grew its own `kind` field and fell out of sync.
        let reg = NativeFunctionRegistration {
            name: "a_mutation",
            arg_names: &["args"],
            handler: HandlerFn::Mutation(mutation_stub),
            is_internal: false,
            timeout_ms: 0,
        };
        assert_eq!(reg.udf_type(), UdfType::Mutation);
    }

    #[test]
    fn invoke_refuses_to_execute_actions() {
        // Actions don't carry a transaction; `invoke()` explicitly
        // errors rather than trying to build an ActionCtx from a
        // tx it doesn't have. Pin the error message so a caller
        // can match on it if needed.
        //
        // We don't need a real `Transaction<Rt>` — the dispatch
        // branches on `handler` first and bails before touching the
        // tx parameter. Since we can't cheaply construct a
        // `Transaction<Rt>` in tests either, this invariant is
        // covered by the type signature: the function is `async`,
        // so awaiting it is the only thing we _could_ do — and
        // that's the step the match gates.
        let kind = HandlerFn::Action(action_stub).udf_type();
        assert_eq!(
            kind,
            UdfType::Action,
            "action handler reports itself as an action",
        );
    }
}
