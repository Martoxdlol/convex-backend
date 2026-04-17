//! Bridge between `convex_native_core::NativeFunctionRunner` and
//! `udf::validation`'s global native-function resolver hook.
//!
//! The HTTP / WebSocket / sync entry points funnel every client
//! request through `ValidatedPathAndArgs::new` which, up to and
//! including the pre-fix behaviour, only knew how to look up
//! functions in the `_modules` system table. That table is empty on
//! any deployment that didn't run `npx convex dev` at least once —
//! i.e. every pure-native topology (see
//! `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md`).
//!
//! [`install_native_resolver`] registers a [`NativeRegistryResolver`]
//! against `udf::validation`'s global `OnceLock`. Call it once from
//! `local_backend::make_app` after the native registry has been
//! collected from `inventory`. Subsequent calls are idempotent —
//! the `OnceLock` rejects second `set` attempts.

use std::sync::Arc;

use convex_native_core::NativeFunctionRunner;
use udf::validation::{
    install_native_function_resolver,
    NativeFunctionDescriptor,
    NativeFunctionResolver,
};

/// Adapter that exposes a `NativeFunctionRunner`'s in-memory
/// registry to `udf::validation` without leaking the concrete
/// type into the `udf` crate's dependency set.
pub struct NativeRegistryResolver {
    runner: NativeFunctionRunner,
}

impl NativeRegistryResolver {
    pub fn new(runner: NativeFunctionRunner) -> Self {
        Self { runner }
    }
}

impl NativeFunctionResolver for NativeRegistryResolver {
    fn lookup(&self, function_name: &str) -> Option<NativeFunctionDescriptor> {
        // `NativeFunctionRunner::get` returns the full
        // `NativeFunctionRegistration` for a name — it's the single
        // source of truth the composite runner already uses to
        // dispatch, so reading from it here keeps validation and
        // dispatch in lockstep.
        let registration = self.runner.get(function_name)?;
        Some(NativeFunctionDescriptor {
            udf_type: registration.udf_type(),
            is_internal: registration.is_internal,
        })
    }
}

/// Install the native-function resolver globally. Returns `true`
/// when this call populated the `OnceLock`; `false` if a resolver
/// was already installed (tests that call `make_app` multiple times
/// in the same process).
pub fn install_native_resolver(runner: NativeFunctionRunner) -> bool {
    install_native_function_resolver(Arc::new(NativeRegistryResolver::new(runner)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_runner_reports_none() {
        let runner =
            NativeFunctionRunner::from_inventory().expect("empty inventory should collect");
        let resolver = NativeRegistryResolver::new(runner);
        assert!(resolver.lookup("nonexistent").is_none());
    }
}
