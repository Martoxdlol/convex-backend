//! Native function registration and lookup.
//!
//! Each `#[convex::query]`, `#[convex::mutation]`, and `#[convex::action]`
//! proc macro emits one `inventory::submit!(NativeFunctionRegistration { ...
//! })`. `NativeFunctionRegistry::collect` aggregates them into a name ->
//! handler map used by the `NativeFunctionRunner`.
//!
//! The concrete handler signature is deliberately left as a future-filled
//! type (`HandlerFn`) — phase 1.2.4 will pin down the exact shape. Keeping
//! it opaque here means the registry type compiles on its own and can be
//! wired into the runner before proc macros are wired up.

use std::collections::HashMap;

use common::types::UdfType;

/// Erased handler function pointer.
///
/// The exact signature is filled in once the context wrappers
/// (`QueryCtx`, `MutationCtx`, `ActionCtx`) are stable in phase 1.3. Today
/// it is a unit-function placeholder so the registry can be constructed and
/// inspected by tests.
pub type HandlerFn = fn();

/// Metadata for one native function.
pub struct NativeFunctionRegistration {
    /// Fully qualified function name in dotted form (e.g. `"users.get"`).
    pub name: &'static str,
    /// Query / Mutation / Action.
    pub udf_type: UdfType,
    /// Argument names in declaration order.
    pub arg_names: &'static [&'static str],
    /// Handler fn pointer (type-erased, see [`HandlerFn`]).
    pub handler: HandlerFn,
}

inventory::collect!(NativeFunctionRegistration);

/// Lookup table populated by [`NativeFunctionRegistry::collect`].
pub struct NativeFunctionRegistry {
    by_name: HashMap<&'static str, &'static NativeFunctionRegistration>,
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
        Ok(Self { by_name })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry() {
        let registry = NativeFunctionRegistry::collect().expect("collect");
        // No proc-macro registrations in this crate's tests.
        assert!(registry.is_empty());
        assert!(registry.get("nonexistent").is_none());
    }
}
