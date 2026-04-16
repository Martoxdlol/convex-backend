//! Authentication / identity helpers exposed through contexts.
//!
//! Wraps the existing `keybroker::Identity` with a thin native-Rust
//! surface that matches the design-doc example:
//!
//! ```ignore
//! if !ctx.auth().is_authenticated() {
//!     anyhow::bail!("unauthorized");
//! }
//! let user_id = ctx.auth().subject().context("missing subject")?;
//! ```
//!
//! Today the helpers are read-only views. Additional helpers (role
//! checks etc.) can bolt on as the design evolves.

use keybroker::Identity;

/// Borrowed view over the current request's identity. Obtained via
/// `QueryCtx::auth()` / `MutationCtx::auth()` / `ActionCtx::auth()`.
pub struct AuthInfo<'a> {
    inner: &'a Identity,
}

impl<'a> AuthInfo<'a> {
    pub(crate) fn new(inner: &'a Identity) -> Self {
        Self { inner }
    }

    /// True if the caller presented valid credentials (user, admin,
    /// system — anything but anonymous).
    pub fn is_authenticated(&self) -> bool {
        !matches!(self.inner, Identity::Unknown(_))
    }

    /// True for admin callers (deploy keys, Convex console, etc.).
    pub fn is_admin(&self) -> bool {
        self.inner.is_admin()
    }

    /// True for system-only identities (internal backend calls).
    pub fn is_system(&self) -> bool {
        self.inner.is_system()
    }

    /// Access the underlying `Identity` for use-cases the wrapper
    /// doesn't cover. Treat this as an escape hatch.
    pub fn raw(&self) -> &Identity {
        self.inner
    }
}
