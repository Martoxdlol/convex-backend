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

use keybroker::{
    Identity,
    UserIdentity,
};

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

    /// The authenticated user's identity, when the caller presented a
    /// JWT. Returns `None` for anonymous / admin / system identities —
    /// those don't carry a JWT subject.
    ///
    /// The returned [`UserIdentity`] carries `subject` (the JWT `sub`
    /// claim), `issuer`, expiration, and attribute map. This is the
    /// Rust analogue of the JS `ctx.auth.getUserIdentity()` syscall.
    pub fn user_identity(&self) -> Option<UserIdentity> {
        self.inner.user_identity()
    }

    /// Convenience shortcut for `self.user_identity()?.subject`. Most
    /// callers only need the JWT `sub` claim — this avoids the
    /// boilerplate of reaching through the full `UserIdentity`.
    pub fn subject(&self) -> Option<String> {
        self.user_identity().map(|u| u.subject)
    }

    /// Access the underlying `Identity` for use-cases the wrapper
    /// doesn't cover. Treat this as an escape hatch.
    pub fn raw(&self) -> &Identity {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_identity_reads_as_unauthenticated() {
        // `Identity::Unknown(None)` is the anonymous / no-creds case —
        // all the convenience predicates should report "no auth".
        let id = Identity::Unknown(None);
        let auth = AuthInfo::new(&id);
        assert!(!auth.is_authenticated());
        assert!(!auth.is_admin());
        assert!(!auth.is_system());
    }

    #[test]
    fn system_identity_is_authenticated_and_system() {
        let id = Identity::system();
        let auth = AuthInfo::new(&id);
        assert!(auth.is_authenticated(), "system identity counts as auth");
        assert!(auth.is_system());
        assert!(!auth.is_admin(), "system is not admin");
    }

    #[test]
    fn raw_returns_the_wrapped_identity_variant() {
        // The `raw()` escape hatch is how advanced callers reach into
        // keybroker features not surfaced by `AuthInfo`. Pin the
        // variant round-trip so we don't accidentally wrap or clone
        // across the boundary.
        let id = Identity::system();
        let auth = AuthInfo::new(&id);
        assert!(matches!(auth.raw(), Identity::System(_)));
    }

    #[test]
    fn is_authenticated_is_true_for_every_non_unknown_variant() {
        // The predicate's definition is "anything but Unknown".
        // Exercise the non-Unknown variants we can construct without
        // dragging in signing keys (System) and confirm the
        // contract. If the definition flips (e.g. is_authenticated
        // starts requiring a User identity), this test flags it.
        let sys = Identity::system();
        assert!(AuthInfo::new(&sys).is_authenticated());
    }

    #[test]
    fn user_identity_is_none_for_unknown_and_system() {
        // `user_identity()` is the Rust analogue of
        // `ctx.auth.getUserIdentity()` — it should only return a
        // value when the caller presented a JWT. The non-user
        // variants (anonymous, system) return `None`, same as the
        // JS contract.
        assert!(AuthInfo::new(&Identity::Unknown(None))
            .user_identity()
            .is_none());
        assert!(AuthInfo::new(&Identity::system()).user_identity().is_none());
    }

    #[test]
    fn subject_shortcut_is_none_when_no_user_identity() {
        // `subject()` is a convenience over `user_identity()?.subject`;
        // when there's no JWT, it must return `None` rather than
        // panicking or returning an empty string.
        assert!(AuthInfo::new(&Identity::system()).subject().is_none());
    }
}
