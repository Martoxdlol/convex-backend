//! Error-handling helpers for native functions.
//!
//! Convex distinguishes **user errors** (bad input, 4xx-style) from
//! **system errors** (internal bugs, 5xx-style) through the
//! `errors::ErrorMetadata` tag attached to an `anyhow::Error`. Queries,
//! mutations, and actions should emit user-facing errors tagged this
//! way so the HTTP / RPC layer can map them to the right status
//! code and the Convex client library can surface them to
//! application code without leaking system internals.
//!
//! This module re-exports the most common builders with docstrings
//! oriented at native-function authors. Use them from inside
//! `#[convex::query]` / `#[convex::mutation]` / `#[convex::action]`
//! bodies:
//!
//! ```ignore
//! if !ctx.auth().is_authenticated() {
//!     return Err(convex_native::errors::unauthenticated(
//!         "MissingToken",
//!         "Request requires an auth token",
//!     ).into());
//! }
//! if user.tier == Tier::Free {
//!     anyhow::bail!(convex_native::errors::forbidden(
//!         "FreeTierUnsupported",
//!         "This operation is not available on the Free tier",
//!     ));
//! }
//! ```

pub use errors::ErrorMetadata;

/// 400 Bad Request — the caller did something wrong (invalid input,
/// malformed payload, etc.).
pub fn bad_request(
    short_msg: impl Into<std::borrow::Cow<'static, str>>,
    msg: impl Into<std::borrow::Cow<'static, str>>,
) -> ErrorMetadata {
    ErrorMetadata::bad_request(short_msg, msg)
}

/// 404 Not Found — the requested resource doesn't exist. Do **not**
/// use this for "UDF not found"; that should be `bad_request` per
/// Convex convention.
pub fn not_found(
    short_msg: impl Into<std::borrow::Cow<'static, str>>,
    msg: impl Into<std::borrow::Cow<'static, str>>,
) -> ErrorMetadata {
    ErrorMetadata::not_found(short_msg, msg)
}

/// 401 Unauthenticated — request is missing credentials or the
/// credentials are invalid.
pub fn unauthenticated(
    short_msg: impl Into<std::borrow::Cow<'static, str>>,
    msg: impl Into<std::borrow::Cow<'static, str>>,
) -> ErrorMetadata {
    ErrorMetadata::unauthenticated(short_msg, msg)
}

/// 403 Forbidden — credentials are valid but the caller isn't
/// allowed to perform this operation.
pub fn forbidden(
    short_msg: impl Into<std::borrow::Cow<'static, str>>,
    msg: impl Into<std::borrow::Cow<'static, str>>,
) -> ErrorMetadata {
    ErrorMetadata::forbidden(short_msg, msg)
}

/// 409 Conflict — the request collides with concurrent state (e.g.
/// duplicate insert against a unique constraint).
pub fn conflict(
    short_msg: impl Into<std::borrow::Cow<'static, str>>,
    msg: impl Into<std::borrow::Cow<'static, str>>,
) -> ErrorMetadata {
    ErrorMetadata::conflict(short_msg, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builders_return_tagged_errors() {
        let e: anyhow::Error = bad_request("BadInput", "email is required").into();
        // Roundtrip: we can recover the short_msg from the anyhow chain.
        let meta = e
            .downcast_ref::<ErrorMetadata>()
            .expect("attached metadata");
        assert_eq!(meta.short_msg, "BadInput");
    }

    fn downcast(e: anyhow::Error) -> ErrorMetadata {
        e.downcast::<ErrorMetadata>().expect("attached metadata")
    }

    #[test]
    fn not_found_preserves_short_and_long_message() {
        let meta = downcast(not_found("UserMissing", "No user with that email").into());
        assert_eq!(meta.short_msg, "UserMissing");
        assert!(format!("{}", meta.msg).contains("No user with that email"));
    }

    #[test]
    fn unauthenticated_preserves_short_msg() {
        let meta =
            downcast(unauthenticated("MissingToken", "Request requires an auth token").into());
        assert_eq!(meta.short_msg, "MissingToken");
    }

    #[test]
    fn forbidden_preserves_short_msg() {
        let meta = downcast(forbidden("FreeTierUnsupported", "Not on free tier").into());
        assert_eq!(meta.short_msg, "FreeTierUnsupported");
    }

    #[test]
    fn conflict_preserves_short_msg() {
        let meta = downcast(conflict("DuplicateEmail", "That email is already registered").into());
        assert_eq!(meta.short_msg, "DuplicateEmail");
    }

    #[test]
    fn bad_request_accepts_owned_strings() {
        // `impl Into<Cow<'static, str>>` — the API should accept
        // `String` as well as `&'static str`. This test pins that
        // contract so it doesn't regress silently.
        let short = String::from("DynamicShortMsg");
        let long = String::from("Dynamically constructed long message");
        let meta = downcast(bad_request(short, long).into());
        assert_eq!(meta.short_msg, "DynamicShortMsg");
    }
}
