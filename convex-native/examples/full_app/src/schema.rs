//! Schema — every `#[derive(ConvexDocument)]` for this app.

use convex_native::{
    prelude::*,
    ConvexDocument,
    ConvexEnum,
    ConvexNested,
};

/// Subscription tier — demonstrates `#[derive(ConvexEnum)]`.
/// Variants serialize as snake-case strings by default
/// (`Admin` → `"admin"`). Override per-variant via
/// `#[convex(rename = "...")]`.
#[derive(ConvexEnum, Debug, Clone, PartialEq)]
pub enum Tier {
    Free,
    Pro,
    #[convex(rename = "enterprise-legacy")]
    EnterpriseLegacy,
}

/// Nested profile — demonstrates `#[derive(ConvexNested)]`. Not a
/// table of its own; only valid as a field of another document.
#[derive(ConvexNested, Debug, Clone)]
pub struct Profile {
    pub tier: Tier,
    pub display_name: String,
}

/// User document. Has:
///
/// - A standard database index (`by_email`).
/// - A text index on `display_name` for full-text search.
/// - A nested `Profile`.
/// - An optional `avatar_url` — exercises the
///   `Option<T>` → `Union(Null, T)` validator path.
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
#[convex(text_index(
    name = "display_name_search",
    search_field = "profile.display_name",
    filter_fields = ["email"],
))]
pub struct User {
    pub email: String,
    pub profile: Profile,
    pub avatar_url: Option<String>,
    pub created_at: f64,
}

/// Messages belonging to a user. Demonstrates the typed
/// `Id<T>` foreign key — `author: Id<User>` reflects as
/// `Validator::Id("users")`, so a stale id from another table
/// fails validation at write time.
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "messages")]
#[convex(index(name = "by_author", fields = ["author", "created_at"]))]
pub struct Message {
    pub author: Id<User>,
    pub body: String,
    pub created_at: f64,
}
