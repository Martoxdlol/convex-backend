//! Marker traits identifying native function references.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 2.3.
//!
//! Each `#[convex::query]` / `#[convex::mutation]` / `#[convex::action]`
//! proc macro also generates a ZST marker type (PascalCase of the
//! function name) implementing the matching trait. Downstream,
//! `ActionCtx::run_query`, `run_mutation`, etc. take a marker value +
//! typed args and return the typed output.
//!
//! Example (what the macro emits for an `#[convex::query]`):
//!
//! ```ignore
//! #[convex::query]
//! async fn get_user(ctx: &mut QueryCtx, email: String) -> Result<Option<User>> { .. }
//!
//! // Macro generates (approximately):
//! #[allow(non_camel_case_types)]
//! pub struct GetUser;
//! #[derive(ConvexNested)]
//! pub struct GetUserArgs { pub email: String }
//! impl ConvexQueryFunction for GetUser {
//!     type Args = GetUserArgs;
//!     type Output = Option<User>;
//!     fn name() -> &'static str { "get_user" }
//! }
//! ```
//!
//! Then a typed sub-call looks like:
//!
//! ```ignore
//! let user: Option<User> = ctx.run_query(GetUser, GetUserArgs {
//!     email: "a@b".into(),
//! }).await?;
//! ```

use crate::convert::{
    FromConvex,
    ToConvex,
};

/// Marker implemented by the ZST generated for every `#[convex::query]`.
pub trait ConvexQueryFunction {
    type Args: ToConvex + FromConvex + Send + 'static;
    type Output: ToConvex + FromConvex + Send + 'static;
    fn name() -> &'static str;
}

/// Marker implemented by the ZST generated for every `#[convex::mutation]`.
pub trait ConvexMutationFunction {
    type Args: ToConvex + FromConvex + Send + 'static;
    type Output: ToConvex + FromConvex + Send + 'static;
    fn name() -> &'static str;
}

/// Marker implemented by the ZST generated for every `#[convex::action]`.
pub trait ConvexActionFunction {
    type Args: ToConvex + FromConvex + Send + 'static;
    type Output: ToConvex + FromConvex + Send + 'static;
    fn name() -> &'static str;
}
