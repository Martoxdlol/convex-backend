use convex_native::ConvexDocument;

/// A todo item owned by some caller-supplied string.
///
/// `ConvexDocument` expands to:
/// - `TodoField` enum — typed field references for indexes / filters.
/// - `TodoIndex` enum — `TodoIndex::ByOwner` here.
/// - `TodoPatch` struct — all fields `Option<...>`, for partial updates.
/// - `TodoWithId` struct — the stored form with `_id` attached.
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "todos")]
#[convex(index(name = "by_owner", fields = ["owner"]))]
pub struct Todo {
    pub owner: String,
    pub text: String,
    pub done: bool,
    pub created_at: f64,
}
