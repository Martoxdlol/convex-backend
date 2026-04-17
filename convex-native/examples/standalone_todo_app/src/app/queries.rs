use convex_native::{
    convex,
    QueryCtx,
    Rt,
};

use crate::app::schema::{
    Todo,
    TodoField,
    TodoIndex,
};

/// List every todo owned by `owner`.
#[convex::query]
pub async fn list_for_owner(
    ctx: &mut QueryCtx<'_, Rt>,
    owner: String,
) -> anyhow::Result<Vec<Todo>> {
    ctx.db()
        .query::<Todo>()
        .with_index(TodoIndex::ByOwner)
        .eq(TodoField::Owner, owner)?
        .collect()
        .await
}

/// Count the unfinished todos for `owner`. Used by the `summarise`
/// action to demonstrate action → query sub-calls.
#[convex::query]
pub async fn count_pending(ctx: &mut QueryCtx<'_, Rt>, owner: String) -> anyhow::Result<i64> {
    let todos = ctx
        .db()
        .query::<Todo>()
        .with_index(TodoIndex::ByOwner)
        .eq(TodoField::Owner, owner)?
        .collect()
        .await?;
    Ok(todos.into_iter().filter(|t| !t.done).count() as i64)
}
