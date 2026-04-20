use convex_native::{
    convex,
    prelude::*,
    MutationCtx,
    Rt,
};

use crate::app::schema::{
    Todo,
    TodoPatch,
};

/// Insert a new todo. Returns the generated `Id<Todo>`.
#[convex::mutation]
pub async fn create(
    ctx: &mut MutationCtx<'_, Rt>,
    owner: String,
    text: String,
) -> anyhow::Result<Id<Todo>> {
    let now = ctx.unix_timestamp().as_secs_f64();
    let id = ctx
        .db()
        .insert(Todo {
            owner,
            text,
            done: false,
            created_at: now,
        })
        .await?;
    ctx.log().info(format!("created todo {id}"));
    Ok(id)
}

/// Flip `done` to true. No-op on an already-done todo.
#[convex::mutation]
pub async fn mark_done(ctx: &mut MutationCtx<'_, Rt>, id: Id<Todo>) -> anyhow::Result<()> {
    ctx.db()
        .patch(
            id,
            TodoPatch {
                done: Some(true),
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}
