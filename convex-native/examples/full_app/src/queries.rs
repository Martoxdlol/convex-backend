//! Queries — read-only handlers against the schema.

use convex_native::{
    convex,
    prelude::*,
    QueryCtx,
    Rt,
};

use crate::schema::{
    Message,
    MessageField,
    MessageIndex,
    User,
    UserField,
    UserIndex,
};

/// Look a user up by email (single-result `.unique()`).
#[convex::query]
pub async fn get_by_email(
    ctx: &mut QueryCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<Option<User>> {
    ctx.db()
        .query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, email)?
        .unique()
        .await
}

/// List the most recent N messages by a given author.
/// Demonstrates range filters + order + limit on the typed builder.
#[convex::query]
pub async fn recent_messages_by(
    ctx: &mut QueryCtx<'_, Rt>,
    author: Id<User>,
    limit: i64,
) -> anyhow::Result<Vec<Message>> {
    ctx.db()
        .query::<Message>()
        .with_index(MessageIndex::ByAuthor)
        .eq(MessageField::Author, author)?
        .order(convex_native::Order::Desc)
        .take(limit.max(0) as usize)
        .await
}

/// Count all users — demonstrates `count_all<T>`.
#[convex::query]
pub async fn count_users(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<i64> {
    Ok(ctx.db().count_all::<User>().await? as i64)
}
