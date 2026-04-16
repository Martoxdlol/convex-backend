//! Compile-time surface tests for QueryCtx / MutationCtx.
//!
//! These tests don't execute queries — the database plumbing isn't wired
//! through to the builder yet (Phase 1.4 work). They verify that the
//! typed API *shapes* are sound: `get`, `insert`, `patch`, `delete`,
//! `query` and the builder chain all type-check against a derived
//! `ConvexDocument`.
//!
//! Since we don't have a concrete test `Runtime` available as a
//! dev-dependency here, the async helper fns below are generic over `RT`
//! and only used as function-pointer casts in the runtime test — the real
//! assertion is that they compile.

use convex_native::{
    ctx::query_builder::Order,
    ConvexDocument,
    FieldReference,
    Id,
    IndexReference,
    MutationCtx,
    QueryCtx,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "widgets")]
#[convex(index(name = "by_owner", fields = ["owner"]))]
pub struct Widget {
    pub owner: String,
    pub count: i64,
}

#[allow(dead_code)]
async fn _widget_query<RT: common::runtime::Runtime>(ctx: &mut QueryCtx<'_, RT>) {
    // Compile check: auth() returns AuthInfo usable directly.
    let _is_auth: bool = ctx.auth().is_authenticated();
    let _is_admin: bool = ctx.auth().is_admin();
    // Compile check: unix_timestamp() is available.
    let _ts: common::runtime::UnixTimestamp = ctx.unix_timestamp();

    let _ = ctx
        .db()
        .query::<Widget>()
        .with_index(WidgetIndex::ByOwner)
        .eq(WidgetField::Owner, "alice".to_string())
        .unwrap()
        .order(Order::Desc)
        .limit(10)
        .first()
        .await;

    // Range operators type-check against the declared field.
    let _ = ctx
        .db()
        .query::<Widget>()
        .with_index(WidgetIndex::ByOwner)
        .gte(WidgetField::Count, 10_i64)
        .unwrap()
        .lt(WidgetField::Count, 100_i64)
        .unwrap()
        .count()
        .await;

    let id: Id<Widget> = "jd72jdw7t0x9grf04vg5f65t0s7g4k1d".parse().unwrap();
    let _doc: Option<Widget> = ctx.db().get(id).await.unwrap();

    // Metadata-carrying variant.
    let with_meta: Option<convex_native::DocumentWithMeta<Widget>> =
        ctx.db().get_with_meta(id).await.unwrap();
    if let Some(m) = with_meta {
        let _: &Widget = &m; // Deref
        let _: common::document::CreationTime = m.creation_time;
    }

    // try_get + exists compile against the typed id.
    let _: Widget = ctx.db().try_get(id).await.unwrap();
    let _: bool = ctx.db().exists(id).await.unwrap();

    // unique() and take() terminals compile cleanly.
    let _: Option<Widget> = ctx
        .db()
        .query::<Widget>()
        .with_index(WidgetIndex::ByOwner)
        .eq(WidgetField::Owner, "alice".to_string())
        .unwrap()
        .unique()
        .await
        .unwrap();
    let _: Vec<Widget> = ctx.db().query::<Widget>().take(3).await.unwrap();
}

#[allow(dead_code)]
async fn _widget_mutation<RT: common::runtime::Runtime>(ctx: &mut MutationCtx<'_, RT>) {
    let w = Widget {
        owner: "bob".into(),
        count: 1,
    };
    let id: Id<Widget> = ctx.db().insert(w.clone()).await.unwrap();
    let _patched: Widget = ctx
        .db()
        .patch(
            id,
            WidgetPatch {
                count: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let _replaced: Widget = ctx.db().replace(id, w).await.unwrap();
    let _deleted: Widget = ctx.db().delete(id).await.unwrap();
}

#[test]
fn ctx_api_compiles() {
    // Real assertion: the generic async fns above compile. Here we just
    // use the field/index enum surface so rust-analyzer doesn't flag
    // them as unused.
    assert_eq!(WidgetField::Owner.as_str(), "owner");
    assert_eq!(WidgetField::Count.as_str(), "count");
    assert_eq!(WidgetIndex::ByOwner.as_str(), "by_owner");
    assert_eq!(WidgetIndex::ByOwner.fields(), &["owner"]);
}
