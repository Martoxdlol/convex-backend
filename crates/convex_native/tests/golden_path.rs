//! Golden-path end-to-end test exercising the full developer surface.
//!
//! Demonstrates a realistic mini-app:
//! - Schema: `User` + `Message` tables with indexes
//! - Nested `Profile` via `#[derive(ConvexNested)]`
//! - String enum `Tier` via `#[derive(ConvexEnum)]`
//! - Tagged union `Notification` via `#[derive(ConvexUnion)]`
//! - Typed `#[convex::query]` / `#[convex::mutation]` that accept the derived
//!   struct values
//! - `#[convex::action]` that invokes a sub-query, a sub-mutation, a scheduler
//!   call, and a storage call through mock callbacks
//! - An `#[convex::http_action]` registered with the router
//! - Everything assembled through `ConvexBackend::new()`

use std::{
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use convex_native::{
    convex,
    diff_schemas,
    ActionCtx,
    ConvexBackend,
    ConvexDocument,
    ConvexEnum,
    ConvexNested,
    ConvexUnion,
    FromConvex,
    HttpActionCtx,
    HttpRequest,
    HttpResponse,
    Id,
    MutationCtx,
    NativeActionCallbacks,
    QueryCtx,
    Rt,
    SchemaChange,
    StorageId,
    ToConvex,
};
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    TableNamespace,
};

#[derive(ConvexEnum, Debug, Clone, PartialEq)]
pub enum Tier {
    Free,
    Pro,
    Enterprise,
}

#[derive(ConvexNested, Debug, Clone, PartialEq)]
pub struct Profile {
    pub tier: Tier,
    pub display_name: String,
}

#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User {
    pub email: String,
    pub profile: Profile,
}

#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "messages")]
#[convex(index(name = "by_author", fields = ["author"]))]
pub struct Message {
    pub author: Id<User>,
    pub body: String,
}

#[derive(ConvexUnion, Debug, Clone, PartialEq)]
#[convex(tag = "kind")]
pub enum Notification {
    Email { to: String },
    Push { token: String },
}

// ── Functions ────────────────────────────────────────────────────

#[convex::query]
pub async fn get_user_count(
    _ctx: &mut QueryCtx<'_, Rt>,
    filter: Option<Tier>,
) -> anyhow::Result<i64> {
    let _ = filter;
    Ok(5)
}

#[convex::mutation]
pub async fn record_note(_ctx: &mut MutationCtx<'_, Rt>, body: String) -> anyhow::Result<()> {
    let _ = body;
    Ok(())
}

#[convex::action]
pub async fn notify_user(
    ctx: &mut ActionCtx<'_, Rt>,
    notification: Notification,
) -> anyhow::Result<String> {
    // Typed sub-query.
    let count: i64 = ctx
        .run_query(GetUserCount, GetUserCountArgs { filter: None })
        .await?;

    // Typed sub-mutation.
    ctx.run_mutation(
        RecordNote,
        RecordNoteArgs {
            body: format!("notify count={count}"),
        },
    )
    .await?;

    // Scheduler (mutation).
    ctx.scheduler()
        .run_after(
            Duration::from_secs(10),
            RecordNote,
            RecordNoteArgs {
                body: "scheduled".into(),
            },
        )
        .await?;

    // Storage round-trip.
    let id = ctx
        .storage()
        .store(Bytes::from_static(b"payload"), "application/octet-stream")
        .await?;
    let url = ctx.storage().get_url(id.clone()).await?.unwrap_or_default();
    let _deleted = ctx.storage().delete(id).await?;

    Ok(format!("notification={notification:?} url={url}"))
}

#[convex::http_action(method = "GET", path = "/health")]
#[allow(dead_code)]
async fn http_health(
    _ctx: &mut HttpActionCtx<'_, Rt>,
    _req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    Ok(HttpResponse::json(200, serde_json::json!({"status": "ok"})))
}

/// Exercises `scheduler().run_at(...)` in an end-to-end action flow.
///
/// Computes an absolute wall-clock timestamp ~10s in the future and
/// hands it to the scheduler. The scheduler internally converts to a
/// `Duration` via `delay_until`; we assert downstream through the
/// mock that the recorded delay lands in a tolerance band around 10s.
#[convex::action]
pub async fn notify_at_timestamp(ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    use std::time::SystemTime;
    let now = common::runtime::UnixTimestamp::from_system_time(SystemTime::now())
        .expect("clock after epoch");
    let ten_seconds_out =
        common::runtime::UnixTimestamp::from_secs_f64(now.as_secs_f64() + 10.0).unwrap();
    ctx.scheduler()
        .run_at(
            ten_seconds_out,
            RecordNote,
            RecordNoteArgs {
                body: "deadline".into(),
            },
        )
        .await?;
    Ok(())
}

// ── Mock callbacks ───────────────────────────────────────────────

#[derive(Default)]
struct MockState {
    queries: Vec<String>,
    mutations: Vec<String>,
    schedules: Vec<(String, Duration)>,
    stores: usize,
    deletes: usize,
}

struct Mock {
    state: Mutex<MockState>,
}

#[async_trait]
impl NativeActionCallbacks for Mock {
    async fn run_query_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.state.lock().unwrap().queries.push(name.to_string());
        Ok(ConvexValue::Int64(5))
    }

    async fn run_mutation_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.state.lock().unwrap().mutations.push(name.to_string());
        Ok(ConvexValue::Null)
    }

    async fn schedule(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
        delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        self.state
            .lock()
            .unwrap()
            .schedules
            .push((name.to_string(), delay));
        Ok(DeveloperDocumentId::MIN)
    }

    async fn storage_store(
        &self,
        _ns: TableNamespace,
        _body: Bytes,
        _content_type: &str,
    ) -> anyhow::Result<StorageId> {
        self.state.lock().unwrap().stores += 1;
        Ok(StorageId("mock-storage".into()))
    }

    async fn storage_get_url(
        &self,
        _ns: TableNamespace,
        _id: StorageId,
    ) -> anyhow::Result<Option<String>> {
        Ok(Some("https://mock/url".into()))
    }

    async fn storage_delete(&self, _ns: TableNamespace, _id: StorageId) -> anyhow::Result<bool> {
        self.state.lock().unwrap().deletes += 1;
        Ok(true)
    }
}

#[tokio::test]
async fn full_app_works_end_to_end() {
    let mock = Arc::new(Mock {
        state: Mutex::new(MockState::default()),
    });

    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_callbacks(mock.clone())
        .build()
        .expect("build");

    // Schema asserts.
    let schema = built.schema.as_ref().unwrap();
    assert!(schema.tables.keys().any(|t| t.to_string() == "users"));
    assert!(schema.tables.keys().any(|t| t.to_string() == "messages"));
    let users_def = schema
        .tables
        .iter()
        .find(|(n, _)| n.to_string() == "users")
        .unwrap()
        .1;
    assert!(users_def.indexes.keys().any(|d| d.as_str() == "by_email"));

    // Diff the schema against itself — should be empty.
    let changes = diff_schemas(schema, schema);
    assert!(changes.is_empty());

    // And against an empty schema — everything shows up as added.
    let empty = common::schemas::DatabaseSchema {
        tables: Default::default(),
        schema_validation: true,
    };
    let changes = diff_schemas(&empty, schema);
    assert!(changes
        .iter()
        .any(|c| matches!(c, SchemaChange::TableAdded(_))));

    // Router asserts.
    let router = built.router.as_ref().unwrap();
    assert!(router.lookup("GET", "/health").is_some());

    // Drive the action end-to-end — exercises sub-query, sub-mutation,
    // scheduler, and storage paths.
    let args = NotifyUserArgs {
        notification: Notification::Email { to: "a@b".into() },
    };
    let obj = match args.to_convex().unwrap() {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };
    let ret = built
        .run_action("notify_user", TableNamespace::Global, obj)
        .await
        .expect("action ran");
    let ConvexValue::String(s) = ret else {
        panic!("expected String")
    };
    assert!(s.as_ref().contains("https://mock/url"));

    // And confirm all side effects landed via the mock.
    let state = mock.state.lock().unwrap();
    assert_eq!(state.queries, vec!["get_user_count".to_string()]);
    assert_eq!(
        state.mutations,
        vec!["record_note".to_string()],
        "sub-mutation only — scheduled call is deferred, not executed"
    );
    assert_eq!(state.schedules.len(), 1);
    assert_eq!(state.schedules[0].0, "record_note");
    assert_eq!(state.schedules[0].1, Duration::from_secs(10));
    assert_eq!(state.stores, 1);
    assert_eq!(state.deletes, 1);
}

#[tokio::test]
async fn scheduler_run_at_lands_as_absolute_delay() {
    // Runs `notify_at_timestamp` through the full `ConvexBackend`
    // stack. The action computes a `UnixTimestamp` ten seconds in the
    // future and schedules against it via `scheduler().run_at(...)`.
    // The scheduler's `delay_until` helper converts the absolute
    // timestamp back to a `Duration`, which the mock records.
    //
    // Tolerance: 9s..=11s absorbs the two separate `SystemTime::now()`
    // reads (one in the action, one inside `delay_until`) that can
    // drift by milliseconds under CI load.
    let mock = Arc::new(Mock {
        state: Mutex::new(MockState::default()),
    });
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_callbacks(mock.clone())
        .build()
        .expect("build");

    let args_value = (NotifyAtTimestampArgs {}).to_convex().unwrap();
    let obj = match args_value {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };
    built
        .run_action("notify_at_timestamp", TableNamespace::Global, obj)
        .await
        .expect("action ran");

    let state = mock.state.lock().unwrap();
    assert_eq!(state.schedules.len(), 1, "one schedule call recorded");
    let (name, delay) = &state.schedules[0];
    assert_eq!(name, "record_note");
    assert!(
        *delay >= Duration::from_secs(9) && *delay <= Duration::from_secs(11),
        "run_at should land ~10s out after delay_until conversion: {delay:?}",
    );
}

#[test]
fn document_with_nested_and_enum_round_trips() {
    let user = User {
        email: "a@b".into(),
        profile: Profile {
            tier: Tier::Enterprise,
            display_name: "Alice".into(),
        },
    };
    let cv = user.clone().to_convex().unwrap();
    let back = User::from_convex(cv).unwrap();
    assert_eq!(back, user);

    let n = Notification::Push {
        token: "abc".into(),
    };
    let back_n = Notification::from_convex(n.clone().to_convex().unwrap()).unwrap();
    assert_eq!(back_n, n);
}
