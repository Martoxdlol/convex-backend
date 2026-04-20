//! Coverage for the `convex_native_core::testing` helpers —
//! `TestCallbacks` + `CallRecord` + `args!`.
//!
//! Drives `summarise` (an action that sub-calls
//! `CountPending`) with a `TestCallbacks` that intercepts the
//! sub-call, returning a canned value; then asserts the call
//! landed in the history.

use std::sync::Arc;

use convex_native_core::{
    __private::ConvexValue,
    testing::{
        CallRecord,
        TestCallbacks,
    },
    NativeActionCallbacks,
    NativeFunctionRunner,
};
use value::TableNamespace;

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

#[tokio::test(flavor = "multi_thread")]
async fn test_callbacks_intercept_sub_query_and_record_history() -> anyhow::Result<()> {
    let (callbacks, history) = TestCallbacks::new()
        .on_query("count_pending", |_args| Ok(ConvexValue::Int64(42)))
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;

    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let args = convex_native_core::testing::args! { "owner" => "alice".to_string() };
    let out = runner
        .run_action_with_callbacks("summarise", TableNamespace::Global, args, callbacks)
        .await?;

    assert!(
        matches!(out, ConvexValue::Int64(42)),
        "`summarise` returned the TestCallbacks-stubbed sub-query value; got {out:?}",
    );

    let q_count =
        history.count(|r| matches!(r, CallRecord::Query { name, .. } if name == "count_pending"));
    assert_eq!(
        q_count, 1,
        "TestHistory should record the one sub-query; got {q_count}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn untyped_run_query_raw_reaches_callbacks_by_name() -> anyhow::Result<()> {
    // `untyped_count_pending` calls `ctx.run_query_raw("count_pending", ...)`
    // by string name — the plumbing underneath typed `run_query`
    // but also the surface deployers use when the callee is
    // picked dynamically. Proves the string-name path reaches
    // `NativeActionCallbacks::run_query_by_name` just like the
    // typed marker path.
    let (callbacks, history) = TestCallbacks::new()
        .on_query("count_pending", |_args| Ok(ConvexValue::Int64(11)))
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let args = convex_native_core::testing::args! { "owner" => "bob".to_string() };
    let out = runner
        .run_action_with_callbacks(
            "untyped_count_pending",
            TableNamespace::Global,
            args,
            callbacks,
        )
        .await?;
    assert!(
        matches!(out, ConvexValue::Int64(11)),
        "expected Int64(11) from the stub; got {out:?}",
    );
    let q_count =
        history.count(|r| matches!(r, CallRecord::Query { name, .. } if name == "count_pending"));
    assert_eq!(q_count, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_sub_mutation_routes_through_run_mutation_by_name() -> anyhow::Result<()> {
    // `chain_create_from_action` does `ctx.run_mutation_raw("create_todo", ...)`.
    // Previous tests cover the sub-*query* path (summarise /
    // untyped_count_pending) and the sub-*action* local-runner
    // path (chain_echo). This closes the third leg: an action
    // reaching the callback layer's run_mutation_by_name entry
    // point by string name. A regression on that branch would
    // mis-route to run_query_by_name or bail with "no mutation
    // handler" without this coverage.
    let (callbacks, history) = TestCallbacks::new()
        .on_mutation("create_todo", |_args| {
            Ok(ConvexValue::try_from("stub-id".to_string()).unwrap())
        })
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let args = convex_native_core::testing::args! {
        "owner" => "alice".to_string(),
        "text"  => "ship".to_string(),
    };
    let out = runner
        .run_action_with_callbacks(
            "chain_create_from_action",
            TableNamespace::Global,
            args,
            callbacks,
        )
        .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "stub-id"),
        other => panic!("expected stub-id, got {other:?}"),
    }
    let m_count =
        history.count(|r| matches!(r, CallRecord::Mutation { name, .. } if name == "create_todo"));
    assert_eq!(m_count, 1, "expected one run_mutation callback hit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_db_get_routes_through_read_document_at_snapshot() -> anyhow::Result<()> {
    // ActionCtx::db().get(id) routes through
    // NativeActionCallbacks::read_document_at_snapshot — a
    // distinct callback method from run_query_by_name /
    // run_mutation_by_name. TestCallbacks exposes it via
    // on_doc_read(table, ...). Stub the `todos` table to
    // synthesise a read result and assert the fixture action
    // returns the decoded text.
    use convex_native_core::__private::{
        ConvexObject,
        FieldName,
    };
    // Synthesise a `Todo` object for the stub to return.
    let mut fields: std::collections::BTreeMap<FieldName, ConvexValue> =
        std::collections::BTreeMap::new();
    fields.insert(
        "owner".parse()?,
        ConvexValue::try_from("alice".to_string())?,
    );
    fields.insert(
        "text".parse()?,
        ConvexValue::try_from("stubbed".to_string())?,
    );
    fields.insert("done".parse()?, ConvexValue::Boolean(false));
    fields.insert("created_at".parse()?, ConvexValue::Float64(0.0));
    fields.insert("metadata".parse()?, ConvexValue::Null);
    let obj = ConvexObject::try_from(fields)?;

    let (callbacks, _history) = TestCallbacks::new()
        .on_doc_read("todos", move |_id| Ok(Some(obj.clone())))
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    // The id payload is irrelevant — the on_doc_read closure
    // ignores it — but it must be a valid DeveloperDocumentId
    // string so arg decoding succeeds. `MIN` is a good pick: a
    // stable test-only sentinel that parses through FromStr.
    let id_str = value::DeveloperDocumentId::MIN.encode();
    let out = runner
        .run_action_with_callbacks(
            "read_todo_from_action",
            TableNamespace::Global,
            convex_native_core::testing::args! { "id" => id_str },
            callbacks,
        )
        .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "stubbed"),
        other => panic!("expected stubbed text, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_db_get_many_issues_one_read_per_id() -> anyhow::Result<()> {
    // ActionCtx::db().get_many(ids) fans out to one
    // read_document_at_snapshot callback per id, preserving
    // input order. Distinct shape from .get(id) (single call);
    // a regression that mis-counted iterations would slip past
    // the single-doc test.
    use convex_native_core::__private::{
        ConvexObject,
        FieldName,
    };
    use value::ConvexArray;
    let mut fields: std::collections::BTreeMap<FieldName, ConvexValue> =
        std::collections::BTreeMap::new();
    fields.insert(
        "owner".parse()?,
        ConvexValue::try_from("alice".to_string())?,
    );
    fields.insert("text".parse()?, ConvexValue::try_from("row".to_string())?);
    fields.insert("done".parse()?, ConvexValue::Boolean(false));
    fields.insert("created_at".parse()?, ConvexValue::Float64(0.0));
    fields.insert("metadata".parse()?, ConvexValue::Null);
    let obj = ConvexObject::try_from(fields)?;
    let (callbacks, _history) = TestCallbacks::new()
        .on_doc_read("todos", move |_id| Ok(Some(obj.clone())))
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    // Three valid DeveloperDocumentId strings; the stub returns
    // Some for every id. Expected hit count = 3.
    let id_str = value::DeveloperDocumentId::MIN.encode();
    let id_vals = vec![
        ConvexValue::try_from(id_str.clone())?,
        ConvexValue::try_from(id_str.clone())?,
        ConvexValue::try_from(id_str)?,
    ];
    let args_obj = {
        let mut m: std::collections::BTreeMap<FieldName, ConvexValue> =
            std::collections::BTreeMap::new();
        m.insert(
            "ids".parse()?,
            ConvexValue::Array(ConvexArray::try_from(id_vals)?),
        );
        ConvexObject::try_from(m)?
    };
    let out = runner
        .run_action_with_callbacks(
            "get_many_from_action",
            TableNamespace::Global,
            args_obj,
            callbacks,
        )
        .await?;
    match out {
        ConvexValue::Int64(n) => assert_eq!(n, 3),
        other => panic!("expected 3, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_db_try_get_errors_loudly_on_missing_document() -> anyhow::Result<()> {
    // ActionCtx::db().try_get(id) is .get(id)?.ok_or_else(...);
    // a separate error-shaped surface from .get. With the stub
    // resolving every read to None, try_get must error rather
    // than silently returning None. Pins the "blow up loudly"
    // contract for missing documents in action ctxs.
    let (callbacks, _history) = TestCallbacks::new()
        .on_doc_read("todos", |_id| Ok(None))
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let id_str = value::DeveloperDocumentId::MIN.encode();
    let err = runner
        .run_action_with_callbacks(
            "try_get_from_action",
            TableNamespace::Global,
            convex_native_core::testing::args! { "id" => id_str },
            callbacks,
        )
        .await
        .expect_err("try_get on a missing id must error");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("missing") || msg.contains("no such"),
        "expected missing-document error; got: {msg}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unregistered_sub_query_bails_loudly() -> anyhow::Result<()> {
    // No `on_query` registered, so `ctx.run_query(CountPending,
    // ...)` should produce a clear error that names the missing
    // handler — regression guard against silently returning null.
    let (callbacks, _history) = TestCallbacks::new().build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let args = convex_native_core::testing::args! { "owner" => "x".to_string() };
    let err = runner
        .run_action_with_callbacks("summarise", TableNamespace::Global, args, callbacks)
        .await
        .expect_err("sub-query should fail without a handler stub");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("count_pending"),
        "error should name the missing sub-query handler; got: {msg}",
    );
    Ok(())
}
