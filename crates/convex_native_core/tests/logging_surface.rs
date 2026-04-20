//! Tests `ctx.log()` captures lines into the shared buffer.

use std::sync::Arc;

use convex_native_core::{
    convex,
    ActionCtx,
    ConvexDocument,
    LogBuffer,
    LogLevel,
    NativeActionCallbacks,
    NoopCallbacks,
    Rt,
    ToConvex,
};
use value::{
    ConvexValue,
    TableNamespace,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "log_units")]
pub struct LogUnit {
    pub name: String,
}

#[convex::action]
pub async fn log_demo(ctx: &mut ActionCtx<'_, Rt>, who: String) -> anyhow::Result<String> {
    ctx.log().info(format!("hello {who}"));
    ctx.log().warn("careful");
    Ok("ok".into())
}

#[tokio::test]
async fn action_captures_log_lines_into_ctx_buffer() {
    use convex_native_core::NativeFunctionRunner;

    // We need visibility into the log buffer, so construct the action
    // context manually with an external buffer rather than routing
    // through `run_action_with_callbacks`.
    let _runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    let buffer = LogBuffer::new();

    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    // Build a hand-rolled ActionCtx with an external log buffer so we
    // can inspect what the handler emitted. The runner's dispatch
    // path uses the same constructor to surface log lines through
    // the standard log-streaming mechanism.
    let mut ctx = ActionCtx::<Rt>::with_callbacks_and_log_buffer(
        None,
        callbacks,
        TableNamespace::Global,
        buffer.clone(),
    );

    let _ret = log_demo(&mut ctx, "world".into()).await.unwrap();

    let lines = buffer.snapshot();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].level, LogLevel::Info);
    assert_eq!(lines[0].message, "hello world");
    assert_eq!(lines[1].level, LogLevel::Warn);
    assert_eq!(lines[1].message, "careful");
}

#[test]
fn log_buffer_is_cheap_to_clone_and_shared() {
    let a = LogBuffer::new();
    let b = a.clone();
    use convex_native_core::Logger;
    Logger::new(&a).info("hi");
    // Both handles see the same entries.
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    b.clear();
    assert_eq!(a.len(), 0);
}

// Silence "item never used" for types re-exported only to verify the
// prelude surface.
#[allow(dead_code)]
fn _touch_types() {
    let _: ConvexValue = ConvexValue::Null;
    let _: fn(Vec<u8>) -> anyhow::Result<ConvexValue> = |b| b.to_convex();
}
