//! End-to-end conductor example that dispatches a real action
//! against the worker pool.
//!
//! Pair this with the `worker_with_functions` example to see the
//! full round-trip:
//!
//! ```sh
//! # Terminal 1 — worker.
//! cargo build --examples -p convex_native_distributed
//! CONVEX_MODE=worker CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
//!   ./target/debug/examples/worker_with_functions
//!
//! # Terminal 2 — dispatch a `touch` action through the conductor.
//! CONVEX_MODE=conductor CONVEX_WORKER_ENDPOINTS=http://127.0.0.1:4567 \
//!   ./target/debug/examples/conductor_dispatch alice
//! ```
//!
//! The binary dispatches one `touch(owner)` action per positional
//! arg and prints each response. Unlike `conductor.rs` (which only
//! probes health), this example covers the dispatch path: proto
//! encode → P2C routing → tonic transport → worker decode →
//! handler → reverse. Regressions in any layer show up here.

use std::sync::Arc;

use common::types::UdfType;
use convex_native::{
    distributed::{
        ConvexMode,
        ExecuteRequest,
    },
    FromConvex,
    ToConvex,
};
use convex_native_distributed::{
    read_mode_from_env,
    read_worker_endpoints_from_env,
    CapturingConductorLogs,
    DistributedFunctionRunner,
    TonicWorkerClient,
    WorkerClient,
};
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = read_mode_from_env();
    anyhow::ensure!(
        matches!(mode, ConvexMode::Conductor | ConvexMode::Standalone),
        "expected CONVEX_MODE=conductor, got {mode:?}"
    );

    let endpoints = read_worker_endpoints_from_env()?;
    anyhow::ensure!(!endpoints.is_empty(), "CONVEX_WORKER_ENDPOINTS is empty");

    // Build a worker client per endpoint. `TonicWorkerClient::connect`
    // does a full connect before returning, so an unreachable worker
    // fails fast here rather than on first dispatch.
    let mut workers: Vec<Arc<dyn WorkerClient>> = Vec::new();
    for ep in &endpoints {
        workers.push(TonicWorkerClient::connect(ep.clone()).await?);
    }

    // Capturing log sink so we can show worker-side `ctx.log()`
    // output on the conductor stdout. A production deployer would
    // wire this into their log shipping stack instead.
    let logs = Arc::new(CapturingConductorLogs::new());
    let runner = DistributedFunctionRunner::new(workers)?
        .with_log_sink(logs.clone());

    // Each positional CLI arg becomes one owner to `touch`.
    let owners: Vec<String> = std::env::args().skip(1).collect();
    let owners = if owners.is_empty() {
        vec!["alice".to_string()]
    } else {
        owners
    };

    for owner in &owners {
        // Encode the action's typed Args into a ConvexObject the
        // wire path expects. In a deployer's own binary this is
        // usually done via `ctx.run_action(Marker, Args { .. })`
        // — here we're outside an action context so we build the
        // shape by hand.
        let args_obj = touch_args_object(owner.clone())?;
        let resp = runner
            .execute(
                ExecuteRequest {
                    name: "touch".to_string(),
                    namespace: TableNamespace::Global,
                    args: args_obj,
                    timeout: None,
                    min_registry_version: None,
                    execution_context: None,
                },
                UdfType::Action,
            )
            .await?;
        match resp.result {
            Ok(v) => {
                let count: i64 = i64::from_convex(v)?;
                println!("touch({owner}) => {count} widget(s)");
            },
            Err(msg) => {
                eprintln!("touch({owner}) => handler error: {msg}");
            },
        }
    }

    // Surface any worker-side log lines the conductor captured.
    let captured = logs.snapshot();
    if !captured.is_empty() {
        eprintln!("── worker log output ──");
        for (worker, kind, lines) in captured {
            for line in lines {
                eprintln!("[{worker} {kind:?}] {line}");
            }
        }
    }
    Ok(())
}

/// Build the `ConvexObject` the `touch` action expects — shaped
/// as `{ owner: "<string>" }`. Deployer-side code normally goes
/// through the typed-marker path `ctx.run_action(Marker, Args)`;
/// this helper exists because we're dispatching from outside any
/// ctx here.
fn touch_args_object(owner: String) -> anyhow::Result<ConvexObject> {
    let mut map: std::collections::BTreeMap<value::FieldName, ConvexValue> =
        std::collections::BTreeMap::new();
    map.insert("owner".parse()?, ConvexValue::try_from(owner)?);
    Ok(ConvexObject::try_from(map)?)
}

// `FromConvex` / `ToConvex` brought in just so the turbofish at the
// response-decoding line has them in scope.
#[allow(dead_code)]
fn _use_conversion_traits() {
    let _: fn(ConvexValue) -> anyhow::Result<i64> = i64::from_convex;
    let _: fn(i64) -> anyhow::Result<ConvexValue> = <i64 as ToConvex>::to_convex;
}
