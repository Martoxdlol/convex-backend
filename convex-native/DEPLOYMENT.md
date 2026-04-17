# Deployment

How to deploy a binary built against `convex_native` /
`convex_native_distributed`. The framework supports three
topologies — pick the one that matches your scale and
operational model.

> **Scope.** This covers the deployer-facing surface:
> `CONVEX_MODE`, worker / conductor wiring, rolling updates,
> observability hooks, graceful shutdown. For the *framework*
> internals (composite runner, gRPC proto, etc.) read
> `COMPOSITE_RUNNER.md` and `native-rust-functions.md` §10.

Cross-links: `QUICKSTART.md` (hello-world), `USAGE.md` (full
developer surface), `STATUS.md` (shipped-vs-outstanding),
`MIGRATION.md` (JS → Rust cheatsheet), `examples/README.md`
(reading samples + runnable in-tree examples).

---

## 1. Topologies

### A. Standalone (development / small apps)

One process: `convex-local-backend` with the composite runner
linked in. HTTP sync + function dispatch + Database all live
in the same binary.

```sh
# No env vars needed — standalone is the default.
convex-local-backend \
  --port 3210 \
  --instance-name mydeploy \
  --instance-secret $SECRET \
  --db mydeploy.sqlite
```

Use when:
- Development or CI.
- Single-machine deployments with <100 QPS.
- You don't want to operate a multi-process topology.

Limitation: the binary requires the full build chain
(including V8 / `isolate`, which needs `rush install` in
`npm-packages/` once per checkout). The function runner inside
is the **composite** — native queries / mutations / actions
dispatch through the native registry, everything else falls
through to the JS runtime.

### B. Worker-in-one-binary (production, hybrid deploy)

Same binary as Standalone, but with `CONVEX_MODE=worker`:
alongside the HTTP server it also spawns a tonic
`FunctionExecutionService` on `CONVEX_WORKER_BIND_ADDR`. A
remote conductor (Topology C) can dispatch native function
calls to this process over gRPC.

```sh
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
  convex-local-backend \
  --port 3210 \
  --instance-name mydeploy \
  --instance-secret $SECRET \
  --db mydeploy.sqlite
```

Graceful shutdown: the worker's gRPC drain is tied to the HTTP
server's `zombify_rx` — Ctrl-C / `POST /preempt` stops both
together.

### C. Pure worker + dedicated conductor (production, horizontal)

Workers run a lean binary (the deployer's own build) exposing
only the `FunctionExecutionService`. A dedicated conductor
process pools the workers behind
`DistributedFunctionRunner` and dispatches each call via
Power-of-2-Choices. This is the shape `native-rust-functions.md`
§10 describes.

```sh
# On each worker node:
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
  ./my-deployer-binary

# Conductor:
CONVEX_MODE=conductor \
  CONVEX_WORKER_ENDPOINTS="http://worker-a:4567,http://worker-b:4567" \
  ./my-conductor-binary
```

See `crates/convex_native_distributed/examples/conductor.rs`
for a minimal conductor template a deployer can crib from.
`convex-local-backend` does **not** accept
`CONVEX_MODE=conductor` — a conductor-only role doesn't fit a
binary that always boots a local `Database<RT>`. Build a
standalone conductor binary for this topology.

---

## 2. Environment variables

| Var | Consumed by | Default | Effect |
|-----|-------------|---------|--------|
| `CONVEX_MODE` | all | `standalone` | `standalone` / `worker` / `conductor` (conductor only on custom binaries). |
| `CONVEX_WORKER_BIND_ADDR` | worker | `0.0.0.0:4567` | Socket the gRPC `FunctionExecutionService` listens on. |
| `CONVEX_WORKER_ENDPOINTS` | conductor | none | Comma-separated `http://host:port` endpoints for the worker pool. Conductors refuse to start on an empty list. |

Parsers live in `convex_native_distributed::mode::{read_mode_from_env,
read_worker_bind_addr_from_env, read_worker_endpoints_from_env}` — the
custom binary calls them.

---

## 3. Rolling updates

During a deploy you typically have two versions of the same
binary running side-by-side. `convex_native::VERSION` is the
default `registry_version` the worker reports in its `Health`
response. The conductor gates on it:

```rust
let runner = DistributedFunctionRunner::new(workers)?
    .with_min_registry_version("1.2.0");
```

Workers below the floor reject dispatch with
`Code::FailedPrecondition` so the conductor can route around
half-deployed nodes. Per-call `ExecuteRequest::min_registry_version`
overrides the conductor-level floor when necessary.

Recommended sequence:
1. Start new workers (new version) alongside old — the
   conductor sees both in the pool but the version floor
   excludes the old ones from new traffic.
2. Wait for in-flight requests on old workers to drain —
   `Health::in_flight` shows the count.
3. Send `SIGTERM` to each old worker. The drain semantics on
   `NativeFunctionRunner::begin_drain()` / `await_drain(timeout)`
   stop accepting new invocations and let outstanding ones
   finish (Phase 4.3).
4. Remove old endpoints from `CONVEX_WORKER_ENDPOINTS` on
   conductor restart.

---

## 4. Observability

Three pluggable sinks that deployers wire into their metrics
stack:

### Worker-side: `NativeMetricsSink`

Per-function latency + outcome. Installed on the
`NativeFunctionRunner` at construction. Default
`NoopMetrics`; in-memory `CountingMetrics` for tests.

```rust
let runner = NativeFunctionRunner::from_inventory()?
    .with_metrics(Arc::new(MyPrometheusSink));
```

Drives `native_function_execution_seconds{fn_name}` +
`native_function_errors_total{fn_name}` (design §12.3).

### Conductor-side: `ConductorMetricsSink`

Per-dispatch worker label + `ConductorOutcome::{Ok, Retried,
Error(tonic::Code)}` + total latency. Installed on the
`DistributedFunctionRunner`. Default `NoopConductorMetrics`;
in-memory `CountingConductorMetrics` for tests.

```rust
let runner = DistributedFunctionRunner::new(workers)?
    .with_metrics(Arc::new(MyPrometheusSink));
```

Drives `native_funrun_requests_total{worker, type}` +
`native_funrun_request_duration_seconds` +
`native_funrun_errors_total{type, retryable}`.

`DistributedFunctionRunner::in_flight_per_worker()` exposes the
`native_funrun_in_flight_per_worker` gauge snapshot for
dashboards.

### Conductor-side: `ConductorLogSink`

Forwards the worker's `ExecuteResponse::log_lines` into the
conductor's log stack. Default `NoopConductorLogs`; in-memory
`CapturingConductorLogs` for tests. Only called when a
response actually carries log lines.

```rust
let runner = DistributedFunctionRunner::new(workers)?
    .with_log_sink(Arc::new(MySyslogSink));
```

### Distributed tracing

Every runner entry point carries a `#[fastrace::trace]` span.
`ExecuteRequest.execution_context` propagates the request-id /
execution-id chain across the gRPC boundary so worker-side
spans correlate with the originating conductor request.

---

## 5. Graceful shutdown

Worker: the tonic server is built with a `shutdown_future`
(`serve_worker_with_shutdown`). `convex-local-backend` wires
that to the same `zombify_rx` the HTTP server drains on:

- Ctrl-C
- `POST /preempt` on the backend
- SIGTERM (when the deployer's process supervisor sends one)

The sequence inside a worker:
1. Shutdown signal arrives → `zombify_rx` fires.
2. `NativeFunctionRunner::begin_drain()` flips — new invocations
   get `"runner is draining"` errors.
3. In-flight invocations complete (or hit their per-function
   timeout); drain state is visible via `in_flight()`.
4. Tonic server finishes its serve future and exits.

Health reports `accepts_traffic = false` during drain so the
conductor stops routing to the worker.

---

## 6. Introspection

Every binary built on top of `convex_native` has introspection
baked in:

```rust
let built = ConvexBackend::new()
    .with_native_functions()
    .with_native_schema()
    .with_http_routes()
    .with_crons()
    .build()?;

println!("{}", built.describe_pretty());
```

The output is a stable JSON envelope (`version: 1`) covering:
- `schema` — tables + indexes + `document_type` validators.
- `functions` — name, kind, args, `internal`, `timeout_ms`.
- `http_routes` — method + path + handler name.
- `crons` — name + schedule + target + target kind.

Tooling (code generation, deployment gates, CI checks) consumes
this envelope without booting the full backend. See
`convex_native::introspect::{describe_json, describe_pretty,
describe_json_full, describe_pretty_full}`.

---

## 7. Container / Kubernetes patterns

- **Workers**: one per pod, horizontal via a `Deployment` +
  `Service` targeting `CONVEX_WORKER_BIND_ADDR`. Liveness:
  `Health.accepts_traffic`. Readiness: the same, AND the pod's
  registry_version matches the floor the conductor expects.
- **Conductor**: single replica (or an HA pair); reads
  `CONVEX_WORKER_ENDPOINTS` from a configmap refreshed by a
  sidecar watching the worker `Service`'s endpoints, or
  re-resolves DNS periodically.
- **Rolling deploy**: pin `min_registry_version` on the
  conductor before starting the new worker rollout; old workers
  reject new traffic but keep serving in-flight calls.
- **Limits**: set a `terminationGracePeriodSeconds` longer than
  the longest per-function timeout so the drain has time to
  complete.

---

## 8. Minimum-viable worker binary

```rust
use std::sync::Arc;

use convex_native::{
    distributed::ConvexMode,
    NativeFunctionRunner,
};
use convex_native_distributed::{
    build_worker_server,
    read_mode_from_env,
    read_worker_bind_addr_from_env,
};

// Include your `#[convex::query/mutation/action]` modules so
// their inventory registrations are linked into this binary.
mod my_app;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = read_mode_from_env();
    anyhow::ensure!(
        matches!(mode, ConvexMode::Worker | ConvexMode::Standalone),
        "expected CONVEX_MODE=worker, got {mode:?}"
    );

    let addr = read_worker_bind_addr_from_env()?;
    let native = Arc::new(NativeFunctionRunner::from_inventory()?);
    eprintln!(
        "worker: listening on {addr}, {} fn(s) registered",
        native.len(),
    );
    let (mut builder, service) = build_worker_server(native);
    builder.add_service(service).serve(addr).await?;
    Ok(())
}
```

See `examples/minimal_app/` for the same shape extended with
schema, mutations, an HTTP action, and a cron. See
`crates/convex_native_distributed/examples/{worker,conductor}.rs`
for the runnable in-tree references.
