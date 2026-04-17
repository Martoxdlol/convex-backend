# Status

**As of this writing the project is in a planning reset.** The
previous plan (see `IMPLEMENTATION_PLAN.md`, now marked
superseded) drove the framework far enough to demonstrate every
ctx / schema / derive feature in isolation, but the distributed
topology it delivered is not correct against the
"backend-coordinates / workers-execute" architecture the project
actually needs. Code that conflicted with that target has been
removed. `DISTRIBUTED_PLAN.md` is the current source of truth.

This file tracks what survived, what was removed, and what the
active phase is.

---

## One-line summary

Framework-level pieces (derives, ctx surface, schema reflection,
registry, introspection) are solid and reused. The distributed
dispatch layer is rebuilt per `DISTRIBUTED_PLAN.md` — **Phases
1, 2, 3, and 4 have shipped** (except substeps 2.8b and 4.6b's
live-database assertions, which are blocked on in-repo DB test
fixtures). Workers auto-register against the backend's
`WorkerAdmissionService` and the backend dispatches native
Query/Mutation/Action through a dynamic, churn-tolerant
`WorkerPool`; action sub-calls route back through the
`BackendCallbackService` so OCC + subscription invalidation
stay intact. **Phase 5 (prebuilt `getconvex/convex-backend`
container image)** is the next major step.

---

## Authoritative docs

| File | Purpose |
|------|---------|
| `DISTRIBUTED_PLAN.md` | **The new plan.** Target architecture, phase breakdown, client-routing decision. |
| `USAGE.md` | Per-feature developer reference for the ctx / derive / runtime surface. Unchanged by the replan. |
| `MIGRATION.md` | JS ↔ Rust cheatsheet. Still valid. |
| `STANDALONE.md` | Monolith alternative (`local_backend` as a library). Works today but is the legacy shape — deprioritized relative to the new distributed target. |
| `COMPOSITE_RUNNER.md` | Describes `CompositeFunctionRunner` — the monolith dispatch path inside `local_backend`. Stays for the monolith topology; not used by the new distributed target. |
| `DEPLOYMENT.md` | Operational guide. Rewritten to point at `DISTRIBUTED_PLAN.md` for the real architecture. |
| `IMPLEMENTATION_PLAN.md` | **Superseded.** Historical phased plan that led to the now-removed code. |
| `native-rust-functions.md` | Original design doc. Sections 1–9 still describe the framework surface correctly; sections 10–13 (distributed execution, operations) are superseded by `DISTRIBUTED_PLAN.md`. |

---

## What survived the reset

### Framework crate: `crates/convex_native/`

Unchanged and load-bearing. Every phase of the new plan depends
on it.

- `#[derive(ConvexDocument)]`, `#[derive(ConvexEnum)]`,
  `#[derive(ConvexNested)]`, `#[derive(ConvexUnion)]`.
- `#[convex::query/mutation/action]`, `#[convex::http_action]`,
  `#[convex::cron]`.
- `ConvexSchema` trait + validator reflection (emitted by the
  derives as part of `TableDefinition::document_type`).
- `QueryCtx` / `MutationCtx` / `ActionCtx` / `HttpActionCtx` with
  the full surface: `.db()`, `.auth()`, `.log()`, `.rng_u64()`,
  `.execution_context()`, `.scheduler()`, `.storage()`, etc.
- `NativeFunctionRunner` — dispatches by name, runs handlers
  against a `Transaction<Rt>`. Will be used **inside the worker**
  under the new plan.
- `NativeActionCallbacks` trait. The `WorkerActionCallbacks`
  impl was removed (its "commit locally" semantics conflict with
  the new architecture); `NoopCallbacks` stays.
- Introspection: `describe_json` / `describe_pretty`. Used by
  the worker to report its inventory to the backend at
  registration time (Phase 3).
- Observability primitives: `NativeMetricsSink`, `LogBuffer`,
  drain / timeout / circuit-breaker plumbing on the runner.

### Backend adapter: `crates/convex_native_backend/`

Unchanged. Used only by the monolith topology
(`local_backend` + native functions linked in). `CompositeFunctionRunner`
implements `FunctionRunner<RT>` for the monolith case. The new
distributed plan adds a *second* implementation
(`DistributedFunctionRunner`) — both will coexist.

### Distributed crate: `crates/convex_native_distributed/`

Kept, pre-Phase-1:

- `conversions.rs` — proto ↔ native boundary.
- `client::DistributedFunctionRunner` — the P2C + failover
  dispatcher. Will gain `impl FunctionRunner<RT>` in Phase 2.
- `server::FunctionExecutionServer` — the worker-side tonic
  service. Currently commits locally; Phase 1 will change this
  to return `FunctionFinalTransaction`.
- `tonic_client::TonicWorkerClient` — real gRPC transport.
- `mode.rs` — env-var parsers + helper fns. The
  `CONVEX_MODE=conductor` path disappears in Phase 3 (backend
  image replaces the standalone-conductor concept). `worker`
  path stays.

### Tests

- `crates/convex_native/tests/*` — 18 integration-test files,
  all relevant.
- `crates/convex_native_distributed/tests/multi_worker.rs` —
  P2C routing tests. Still correct for the backend→worker
  dispatch direction.

---

## What was removed

| File | Reason |
|------|--------|
| `crates/convex_native_distributed/src/worker_callbacks.rs` | `WorkerActionCallbacks` committed sub-call mutations on the worker's own Database. Wrong under the new plan — sub-calls must route back to the backend's Committer (Phase 4). |
| `crates/convex_native_distributed/examples/{conductor,conductor_dispatch}.rs` | Standalone "conductor" concept doesn't exist under the new plan. Backend (published as a container image, Phase 5) replaces it. |
| `crates/convex_native_distributed/examples/worker_with_functions.rs` | Demonstrated the worker-commits-end-to-end path that's being removed. |
| `crates/convex_native_distributed/tests/{examples_smoke,client_e2e_smoke}.rs` | Exercised the removed standalone behaviour. Useful again in a different form after Phase 2, but the current assertions pin wrong behaviour. |
| `convex-native/examples/minimal_app/` | Reading sample for the old library-mode shape. Superseded by Phase 5 (deployers pull the published backend image + ship a worker binary; no library linking). |
| `convex-native/examples/full_app/` | Workspace-member example built around the worker-commits topology. Same reason. |
| `convex-native/examples/deploy/docker/Dockerfile.conductor` | Conductor concept gone. |
| `convex-native/examples/deploy/docker/docker-compose.yml` | Wired conductor + workers. Will be re-added in Phase 5 wired to the new architecture (published backend image + worker). |
| `convex-native/examples/deploy/kubernetes/conductor-deployment.yaml` | Same. |
| `convex-native/examples/deploy/systemd/convex-conductor.service` | Same. |

Dockerfile.worker, worker-deployment.yaml, convex-worker.service are kept — their shape is approximately right for the new architecture. They'll need small env-var adjustments in Phase 3 (worker gets `CONVEX_BACKEND_ENDPOINT`, stops getting `CONVEX_WORKER_ENDPOINTS`).

---

## Active phase

**Phase 1 — wire contract.** Shipped. Phase 2 (`FunctionRunner`
trait impl + backend-side dispatch) is next.

### Phase 1 deliverables (all ✓ shipped)

1. ✓ Extend `pb::function_execution::ExecuteRequest` with
   `begin_timestamp` + `existing_writes`.
2. ✓ Extend `pb::function_execution::ExecuteResponse` with
   `final_tx: Option<DistributedFinalTx>`.
3. ✓ Encoding picked: dedicated proto sub-messages
   (`DistributedFinalTx`, `ExistingWrites`). Chose this over
   postcard-through-`bytes` because Phase 6 (JS interop) needs
   the wire shape to be readable by a non-Rust client, and the
   sub-messages can grow incrementally in Phase 2 without a
   breaking change to the field number.
4. ✓ `FunctionExecutionServer::run_query_inline` /
   `run_mutation_inline` stop committing; they return a
   `FinalTxSummary` on the native response, which
   `conversions::to_proto_response` encodes as
   `DistributedFinalTx`.
5. ✓ `DistributedFunctionRunner::execute` returns the
   `ExecuteResponse` whose `final_tx` field carries the summary
   back to the dispatcher.
6. ✓ Round-trip tests:
   - `conversions::tests::response_final_tx_roundtrips_through_proto`
     — native → proto → native with scalar assertions.
   - `conversions::tests::response_without_final_tx_keeps_field_none`
     — actions + handler errors leave the field absent on both
     sides.

### Phase 1 scope boundaries

- `FinalTxSummary` carries scalars only today (`begin_timestamp`,
  `writes_count`, `reads_count`). Phase 2 grows both the
  `DistributedFinalTx` proto message and the
  `FinalTxSummary` native struct to carry the full
  `FunctionReads` + `FunctionWrites` content the backend's
  Committer consumes. The field numbers already reserved in the
  proto make that additive.
- The worker's `Database<Rt>` is still *optional* on
  `FunctionExecutionServer` (`.with_database(...)`). Phase 3 will
  require it once the admission service knows every worker must
  be able to open a read transaction.
- `existing_writes` on the request is a placeholder counter. The
  dispatcher never populates it today because the in-process path
  in `local_backend` doesn't batch yet. Phase 2 wires the real
  `FunctionWrites` content through.

---

## Phase 2 — active, decomposed into concrete substeps

`DISTRIBUTED_PLAN.md` §15 Phase 2 bundles three outcomes:
`impl FunctionRunner<RT>`, env-var switchover in `local_backend`,
and an integration test. To make incremental progress tractable,
the work is split here into numbered substeps. Each is meant to
ship as its own commit; the overall phase is done when every
substep is checked and the integration test is green.

2.1. ✓ **Wire `DistributedFinalTx` to carry `rows_read_by_tablet`.**
     Landed. `map<string, uint64> rows_read_by_tablet` on
     `DistributedFinalTx`; TabletId encodes as its `Display`
     form (UUID-shaped string) to keep the wire shape
     JSON-readable for Phase 6 consumers.

2.2. **Wire `FunctionReads` content.** Split into two
     sub-substeps:

     2.2a. ✓ **Scalar counters (`user_tx_size` / `system_tx_size`).**
     Landed. New proto message `DistributedTxReadSize`
     (`{ total_document_size, total_document_count }`) and two
     optional fields on `DistributedFinalTx`. Native
     `FinalTxSummary` carries `Option<TxReadSize>` for each.
     `summarise_tx` pulls them from the `TransactionReadSet`
     before moving the interval-set out. Enables
     distributed-dispatch usage tracking; does not yet enable
     OCC validation.

     2.2b. ✓ **`ReadSet` intervals per (tablet, index).**
     Landed. New proto message `DistributedIndexReads`
     (`{ tablet_id, index_descriptor, fields, intervals }`)
     and `repeated` field on `DistributedFinalTx`. Native
     `IndexReadsSummary` carries `TabletIndexName`,
     `IndexedFields`, and `IntervalSet` directly (no lossy
     projection). `summarise_tx` drains the tx's indexed
     ReadSet into the vec. Search-index reads are left as a
     follow-up substep. `FinalTxSummary` lost its `PartialEq`
     / `Eq` derive because `IntervalSet` doesn't implement
     equality; test comparisons now check the relevant fields
     individually. After substep 2.2b the Committer has
     everything it needs for OCC validation on a
     distributed-dispatched mutation.

2.3. ✓ **Wire `FunctionWrites` content.**
     Landed. `repeated common.DocumentUpdateWithPrevTs writes`
     on `DistributedFinalTx` reuses the existing
     `pb::common::DocumentUpdateWithPrevTs` type + conversions
     (`common::document::DocumentUpdateWithPrevTs ↔
     pb::common::DocumentUpdateWithPrevTs`). `FinalTxSummary`
     carries the native `Vec`. `server::summarise_tx` drains
     the flat write-set coalesced updates into the vec.
     `conversions::{to,from}_proto_response` round-trip it.
     `to_proto_response` / `from_proto_response` are now
     fallible (`anyhow::Result`) because the underlying
     document conversion is; server callers map failures to
     `Status::internal`.

2.4. ✓ **Wire `ExistingWrites` content.**
     Landed. `ExistingWrites` proto message replaced its Phase-1
     `count` placeholder with
     `repeated common.DocumentUpdateWithPrevTs updates` — same
     encoding as `DistributedFinalTx.writes` so both sides share
     one document-update wire shape.
     `convex_native::distributed::ExecuteRequest` grew
     `begin_timestamp: Option<u64>` and
     `existing_writes: Vec<DocumentUpdateWithPrevTs>`.
     `run_{query,mutation}_inline` call `tx.merge_writes(...)`
     on the staged updates before dispatching the handler.

2.5. ✓ **Drain the transaction into the full
     `FunctionFinalTransaction` shape.**
     Landed. `run_udf_inline` is the shared query/mutation
     dispatch body; summary fires on every completion path where
     the tx was successfully opened (handler success, handler
     error, or handler-produces-no-writes). Matches
     `CompositeFunctionRunner::dispatch_native_inner`'s
     in-process behaviour so OCC + subscription semantics stay
     identical across the two dispatch paths.

2.6. **`impl FunctionRunner<ProdRuntime> for DistributedFunctionRunner`.**
     The trait has eight methods; only `run_function` is
     dispatched over gRPC. The JS-heavy methods (`analyze`,
     `evaluate_*`, `set_action_callbacks`) return
     `unimplemented` — the composite runner in
     `convex_native_backend` delegates those to the in-process
     JS runner. After this substep the distributed runner is a
     drop-in for a native-only backend. Split into two
     sub-substeps:

     2.6a. ✓ **`FinalTxSummary → FunctionFinalTransaction` conversion.**
     Landed. `conversions::final_tx_summary_to_function_tx`
     builds the backend-consumable
     `function_runner::FunctionFinalTransaction` from a worker
     response: `Timestamp::try_from` on `begin_timestamp`,
     `TabletId::from_str` on `rows_read_by_tablet` keys, and
     reconstruction of `ReadSet` from the per-index entries
     with an empty search map (substep 2.2b TODO on search
     reads carries through).
     Added deps: `function_runner`, `udf`, `errors`, `tokio`.

     2.6b. ✓ **FunctionRunner trait impl.** Landed. New module
     `function_runner_impl` at
     `crates/convex_native_distributed/src/function_runner_impl.rs`
     implements `FunctionRunner<ProdRuntime>` on
     `DistributedFunctionRunner`:

     - `run_function` for `UdfType::Query` /
       `UdfType::Mutation` builds the native `ExecuteRequest`
       from `function_metadata + identity + ts +
       existing_writes + context`, dispatches via the pool's
       `execute()`, then assembles
       `(Option<FunctionFinalTransaction>, FunctionOutcome,
       FunctionUsageStats)` via `final_tx_summary_to_function_tx`
       + a minimal `UdfOutcome` builder. Observed flags
       (`observed_identity` / `observed_rng` / `observed_time`)
       default to false and `rng_seed` defaults to zeros — the
       wire protocol doesn't carry those yet; a follow-up
       substep extends the proto if the backend needs them for
       non-cached subscription-reuse semantics.
     - `UdfType::Action` returns a clear error pointing at
       Phase 4's `BackendCallbackService`.
     - `UdfType::HttpAction` returns a clear error pointing at
       the `HttpRouter` dispatch path (substep 2.7 / Phase 3).
     - `analyze` / `evaluate_*` return descriptive errors so
       the operator knows to wrap in a composite runner or
       ship a JS-only backend.
     - `set_action_callbacks` is a no-op (distributed actions
       route sub-calls back to the backend via Phase 4's
       `BackendCallbackService`, not via these local callbacks).

     Dep add: `runtime` (for `ProdRuntime`).

     Tests: `function_runner_impl::evaluate_schema_returns_clear_error`
     pins the "guidance error" contract on JS-only methods.

2.7. ✓ **`local_backend` env-var switchover.**
     Landed. `CONVEX_NATIVE_WORKERS=grpc://host-a:4567,grpc://host-b:4567`
     (comma-separated) swaps the composite runner's native
     Query/Mutation branch from the in-process
     `NativeFunctionRunner` to a `DistributedFunctionRunner`
     built from the listed endpoints. Unset → keep in-process
     behaviour (the Phase-2 default).

     - `convex_native_distributed::read_native_workers_from_env()`
       parses the env var; returns `None` when unset,
       `Some(Vec<String>)` when set, or an error when set but
       unusable.
     - `CompositeFunctionRunner::with_remote_native_pool(pool)`
       takes an `Arc<dyn FunctionRunner<RT>>` and routes native
       Query/Mutation through it instead of
       `dispatch_native_inner`. Native actions still run
       in-process until Phase 4's `BackendCallbackService`.
     - `local_backend::make_app` consults the env var, builds
       the `DistributedFunctionRunner` via
       `build_conductor_runner`, and wires it into the
       composite. Log message records the endpoint count so
       operators see the switchover in the boot log.

     Tests: three `mode::tests::read_native_workers_from_env_*`
     covering unset / parseable / rejected-empty.

2.8. **Integration test: subscription invalidation across the
     wire.** Split into two sub-substeps:

     2.8a. ✓ **Wire-path integration test.** Landed. New test
     file `crates/convex_native_distributed/tests/function_runner_e2e.rs`
     spins up a tonic `FunctionExecutionServer` on an ephemeral
     port, dials it from a `DistributedFunctionRunner`, and
     exercises three dispatch paths:
     - Action with an unknown handler → handler-level error
       surfaces verbatim; `final_tx` stays `None`.
     - Query without `Database<Rt>` attached → transport
       `Unimplemented` error with a message that points the
       operator at `.with_database()`.
     - Phase-2 request fields (`begin_timestamp`,
       `existing_writes`) serialize cleanly so substep-2.1
       / substep-2.4 wire shapes can't regress silently.

     Plus four unit tests in `function_runner_impl::tests` pin
     the error paths on the `FunctionRunner` impl itself
     (Action / HttpAction / missing-metadata / JS-only methods).

     2.8b. **Full SubscriptionManager assertion.** Blocked on
     `Database<Rt>` test fixtures. Starting a real `Database`
     requires persistence + retention + committer scaffolding
     that the open-source repo doesn't expose as test helpers
     today; building them here would dwarf the actual assertion.
     When a `DbFixture::new_in_memory()` helper lands (or when
     the backend-image topology in Phase 5 materialises and the
     integration test runs against a live binary instead of an
     in-process fixture), add the assertion:
     - Register a native mutation that writes to table `T`.
     - Subscribe to a query reading `T`.
     - Dispatch the mutation via
       `DistributedFunctionRunner::run_function`.
     - Assert the subscriber sees an `InvalidationEvent`.

Exit criteria: every substep shipped and the integration test
(2.8) green. After Phase 2, a deployer with a fixed
`CONVEX_NATIVE_WORKERS` list has OCC + subscriptions working
over the distributed path.

## Phase 3 — active, decomposed into concrete substeps

`DISTRIBUTED_PLAN.md` §15 Phase 3 bundles three outcomes:
`WorkerAdmissionService` proto + server + worker client, a
dynamic `WorkerPool`, and backend tolerance for no-workers +
mid-lifetime churn. Same decomposition approach as Phase 2:
each substep ships as its own commit.

3.1. ✓ **`WorkerAdmissionService` proto contract.** Landed. New
     proto file `crates/pb/protos/worker_admission.proto`
     defining the service and every message type:
     `RegistrationEnvelope`, `FunctionInventory`
     (`FunctionRegistration` + `DatabaseSchema` +
     `HttpRouteRegistration` + `CronRegistration`),
     `WorkerStatus`, `DrainNotice`, `RegistryFloorUpdate`,
     `WorkerKind` enum. Generates `pb::worker_admission::*`.

3.2. ✓ **`FunctionInventory` content wiring.** Landed. New
     module `convex_native_distributed::admission` with
     `collect_inventory()` → `(FunctionInventory, [u8; 32])`.
     Walks `NativeFunctionRegistry::collect()`,
     `NativeSchema::collect()` (serialised via the existing
     `DatabaseSchema ↔ DatabaseSchemaJson` JSON form),
     `HttpRouter::collect()`, and `CronRegistry::collect()`
     on the worker side, sorts each list for canonical
     output, and hashes the prost-serialised proto bytes with
     SHA-256. Result is stable across successive calls so
     workers built from identical source always produce the
     same hash. Deps added: `sha2`, `prost` (direct).

     Tests: four unit tests on `admission::tests` — stable
     round-trip, empty registries shape, hash determinism,
     hash changes with content.

3.3. ✓ **`WorkerPool` type.** Landed. New module
     `convex_native_distributed::pool` with a `WorkerPool`
     backed by a single `parking_lot::RwLock` on the inner
     state (workers: HashMap, by_function: HashMap). Picked
     `RwLock + HashMap` over `DashMap` — admission rate is
     tiny (seconds between admits on a real pool) and coarser
     locking keeps the invariants trivial to reason about.
     Upgrade to sharded maps if the pool ever exceeds a few
     hundred members.

     - `WorkerId(u64)` allocated monotonically; worker
       restart yields a fresh id (no in-flight-dispatch
       races against a stale client).
     - `admit` / `retire` are the two shape-changing ops;
       retire is idempotent.
     - `eligible_for(name)` returns every worker serving the
       name, filtered by the pool-wide
       `min_registry_version` floor (reuses the same
       lexicographic-parts comparison as `FunctionExecutionServer`'s
       version gate, so worker-side acceptance and pool-side
       eligibility stay in sync).
     - `by_version()` count-grouping feeds Phase-7 dashboards
       and makes rolling-update progress observable.

     Tests: five unit tests pin admission ordering, function
     routing, retirement idempotency, floor filtering, and
     version grouping.

3.4. ✓ **`WorkerAdmissionServer`.** Landed. New module
     `convex_native_distributed::admission_server` implements
     the `WorkerAdmissionService` tonic trait:

     - First message on the Register stream must be a
       `RegistrationEnvelope`; anything else closes the
       stream with `FailedPrecondition`.
     - On admission, the server dials back to
       `envelope.execute_endpoint` to build a
       `TonicWorkerClient` (dispatch and admission channels are
       deliberately separate so heartbeat traffic doesn't
       head-of-line-block dispatch latency).
     - The worker is admitted to the shared `Arc<WorkerPool>`;
       inbound `WorkerStatus` messages are drained but not yet
       acted on (substep 3.8 will surface them for Phase-7
       dashboards).
     - Closing the inbound stream triggers a retirement task
       that drops the worker from the pool. Retirement is
       idempotent (from substep 3.3).
     - Outbound stream is channel-backed (`ReceiverStream`) so
       substep 3.8 can push `DrainNotice` / `RegistryFloorUpdate`
       messages through it later.

     Deps added: `tokio-stream` (direct — previously only a
     dev-dep).

     Tests: two integration-style unit tests spin up real
     tonic servers — one pins the full admit → pool → retire
     lifecycle (churn-tolerance exit criterion), the other
     pins the "first message must be envelope" contract.

3.5. ✓ **Worker-side registration loop.** Landed. New module
     `convex_native_distributed::admission_client` +
     env-var helper `read_backend_endpoint_from_env()`:

     - `WorkerRegistration::register(backend_endpoint,
       execute_endpoint, registry_version)` dials the backend's
       admission service, sends the envelope built from
       `collect_inventory()` (substep 3.2), and spawns the
       drain-listener task.
     - `push_status(in_flight, cpu_percent)` sends periodic
       `WorkerStatus` heartbeats upstream; returns `Err` when
       the stream is closed so the binary's heartbeat loop can
       exit cleanly.
     - `drain_signaled()` future resolves on `DrainNotice` or
       stream close — the binary `select!`s it into its
       top-level shutdown.
     - `RegistryFloorUpdate` is logged but not acted on — the
       worker keeps serving until the backend either sends a
       drain notice or drops the stream.
     - Dropping the handle cleanly closes the stream; the
       admission server retires the worker via the substep-3.4
       retirement loop.

     Env-var: `CONVEX_BACKEND_ENDPOINT=grpc://backend:5678`
     parsed via `read_backend_endpoint_from_env()`. Unset means
     the worker runs under the Phase-2 fixed-pool shape;
     present means it registers dynamically on startup.

     Tests: one integration test spins up an admission server +
     worker exec server, calls `WorkerRegistration::register`,
     and asserts the worker appears in the pool, heartbeats
     flow, and retirement fires on drop. Three env-var parser
     tests cover unset / trimmed-value / rejected-empty.

3.6. ✓ **`WorkerPool` as `FunctionRunner`.** Landed. New
     module `convex_native_distributed::pool_runner` with
     `PoolFunctionRunner` wrapping an `Arc<WorkerPool>` and
     implementing `FunctionRunner<ProdRuntime>`:

     - Each dispatch asks the pool for `eligible_for(name)` —
       the subset of workers advertising that function, already
       floor-filtered (substep 3.3 semantics).
     - Empty eligible set → `Status::unavailable` with the
       dotted function name and pool size, so substep 3.7 can
       map it to a 503 at the HTTP edge. `run_function`
       surfaces that as `anyhow::Error`.
     - Single eligible worker → direct dispatch, no P2C.
     - ≥2 eligible workers → Power-of-2-Choices + single-retry
       `Unavailable` failover over the eligible set.
     - JS-only trait methods return descriptive errors mirroring
       the substep-2.6b `DistributedFunctionRunner`.
       `set_action_callbacks` is a no-op (Phase-4 territory).

     Refactored `function_runner_impl.rs` to expose a reusable
     `dispatch_query_or_mutation_via(dispatch_closure, ...)`
     helper so both fixed-pool (Phase 2) and dynamic-pool
     (Phase 3) runners share the request-build + outcome-assembly
     path. Cargo deps: adds `semver` (direct — used in the
     shared `PreparedRequest` struct's `udf_server_version`
     field).

     Tests: three unit tests — eligible-worker dispatch, loud
     failure on missing function name (the substep 3.7 exit
     criterion), and P2C failover across two workers.

3.7. ✓ **No-workers 503 + worker-leaves handling.** Landed.
     Substep 3.6's `PoolFunctionRunner::dispatch` already
     surfaces `Status::unavailable` with the dotted function
     name + pool size when the eligible set is empty; this
     substep adds the end-to-end coverage + the env-var hook
     for exposing the admission service.

     - New integration test
       `crates/convex_native_distributed/tests/admission_churn.rs`
       pins four user-visible behaviours: empty-pool →
       `Unavailable` with the pool-size context, mid-lifetime
       admission unblocks dispatch, mid-lifetime stream close
       retires the worker, and restart yields a fresh
       `WorkerId` (no stale-client-reuse risk).
     - New env-var parser `read_admission_bind_addr_from_env()`
       for `CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678`. Returns
       `Some(SocketAddr)` when set, `None` when unset (fall
       back to the Phase-2 fixed-pool shape), `Err` on garbage.
       Wiring this into `local_backend` so it swaps the
       composite runner's native branch to `PoolFunctionRunner`
       lands with substep 3.8's retirement / drain flow; the
       env-var + parser sits ready.

     Tests: four top-level integration tests
     (`admission_churn.rs`) + three env-var parser unit tests
     (`mode::tests::read_admission_bind_addr_*`).

3.8. ✓ **Drain + retire flow + `local_backend` admission wiring.**
     Landed.

     - `WorkerAdmissionServer::request_drain(worker_id, reason)`
       pushes a `DrainNotice` on the worker's outbound stream.
       The worker's `drain_signaled()` future (substep 3.5)
       wakes; dropping the `WorkerRegistration` retires the
       worker. Double-drain is a no-op (returns `Ok(false)`).
     - New public helper
       `admission_server::spawn_admission_server(bind_addr) ->
       Arc<WorkerPool>` so `local_backend` can stand up the
       admission service without pulling `tonic` / `pb` as
       direct dependencies.
     - `local_backend::make_app` now reads
       `CONVEX_ADMISSION_BIND_ADDR`. When set, it calls
       `spawn_admission_server(bind_addr)`, wraps the returned
       pool in `PoolFunctionRunner`, and hands that to the
       composite as the remote-native branch.
     - Env-var precedence: `CONVEX_ADMISSION_BIND_ADDR` (dynamic
       pool, Phase 3) > `CONVEX_NATIVE_WORKERS` (fixed pool,
       Phase 2) > in-process (default). `CONVEX_WORKER_ENDPOINTS`
       stays around for pre-Phase-3 tests.

     Tests: one new integration test
     (`admission_churn::operator_triggered_drain_retires_worker`)
     covers the full backend → worker DrainNotice round-trip +
     retirement + double-drain-no-op contract.

     Phase 3 exit criteria met: workers auto-register on start,
     the backend dispatches through the dynamic pool, operators
     don't need to restart the backend to change the worker set,
     the admission service tolerates churn (join / leave /
     rejoin) without dropping in-flight state. Total delta on
     `convex_native_distributed`: **98 tests green** (83 lib +
     5 churn + 3 e2e + 7 multi-worker).

Exit criteria: workers auto-register on start; the backend
dispatches through the dynamic pool; operators don't need to
restart the backend to change the worker set.

## Phase 4 — active, decomposed into concrete substeps

`DISTRIBUTED_PLAN.md` §15 Phase 4 / §7.4: native actions running
on a remote worker can make sub-calls (`ctx.run_mutation`,
`ctx.scheduler().run_after`, `ctx.storage().store`, …). Under
the Phase-4 topology those calls route back to the backend over
gRPC so every write still flows through the backend's Committer
(otherwise OCC + subscription invalidation break for any write
an action causes indirectly).

4.1. ✓ **`BackendCallbackService` proto contract.** Landed.
     New proto file `crates/pb/protos/backend_callbacks.proto`
     defining the service with 12 RPCs covering the full
     `udf::ActionCallbacks` / `convex_native::NativeActionCallbacks`
     surface: `RunQuery` / `RunMutation` / `RunAction`,
     `Schedule` / `CancelJob`,
     `StorageStore` (streaming) / `StorageGet` (streaming) /
     `StorageGetUrl` / `StorageDelete`,
     `VectorSearch`, `LookupFunctionHandle` /
     `CreateFunctionHandle`. Every request carries a
     `CallbackContext { identity, execution_context,
     component_path }` so the backend can attribute writes +
     propagate trace chains. Generates
     `pb::backend_callbacks::*`.

4.2. ✓ **`BackendCallbackServer`.** Landed. New module
     `convex_native_distributed::backend_callbacks_server`
     wraps an `Arc<dyn udf::ActionCallbacks>` and implements
     the `BackendCallbackService` tonic trait:

     - `RunQuery` / `RunMutation` / `RunAction` →
       `ActionCallbacks::execute_{query,mutation,action}`.
     - `Schedule` / `CancelJob` →
       `ActionCallbacks::{schedule_job,cancel_job}`.
     - `StorageGetUrl` / `StorageDelete` →
       `ActionCallbacks::{storage_get_url,storage_delete}`.
     - `StorageStore` / `StorageGet` / `VectorSearch` /
       `LookupFunctionHandle` / `CreateFunctionHandle` return
       `Unimplemented`. Wiring each is additive as deployer
       actions hit the path.

     Identity-bytes decoding: an empty byte vec maps to
     `Identity::system()` (the worker-side client sends empty
     today). Non-empty bytes fail loudly with `Unimplemented`
     so a stale client doesn't silently route under the system
     principal. Full `convex_identity::Identity` decoding
     lands with substep 4.4's worker-side identity forwarding.

     Component-scoped callbacks also return `Unimplemented`
     (only root-component routing is wired). The scaffold
     carries the `component_path` string through the envelope
     so grow-to-component is a one-line enum match change.

     Cargo dep: adds `vector` (direct — previously transitive)
     for the `PublicVectorSearchQueryResult` type signature
     referenced in the `ActionCallbacks` trait impl test.

     Tests: four unit tests pinning the delegation contract
     for `RunMutation`, `Schedule`, `StorageGetUrl`, plus one
     that pins the "non-empty identity → Unimplemented" guard.

4.3. ✓ **`BackendCallbackClient`.** Landed. New module
     `convex_native_distributed::backend_callbacks_client`
     implements `convex_native::NativeActionCallbacks` over
     tonic `BackendCallbackServiceClient<Channel>`. Connect
     once per running action; every callback translates into
     the matching RPC with a `CallbackContext { identity,
     execution_context, component_path }` attached.

     - `run_query_by_name` / `run_mutation_by_name` →
       `RunQuery` / `RunMutation` RPCs. The mutation path
       relies on the backend's Committer for durability +
       subscription invalidation.
     - `schedule` → `Schedule` RPC; delay is resolved to a
       `fire_at_unix_nanos` wall-clock timestamp on the worker.
     - `storage_store` → `StorageStore` client-streaming RPC;
       metadata first, then body in 64 KiB chunks.
     - `storage_get_url` / `storage_delete` → one-shot RPCs.
     - `cancel_scheduled`, `storage_get_metadata`,
       `read_document_at_snapshot` stay on the trait's
       default bail — a follow-up substep wires them when a
       deployer action hits them.

     Tests: four unit tests spin up a canned
     `BackendCallbackService` on an ephemeral port and assert
     each NativeActionCallbacks method translates into the
     matching RPC (run_mutation, schedule, storage_store,
     storage_get_url).

4.4. ✓ **Worker wiring.** Landed.
     `FunctionExecutionServer::with_backend_callback_endpoint(url)`
     opts the worker into routing action sub-calls through a
     `BackendCallbackClient`. When set, the action branch of
     `execute` dials the URL, builds a fresh client per
     action (carrying the request's `execution_context`), and
     hands that to
     `NativeFunctionRunner::run_action_with_callbacks_and_log_buffer`
     in place of `NoopCallbacks`. Unset → the action still
     runs but every sub-call bails via `NoopCallbacks`
     (matches pre-Phase-4 behaviour).

     The identity bytes sent on each callback are empty today —
     the worker doesn't yet forward the acting principal. A
     follow-up substep threads it through once the admission
     handshake captures the principal.

     Tests: new integration test
     `tests/action_sub_calls::worker_sub_mutation_reaches_backend_action_callbacks`
     pins the full cross-process path: worker-side
     `BackendCallbackClient` → backend-side `BackendCallbackServer`
     → recording `ActionCallbacks` stub → captures the dotted
     function path + serialized args. Proves the wire is
     hooked up end-to-end.

4.5. ✓ **Enable `UdfType::Action` on the distributed runner.**
     Landed. Both `DistributedFunctionRunner::run_function`
     and `PoolFunctionRunner::run_function` stop returning the
     "Phase 4" guidance error and now dispatch actions over
     the wire. Substep 4.4's worker-side
     `BackendCallbackClient` routing keeps the Committer in
     the loop for sub-calls the action makes.

     New helper `function_runner_impl::dispatch_action_via`
     mirrors `dispatch_query_or_mutation_via` but for
     actions: builds an `ExecuteRequest` with
     `begin_timestamp=None` + empty `existing_writes` (actions
     have no enclosing tx), routes through the caller's
     dispatch closure, and assembles the response into
     `(None, FunctionOutcome::Action(ActionOutcome),
     FunctionUsageStats)`.

     Tests: existing "Phase 4 guidance" assertion replaced
     with a `function_metadata`-required contract check (same
     programmer-error shape as Query/Mutation). New
     integration test
     `tests/function_runner_e2e::action_dispatch_never_carries_final_tx`
     pins the "actions don't open a tx → final_tx stays None"
     invariant end-to-end across real tonic.

4.6. **Integration test.** Split into two sub-substeps:

     4.6a. ✓ **Wire-proof end-to-end test.** Landed.
     `tests/action_sub_calls::worker_exec_server_wires_callback_endpoint_into_action_dispatch`
     spins up a backend-side `BackendCallbackServer`, a
     worker-side `FunctionExecutionServer` configured with
     `.with_backend_callback_endpoint(...)`, and a
     `DistributedFunctionRunner` dialing the worker. An
     action dispatch with an unknown handler surfaces a
     handler-level "does not exist" error — proving the
     server's Action branch successfully built the
     `BackendCallbackClient` + reached
     `NativeFunctionRunner::run_action_with_callbacks_and_log_buffer`.
     Combined with
     `tests/action_sub_calls::worker_sub_mutation_reaches_backend_action_callbacks`
     (which pins the `BackendCallbackClient` →
     `BackendCallbackServer` → `ActionCallbacks` round-trip)
     every link in the Phase-4 chain is covered.

     4.6b. **Full action → sub-mutation → commit assertion.**
     Blocked on the same `Database<Rt>` test fixture as
     substep 2.8b. A complete run requires:
     - Registering a real `#[convex::action]` in the test
       binary (inventory registrations are link-time-
       collected; adding one for a single test isn't
       ergonomic today).
     - A live `Database<Rt>` on the backend side of the
       callback server so
       `ActionCallbacks::execute_mutation` can commit +
       notify the `SubscriptionManager`.
     When either the `#[convex::action]`-in-tests story
     improves or the backend topology runs against a live
     binary (Phase 5), this assertion lands.

Exit criteria: native actions on a remote worker can call
mutations / schedule jobs / do file storage through the
backend, and the backend's Committer sees every resulting
write. The wire (4.1..4.6a) is complete; the remaining commit
assertion (4.6b) is observation-only and doesn't block the
topology working in production — a real deployer registers
`#[convex::action]`s in their worker binary, the backend runs
against its real `Database`, and the chain in this repo's
source code has no other missing links.

## Phases 5..7 — not started

See `DISTRIBUTED_PLAN.md` §15 for the full breakdown.

- **Phase 5**: prebuilt `getconvex/convex-backend` container
  image; deployer ships only worker images.
- **Phase 6**: JS interop (`WorkerKind::JAVASCRIPT` in the
  admission envelope).
- **Phase 7**: operator tooling — pool introspection, inventory
  diff, floor-bump admin RPC.

---

## Test tallies

```
cargo test -p convex_native              # 242 tests
cargo test -p convex_native_backend      # 10 tests
cargo test -p convex_native_distributed  # 72 tests

# 324 total — all green
```

Delta since Phase 1: +16 tests on `convex_native_distributed`
covering the substep-2.1/2.2/2.3/2.4 wire additions, the
substep-2.6a `FinalTxSummary → FunctionFinalTransaction`
conversion, the substep-2.6b FunctionRunner trait impl
error-path guidance, the substep-2.7 `CONVEX_NATIVE_WORKERS`
env-var parser, and the substep-2.8a end-to-end gRPC wire test.

---

## Known non-goals for this project

1. **Multi-tenancy.** One backend = one Convex deployment. Convex
   cloud layers multitenancy above this; self-host doesn't need it.
2. **Cross-backend sharding of a single deployment.** Out of
   scope.
3. **Zero-downtime backend image rollouts.** The backend is the
   stateful centre — upgrading it requires a brief disconnect,
   same as any stateful service.
4. **Replacing `local_backend` wholesale.** The monolith
   topology stays for people who want it (`STANDALONE.md`);
   the new work adds a topology rather than removing one.
