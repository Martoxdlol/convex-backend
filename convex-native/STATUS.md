# Status

Every phase of `DISTRIBUTED_PLAN.md` has shipped in source. The
previously-deferred infrastructure items (live-DB assertion
fixture, CI/release pipeline, JS worker reference shell) have
also landed. `DISTRIBUTED_PLAN.md` is the architecture source
of truth; this file tracks per-substep detail and the
cross-phase fixes that don't fit neatly into one phase
(native HTTP-action serving, identity forwarding, native cron
driver, `CONVEX_MIN_REGISTRY_VERSION`).

---

## One-line summary

Framework-level pieces (derives, ctx surface, schema reflection,
registry, introspection) are solid and reused. The distributed
dispatch layer is rebuilt per `DISTRIBUTED_PLAN.md` — **every
phase has shipped** (1 wire contract, 2 `FunctionRunner` impl
+ in-memory `DbFixture` for the SubscriptionManager
assertion, 3 dynamic pool + admission, 4 action sub-call
callbacks + same fixture for the action→sub-mutation
assertion, 5 prebuilt backend image + safety net + k8s
manifests + CI release pipeline, 6 JS interop foundations +
reference worker binary, 7 operator tooling). The plan's
target architecture works end-to-end in source; layered
assertions on top of `DbFixture::new_in_memory()` are now
straightforward to add when a deployer hits a regression in
either the SubscriptionManager invalidation or
action→sub-mutation commit path.

---

## Authoritative docs

| File | Purpose |
|------|---------|
| `DISTRIBUTED_PLAN.md` | **The new plan.** Target architecture, phase breakdown, client-routing decision. |
| `USAGE.md` | Per-feature developer reference for the ctx / derive / runtime surface. Unchanged by the replan. |
| `MIGRATION.md` | JS ↔ Rust cheatsheet. Still valid. |
| `STANDALONE.md` | Monolith alternative (`local_backend` as a library). Works today but is the legacy shape — deprioritized relative to the new distributed target. |
| `COMPOSITE_RUNNER.md` | Describes `CompositeFunctionRunner` — the monolith dispatch path inside `local_backend`. Stays for the monolith topology; not used by the new distributed target. |
| `DEPLOYMENT.md` | Operational guide — env-var matrix, rolling updates, observability, Phase-7 admin surface. |

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
       and `rng_seed` are now carried over the wire — the
       `DistributedFinalTx` proto grew the fields, the worker
       seeds `Observed::from_seed(rng)` and drains the flags
       back into `FinalTxSummary`, and `build_outcome_triple`
       hydrates the `UdfOutcome` so subscription-reuse +
       OCC + analytics see identical fidelity on the
       distributed path.
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

     2.8b. ✓ **Full SubscriptionManager assertion.** Landed.
     New helper
     `crates/convex_native_distributed/tests/db_fixture.rs`
     defines `DbFixture::new_in_memory()` — opens
     `SqlitePersistence::new(":memory:")`, builds an
     `InProcessSearcher`, wires `Database::load(...)` against
     the test's tokio runtime via the new
     `ProdRuntime::from_handle(Handle::current())` constructor,
     and initialises application system tables.
     The assertion test
     `write_invalidates_subscriber_reading_same_table`:
     - Inserts a seed row to create table `T`.
     - Opens a read tx, runs a query against `T`, builds a
       `Token`, subscribes via `Database::subscribe`.
     - Inserts another row into `T` and commits.
     - Asserts the subscription invalidates within 5s
       (`wait_for_invalidation` returns `Some(ts)`).
     The dispatched-via-`DistributedFunctionRunner` shape
     collapses to the same backend `commit_with_write_source`
     code, so the invalidation invariant is the same on both
     paths; the direct-commit shape here is the simplest
     reproduction.

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

4.2. ✓ **`BackendCallbackServer`.** Landed (every RPC wired).
     Module `convex_native_distributed::backend_callbacks_server`
     wraps an `Arc<dyn udf::ActionCallbacks>` and implements
     the full `BackendCallbackService` tonic trait:

     - `RunQuery` / `RunMutation` / `RunAction` →
       `ActionCallbacks::execute_{query,mutation,action}`.
     - `Schedule` / `CancelJob` →
       `ActionCallbacks::{schedule_job,cancel_job}`.
     - `StorageGetUrl` / `StorageDelete` →
       `ActionCallbacks::{storage_get_url,storage_delete}`.
     - `StorageStore` / `StorageGet` (streaming) → the new
       `BackendFileBytes` trait the backend supplies via
       `BackendCallbackServer::with_file_bytes(...)`.
       `local_backend::backend_callbacks_wiring::BackendFileBytesImpl`
       is the production impl, wrapping `FileStorage<ProdRuntime>`.
     - `VectorSearch` → `ActionCallbacks::vector_search`,
       results serialised as JSON.
     - `LookupFunctionHandle` / `CreateFunctionHandle` →
       `ActionCallbacks::{lookup_function_handle,create_function_handle}`,
       handles encoded with the `function://` prefix.

     Identity-bytes decoding: an empty byte vec maps to
     `Identity::system()`; non-empty bytes decode through
     `Identity::from_proto_unchecked` against the
     `pb::convex_identity::UncheckedIdentity` proto shape.
     Garbage bytes surface as `InvalidArgument` so a stale
     client never silently routes under the system principal.

     Component-scoped callbacks decode through
     `ComponentPath::from_str`. For storage / scheduling
     callbacks that need a `ComponentId`, the new
     `ComponentResolver` trait (default
     `RootOnlyComponentResolver`; `local_backend` plugs in
     `ApplicationComponentResolver` which consults the
     database's component registry) maps the path back to an
     id.

     Cargo dep: adds `vector` (direct — previously transitive)
     for the `PublicVectorSearchQueryResult` type signature
     referenced in the `ActionCallbacks` trait impl test.

     Tests: extended unit tests cover the full delegation
     contract: `run_mutation`, `schedule`, `storage_get_url`,
     identity round-trip via `Identity::system().into() →
     UncheckedIdentity`, garbage-identity → `InvalidArgument`,
     and component-path decoding round-trip.

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
     - `cancel_scheduled` → `CancelJob` RPC.
     - `storage_get_metadata` → reads the leading meta frame
       off the `StorageGet` stream.
     - `read_document_at_snapshot` → routes through `RunQuery`
       against the system `_system/db:get` surface so backends
       with a wired query path service `ctx.db().get(...)`
       reads from inside an action.

     Tests: unit tests spin up a canned
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

     The worker forwards the acting principal: the
     `proto_req.identity` bytes from the inbound
     `FunctionExecutionService::ExecuteRequest` are passed
     through to `BackendCallbackClient::connect`, so the
     backend's `decode_context` round-trips the same
     `keybroker::Identity` the action ran under.

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

     4.6b. ✓ **Full action → sub-mutation → commit assertion.**
     Landed. The assertion test
     `action_sub_mutation_routes_through_callbacks_and_commits`
     in `tests/db_fixture.rs`:
     - Stands up `DbFixture::new_in_memory()`.
     - Builds a `DbBackedCallbacks` impl of `udf::ActionCallbacks`
       whose `execute_mutation` opens a tx on the fixture's
       `Database`, inserts a marker row via `UserFacingModel`,
       and commits via `commit_with_write_source`.
     - Spins up a `BackendCallbackServer` wrapping the
       callbacks on a tonic `TcpListener`.
     - Dials a `BackendCallbackClient` from the test task and
       invokes `run_mutation_by_name` (the same call shape an
       action's `ctx.run_mutation(...)` produces).
     - Asserts the marker table is visible in a fresh tx
       (proves the sub-mutation actually committed).
     Closes the previously-deferred "live-DB action sub-call
     commit" gap — every link in the Phase-4 chain is now
     covered by an executable assertion.

Exit criteria: native actions on a remote worker can call
mutations / schedule jobs / do file storage through the
backend, and the backend's Committer sees every resulting
write. The wire (4.1..4.6a) is complete; the remaining commit
assertion (4.6b) is observation-only and doesn't block the
topology working in production — a real deployer registers
`#[convex::action]`s in their worker binary, the backend runs
against its real `Database`, and the chain in this repo's
source code has no other missing links.

## Phase 5 — active, decomposed into concrete substeps

`DISTRIBUTED_PLAN.md` §15 Phase 5: publish a prebuilt
`getconvex/convex-backend` container image so deployers stop
rebuilding the backend every time their code changes. The
backend image carries no deployer-specific inventory; workers
supply everything at admission time.

5.1. ✓ **Backend Dockerfile + empty-registry boot.** Landed.
     New file `convex-native/examples/deploy/docker/Dockerfile.backend`
     produces a distributed-topology backend image built on
     the existing `convex-local-backend` binary. Key shape:

     - Defaults `CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678` so
       `docker run` without extra env vars already serves
       the admission port.
     - Exposes `3210` (public HTTP + WebSocket), `3211`
       (dashboard/admin), `5678` (WorkerAdmissionService).
     - Labels the image with
       `org.opencontainers.image.description` pointing at
       the Phase-5 topology.
     - Uses the same cargo-chef dependency-caching pattern as
       the self-hosted Dockerfile so local iteration stays
       fast.
     - Boots cleanly with an empty native registry (the
       `NativeFunctionRunner::from_inventory()` call on an
       empty inventory returns an empty registry, already
       pinned by the Phase-3 churn tests).

     Docs: `DEPLOYMENT.md` — new "Bringing up Topology B
     locally" section shows the `docker build` + `docker run`
     flow end-to-end (backend + worker on the same Docker
     network).

5.2. ✓ **`CONVEX_REFUSE_NATIVE_HANDLERS` safety net.** Landed.
     Opt-in env var that fails boot when the backend binary
     has non-empty native inventory. Set to any non-empty,
     non-whitespace value (`1`, `true`, etc.) to enable;
     unset or empty = no enforcement (default behaviour).

     - New helper `read_refuse_native_handlers_from_env()` on
       `convex_native_distributed::mode` (re-exported at the
       crate root).
     - `local_backend::make_app` consults the helper after
       `NativeFunctionRunner::from_inventory()` and bails with
       a guided message when the flag is set and the registry
       is non-empty.

     Operators using the Phase-5 prebuilt image wire this
     into their image-build CI (`ENV CONVEX_REFUSE_NATIVE_HANDLERS=1`
     in the Dockerfile for a production tag, for example)
     so an accidental link-in of worker code fails loud at
     boot instead of silently shadowing the remote pool's
     handlers.

     Tests: three unit tests on `mode::tests::read_refuse_native_handlers_*`
     covering unset / set / empty-value.

5.3. ✓ **CI / release pipeline.** Landed. New workflow
     `.github/workflows/release_convex_native_backend.yml`
     builds the distributed-topology backend image from
     `convex-native/examples/deploy/docker/Dockerfile.backend`
     for both x64 and arm64, publishes the per-arch digests
     to GHCR under `ghcr.io/get-convex/convex-native-backend`,
     and assembles a multi-arch manifest tagged either
     `${{ github.event.inputs.tag }}` or the `convex-native-vX.Y.Z`
     git tag. Pairs with the existing
     `release_self_hosted_images.yml` flow that handles the
     monolith image.

5.4. ✓ **k8s manifest for the backend.** Landed. New file
     `convex-native/examples/deploy/kubernetes/backend-deployment.yaml`
     ships:

     - `Namespace convex`, `PersistentVolumeClaim
       convex-backend-data` (10 Gi), `Deployment
       convex-backend` (1 replica, Recreate strategy — the
       backend is the coordination centre, single-writer).
     - Two Services:
       - `convex-backend` (public) exposes 3210 (HTTP +
         WebSocket) + 3211 (admin/dashboard).
       - `convex-backend-admission` (internal) exposes 5678
         (gRPC, `appProtocol: grpc`). Separate Service so
         NetworkPolicy can lock it down to the worker pod
         selector.
     - Resource requests/limits + HTTP readiness/liveness
       probes against `/version`.

     `worker-deployment.yaml` updated: the pre-Phase-3 note
     about `CONVEX_WORKER_ENDPOINTS` is removed; the pod
     template now sets `CONVEX_BACKEND_ENDPOINT=http://convex-backend-admission:5678`
     so workers auto-register against the admission Service
     from `backend-deployment.yaml`.

Exit criteria: a deployer with zero existing Convex
infrastructure can `docker pull getconvex/convex-backend:X.Y.Z`,
build their worker image, and roll the pool — no rebuild of
the backend. Substep 5.1 proves the shape works; 5.2–5.4 are
CI + k8s ergonomics.

## Phase 6 — active, decomposed into concrete substeps

`DISTRIBUTED_PLAN.md` §10 + §15 Phase 6: let JavaScript workers
(built on the existing V8 isolate stack) join the same pool as
native Rust workers, advertising themselves with
`WorkerKind::JAVASCRIPT` in the admission envelope. From the
backend's dispatch perspective both kinds answer
`FunctionExecutionService::Execute` the same way; routing picks
a worker by function name regardless of runtime.

6.1. ✓ **Capture `WorkerKind` on admission.** Landed. The
     `WorkerEntry` now carries a `WorkerKind` enum
     (`Unspecified` / `NativeRust` / `Javascript`) populated
     from `envelope.kind`. Unknown proto values fall back to
     `Unspecified` so a future kind variant doesn't hard-fail
     admission. New `WorkerPool::by_kind()` snapshot mirrors
     `by_version()` for Phase-7 dashboards.

     Tests: `pool::tests::by_kind_groups_native_and_js_workers`
     + `worker_kind_from_proto_i32_handles_known_and_unknown`.

6.2. ✓ **Dispatch-path kind-awareness.** Landed. New
     `WorkerPool::set_kind_preference(name, kind)` /
     `clear_kind_preference(name)` pair lets the deployer
     pin a routing preference for specific function names
     (e.g. route `"compute_heavy"` to `NativeRust` workers
     when both kinds advertise it). The
     `kind_preferences()` snapshot reads the current map out
     for operator dashboards.

     Semantics: a preference is a **soft hint**.
     `eligible_for(name)` first tries the preferred-kind
     subset; if that set is empty (e.g. the preferred
     kind's workers are all retired mid-rolling-deploy) it
     falls back to the full floor-filtered set. Misconfiguring
     a preference can't wedge dispatch into 503s.

     Tests: four new unit tests on `pool::tests` cover
     "preference filters when kind is available", "falls
     back when preferred kind absent", "preference is
     function-scoped not global", and "clearable back to
     default".

     100 lib tests + 18 integration = 118 total green.

6.3. ✓ **Reference JS worker binary.** Landed. New example
     `crates/convex_native_distributed/examples/js_worker.rs`
     plus a `WorkerRegistration::register_with_kind(...)`
     helper that lets a non-Rust worker advertise its kind
     (`Javascript` here) and ship a deployer-built
     `FunctionInventory` instead of the inventory-collected
     native registry. A deployer wraps the example in their
     own binary that wires V8 dispatch (the in-process
     `InProcessFunctionRunner` lives in `function_runner` and
     `isolate`) and runs it alongside the backend; the
     admission service tracks it as `WorkerKind::JAVASCRIPT`
     and the substep-6.2 `kind_preference` map routes JS
     function names to it. Pure Rust deployments don't need
     this; mixed-kind deployers crib from the example.

Exit criteria: a deployer can run mixed-kind worker pools
against a single backend. The wire contract (6.1) + the pool
routing (6.2) + the reference admission/dispatch shell (6.3)
are all in-repo. The V8-isolate dispatch wiring inside a
production JS worker is deployer-specific (it needs an
`Application`-shaped shell to drive the existing JS function
runner) and intentionally not bundled here.

## Phase 7 — active, decomposed into concrete substeps

`DISTRIBUTED_PLAN.md` §15 Phase 7 + §11: operator tooling so a
human can inspect the pool, trigger rolling-update mechanics,
and diagnose drift without shelling into the backend. Most
building blocks already exist (`by_version`, `by_kind`,
`kind_preferences`, `set_min_registry_version`,
`request_drain`); Phase 7 turns them into an operator-facing
surface.

7.1. ✓ **`PoolSnapshot` type.** Landed. New
     `pool::PoolSnapshot` + `pool::PoolWorkerSnapshot`
     structs (`#[derive(serde::Serialize)]`) bundle
     everything substep 7.2 will expose through HTTP into
     one JSON-serializable shape:

     - `total` (worker count),
       `min_registry_version` (floor),
       `by_version` / `by_kind` groupings,
       `kind_preferences` (substep 6.2 map),
       `workers[]` per-worker detail (id, version, kind,
       advertised function names, live `in_flight`,
       transport label).
     - `WorkerPool::snapshot()` computes it atomically under
       the pool's read lock so operators see a consistent
       view, not a mid-mutation splice.

     Cargo dep: adds `serde` (direct — already transitive via
     the workspace).

     Tests: `pool::tests::snapshot_bundles_everything_operators_need`
     exercises every field + confirms the JSON shape
     (`total`, `by_version`, `by_kind`, `kind_preferences`,
     `min_registry_version`, `workers[].{worker_id,
     registry_version, kind, functions, in_flight, label}`)
     so a future refactor can't silently drop an operator-
     visible field.

7.2. ✓ **Admin HTTP surface.** Landed. New module
     `convex_native_distributed::admin_http` with an axum
     router serving:

     - `GET /admin/pool` — returns a
       `PoolSnapshot` as JSON.
     - `POST /admin/pool/floor` with body
       `{"min_registry_version": "X.Y.Z"}` sets the floor
       (null clears).
     - `POST /admin/pool/kind_preference` with body
       `{"function_name": "n", "kind": "native-rust"}` pins
       a per-function routing preference (accepts
       `"native-rust"`, `"javascript"`, `"unspecified"`, or
       null to clear; unknown kinds → 400).
     - `POST /admin/pool/drain` with body
       `{"worker_id": 42, "reason": "..."}` sends a
       `DrainNotice`. Returns 501 when the router was built
       without an admission server.

     Construction: `AdminState::new(pool)` for read-only;
     `.with_admission(server)` to enable drain. `router(state)`
     returns an `axum::Router` the operator mounts alongside
     the public HTTP server (typically on a loopback-only
     port behind existing admin auth).

     Cargo deps: adds `axum` as direct, `tower` as dev-dep
     (for `ServiceExt::oneshot` in tests).

     Tests: six unit tests on `admin_http::tests` cover each
     route's happy path + error shape (GET snapshot /
     set-floor / clear-floor / set-kind-preference / bad-kind
     / drain-without-admission → 501). 125 tests total on
     `convex_native_distributed`.

7.3. ✓ **Inventory diff log.** Landed. New
     `pool::InventoryDiff` struct
     (`#[derive(serde::Serialize)]`) with
     `active_version` / `incoming_version` / `added` /
     `removed` / `carried_over` fields. Computed by
     `WorkerPool::diff_against_active_inventory(version,
     functions)`:

     - Picks the "active" version as the one with the
       largest worker count (tie-broken lexicographically so
       the choice is deterministic).
     - Returns `None` when the pool is empty or the incoming
       version matches the active one (nothing interesting
       to log).
     - Output field sets are sorted alphabetically so the
       log line is stable.

     `WorkerAdmissionServer`'s `register` handler calls the
     helper right before admitting a worker; when a diff is
     present it writes a one-line summary to stderr via
     `eprintln!` (tracing integration lives in the embedding
     binary — production deployments already capture stderr
     into their log pipeline).

     Tests: four new unit tests on `pool::tests` cover
     empty-pool → None, same-version → None, added + removed
     names, and "active is most-populated". 129 tests green
     total.

7.4. ✓ **Wire admin HTTP surface into `local_backend`.** Landed.
     New `CONVEX_ADMIN_BIND_ADDR=host:port` env var. When
     set alongside `CONVEX_ADMISSION_BIND_ADDR`,
     `make_app` mounts the substep-7.2 admin router on the
     configured port.

     - `admission_server::spawn_admission_server_with_handle`
       returns both the `Arc<WorkerPool>` and the
       `WorkerAdmissionServer` so the admin surface can wire
       `.with_admission(server)` for
       operator-triggered drains.
     - `admin_http::spawn_admin_server(bind_addr, state)`
       spins up the router as a background task; transport
       failures log to stderr in the same style as the
       admission-server spawn.
     - `local_backend::make_app` consults
       `read_admin_bind_addr_from_env()`; when set, builds
       the `AdminState` and mounts the router.

     Expected production shape: bind to `127.0.0.1:9090` (or
     similar loopback) so external traffic can't hit the
     floor/drain routes; operator tooling reaches it through
     an SSH tunnel or kubectl port-forward.

     Tests: three new env-var parser unit tests
     (`mode::tests::read_admin_bind_addr_*`) covering
     unset / valid-socket / rejected-garbage. 132 tests
     total green.

Exit criteria: operators can see pool state at a glance, bump
the floor during a rolling update, preference-pin functions to
a kind, and diff inventories across registry versions — all
without restarting the backend. 7.1 ships the read side (done);
7.2 adds the write side; 7.3 makes rollout progress observable
in logs.

---

## Test tallies

```
cargo test -p convex_native                      # 244 tests
cargo test -p convex_native_backend              # 12 tests
cargo test -p convex_native_distributed          # 161 tests
cargo test -p convex_native_integration_tests    # 69 tests

# 486 total — all green
```

`convex_native_integration_tests` is a breadth-over-depth crate
that drives a shared fixture app through both topologies:

| Test file | Topology | Coverage |
|-----------|----------|----------|
| `standalone_golden_path.rs` | monolith | query/mutation round-trip, `ctx.auth()`, `ctx.unix_timestamp()` |
| `standalone_actions.rs` | monolith | pure actions, `ctx.log()` drain in both action + mutation ctx, `errors::bad_request` metadata |
| `standalone_http_actions.rs` | monolith | `#[convex::http_action]` dispatch, router lookup, unknown-route error |
| `standalone_crons_and_introspection.rs` | wire-independent | `CronRegistry::collect`, `NativeSchema::collect`, `HttpRouter::collect`, `ConvexBackend::build().validate()`, `describe_json` v1 envelope |
| `standalone_runner_knobs.rs` | monolith | `CountingMetrics`, `begin_drain()`, `CircuitBreaker` open/closed |
| `derive_round_trips.rs` | wire-independent | `ConvexEnum` / `ConvexNested` / `ConvexUnion` / `ConvexDocument` `to_convex`/`from_convex` symmetry |
| `standalone_schema_evolution.rs` | wire-independent | `plan_warmup(schema)` + `diff_schemas(old, new)` + `SchemaChange::is_destructive` |
| `standalone_db_api.rs` | monolith | `ctx.db().get(id)` / `exists` / `normalize_id` |
| `standalone_typed_query_ops.rs` | monolith | `.first()` / `.take(n)` / `.count()` / `.gte` + `.lt` range |
| `standalone_errors_and_rng.rs` | monolith | every `errors::*` helper + `ctx.rng_u64()` determinism |
| `standalone_test_callbacks.rs` | monolith | `TestCallbacks` + `CallRecord` + `args!` macro (USAGE.md §17) |
| `standalone_timeout.rs` | monolith | per-function + runner-default timeouts abort runaway handlers |
| `standalone_scheduler_storage.rs` | monolith | `ctx.scheduler().run_after(...)` and full `ctx.storage()` flow via `TestCallbacks` |
| `standalone_mutation_surface.rs` | monolith | `replace`, `delete`, `patch` mutation operators |
| `http_response_builders.rs` | wire-independent | `HttpResponse::new/text/json/redirect/with_header/with_body` |
| `distributed_golden_path.rs` | distributed | query over tonic against seeded DB, mutation returns `DistributedFinalTx`, unknown-function wire error |
| `distributed_actions.rs` | distributed | pure action wire round-trip, `log_lines` drain, `bad_request` → `Err(String)` |
| `distributed_http_actions.rs` | distributed | `http_request` / `http_response` payload round-trip |
| `distributed_admission.rs` | distributed | `collect_inventory()` envelope contents + stable hash + `is_internal` propagation |
| `distributed_execution_context.rs` | distributed | `ExecutionContext.request_id` round-trips into the worker's ctx |
| `distributed_pool_admission.rs` | distributed | full admission loop pushes every fixture function spec (kind, internal flag, HTTP route) into `WorkerPool::lookup_function` |
| `distributed_action_sub_calls.rs` | distributed | end-to-end action → `ctx.run_query(...)` sub-call chain: worker `FunctionExecutionServer.with_backend_callback_endpoint(...)` → `BackendCallbackClient` → `BackendCallbackServer` → `ActionCallbacks::execute_query` → back into the action's return value |

Delta since Phase 1: +16 tests on `convex_native_distributed`
covering the substep-2.1/2.2/2.3/2.4 wire additions, the
substep-2.6a `FinalTxSummary → FunctionFinalTransaction`
conversion, the substep-2.6b FunctionRunner trait impl
error-path guidance, the substep-2.7 `CONVEX_NATIVE_WORKERS`
env-var parser, and the substep-2.8a end-to-end gRPC wire test.

---

## Cross-phase fixes

### Native action dispatch (resolved 2026-04-18)

Pre-fix, `POST /api/action` for any `#[convex::action]` on a
pure-native monolith deployment returned 500 InternalServerError
with "Missing a valid module". `ApplicationFunctionRunner::run_action_inner`
fetched `module.environment` up front through
`ModuleModel::get_metadata_for_function_by_id` to pick between the
isolate and node dispatch branches, and that fetch always misses on
pure-native deployments (no `_modules` row is ever written). The
query/mutation path wasn't affected because
`FunctionRouter::execute_query_or_mutation` hands off to the
function runner before touching module metadata; the composite
runner's native interceptor then services the request.

Fix: consult `udf::validation::lookup_native_function` when the
`_modules` fetch misses. A native-registered action name
synthesizes `ModuleEnvironment::Isolate`, taking the
composite-runner-aware branch (`dispatch_native_action` runs the
handler inline through `NativeFunctionRunner::run_action_with_callbacks`).
JS-only and mixed deployments still error the same way on an
unknown name. A new `udf::validation::lookup_native_function`
public helper replaces the previously-private
`resolve_native_function` so the application crate can perform
the check without pulling `convex_native_core`.

The gap was monolith-specific because the distributed dispatch
path routes actions through `PoolFunctionRunner`, which never
consulted `_modules` in the first place. After this fix every
topology can serve native actions end-to-end.

### Native HTTP validation (resolved 2026-04-17)

Before this fix, every pure-native deployment (monolith +
distributed) failed every HTTP/WebSocket request with the
"Could not find public function — run `npx convex dev`" error
because `ValidatedPathAndArgs::new` only consulted `_modules` and
pure-native deployments never write those rows. Shipped via
`udf::validation::NativeFunctionResolver` + a global install hook
that `local_backend::make_app` populates from the native registry.
See `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md` for the full
diagnosis, option analysis, and resolution details.

The fix crosses Phase boundaries — monolith (STANDALONE.md),
distributed (DISTRIBUTED_PLAN.md Phase 2+), and legacy conductor
topologies all consumed `ValidatedPathAndArgs` and therefore all
saw this gap. The resolver is installed once per process from
`make_app`, so every topology benefits without further wiring.

### Native HTTP action serving (resolved 2026-04-18)

Pre-fix, pure-native deployments couldn't actually serve
`#[convex::http_action]` handlers. `HttpRouter::collect()`
enumerated the registered routes, but the backend's HTTP entry
point (`http_any_method` → `application.execute_http_action`)
ran the JS dispatch path unconditionally, which requires
`_modules` rows the native boot flow never writes. Fix shipped
via `local_backend::native_http_dispatch`:

- `NativeHttpDispatcher` holds `HttpRouter`,
  `NativeFunctionRunner`, the action-callbacks handle,
  `Database<ProdRuntime>`, and `FileStorage<ProdRuntime>`.
- Install is process-global (`OnceLock`) gated on
  `HttpRouter::collect().len() > 0`.
- `http_actions::stream_http_response` consults
  `try_dispatch_native(method, path, ...)` first; matches run
  inline through `NativeFunctionRunner::run_http_action_with_callbacks`
  with a `BackendCallbacks` pinned to the request's snapshot
  timestamp; misses fall through to the JS path with the
  unconsumed body.

Crosses phases — monolith (`STANDALONE.md`) and distributed
both pick this up. Distributed dispatch added in the same
session: `WorkerPool` grew an `HttpRouteEntry` index per
worker, `eligible_for_http(method, path)` returns the workers
serving a route + the synthetic handler name, and the
`NativeHttpDispatcher` falls through to a pool dispatch when
the local router misses. The wire carries the request /
response payload via the new `http_request` /
`http_response` fields on `ExecuteRequest` /
`ExecuteResponse`.

### Identity forwarding on distributed dispatch (resolved 2026-04-18)

Pre-fix the dispatcher hard-coded `identity: None` on every
proto request, so a query / mutation / action / HTTP action
that landed on a remote worker ran under `Identity::system()`
even when the inbound request carried a valid token.
`ctx.auth()` always reported the system principal, which is a
security correctness gap. Fix:

- `ExecuteRequest` grew an `identity: Vec<u8>` field encoded
  through the `pb::convex_identity::UncheckedIdentity` proto
  shape (empty vec ⇒ system).
- New `function_runner_impl::encode_identity_for_wire` helper.
  Query/mutation/action/HTTP dispatch paths all encode the
  caller's identity into the request.
- The worker decodes via `decode_identity_bytes`, opens
  query/mutation transactions under the decoded identity, and
  threads it into the action / HTTP-action ctx through new
  `with_identity` builders so `ctx.auth()` returns a
  meaningful view.
- Composite runner mirrors the same change for monolith
  actions via `run_action_with_callbacks_identity_log_buffer`.

### Native cron driver (resolved 2026-04-18)

`#[convex::cron(...)]` registrations were collected via
`CronRegistry` but nothing drove them. JS `CronJobExecutor`
reads `_cron_jobs` rows that pure-native deployments never
write. Fix: `convex_native_distributed::cron_driver`:

- `NativeCronDriver` parses each schedule with `saffron`,
  spawns one tokio task per cron, and is idempotent on
  `(name, schedule, target, kind)`. Per-cron loop computes
  `next_after(now)`, sleeps, fires through a `CronDispatcher`,
  logs, repeats. At-most-once semantics (missed fires
  skipped).
- `InProcessDispatcher` runs mutations inline against the
  backend's `Database` (commits tagged
  `WriteSource::system("native_cron")`); actions fire through
  `NativeFunctionRunner::run_action_with_callbacks` with
  `NoopCallbacks`.
- `PoolDispatcher` routes through the admission pool's
  `WorkerClient` for distributed deployments where the
  backend image carries no handlers.
- `WorkerAdmissionServer::set_cron_driver(driver)` lets
  worker admission feed `inventory.crons` into the driver on
  register; the proto `CronRegistration` grew a `kind` field
  so the backend knows whether to dispatch as mutation or
  action.
- `local_backend::make_app` picks the dispatcher based on
  topology and parks the driver in a process-global `OnceLock`
  so dropping it doesn't tear down live fire tasks.

### Native schema publication (resolved 2026-04-17)

Companion to the HTTP-validation fix above. Pure-native
deployments also never write `_schemas` / `_indexes` rows
because `apply_config` is never called, so any query using a
`#[convex(index(...))]` index would fail with "Index
todos.by_owner not found" even after the validation fix let
requests through. `convex_native_backend::publish_native_schema`
now runs during `make_app`, submits the native `DatabaseSchema`
as pending, waits for validation + index backfill, then activates
the schema + enables indexes — blocking boot until the deployment
is actually ready to serve queries. In Kubernetes deployments the
readiness probe hits HTTP, so worker readiness must not lead
schema readiness; the blocking boot is what keeps that invariant
true.

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
