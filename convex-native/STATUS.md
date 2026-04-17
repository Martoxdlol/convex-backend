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
dispatch layer is being rebuilt per `DISTRIBUTED_PLAN.md` —
**Phase 1 (wire contract) has shipped**: `ExecuteResponse`
carries a `DistributedFinalTx`, the worker no longer commits
locally, and round-trip tests cover the native ↔ proto boundary.
**Phase 2 (backend-side `FunctionRunner` impl)** is the next
step.

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

## Phases 2..7 — not started

See `DISTRIBUTED_PLAN.md` §15 for the full breakdown.

- **Phase 2**: `impl FunctionRunner<RT> for DistributedFunctionRunner`;
  `local_backend` env-var to swap runners. After Phase 2, OCC +
  subscriptions work against remote workers.
- **Phase 3**: dynamic worker pool + `WorkerAdmissionService`
  (workers register on start; inventory carried in the
  registration envelope).
- **Phase 4**: `BackendCallbackService` — action sub-calls
  route back to the backend's Committer.
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
cargo test -p convex_native_distributed  # 54 tests

# 306 total — all green
```

The number is lower than the peak (309) because removed tests
were pinning behaviour that's no longer correct.

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
