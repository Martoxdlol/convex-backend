# convex_native_distributed — agent notes

**⚠ Under active replan.** The previous topology (a standalone
"conductor" process that dispatched to workers which committed
locally) is being replaced by the architecture in
`../../convex-native/DISTRIBUTED_PLAN.md` — the backend process
coordinates OCC / subscriptions / committing, and workers only
execute native handlers and return reads / writes over gRPC.

This crate holds the gRPC scaffolding used by *both* the old and
the new path. What it currently does, what's shipping, and what's
about to change are all tracked in
`../../convex-native/STATUS.md`.

Read alongside:
- `../../convex-native/DISTRIBUTED_PLAN.md` — target architecture
  and phased delivery (source of truth).
- `../../convex-native/STATUS.md` — what survived the reset, what
  was removed, what's the active phase.
- `../../convex-native/README.md` — project landing page.

## Crate layout

```
src/
├── lib.rs          -- module layout table + re-exports
├── conversions.rs  -- pb::function_execution::* ↔ convex_native::distributed::*
├── server.rs       -- FunctionExecutionServer (worker-side tonic impl)
├── client.rs       -- DistributedFunctionRunner (P2C) + WorkerClient trait + MockWorkerClient
├── tonic_client.rs -- TonicWorkerClient (real gRPC transport impl of WorkerClient)
└── mode.rs         -- CONVEX_MODE env-var parsers + build_worker_server / serve_worker_with_{database,shutdown}
examples/
└── worker.rs       -- minimal tonic server binary a deployer can crib from
tests/
└── multi_worker.rs -- N-worker P2C + failover + version-gate tests
```

Proto contract: `../pb/protos/function_execution.proto`. Generated
Rust types live at `pb::function_execution::*`. Phase 1 of
`DISTRIBUTED_PLAN.md` extends this proto with
`begin_timestamp` / `existing_writes` on `ExecuteRequest` and
`FunctionFinalTransaction` on `ExecuteResponse`.

## What was removed in the reset

- `src/worker_callbacks.rs` — `WorkerActionCallbacks` committed
  sub-call mutations on the worker's own `Database`. Wrong under the
  new plan (sub-calls must route back to the backend's Committer).
  Replaced in `server.rs` by `NoopCallbacks` with a `TODO(phase-4)`
  pointer at `BackendCallbackService`.
- `examples/conductor.rs`, `examples/conductor_dispatch.rs`,
  `examples/worker_with_functions.rs` — all tied to the
  worker-commits-end-to-end topology.
- `tests/examples_smoke.rs`, `tests/client_e2e_smoke.rs` —
  exercised the removed standalone behaviour.

## Conventions

- **Handler monomorphism.** The worker side is monomorphic over
  `convex_native::Rt` (same as the composite runner). The worker
  will take `Database<Rt>` directly via
  `FunctionExecutionServer::with_database(db)` only while the
  pre-Phase-1 transport shape is in place; Phase 1 moves the
  commit back to the backend and the worker's DB becomes read-only
  state (opened at `begin_timestamp` the backend supplies).
- **Wire format stability.** All proto ↔ native translation goes
  through `conversions.rs`. Phase 1's `FunctionFinalTransaction`
  conversions land there too (the `pb::function_runner::*` types
  already exist for Funrun — reuse).
- **WorkerClient is the test seam.** Don't hard-code
  `TonicWorkerClient` in new client-side logic —
  `DistributedFunctionRunner` takes `Vec<Arc<dyn WorkerClient>>`,
  so mock clients can exercise everything except the transport.
- **Every new feature gets a test.** Phase 1 lands with round-trip
  tests: client encodes a request with mocked `begin_ts` +
  `existing_writes`; worker handler reads from that ts; backend
  decodes the returned `final_tx` and asserts its structure.

## Dev workflow

```sh
cargo check -p convex_native_distributed
cargo test -p convex_native_distributed
cargo build --examples -p convex_native_distributed
cargo +nightly fmt -p convex_native_distributed
```

## Active phase

**Phase 1 — wire contract.** Shipped; see
`../../convex-native/STATUS.md` for the full deliverable list.

Recap of what landed:

1. ✓ `ExecuteRequest.begin_timestamp` + `ExecuteRequest.existing_writes`
   (sub-message placeholder for Phase 2 to grow).
2. ✓ `ExecuteResponse.final_tx: Option<DistributedFinalTx>`.
3. ✓ Conversions: `conversions::final_tx_to_proto` /
   `final_tx_from_proto` move between proto `DistributedFinalTx`
   and native `convex_native::distributed::FinalTxSummary`.
4. ✓ `FunctionExecutionServer::run_{query,mutation}_inline` no
   longer commit; they return a `FinalTxSummary` on the native
   response. `summarise_tx` drains the worker's `Transaction<Rt>`
   into that summary.
5. ✓ `DistributedFunctionRunner::execute` surfaces the native
   `ExecuteResponse` (with its `final_tx`) to the dispatcher.
6. ✓ Round-trip tests:
   - `conversions::tests::response_final_tx_roundtrips_through_proto`
   - `conversions::tests::response_without_final_tx_keeps_field_none`

**Phase 2 — `impl FunctionRunner<RT> for DistributedFunctionRunner`.**
Next step. See `DISTRIBUTED_PLAN.md` §15 Phase 2. The
`DistributedFinalTx` / `FinalTxSummary` pair grows to carry full
`FunctionReads`/`FunctionWrites` content in step with the impl.

## Phases 2..7

See `DISTRIBUTED_PLAN.md` §15 for the full breakdown:

- **Phase 2**: `impl FunctionRunner<RT> for DistributedFunctionRunner`;
  `local_backend` env-var swaps runners. After Phase 2 OCC +
  subscriptions work against remote workers.
- **Phase 3**: dynamic pool + `WorkerAdmissionService`; removes
  `CONVEX_WORKER_ENDPOINTS` in favour of `CONVEX_BACKEND_ENDPOINT`
  on the worker.
- **Phase 4**: `BackendCallbackService` — action sub-calls route
  back to the backend's Committer.
- **Phase 5**: prebuilt `getconvex/convex-backend` image.
- **Phase 6**: JS interop (`WorkerKind::JAVASCRIPT`).
- **Phase 7**: operator tooling.

## Legacy env-var surface (pre-Phase-3)

- `CONVEX_MODE=worker`: worker serves gRPC on
  `CONVEX_WORKER_BIND_ADDR` (default `0.0.0.0:4567`). This path
  stays; Phase 3 adds an outbound connection to the backend's
  `WorkerAdmissionService` for registration.
- `CONVEX_MODE=conductor` and `CONVEX_WORKER_ENDPOINTS`: **going
  away in Phase 3.** The backend image replaces the standalone
  conductor concept; `mode.rs` keeps the parsers for now so the
  pre-Phase-3 tests stay green.
