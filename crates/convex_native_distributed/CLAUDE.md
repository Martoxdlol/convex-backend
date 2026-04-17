# convex_native_distributed — agent notes

Split-topology support for `convex_native`: a conductor dispatches
function calls over gRPC to a pool of identical worker binaries,
each running the same native registry.

Read alongside `../../convex-native/README.md` (shipped-vs-planned
at the project level) and
`../../convex-native/COMPOSITE_RUNNER.md` (the in-process path
this crate is the distributed counterpart of).

## Crate layout

```
src/
├── lib.rs          -- module layout table + re-exports
├── conversions.rs  -- pb::function_execution::* ↔ convex_native::distributed::*
├── server.rs       -- FunctionExecutionServer (worker-side tonic impl)
├── client.rs       -- DistributedFunctionRunner (conductor P2C) + WorkerClient trait + MockWorkerClient
├── tonic_client.rs -- TonicWorkerClient (real gRPC transport impl of WorkerClient)
└── mode.rs         -- CONVEX_MODE env-var parsers + build_worker_server/build_conductor_runner
examples/
├── worker.rs       -- minimal tonic server binary a deployer can crib from
└── conductor.rs    -- minimal health-probe binary a deployer can crib from
tests/
├── multi_worker.rs       -- N-worker conductor + failover + version gate tests
└── examples_smoke.rs     -- subprocess smoke test spawning worker + conductor
```

Proto contract: `../pb/protos/function_execution.proto` (two RPCs,
`Execute` + `Health`). Generated Rust types live at
`pb::function_execution::*`.

## Conventions

- **Handler monomorphism.** The worker side is monomorphic over
  `convex_native::Rt` (just like the composite runner) — no attempt
  to support arbitrary RTs. Keep TypeId checks + unsafe casts out
  of this crate; the worker takes `Database<Rt>` directly via
  `FunctionExecutionServer::with_database(db)`.
- **Wire format stability.** All proto ↔ native translation goes
  through `conversions.rs`. Anything on the wire that the rest of
  the crate needs to read or write should get a `to_proto_*` and
  `from_proto_*` helper, not bespoke inline encoding. The
  conversions module is the testable boundary.
- **WorkerClient is the test seam.** Don't hard-code
  `TonicWorkerClient` in new client-side logic —
  `DistributedFunctionRunner` takes `Vec<Arc<dyn WorkerClient>>`,
  so mock clients can exercise everything except the transport.
- **Every new feature gets a test.** The crate has 46 tests today
  (38 unit + 7 multi-worker integration + 1 subprocess smoke);
  new work should keep that ratio.

## Dev workflow

```sh
cargo check -p convex_native_distributed
cargo test -p convex_native_distributed           # 46 tests last known
cargo build --examples -p convex_native_distributed
cargo +nightly fmt -p convex_native_distributed

# End-to-end smoke:
CONVEX_MODE=worker CONVEX_WORKER_BIND_ADDR=127.0.0.1:45671 \
    cargo run -q -p convex_native_distributed --example worker &
CONVEX_MODE=conductor CONVEX_WORKER_ENDPOINTS=http://127.0.0.1:45671 \
    cargo run -q -p convex_native_distributed --example conductor
```

## What's shipped vs planned

`convex-native/README.md` is authoritative. Short version:

- Phase 3.1–3.6 all shipped as crate features.
- `FunctionExecutionServer` handles actions via
  `run_action_with_callbacks`; with `.with_database(db)` also
  queries and mutations inline (queries drop the tx; mutations
  commit via `commit_with_write_source`).
- `DistributedFunctionRunner` does P2C with single-retry failover
  and a `min_registry_version` floor for Phase 4.7 rolling
  deploys; `with_failover(bool)` toggles the failover attempt.

Outstanding: only the conductor half of the unified binary. The
worker half shipped: `convex-local-backend` now accepts
`CONVEX_MODE=worker` and spawns a tonic `FunctionExecutionService`
alongside the HTTP server via `serve_worker_with_database(addr,
native, db)`. Conductor mode stays behind the
`examples/conductor` binary because `convex-local-backend` always
boots a local `Database`, which defeats the conductor topology.
