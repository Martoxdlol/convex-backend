# convex_native_distributed — agent notes

The gRPC transport + admission pool crate that implements the
distributed topology from
`../../convex-native/DISTRIBUTED_PLAN.md`. Every phase (1–7) of
that plan has shipped in source. See
`../../convex-native/STATUS.md` for the per-substep tracker.

The backend process coordinates OCC, subscriptions, and
committing; a pool of worker processes execute native handlers
and return reads / writes over gRPC. Actions running on a worker
route sub-calls back to the backend through
`BackendCallbackService` so every write still flows through the
backend's Committer.

Read alongside:
- `../../convex-native/DISTRIBUTED_PLAN.md` — target architecture
  and phase breakdown (source of truth).
- `../../convex-native/STATUS.md` — what's shipped vs outstanding.
- `../../convex-native/DEPLOYMENT.md` — operator env-var matrix.
- `../../convex-native/README.md` — project landing page.

## Crate layout

```
src/
├── lib.rs                       -- module layout table + re-exports
├── conversions.rs               -- pb::function_execution::* ↔ native shapes
├── server.rs                    -- FunctionExecutionServer (worker-side tonic impl)
├── client.rs                    -- DistributedFunctionRunner (fixed pool, P2C) + WorkerClient trait
├── tonic_client.rs              -- TonicWorkerClient (real gRPC transport)
├── function_runner_impl.rs      -- FunctionRunner<RT> impls shared between fixed + dynamic pool
├── mode.rs                      -- CONVEX_MODE / CONVEX_*_ENDPOINT / *_BIND_ADDR env parsers
├── admission.rs                 -- collect_inventory() + RegistrationEnvelope helpers
├── admission_server.rs          -- WorkerAdmissionServer (backend-side tonic impl)
├── admission_client.rs          -- WorkerRegistration (worker-side registration loop)
├── pool.rs                      -- WorkerPool (dynamic admission/retire + eligibility)
├── pool_runner.rs               -- PoolFunctionRunner (FunctionRunner impl over WorkerPool)
├── backend_callbacks_server.rs  -- BackendCallbackServer (backend-side tonic impl)
├── backend_callbacks_client.rs  -- BackendCallbackClient (worker-side NativeActionCallbacks)
├── admin_http.rs                -- axum admin surface (GET /pool, POST floor/drain/kind)
└── cron_driver.rs               -- NativeCronDriver (saffron-based per-cron tasks)
examples/
├── worker.rs                    -- reference native-Rust worker binary
└── js_worker.rs                 -- reference JS worker shell (advertises WorkerKind::JAVASCRIPT)
tests/
├── multi_worker.rs              -- N-worker P2C + failover + version-gate tests
├── function_runner_e2e.rs       -- end-to-end gRPC wire tests
├── admission_churn.rs           -- admit/retire/drain lifecycle tests
├── action_sub_calls.rs          -- BackendCallbackClient ↔ Server round-trip
└── db_fixture.rs                -- DbFixture::new_in_memory() + SubscriptionManager / action-commit assertions
```

Proto contracts:
- `../pb/protos/function_execution.proto` — worker RPC surface.
- `../pb/protos/worker_admission.proto` — admission + drain +
  registry-floor control plane.
- `../pb/protos/backend_callbacks.proto` — action sub-call
  callbacks (run_{query,mutation,action}, schedule/cancel,
  storage_{store,get,get_url,delete}, vector_search,
  function_handle lookup/create).

Generated Rust types: `pb::function_execution::*`,
`pb::worker_admission::*`, `pb::backend_callbacks::*`.

## Conventions

- **Handler monomorphism.** The worker side is monomorphic over
  `convex_native::Rt` (same as the composite runner). `inventory`
  can't hold generic fn pointers.
- **Wire format stability.** All proto ↔ native translation goes
  through `conversions.rs`. The JSON-readable string encodings
  (`TabletId`, component paths, function handles) are part of the
  public contract — keep them stable for JS worker consumers.
- **WorkerClient is the test seam.** `DistributedFunctionRunner`
  takes `Vec<Arc<dyn WorkerClient>>`; `WorkerPool` stores
  `Arc<dyn WorkerClient>` per entry. Mock clients exercise
  everything except transport.
- **Identity forwarding.** Every dispatch path encodes the
  caller's `keybroker::Identity` through
  `pb::convex_identity::UncheckedIdentity` into
  `ExecuteRequest.identity`. The worker decodes it and threads
  the principal into query/mutation transactions + action ctxs
  so `ctx.auth()` returns the real caller.
- **Every new feature gets a test.** Round-trip on the wire;
  churn / drain / failover on the pool; sub-call routing on the
  callback path. See `STATUS.md` for per-substep coverage.

## Dev workflow

```sh
cargo check -p convex_native_distributed
cargo test  -p convex_native_distributed
cargo build --examples -p convex_native_distributed
cargo +nightly fmt -p convex_native_distributed
```

## Env-var surface

Backend side (read in `local_backend::make_app`):

- `CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678` — serve the dynamic
  `WorkerAdmissionService`. Precedence: set → dynamic pool;
  unset + `CONVEX_NATIVE_WORKERS` set → fixed pool; both unset
  → in-process native dispatch.
- `CONVEX_NATIVE_WORKERS=grpc://w1:4567,grpc://w2:4567` — Phase-2
  fixed-pool shape (useful for tests / tiny deployments).
- `CONVEX_ADMIN_BIND_ADDR=127.0.0.1:9090` — serve the axum admin
  surface (pool snapshot / floor / drain / kind_preference).
- `CONVEX_REFUSE_NATIVE_HANDLERS=1` — fail boot if the backend
  binary carries non-empty native inventory. Wired into the
  production image.

Worker side (read in the worker binary):

- `CONVEX_MODE=worker` — serve `FunctionExecutionService` on
  `CONVEX_WORKER_BIND_ADDR` (default `0.0.0.0:4567`).
- `CONVEX_BACKEND_ENDPOINT=grpc://backend:5678` — register via
  `WorkerAdmissionService`. Unset = Phase-2 fixed-pool mode.
- `CONVEX_BACKEND_CALLBACK_ENDPOINT=grpc://backend:5679` — route
  action sub-calls back to the backend's `BackendCallbackService`.

## Phase status

All phases of `DISTRIBUTED_PLAN.md` have shipped. See
`../../convex-native/STATUS.md` for the phase-by-phase tracker
including cross-phase fixes (native HTTP validation, native HTTP
action serving, identity forwarding, native cron driver, native
schema publication).
