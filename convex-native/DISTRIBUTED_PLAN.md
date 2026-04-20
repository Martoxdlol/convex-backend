# Distributed architecture plan — Funrun-parallel for Convex Native

**Status: design proposal. Nothing in this document is built today.**

The `convex_native_distributed` crate as it exists does not deliver
the topology this plan describes. See "What's wrong with the
current shape" at the bottom — summary: worker commits locally, no
read/write set round-trip, no subscription coordination, `DistributedFunctionRunner`
isn't a `FunctionRunner` trait impl. This plan supersedes that
design.

---

## 1. Goals

1. **Single backend binary, prebuilt, published as a public
   container image.** No rebuild when a deployer updates their
   application code. Deployers pull the image, wire env vars,
   done. Analogous to `postgres:16` or `redis:7`.
2. **Workers own the deployer's code.** Every `#[convex::query]` /
   `#[convex::mutation]` / `#[convex::action]` / `#[convex::cron]`
   / `#[derive(ConvexDocument)]` lives in the worker image. A
   "deploy" is "build a new worker image + roll the pool."
3. **Dynamic worker pool.** Workers come and go under an
   autoscaler's control. Backend must tolerate membership
   changes at any moment.
4. **Registry discovered at runtime.** Backend learns the
   function registry, schema, HTTP routes, and crons *from
   workers when they register*. The backend image has no
   deployer-specific inventory.
5. **All of Convex's semantics preserved.** OCC, serializable
   commits, reactive subscriptions, scheduled jobs, crons,
   file storage, auth, sync protocol. Functionally indistinguishable
   from a monolithic `local_backend` to the client.
6. **JS interop.** Workers can be native-Rust, JavaScript (via
   the existing V8/isolate stack), or both — in the same pool.
7. **Single public client endpoint.** Clients hit the backend
   and only the backend. Workers are internal; they expose gRPC
   only to the backend, not to the public.

## 2. Non-goals

1. **Multi-tenancy.** One backend = one Convex deployment. If a
   deployer wants to host N apps they run N backends. (This is
   what self-host customers do; Convex cloud handles multitenancy
   as a separate layer above this one.)
2. **Zero-downtime backend rollouts.** The backend is the
   stateful centre — upgrading it requires brief disconnection,
   same as any other stateful service. We optimize for worker
   rollouts being zero-downtime, because that's what changes
   often.
3. **Cross-backend work distribution.** Workers belong to one
   backend. No sharding of a single deployment across backends.
4. **Replacing `local_backend` wholesale.** `local_backend` stays
   as the monolith shape for people who want it. The new work
   adds a topology, doesn't remove one.

---

## 3. The decision: where do clients hit?

**Recommendation: clients hit the backend only.** Workers are
internal. The backend exposes every public API: WebSocket sync,
HTTP actions, admin RPC, file storage upload/download.

### Option A — Clients hit backend (recommended)

```
 Client ───WebSocket/HTTPS───► Backend ───gRPC───► Workers
                                 │
                                 ▼
                           Persistence (Postgres)
```

**Pros:**
- The backend already needs WebSocket session state (subscriptions
  are keyed to open sessions). Routing clients elsewhere would
  mean duplicating that state on workers → distributed cache-
  coherence problem.
- Auth / identity / rate limiting lives in one place.
- OCC retry loop lives where the Committer lives (the backend).
  If a worker's mutation conflicts, the backend loops. Clients
  never see transient conflicts. If the client hit the worker,
  conflict retry would have to bounce through the client.
- Workers are stateless pure functions of `(fn_name, args, begin_ts)`.
  Trivially autoscalable.
- Matches Convex cloud's actual topology (Funrun is internal;
  clients hit the backend).

**Cons:**
- Backend is a scale bottleneck on the fan-out side (one backend
  per deployment). Mitigated by workers being the heavy path —
  backend does network forwarding + commit, workers do compute.
- HTTP actions go through two hops (client → backend → worker).
  Latency cost; typically <5 ms on same-cluster.

### Option B — Clients hit workers directly

Rejected. You'd need to replicate subscription state, session
state, auth, and OCC-retry across every worker. At that point
the worker is just a backend and the distinction disappears.
Also incompatible with the "backend coordinates" requirement.

### Option C — Hybrid: WebSocket to backend, HTTP actions to workers

Considered. HTTP actions (`#[convex::http_action]`) are raw
request/response; they don't interact with the subscription
layer. Routing them directly to a worker saves one hop.

Rejected anyway because:
- It splits the public surface across two ports / two
  DNS records. Complicates client config.
- HTTP actions can still sub-call mutations (`ctx.run_mutation(...)`),
  which need to commit through the backend's Committer. The
  sub-call would go back to the backend, so you're hopping
  anyway — might as well enter at the backend.
- Workers now have a client-facing port, so they need client
  auth / CORS / rate-limiting duplicated.
- Keeps worker image surface narrow (gRPC only, one port). This
  is a security win.

**Decision: Option A.**

---

## 4. Architecture overview

```
         ┌──────────────────────────────────────┐
 Client ─┤   Backend (prebuilt container image) │
 WSS/TLS │                                       │
         │  ┌────────────────────────────────┐   │
         │  │ Sync layer (WebSocket)         │   │
         │  │  - subscription manager        │   │
         │  │  - session state               │   │
         │  └────────────────────────────────┘   │
         │  ┌────────────────────────────────┐   │
         │  │ HTTP router                    │   │
         │  │  - auth, rate limiting         │   │
         │  │  - file storage uploads        │   │
         │  └────────────────────────────────┘   │
         │  ┌────────────────────────────────┐   │
         │  │ Application<RT>                │   │
         │  │  - Database<RT>                │   │
         │  │    - Committer (single actor)  │   │
         │  │    - OCC conflict checker      │   │
         │  │    - LogWriter / LogReader     │   │
         │  │    - SnapshotManager           │   │
         │  │  - Scheduler worker            │   │
         │  │  - Cron worker                 │   │
         │  │  - Retention worker            │   │
         │  └────────────────────────────────┘   │
         │  ┌────────────────────────────────┐   │
         │  │ WorkerPool (dynamic)           │   │
         │  │  - impl FunctionRunner<RT>     │   │
         │  │  - P2C + failover + metrics    │   │
         │  │  - hot-reload on registration   │   │
         │  └────────────────────────────────┘   │
         │             │                         │
         └─────────────┼─────────────────────────┘
                       │ gRPC dispatch
               ┌───────┼───────────────┐
               ▼       ▼       ▼      ▼
            ┌────┐ ┌────┐  ┌────┐ ┌────┐
            │ W1 │ │ W2 │  │ W3 │ │ W4 │   Workers (native or JS)
            │Rust│ │Rust│  │JS  │ │Rust│   - Carry deployer code
            └──┬─┘ └──┬─┘  └──┬─┘ └──┬─┘   - Stateless
               │      │       │      │      - Autoscaled
               └──────┴───────┴──────┘      - Register on start
                          │
                          ▼
                   Persistence (read-only from workers)
                   (Postgres / SQLite / RDS)
```

**Key invariant:** workers never commit. They read at a
backend-assigned timestamp and return `(reads, writes, result)`.
The backend's Committer is the one and only writer.

---

## 5. Component responsibilities

### 5.1 Backend

The published container image. Built once per backend version,
never rebuilt for deployer changes.

| Owns | Rationale |
|------|-----------|
| HTTP + WebSocket endpoints | Single public surface. |
| Auth (keybroker / Identity) | Centralized identity. |
| `Application<RT>` | The Convex coordinator — unchanged from `local_backend`. |
| `Database<RT>` | Persistence connection, Committer, SubscriptionManager, LogWriter. |
| OCC retry loop | On conflict, re-dispatch to worker with new `begin_ts`. |
| Worker pool registry | DNS-watched / explicitly-registered. |
| Merged function registry | Learned from workers at registration time. |
| Merged schema | Learned from workers; used for write validation. |
| Scheduler + crons | Native Convex workers, dispatched through the pool. |
| File storage | Upload endpoint, object key management. |

**Does not own:** the deployer's functions, schema derive
output, cron schedules, HTTP action handlers, or any
application-specific code. All of that comes from workers.

### 5.2 Worker

Per-deployer image. Contains every `#[convex::*]` registration,
the derived schema, HTTP actions, crons. Rebuilt whenever the
deployer ships code.

| Owns | Rationale |
|------|-----------|
| Function handlers (Rust / JS) | The actual compute. |
| Schema + indexes (declaration) | Reported to backend at registration. |
| HTTP action handlers | Reported as routes; backend forwards matching requests. |
| Transient per-invocation state | LogBuffer, observed flags, rng, ActionCtx. |

**Does not own:** persistence writes, OCC, subscription
management, session state, or durable scheduling state. Those
all belong to the backend.

Workers can be either:
- **Native (`convex_native_distributed` worker)** — Rust
  functions registered via `inventory::submit!`. No V8.
- **JS (Funrun-style)** — existing V8 isolate farm. Implements
  the same protocol. Same worker pool from the backend's view.

### 5.3 Worker pool (backend-side)

Replaces today's fixed `DistributedFunctionRunner { workers:
Vec<...> }` with a dynamic pool:

```rust
pub struct WorkerPool {
    // Keyed by stable worker id (assigned at registration).
    workers: DashMap<WorkerId, WorkerEntry>,
    // (fn_name → Vec<WorkerId>) lookup for dispatch-by-name.
    by_function: DashMap<String, Vec<WorkerId>>,
    // Currently-active registry version floor (rolling updates).
    min_registry_version: ArcSwap<Option<String>>,
    // ...
}

impl FunctionRunner<ProdRuntime> for WorkerPool { /* ... */ }
```

Responsible for:
- Accepting worker registrations.
- Handing out worker clients for each dispatch (P2C load
  balancing, failover).
- Reflecting pool churn into the `by_function` index — adding
  newly-registered names, removing names no longer served
  after a worker leaves.

### 5.4 Persistence

Unchanged from `local_backend`. Postgres / SQLite / RDS /
whatever the self-host user configures. Accessed:

- **By the backend**: via `Database::load(persistence, ...)` for
  commits, retention, scheduler reads, cron reads.
- **By workers**: via `Database::load(persistence, ...)` too, BUT
  workers never call `commit_with_write_source`. They only
  open transactions, read, stage writes, and return.

---

## 6. Protocol additions

Three new RPCs + a proto surface expansion.

### 6.1 Extend `ExecuteResponse` and `ExecuteRequest`

```proto
message ExecuteRequest {
  // ... existing fields ...

  // Backend-assigned snapshot timestamp. Worker reads at this ts.
  // Required for queries + mutations (not for actions).
  optional uint64 begin_timestamp = 9;

  // Existing writes staged in the backend's in-flight batch.
  // Worker merges these into its transaction before running
  // the handler. Matches the FunctionRunner trait's
  // `existing_writes: FunctionWrites` arg.
  optional FunctionWrites existing_writes = 10;
}

message ExecuteResponse {
  // ... existing fields ...

  // Reads + writes the worker accumulated while running.
  // Backend applies these through its Committer.
  // None for actions (no tx) and for errors where no tx was opened.
  optional FunctionFinalTransaction final_tx = 5;
}

message FunctionFinalTransaction {
  uint64 begin_timestamp = 1;
  FunctionReads reads = 2;         // already exists in pb::
  FunctionWrites writes = 3;       // already exists in pb::
  map<string, uint64> rows_read_by_tablet = 4;
}
```

`pb::FunctionReads` / `pb::FunctionWrites` types already exist
because Funrun uses them for the JS path. Reuse as-is.

### 6.2 Worker registration

Workers connect to the backend on startup and register. Two
options for who initiates:

- **Pull (backend polls)**: backend watches a DNS name for
  worker pods, connects outward. Requires SRV-record or k8s
  service discovery.
- **Push (worker calls backend)**: worker binary takes
  `CONVEX_BACKEND_ENDPOINT=grpc://backend:5678` and registers
  itself on start. Autoscaler-friendly; the new pod shows up
  in the pool within seconds of scheduling.

**Recommendation: push.** Backend exposes a
`WorkerAdmissionService` that workers dial.

```proto
service WorkerAdmissionService {
  // Long-lived streaming RPC. Worker keeps the stream open for
  // its whole lifetime; backend uses it to push registry-floor
  // updates, drain signals, etc. Backend marks the worker as
  // un-registered when the stream closes.
  rpc Register(stream WorkerToBackend) returns (stream BackendToWorker);
}

message WorkerToBackend {
  oneof msg {
    RegistrationEnvelope register = 1;   // sent once, first msg
    WorkerStatus status = 2;              // sent periodically
  }
}

message RegistrationEnvelope {
  // The worker's gRPC endpoint the backend will dispatch to.
  // "dns:worker-a.convex.svc.cluster.local:4567" or similar.
  string execute_endpoint = 1;
  string registry_version = 2;
  WorkerKind kind = 3;   // NATIVE_RUST | JAVASCRIPT
  FunctionInventory inventory = 4;
}

message FunctionInventory {
  repeated FunctionRegistration functions = 1;
  DatabaseSchema schema = 2;              // derived from #[derive(ConvexDocument)]
  repeated HttpRouteRegistration routes = 3;
  repeated CronRegistration crons = 4;
}

message BackendToWorker {
  oneof msg {
    DrainNotice drain = 1;
    RegistryFloorUpdate floor_update = 2;
  }
}
```

The inventory types are straight conversions of
`NativeFunctionRegistration`, `DatabaseSchema`,
`HttpRouteRegistration`, `CronRegistration` — all already
exist in the native crate.

### 6.3 Schema / registry reconciliation

When workers register, their inventories may disagree. Policy:

1. **Workers with the same `registry_version` MUST have identical
   inventories** (same functions, same schema, same routes, same
   crons). Enforced via SHA-256 hash of the canonicalized
   `FunctionInventory`. Mismatches are hard errors.
2. **Workers with different `registry_version`s** are tolerated
   during rollouts. Backend maintains one "active" inventory —
   typically the highest-version inventory that has quorum
   (≥ some operator-configured fraction of the pool). Requests
   dispatch only to workers matching the active version.
3. **`min_registry_version` floor** (already exists in the proto)
   is the operator's lever during a rollout: set it to the new
   version, backend stops dispatching to older workers, drains
   them.

This is the same rolling-update shape Funrun uses in prod.

---

## 7. Request flow

### 7.1 Query

```
Client ─► Backend
          1. SyncWorker receives QueryRequest.
          2. Application::query_udf:
             a. database.begin(identity) → Transaction + begin_ts
             b. application_runner.run_query(...) →
                  WorkerPool::run_function(udf_type=Query, ts, ...)
             c. WorkerPool picks a worker, sends ExecuteRequest{name, args, begin_ts}.
             d. Worker opens its own Transaction at begin_ts,
                runs the handler, returns ExecuteResponse{result, final_tx}.
             e. WorkerPool converts final_tx → FunctionFinalTransaction.
          3. Application returns (final_tx, outcome, usage) up.
          4. SubscriptionManager registers the subscriber with
             final_tx.reads as the subscription's read set.
          5. Result streamed back to client.
```

Queries don't commit (they're read-only) — their `final_tx` is
only used for subscription tracking.

### 7.2 Mutation

```
Client ─► Backend
          1. SyncWorker receives MutationRequest.
          2. Loop (OCC retry):
             a. database.begin(identity) → Transaction + begin_ts
             b. application_runner.run_mutation(...) →
                  WorkerPool::run_function(udf_type=Mutation, ts, ...)
             c. Worker runs, returns final_tx.
             d. Backend merges final_tx.writes into the backend's tx.
             e. database.commit(tx) — Committer does OCC:
                  - If conflict, break and retry from (a) with new ts.
                  - If success, exit loop.
          3. Committer appends to LogWriter.
          4. SubscriptionManager fans out InvalidationEvents to any
             subscriber whose read set intersects final_tx.writes.
          5. Result streamed back to client.
```

**The Committer sees every mutation — across all workers, all
deployments on this backend.** That's where OCC serializability
comes from. Workers can't cause a lost update because their
writes only become durable when the backend's Committer accepts
them.

### 7.3 Action

```
Client ─► Backend
          1. Backend picks a worker, sends ExecuteRequest{name, args}.
             (No begin_ts — actions don't have a tx.)
          2. Worker runs action. If the action calls
             ctx.run_query() / ctx.run_mutation() / ctx.scheduler().run_after(),
             the worker's WorkerActionCallbacks route the sub-call
             back to the backend (via a callback gRPC, see 7.4).
          3. Worker returns result.
          4. Backend forwards to client.
```

### 7.4 Action sub-calls (important)

Today's `WorkerActionCallbacks` commits sub-mutations locally on
the worker. **That's wrong** under this plan. Sub-calls from
actions must route through the backend so OCC + subscription
invalidation works.

New RPC: `BackendCallbackService`, worker-side client, backend-
side server:

```proto
service BackendCallbackService {
  rpc RunQuery(RunQueryRequest) returns (RunQueryResponse);
  rpc RunMutation(RunMutationRequest) returns (RunMutationResponse);
  rpc Schedule(ScheduleRequest) returns (ScheduleResponse);
  rpc StorageStore(stream StorageStoreChunk) returns (StorageStoreResponse);
  // etc.
}
```

When a native action calls `ctx.run_mutation(CreateUser, args)`,
the worker-side callbacks dial the backend and invoke the
mutation through the normal commit path (which may itself
dispatch to another worker). The mutation's writes land in the
backend's Committer, not the originating worker's.

### 7.5 HTTP actions

HTTP actions hit the backend's public HTTP server. The backend
routes by method + path against the merged registry (learned
from workers at registration time), picks a worker, forwards.

```
Client ─► Backend:443 POST /api/webhooks/stripe
             │
             ▼
          router.rs matches "/api/webhooks/stripe" against
          the merged HttpRouter, gets handler name "stripe_webhook"
             │
             ▼
          WorkerPool::dispatch("stripe_webhook", UdfType::HttpAction, ...)
             │
             ▼
          Worker runs handler, returns HttpResponse
             │
             ▼
          Backend forwards to client
```

### 7.6 Scheduled jobs + crons

Scheduler state lives in the backend's DB (`_scheduled_functions`
table). The scheduler worker (in-process on the backend) polls
it, dispatches jobs at fire time via `WorkerPool`. Same for
crons — backend's cron worker runs schedules, dispatches.

This means scheduling semantics are identical to `local_backend`:
- A mutation scheduling a job commits atomically with the rest
  of its writes (the backend's Committer owns both).
- `ctx.scheduler().cancel(id)` in another mutation works
  transactionally.

Workers never see the `_scheduled_functions` table directly.

---

## 8. Bootstrap / registration lifecycle

```
Worker binary starts up
    │
    ├── reads CONVEX_BACKEND_ENDPOINT (mandatory)
    ├── reads CONVEX_WORKER_BIND_ADDR (gRPC port for the backend to dial back)
    │
    ├── dial backend's WorkerAdmissionService
    ├── open bidirectional stream
    ├── send RegistrationEnvelope {
    │       execute_endpoint: self_advertised_addr,
    │       registry_version: convex_native::VERSION,
    │       kind: WorkerKind::NATIVE_RUST,
    │       inventory: { functions, schema, routes, crons },
    │   }
    │
    ├── wait for backend ack
    ├── start serving FunctionExecutionService on CONVEX_WORKER_BIND_ADDR
    │
    └── loop:
        ├── send WorkerStatus every N seconds (in-flight, cpu, etc.)
        ├── handle BackendToWorker msgs:
        │     - DrainNotice → flip readiness, begin drain
        │     - RegistryFloorUpdate → informational
        └── on stream close → shut down

Backend startup
    │
    ├── load persistence
    ├── Database::load
    ├── start HTTP + WebSocket + admission services
    ├── sit idle (no workers registered = no routing table)
    │
    ├── first worker registers
    ├── validate inventory (hash, schema compat, etc.)
    ├── install merged registry → HTTP router / function dispatch / cron worker
    ├── accept client traffic
```

**Backend serves 503 "no workers available" for any function
name not covered by the current pool.** Operator sees this in
metrics; indicates rollout problem.

---

## 9. Deployment flow

### 9.1 Initial deploy

```sh
# Publish once, pull for every deployment.
docker pull getconvex/convex-backend:1.0.0

# Build the worker image once per code change.
docker build -t myco/my-app-worker:v1 .

# Start the backend — no app code inside.
docker run -d --name convex-backend \
  -e CONVEX_DB_URL=postgres://... \
  -e CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678 \
  -p 3210:3210 \
  getconvex/convex-backend:1.0.0

# Start workers — they carry your code.
docker run -d --name worker-a \
  -e CONVEX_BACKEND_ENDPOINT=grpc://convex-backend:5678 \
  -e CONVEX_WORKER_BIND_ADDR=worker-a.example.com:4567 \
  myco/my-app-worker:v1

# Repeat for N workers. Autoscaler takes it from here.
```

### 9.2 Deploying new code

```sh
# Build new worker image.
docker build -t myco/my-app-worker:v2 .

# Push to registry.
docker push myco/my-app-worker:v2

# Roll the worker deployment (kubectl set image / ECS update / etc.).
# Backend observes:
#   - v2 workers register → inventory SHA differs → they wait
#     for quorum before the backend accepts them as "active"
#   - operator bumps CONVEX_MIN_REGISTRY_VERSION=v2 on the backend
#     via admin RPC or ConfigMap refresh
#   - backend routes only to v2 workers
#   - v1 workers drain + exit
#
# Backend never restarted. Client connections preserved.
```

**If schema changed between v1 and v2**, the backend does the
migration at the point the floor flips:

1. Pre-flip: both inventories known. Backend can show a diff.
2. Operator manually triggers schema migration (or automated
   if non-destructive).
3. Backend applies schema changes through the existing
   `schema_registry` module.
4. Then floor flips — v2 is live.

### 9.3 Autoscaling

Workers are stateless. HPA on CPU / in-flight count works
directly. Backend handles registrations/deregistrations
gracefully:

- Worker scales up → registers → enters pool within seconds.
- Worker scales down → gets SIGTERM → sends a DrainNotice of its
  own ("I'm shutting down") → backend stops routing to it →
  worker finishes in-flight calls → exits.
- Autoscaler-initiated termination is indistinguishable from
  operator-initiated.

---

## 10. JS interop

JS workers implement the same protocol, register against the
same backend, join the same pool. Different `WorkerKind` in the
registration envelope:

- `NATIVE_RUST` — workers built on `convex_native_distributed`.
  Registry comes from `NativeFunctionRegistry::collect()`.
- `JAVASCRIPT` — workers built on the V8/isolate stack. Registry
  comes from analyzing the uploaded JS bundle.

From the backend's perspective they're interchangeable — both
answer `FunctionExecutionService::Execute`. The backend doesn't
care what runtime the worker uses; it just wants
`FunctionFinalTransaction` back.

A deployer who has some JS and some Rust code can run both
worker kinds in the same pool. Backend routes each function by
name to a worker that advertised that name. If both kinds
advertise the same name (ambiguous), the worker admission
service rejects the later one — deployer picks which runtime
owns each name.

This also means **the backend image can eventually ship without
any V8 at all**, because JS dispatch happens in JS workers, not
in the backend. Not a priority for v1 but architecturally
clean.

---

## 11. Observability

All three sinks already designed for `convex_native_distributed`
apply unchanged:

- `NativeMetricsSink` (worker-side): per-function latency +
  outcome.
- `ConductorMetricsSink` → renamed `WorkerPoolMetricsSink`
  (backend-side): per-dispatch worker label + outcome.
- `ConductorLogSink` → renamed `WorkerPoolLogSink` (backend-side):
  forwards worker `ctx.log()` into the backend's log pipeline.

Plus new ones:

- `WorkerAdmissionSink`: fires on register / deregister / schema
  mismatch / version mismatch. Operators alert on churn.
- Pool size gauge: number of workers currently in the pool,
  broken down by `registry_version`.
- Inventory diff: when a new `registry_version` appears, log the
  set of added / removed / changed functions so the deploy log
  is self-documenting.

---

## 12. Failure modes

### 12.1 No workers registered

Backend accepts HTTP requests, returns 503 with a clear
diagnostic. Clients retry. WebSocket connections stay open but
can't dispatch.

### 12.2 Worker dies mid-dispatch

The `WorkerPool::run_function` wraps each call with P2C +
single-retry failover (already built on `DistributedFunctionRunner`).
If the worker's stream closes, the pool removes it; in-flight
calls to it get `Unavailable` and retry against a sibling.
Client sees one transient error at most; the backend's OCC
retry loop handles mutations.

### 12.3 Worker returns `final_tx` that fails OCC

Backend re-dispatches with a fresh `begin_ts`. Retry count
is bounded (`COMMIT_MAX_RETRIES` already exists in `database/`).
After exhaustion, the client sees a genuine conflict error.

### 12.4 Schema drift

Worker registers with an inventory whose SHA doesn't match the
active pool. Backend rejects the registration with a clear
error. Operator sees the rejection in logs; deploy pipeline
halts.

### 12.5 Backend restart

All clients disconnect. Workers see the admission stream close;
they enter a reconnect loop. When the backend comes back,
workers re-register automatically. Clients reconnect and
resubscribe.

Not zero-downtime — this is why backend upgrades are rare
(image rev bumps) and done during maintenance windows. Workers
upgrade zero-downtime.

### 12.6 Long OCC conflict chains

If a hot table causes chronic conflicts, the backend's retry
loop burns CPU and dispatch bandwidth. Mitigation is
application-layer (partition the hot table, fine-grained
ranges, etc.) — same as any Convex backend today. Worth
exposing per-function retry-count metrics so the hot spot is
visible.

---

## 13. Persistence access by workers

Workers need to read at `begin_ts`. Two options analyzed.

### Option A — Shared persistence (recommended for v1)

Workers have direct Postgres / SQLite credentials. They call
`Database::load(persistence, ...)` just like the backend does,
but NEVER `commit`.

**Pros:**
- Simple. No new protocol.
- Fast. Reads don't round-trip through the backend.
- Reuses all existing `Database<RT>` machinery.

**Cons:**
- Workers need DB credentials. Defence-in-depth: give them a
  read-only Postgres role.
- Workers see retention-managed data they might not need.

### Option B — IndexReader proxy (later)

Backend exposes an `IndexReaderService` gRPC; workers query it
for document reads. The proxy applies the same retention +
access rules the backend does.

**Pros:**
- Workers don't have DB credentials. Attack surface shrinks.
- Backend can cache aggressively.

**Cons:**
- New protocol. More code.
- Extra network hop per read. Workers would want to cache too.
- Funrun started with Option A and migrated to B for
  multi-tenant reasons. Single-tenant doesn't need it.

**Decision: ship v1 with Option A, layer Option B on later if
security requirements demand it.**

---

## 14. What's wrong with the current `convex_native_distributed`

For a clean slate understanding, here's exactly where today's
code diverges from this plan:

1. **Worker commits locally**: `server.rs:228` calls
   `database.commit_with_write_source(tx, ...)`. Must be
   removed — workers must return `final_tx` instead.
2. **`ExecuteResponse` doesn't carry `final_tx`**: proto
   extension needed.
3. **`DistributedFunctionRunner` doesn't impl
   `FunctionRunner`**: can't plug into `Application`. Needs
   `impl FunctionRunner<ProdRuntime>` with a proto round-trip
   in `run_function`.
4. **No admission / registration protocol**: pool is fixed at
   construction. Needs to become dynamic with `WorkerAdmissionService`.
5. **Inventory is baked into the backend**:
   `NativeFunctionRunner::from_inventory()` runs in the backend
   today (via the composite runner). Must move so only workers
   run it; backend builds its registry from registration msgs.
6. **Action sub-calls commit locally** via `WorkerActionCallbacks`.
   Need a `BackendCallbackService` so sub-calls flow back to the
   backend's Committer.
7. **`CronRegistry::collect()` runs in the backend**: same
   issue as #5. Crons come from workers.
8. **HTTP routes are baked in**: same issue. Backend builds its
   HTTP router from worker-reported routes.

**None of this is conceptually hard. It's work.**

---

## 15. Phased delivery

Each phase is independently shippable and leaves the codebase
in a working state.

### Phase 1 — Wire contract (proto + conversions)

Two days of work. Unblocks every subsequent phase.

- Add `begin_timestamp`, `existing_writes`, `final_tx` fields to
  `ExecuteRequest` / `ExecuteResponse`.
- Write proto conversions to/from `pb::FunctionReads` /
  `pb::FunctionWrites`.
- Round-trip tests.
- Worker stops committing: `run_query_inline` /
  `run_mutation_inline` return `final_tx`, don't call `commit`.
- `DistributedFunctionRunner::execute` returns
  `(ExecuteResponse, Option<FunctionFinalTransaction>)`.

After Phase 1: the mechanics of "worker runs, backend commits"
exist but aren't wired into `Application` yet.

### Phase 2 — FunctionRunner trait impl

Three days.

- `impl FunctionRunner<ProdRuntime> for DistributedFunctionRunner`
  — now a drop-in for `InProcessFunctionRunner`.
- `local_backend`: env var `CONVEX_NATIVE_WORKERS=grpc://...`
  swaps the runner from in-process to distributed.
- Integration test: backend + 1 worker, dispatch a mutation,
  assert the backend's `SubscriptionManager` received an
  `InvalidationEvent` when the mutation wrote.

After Phase 2: **OCC + subscriptions work over the distributed
path.** A deployer with a fixed worker list can already run a
reactive app.

### Phase 3 — Dynamic pool + admission

Four days.

- `WorkerAdmissionService` proto + server + worker client.
- `WorkerPool` replaces the `Vec<Arc<dyn WorkerClient>>` fixed
  pool. DashMap-keyed, churn-tolerant.
- Backend tolerates no-workers (503) and handles registrations
  mid-lifetime.
- Worker binary grows the registration streaming loop.

After Phase 3: autoscaling works. Operators don't have to
restart the backend to change the worker pool.

### Phase 4 — Backend callback service (action sub-calls)

Three days.

- `BackendCallbackService` proto + server + worker client.
- `WorkerActionCallbacks` routes every non-storage callback
  back to the backend. Storage still local-ish (backend serves
  uploads; workers don't).
- Integration tests: action with sub-mutation sees the
  sub-mutation's writes commit through the backend's Committer
  and trigger subscription invalidation.

### Phase 5 — Prebuilt backend image

Two days.

- Strip `NativeFunctionRunner::from_inventory()` call out of
  backend boot — backend starts with an empty registry and
  populates it from registration messages.
- Publish `getconvex/convex-backend` Dockerfile in this repo.
- CI / release pipeline for pushing the image to GHCR / Docker
  Hub on tag.

### Phase 6 — JS interop

Longer; depends on how much of Funrun's server-side is
open-sourceable. Minimum is a `WorkerKind::JAVASCRIPT`
marker + the existing isolate-based executor wrapped in the
new protocol.

### Phase 7 — Operator tools

Dashboard admin endpoints for pool introspection, manual floor
bump, inventory diff. Not strictly required for correctness
but makes operating the system tolerable.

---

## 16. Open questions

1. **Who owns Postgres credential distribution?** Worker needs
   persistence read creds. Does the backend broker them (via
   the admission handshake), or does the operator hand them to
   workers out-of-band (env var)? Leaning out-of-band for v1 —
   simpler.
2. **How strict is SHA-matching across `registry_version`?**
   Does adding a function bump the version? (Yes: any inventory
   change = new version.) Does a no-op formatter change? (Ideally
   no — hash normalized inventory structure, not source.)
3. **What happens if workers register conflicting schemas but
   each other's versions are compatible?** (They shouldn't;
   enforce via hash.)
4. **Does the backend need to commit writes staged by the
   scheduler worker differently from writes staged by a client
   mutation?** (No — all commits go through Committer, same
   pipeline.)
5. **Long-running actions and autoscaling:** if an action is
   in-flight and the worker gets SIGTERM, what's the contract?
   (Match today's `local_backend`:
   `run_until_completion_if_cancelled` on
   `FunctionCaller::Action` is false → actions may be
   interrupted. Document clearly.)
6. **Backend image ABI stability:** a worker built against
   framework vN must be compatible with backend vM. What's the
   compat matrix? (Start: worker minor ≥ backend minor;
   widen later.)

---

## 17. Client routing — final call

**Clients hit the backend.** One public endpoint, one DNS
record, one set of certs. Workers are internal gRPC-only.

This is:
- The topology Convex cloud runs.
- The topology this plan's Funrun parallel requires.
- The simplest topology to operate (one front door).
- The shape that preserves every subscription/OCC/auth invariant.

Any exception would cost more than it saves.

---

## 18. Summary table — before vs after

| Aspect | Today (`convex_native_distributed`) | This plan |
|---|---|---|
| Who commits? | Worker (wrong) | Backend |
| Who sees subscription invalidations? | Nobody | Backend |
| OCC | Per-worker | Global |
| Worker pool | Fixed at construction | Dynamic, auto-scales |
| Inventory location | Baked into backend | Ships in workers, learned at register time |
| Backend rebuild on deploy | Required | Never |
| Schema migration | Manual in `local_backend` | Controlled at registry-version flip |
| JS interop | None | Via `WorkerKind::JAVASCRIPT` |
| Client entry | One endpoint | One endpoint (backend) |
| Reactive queries | Broken | Work |
| Can be published as container image | No (requires deployer's code) | Yes |

---

## Related reading

- `STATUS.md` — current shipped surface.
- `DEPLOYMENT.md` — operational guide for today's topologies.
- `COMPOSITE_RUNNER.md` — composite runner's current dispatch
  logic, replaced in this plan by `WorkerPool`.
- `crates/database/README.md` — Committer + SubscriptionManager
  internals (unchanged by this plan, critical to understand).
- `crates/pb/protos/function_execution.proto` — current proto,
  needs the additions in §6.1 here.
- `crates/function_runner/src/lib.rs:84` — the `FunctionRunner`
  trait the `WorkerPool` must implement.
