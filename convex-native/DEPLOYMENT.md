# Deployment

## Current state — planning reset

The project is mid-rebuild. The topologies that were documented
here previously (standalone conductor + workers, worker-commits-
locally) are gone because they conflicted with the coordination
guarantees Convex requires.

**For the real target architecture read `DISTRIBUTED_PLAN.md`
first.** This file describes the operational surface as it lands
during each phase.

## Topologies

### Topology A — Monolith (`local_backend` with native functions linked in)

**Available today.** All of Convex's semantics work (OCC,
subscriptions, reactivity) because dispatch is in-process.

The path:

1. Deployer depends on `local_backend` + `convex_native` as
   library crates.
2. Writes their `#[convex::query/...]` registrations.
3. Builds their own binary that `use`s the registrations (forces
   inventory linkage) and calls `make_app()`.

See `STANDALONE.md` for the full recipe.

Trade-off: monolithic binary, one process. Rebuild on every code
change. Requires V8 / `rush install` because `local_backend`
pulls in `isolate`. Fine for self-host of small-medium scale;
doesn't meet the "prebuilt backend image + deploy = roll workers"
requirement the new plan is built around.

### Topology B — Backend + dynamic worker pool (DISTRIBUTED_PLAN.md)

**Mostly landed.** Phases 1–4 complete; Phase 5 (prebuilt image
+ release pipeline) is in progress.

Target shape:

```
Client ── Backend (prebuilt container image) ──gRPC──► Worker pool
            │
            ▼
        Persistence
```

- One **backend image** published by the Convex project; carries
  no deployer code. Runs the public HTTP + WebSocket surface, owns
  `Database<RT>` + Committer + SubscriptionManager + scheduler +
  crons.
- **Worker images** built by the deployer; carry every
  `#[convex::*]` registration + derived schema. Register their
  inventory with the backend on startup.
- Clients hit the backend only. Workers are internal gRPC.
- Worker pool is dynamic: pods join and leave under an
  autoscaler's control. Backend tolerates churn.
- Deploys: build a new worker image → roll the pool. Backend
  never rebuilds.

See `DISTRIBUTED_PLAN.md` for the full protocol, phase breakdown,
and decision rationale.

## Env var reference

All of these are consumed by the shipped binaries
(`convex-local-backend`, `convex-backend`, the worker
examples, and any deployer-built worker binary built on top
of `convex_native`).

| Var | Consumed by | Default | Effect |
|-----|-------------|---------|--------|
| `CONVEX_MODE` | worker/backend | `standalone` | `standalone` / `worker`. (Legacy `conductor` is rejected.) |
| `CONVEX_WORKER_BIND_ADDR` | worker | `0.0.0.0:4567` | gRPC bind address for `FunctionExecutionService`. |
| `CONVEX_BACKEND_ENDPOINT` | worker | *unset* | When set, worker dials `WorkerAdmissionService` at this URL and auto-registers. |
| `CONVEX_NATIVE_WORKERS` | backend | *unset* | Comma-separated fixed-pool endpoints (`grpc://host:port,...`). Phase-2 mode; superseded by admission when `CONVEX_ADMISSION_BIND_ADDR` is set. |
| `CONVEX_ADMISSION_BIND_ADDR` | backend | *unset* | Host:port to expose `WorkerAdmissionService` on. Enables the dynamic worker pool. |
| `CONVEX_ADMIN_BIND_ADDR` | backend | *unset* | Host:port to mount the admin HTTP surface. Bind loopback-only in production. |
| `CONVEX_BACKEND_CALLBACK_BIND_ADDR` | backend | *unset* | Host:port for `BackendCallbackService`. Workers dial this to route action sub-calls back to the Committer. |
| `CONVEX_BACKEND_CALLBACK_ENDPOINT` | worker | *unset* | URL of the backend's callback service. Matches the backend's `CONVEX_BACKEND_CALLBACK_BIND_ADDR`. |
| `CONVEX_MIN_REGISTRY_VERSION` | backend | *unset* | Initial floor for the admission pool. Workers below this `registry_version` aren't considered for dispatch. Operators can raise/lower at runtime via `POST /admin/pool/floor`. |
| `CONVEX_REFUSE_NATIVE_HANDLERS` | backend | *unset* | Any non-empty value fails boot when the binary has non-empty native inventory. Pin in the Phase-5 prebuilt image's Dockerfile to fail loud on accidental link-in. |

## Phased operational evolution

The operator experience changes phase by phase. Treat this as the
roadmap you're signing up for.

### Before Phase 1

- `convex_native_distributed` exists but commits locally. Don't
  run it for anything reactive. Fine for pure-RPC use if you
  understand what you're giving up.
- `local_backend` (Topology A) is the only working path.

### After Phase 1 (wire contract)

- Worker returns `FunctionFinalTransaction` instead of committing.
- Backend-side integration not wired yet. No operational change
  for deployers.

### After Phase 2 (FunctionRunner impl)

- `local_backend` gains `CONVEX_NATIVE_WORKERS=grpc://worker-a:4567,...`
  env var. With that set, the backend dispatches native
  functions to remote workers. Without it, functions dispatch
  in-process as today.
- Functional: OCC + subscriptions preserved over gRPC with a
  fixed worker list. No autoscale yet.

### After Phase 3 (dynamic pool + admission)

- Workers gain `CONVEX_BACKEND_ENDPOINT=grpc://backend:5678`.
  On startup they dial the backend's admission service and
  register their inventory.
- Backend's `CONVEX_NATIVE_WORKERS` env var goes away; the pool
  is discovered dynamically.
- Autoscale works out of the box.

### After Phase 4 (backend callbacks)

- Action sub-calls (`ctx.run_query(...)` etc.) work correctly
  over the distributed path. The backend's Committer owns
  every write, whether or not it originated inside an action.

### After Phase 5 (prebuilt backend image)

- `getconvex/convex-backend:X.Y.Z` published to a public
  registry. Deployers stop building `local_backend`; they
  pull the image and run it.
- Deployer effort reduces to "write functions, build worker
  image, roll the pool."

### After Phase 6 (JS interop)

- `WorkerKind::JAVASCRIPT` in the admission envelope. Deployers
  with mixed Rust + JS codebases run both kinds of worker
  against the same backend.

### After Phase 7 (operator tools)

- Pool inspector, inventory diff, manual `min_registry_version`
  floor bump over an admin RPC. Production-grade operations.
- Env var `CONVEX_ADMIN_BIND_ADDR=127.0.0.1:9090` mounts the
  admin HTTP surface:
  - `GET /admin/pool` — pool introspection JSON
    (total, by_version, by_kind, kind_preferences, per-worker
    detail).
  - `POST /admin/pool/floor {"min_registry_version":"X.Y.Z"}`
    — set rolling-update floor (null clears).
  - `POST /admin/pool/kind_preference
    {"function_name":"compute","kind":"native-rust"}` — pin
    per-function kind preference.
  - `POST /admin/pool/drain {"worker_id":N,"reason":"…"}`
    — trigger operator-initiated worker drain.
  - `GET /admin/crons` — list live `NativeCronDriver` jobs
    (name, schedule, target, kind). Returns 501 when no cron
    driver is attached.
  - `POST /admin/crons/remove {"name":"..."}` — drop a cron
    from the firing schedule. Idempotent.
  - Inventory diff on each registry_version change logged
    via `tracing::info!(target="convex_admission")`.
- Bind to loopback + expose through SSH/port-forward; do
  **not** expose this directly on the public network.

## Rolling-update semantics

Unchanged from `DISTRIBUTED_PLAN.md §9.2` once Phase 3 is live:

1. Build new worker image (v2).
2. Push.
3. Roll the worker Deployment (k8s / ECS / whatever).
4. v2 workers register, sit in the pool alongside v1.
5. Operator bumps the floor — either through the admin HTTP route
   `POST /admin/pool/floor {"min_registry_version":"v2"}` (no
   restart needed) or by updating the `CONVEX_MIN_REGISTRY_VERSION`
   env var and rolling the backend pod (env var is consulted at
   boot only — admin HTTP is the runtime path).
6. Backend routes new traffic to v2 only. v1 workers drain.
7. Autoscaler scales v1 replica count to zero.

Backend never restarts. Clients feel zero impact.

## Observability

Three sinks are already scaffolded in the distributed crate and
will survive the rebuild unchanged in shape:

- Worker-side per-function metrics (`NativeMetricsSink`).
- Backend-side per-dispatch metrics (currently
  `ConductorMetricsSink` — will be renamed
  `WorkerPoolMetricsSink` in Phase 3).
- Backend-side log forwarding (currently `ConductorLogSink` →
  `WorkerPoolLogSink`).

Plus new in Phase 3:

- Pool size gauge, broken down by registry version.
- Admission events sink (register / deregister / schema
  mismatch).
- Inventory diff log on registry-version changes.

## Bringing up Topology B locally (post Phase 5.1)

The Phase-5 backend image ships at
`convex-native/examples/deploy/docker/Dockerfile.backend`. Pair
it with the existing `Dockerfile.worker` to run the distributed
topology end-to-end on a single host for a smoke-test.

```sh
# 1. Build the prebuilt backend image once per release.
docker build \
  -f convex-native/examples/deploy/docker/Dockerfile.backend \
  -t getconvex/convex-backend:dev .

# 2. Build your worker image (it links your `#[convex::*]`
#    registrations).
docker build \
  -f convex-native/examples/deploy/docker/Dockerfile.worker \
  -t myco/my-app-worker:dev .

# 3. Create a user-defined network so the worker can dial the
#    backend by name.
docker network create convex-net

# 4. Boot the backend — it exposes the admission port by
#    default (0.0.0.0:5678).
docker run -d --name convex-backend --network convex-net \
  -p 3210:3210 -p 5678:5678 \
  getconvex/convex-backend:dev

# 5. Boot the worker pointing at the backend.
docker run -d --name convex-worker-a --network convex-net \
  -e CONVEX_BACKEND_ENDPOINT=http://convex-backend:5678 \
  -e CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
  -p 4568:4567 \
  myco/my-app-worker:dev

# 6. Verify. Logs on the backend show the worker registering +
#    its advertised function names.
docker logs convex-backend | grep 'admitted worker'
```

Scale out by running additional worker containers — the pool
admits them dynamically. Drop workers by `docker stop`-ing them;
the admission stream closes and the pool retires the worker
automatically.

## What's not here yet

- A deployer-facing "bring up a minimal stack" tutorial with a
  real-world schema + handlers (this file gives the skeleton;
  a fuller walkthrough lands with the Phase 5.3 CI/release
  polish).
- Published `getconvex/convex-backend:X.Y.Z` tagged image on a
  public registry. CI/release pipeline is the remaining
  Phase-5 deliverable.
- k8s manifest for the backend image. The existing
  `worker-deployment.yaml` in
  `convex-native/examples/deploy/kubernetes/` covers the
  worker-side shape; the backend Deployment + Service + PVC
  manifest lands with the CI/release push.
- Migration guide from Topology A to Topology B. Will land when
  CI publishes the first tagged image.

## Cross-references

- `DISTRIBUTED_PLAN.md` — target architecture.
- `STATUS.md` — exact state of the tree.
- `STANDALONE.md` — Topology A recipe (monolith).
- `USAGE.md` — developer-facing feature reference.
