# convex-native examples

Deployment artifacts for the distributed topology. All phases
of `DISTRIBUTED_PLAN.md` have shipped; these examples are the
ready-to-deploy templates a deployer starts from.

## What's here

```
examples/
├── README.md                        this file
└── deploy/
    ├── docker/
    │   ├── Dockerfile.backend       prebuilt backend image
    │   │                             (Phase 5). Publishes as
    │   │                             `getconvex/convex-backend`
    │   │                             once CI/release lands;
    │   │                             build locally today.
    │   └── Dockerfile.worker        per-deployer worker image.
    │                                 Links the deployer's
    │                                 `#[convex::*]` handlers
    │                                 + sets CONVEX_BACKEND_ENDPOINT.
    ├── kubernetes/
    │   ├── backend-deployment.yaml  Namespace + PVC + single-
    │   │                             replica Deployment + two
    │   │                             Services (public-HTTP +
    │   │                             internal-admission).
    │   └── worker-deployment.yaml   Deployment + headless
    │                                 Service for the worker
    │                                 pool; auto-registers
    │                                 against the backend.
    └── systemd/
        └── convex-worker.service    plain-VM worker unit
```

## Two-image deploy

```
                  ┌──────────────────────────────────┐
 Client ─HTTPS/WS─┤  getconvex/convex-backend:X.Y.Z  │◀── admin CLI
                  │  (no deployer code)              │    (loopback only)
                  └──────────┬───────────────────────┘
                             │ gRPC ·5678
                             │ (WorkerAdmissionService +
                             │  BackendCallbackService)
                   ┌─────────┴─────────┐
                   ▼                   ▼
          ┌─────────────────┐  ┌─────────────────┐
          │ myco/my-worker  │  │ myco/my-worker  │   … (scales dynamically)
          │ (links handlers)│  │ (same image)    │
          └─────────────────┘  └─────────────────┘
```

Backend rolls on its own cadence (rare; maintenance-window-grade
since it's stateful). Worker image rolls whenever deployer code
changes — the backend doesn't restart.

## Quick local smoke-test

```sh
# 1. Build the backend image.
docker build \
  -f convex-native/examples/deploy/docker/Dockerfile.backend \
  -t getconvex/convex-backend:dev .

# 2. Build your worker image.
docker build \
  -f convex-native/examples/deploy/docker/Dockerfile.worker \
  -t myco/my-worker:dev .

# 3. Docker network so the worker can dial the backend by name.
docker network create convex-net

# 4. Boot the backend. Defaults bind admission to 0.0.0.0:5678.
docker run -d --name convex-backend --network convex-net \
  -e CONVEX_ADMIN_BIND_ADDR=127.0.0.1:9090 \
  -p 3210:3210 -p 5678:5678 \
  getconvex/convex-backend:dev

# 5. Boot the worker pointing at the backend.
docker run -d --name worker-a --network convex-net \
  -e CONVEX_BACKEND_ENDPOINT=http://convex-backend:5678 \
  -e CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
  myco/my-worker:dev

# 6. Admin probe (pool snapshot).
docker exec convex-backend curl -s http://127.0.0.1:9090/admin/pool | jq .
```

Scale workers with more `docker run` invocations; `kubectl
apply` the manifests in `deploy/kubernetes/` for a real cluster.

## Rolling-update flow

1. Build worker image at version `v2`. Push.
2. `kubectl set image deployment/convex-worker worker=myco/my-worker:v2`
   — v2 pods come up alongside v1.
3. Bump the pool floor:
   ```sh
   curl -X POST http://127.0.0.1:9090/admin/pool/floor \
     -d '{"min_registry_version":"v2"}'
   ```
4. Backend stops routing new dispatches to v1 workers. Inventory
   diff logs to `tracing::info!(target="convex_admission", …)`
   on each new version admitted.
5. Optionally trigger drains on specific workers:
   ```sh
   curl -X POST http://127.0.0.1:9090/admin/pool/drain \
     -d '{"worker_id":3,"reason":"v1 retirement"}'
   ```
6. The backend never restarts. Clients feel zero impact.

## Monolith alternative

For small deployments or local development without Docker, the
monolith topology (`local_backend` as a library, with your
handlers linked in, no worker pool) is still fully supported.
See `STANDALONE.md`. The native + distributed crates are
additive; you opt into the distributed path by setting
`CONVEX_ADMISSION_BIND_ADDR` on the backend + running separate
worker binaries.

## Cross-references

- `DISTRIBUTED_PLAN.md` — full architecture + phase breakdown.
- `STATUS.md` — per-substep shipped-vs-outstanding tracker.
- `DEPLOYMENT.md` — operational guide (env vars, rolling
  updates, observability).
- `USAGE.md` §19 — developer-facing deployment reference with
  complete env-var matrix.
- `STANDALONE.md` — monolith topology recipe.
