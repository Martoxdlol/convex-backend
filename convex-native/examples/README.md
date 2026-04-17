# convex-native examples

Most of what was in this directory has been removed because it
described the "standalone worker commits locally" topology that
the `DISTRIBUTED_PLAN.md` replan supersedes. Only artifacts that
remain approximately correct under the target architecture are
kept here; everything else comes back once Phase 3 (dynamic
worker pool + admission) lands.

## What's here now

```
examples/
├── README.md                       this file
└── deploy/
    ├── docker/
    │   └── Dockerfile.worker       multi-stage build for a worker
    │                                (still approximately right —
    │                                 needs CONVEX_BACKEND_ENDPOINT
    │                                 once Phase 3 is live)
    ├── kubernetes/
    │   └── worker-deployment.yaml  Deployment + headless Service
    │                                for worker pods (same caveat)
    └── systemd/
        └── convex-worker.service   plain-VM worker unit
```

## What was removed

| File | Why |
|------|-----|
| `minimal_app/` | Reading sample for the old library-mode + local_backend shape. Superseded by the prebuilt-backend-image path in `DISTRIBUTED_PLAN.md` Phase 5. |
| `full_app/` | Workspace-member example that tangled worker + standalone modes around the old topology. Will return as a pure worker template after Phase 3. |
| `deploy/docker/Dockerfile.conductor` | The "standalone conductor" concept is gone. The backend image (Phase 5) replaces it. |
| `deploy/docker/docker-compose.yml` | Wired a conductor + worker stack. Will come back wired against the published backend image. |
| `deploy/kubernetes/conductor-deployment.yaml` | Same. |
| `deploy/systemd/convex-conductor.service` | Same. |

## What still builds + runs

Nothing standalone from the distributed crate today. The
previous example binaries (`conductor`, `conductor_dispatch`,
`worker_with_functions`) have been removed because they all
relied on the worker-commits-locally shape; the one remaining
`worker.rs` in `crates/convex_native_distributed/examples/`
still builds but it has no functions registered so it's a
no-op smoke test, not a demo.

For a complete, runnable Convex native app today, the path is
**Topology A — monolith**: `local_backend` as a library, with
your functions linked in. See `STANDALONE.md`.

## When does a proper deploy example come back?

Per `DISTRIBUTED_PLAN.md` §15, the phased delivery puts usable
operator artifacts in view at the following points:

- **After Phase 2**: a `local_backend` binary with
  `CONVEX_NATIVE_WORKERS=grpc://...` set will dispatch to
  remote workers. A docker-compose that demonstrates this
  lands with Phase 2.
- **After Phase 3**: dynamic worker pool. Workers register via
  `CONVEX_BACKEND_ENDPOINT`. Full example stack (backend + N
  workers + autoscale manifest) lands with Phase 3.
- **After Phase 5**: prebuilt backend image
  (`getconvex/convex-backend`). Deployers stop building the
  backend; the examples directory gets a README that reads
  "pull the image + copy this Dockerfile.worker."

## Interim guidance

If you need to deploy anything right now and can't wait:

1. Follow `STANDALONE.md`. The monolith path works today, all
   Convex semantics intact.
2. Use `Dockerfile.worker` + `worker-deployment.yaml` as a
   future-compatible starting point for a worker image, but
   expect to swap env vars (`CONVEX_WORKER_ENDPOINTS` goes
   away; `CONVEX_BACKEND_ENDPOINT` takes over) when Phase 3
   lands.
3. Treat `DISTRIBUTED_PLAN.md` as the north star for what the
   surface will look like.
