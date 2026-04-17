# convex-native examples

End-to-end examples for every deployment mode plus copy-paste
production artifacts. Read `DEPLOYMENT.md` first for the
conceptual overview of the three topologies; this directory is
the runnable / deployable counterpart.

## Directory layout

```
examples/
├── full_app/                  Workspace-member crate — fully buildable
│   ├── Cargo.toml             (cargo run -p convex_full_app_example)
│   ├── Dockerfile
│   ├── deploy/
│   │   ├── docker-compose.yml
│   │   └── kubernetes.yaml
│   └── src/
│       ├── main.rs            CONVEX_MODE-switched entry point
│       ├── lib.rs
│       ├── schema.rs          ConvexDocument / ConvexEnum / ConvexNested
│       ├── queries.rs         3 queries
│       ├── mutations.rs       3 mutations (incl. scheduler use)
│       ├── actions.rs         2 actions (incl. internal)
│       ├── http.rs            HTTP action
│       └── crons.rs           1 cron
│
├── minimal_app/               Reading sample: deployer-project layout
│   └── src/main.rs            (NOT a workspace member; shows the shape
│                               a copy-out-of-repo project would take)
│
└── deploy/                    Topology-level artifacts (docker / k8s /
    ├── docker/                systemd). Use full_app/Dockerfile for a
    │   ├── Dockerfile.worker  single-crate build; these are for when
    │   ├── Dockerfile.conductor  your project has its own layout.
    │   └── docker-compose.yml
    ├── kubernetes/
    │   ├── worker-deployment.yaml
    │   └── conductor-deployment.yaml
    └── systemd/
        ├── convex-worker.service
        └── convex-conductor.service
```

Runnable *in this workspace* (no copy-paste required):

| Crate + example | What it does |
|-----------------|--------------|
| `convex_full_app_example` | **Start here.** Full buildable app — schema, queries, mutations, actions, HTTP, cron. Runs as introspection-print or gRPC worker. See `full_app/README.md`. |
| `convex_native --example tiny_app` | Prints `describe_pretty()` — schema / functions / routes / crons introspection. Doesn't boot a server. |
| `convex_native_distributed --example worker` | Empty-registry gRPC worker — template for custom worker binaries. |
| `convex_native_distributed --example worker_with_functions` | Worker that actually registers `#[convex::query/mutation/action]` handlers and demonstrates `WorkerActionCallbacks` end-to-end. |
| `convex_native_distributed --example conductor` | Probes each worker's Health and prints a summary. |
| `convex_native_distributed --example conductor_dispatch` | End-to-end round trip — dispatches an action to the worker pool and prints the response + captured log lines. |

---

## 1. Running the distributed topology end-to-end

The fastest way to see every deploy-relevant piece talking to
every other piece:

```sh
# One-time: build every example.
cargo build --examples -p convex_native_distributed

# Terminal 1 — start the worker with real functions registered.
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
  RUST_LOG=info \
  ./target/debug/examples/worker_with_functions

# Terminal 2 — probe the worker pool's health.
CONVEX_MODE=conductor \
  CONVEX_WORKER_ENDPOINTS=http://127.0.0.1:4567 \
  ./target/debug/examples/conductor

# Terminal 2 (continued) — actually dispatch a call and print the
# response + worker log lines forwarded through ConductorLogSink.
CONVEX_MODE=conductor \
  CONVEX_WORKER_ENDPOINTS=http://127.0.0.1:4567 \
  ./target/debug/examples/conductor_dispatch alice bob
```

Expected output from `conductor_dispatch`:

```
touch(alice) => 0 widget(s)
touch(bob)   => 0 widget(s)
── worker log output ──
[http://127.0.0.1:4567 Action] [INFO] touch: owner="alice" has 0 widget(s)
[http://127.0.0.1:4567 Action] [INFO] touch: owner="bob"   has 0 widget(s)
```

If the worker in Terminal 1 wasn't built with `.with_database(db)`,
the action's `run_query` sub-call bails with "no callbacks
attached" — that's the `WorkerActionCallbacks` gap. In production
you'd wire a `Database<Rt>` into the worker (see
`FunctionExecutionServer::with_database(...)`); the
`worker_with_functions` example runs without one by default to
keep the build-and-run loop short.

---

## 2. Standalone topology

The full "backend in one process" path — `local_backend` serving
HTTP + WebSocket + the composite runner. Two sub-paths:

### 2.a. Library mode (no fork)

See `STANDALONE.md` for the complete recipe. Short version:

1. Create your own crate outside this repo.
2. Depend on `local_backend` + `convex_native` + a handful of
   transitive crates from this workspace (git-revved).
3. Copy the `main.rs` template from STANDALONE.md §4 — it's a
   minor adaptation of `crates/local_backend/src/main.rs` with
   a single load-bearing `use my_convex_app as _;`.
4. `cargo run -- --port 3210 --instance-name ...`.

`minimal_app/` in this directory is the copy-paste starting
point for that layout.

### 2.b. In-tree fork

Drop your functions inside `crates/local_backend/src/` and add
`mod my_app;` to `lib.rs`. That's one line of repo surgery; the
inventory registrations get linked automatically. See
`STANDALONE.md` §8 for the trade-offs.

---

## 3. Container / Kubernetes / systemd

### Docker (`deploy/docker/`)

Multi-stage build with a `rust:1.82-slim` builder and a
`debian:bookworm-slim` runtime. `Dockerfile.worker` /
`Dockerfile.conductor` are separate so you can tag them
independently and roll them independently.

```sh
cd deploy/docker
# Customise the `cargo build --bin my_worker` line for your
# binary, then:
docker compose up --build
```

`docker-compose.yml` spins up one conductor + two workers on
the same host. Worker-b's port maps to `4568` on the host so
both containers are reachable from outside the compose
network.

### Kubernetes (`deploy/kubernetes/`)

`worker-deployment.yaml` has the full rolling-update story:
`maxSurge: 1, maxUnavailable: 1`, `terminationGracePeriodSeconds:
60` to outlast the longest per-function timeout, and a headless
`Service` so the conductor resolves directly to pod IPs.

`conductor-deployment.yaml` is single-replica by default; pins
`CONVEX_MIN_REGISTRY_VERSION` from an env var so a deploy
pipeline can bump the floor before rolling the worker image.

```sh
cd deploy/kubernetes
# Swap IMAGE_TAG in both files before applying.
kubectl apply -f worker-deployment.yaml
kubectl apply -f conductor-deployment.yaml
```

### systemd (`deploy/systemd/`)

Plain-VM deploy. `convex-worker.service` + `convex-conductor.service`
are hardened with `NoNewPrivileges`, `ProtectSystem=strict`, etc.
Drop into `/etc/systemd/system/` and `systemctl enable --now`.

---

## 4. Rolling update demo

On top of any of the three container-style topologies:

1. Build `myco/convex-worker:v2` with the new registry.
2. Conductor side:
   ```
   CONVEX_MIN_REGISTRY_VERSION=v2
   ```
   (bumped before the rollout starts). Old `v1` workers keep
   serving in-flight calls but reject new ones with
   `Code::FailedPrecondition` — which `DistributedFunctionRunner`
   surfaces as an `Error(FailedPrecondition)` through the
   `ConductorMetricsSink`, letting the deploy pipeline watch
   for the error rate to settle.
3. `kubectl set image deploy/convex-worker worker=myco/convex-worker:v2`
   (or equivalent for your orchestrator). The new workers boot,
   announce `registry_version=v2`, start accepting traffic.
4. When the old pods have drained, the deploy completes.

`DEPLOYMENT.md §3` has the recommended sequence + the knobs that
make each step safe.

---

## 5. Cross-references

- `DEPLOYMENT.md` — conceptual guide + full env-var / metrics
  reference.
- `STANDALONE.md` — library-mode recipe for topology 1.
- `COMPOSITE_RUNNER.md` — how the `local_backend` composite
  runner plugs into the function dispatch path (topology 1).
- `STATUS.md` — what's shipped; any deployment feature not
  called out here is tracked there.
