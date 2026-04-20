# standalone_todo_app — runnable monolith example

A complete deployer crate that defines a `Todo` schema with
query / mutation / action handlers and boots the full Convex
backend in a single process via the batteries-included
`convex_native::run()` entry point. Use this as the starting
template when you follow `convex-native/STANDALONE.md`.

## Layout

```
standalone_todo_app/
├── Cargo.toml          3 deps: convex_native + convex_native_core
│                        + anyhow (convex_native owns tokio).
├── README.md           this file
└── src/
    ├── main.rs         5 lines: sync fn main → convex_native::run()
    ├── lib.rs          declares every app submodule.
    └── app/
        ├── schema.rs     #[derive(ConvexDocument)] Todo
        ├── queries.rs    #[convex::query] list_for_owner, count_pending
        ├── mutations.rs  #[convex::mutation] create, mark_done
        └── actions.rs    #[convex::action] summarise (calls count_pending)
```

## Why two framework deps?

`convex_native` is the batteries crate — re-exports the developer
surface (derives, ctx, prelude) and provides `run()`.
`convex_native_core` is the framework itself; it's a direct dep
because `#[derive(ConvexDocument)]` expands to code referencing
`::convex_native_core::__private::...`, and Cargo only resolves
absolute crate paths against direct dependencies. (The serde
ecosystem has the same constraint: you name both `serde` and, via
the `derive` feature, the proc-macro crate.)

## Prereqs

This crate pulls `local_backend`, which pulls `isolate` (V8). You
need the same JS build prerequisites as the main repo:

```sh
# One-time, from the repo root:
cd npm-packages && rush install && cd -
```

If you've already built `convex-local-backend` from this repo,
the V8 artifacts are cached and this crate reuses them.

## Build and run

From this directory:

```sh
cargo run --release -- \
    --port 3210 \
    --instance-name mydeploy \
    --instance-secret 0000000000000000000000000000000000000000000000000000000000000000 \
    --db sqlite \
    --local-storage ./_run/storage
```

Expected startup log:

```
Starting standalone_todo_app with 5 native function(s) registered
```

Five = `list_for_owner`, `count_pending`, `create`, `mark_done`,
`summarise`. If you see `0`, the `use standalone_todo_app as _;`
line in `src/main.rs` got dropped — `inventory` picks up
registrations via linker sections, so the crate must actually
link.

## Hit the handlers

The backend exposes the standard Convex HTTP+WS surface. From
another shell:

```sh
# Create a todo.
curl -sS http://127.0.0.1:3210/api/mutation \
  -H 'Content-Type: application/json' \
  -d '{
        "path": "mutations:create",
        "args": {"owner": "alice", "text": "write docs"},
        "format": "json"
      }' | jq .

# List alice's todos.
curl -sS http://127.0.0.1:3210/api/query \
  -H 'Content-Type: application/json' \
  -d '{
        "path": "queries:list_for_owner",
        "args": {"owner": "alice"},
        "format": "json"
      }' | jq .

# Count pending via the action (exercises action → query sub-call).
curl -sS http://127.0.0.1:3210/api/action \
  -H 'Content-Type: application/json' \
  -d '{
        "path": "actions:summarise",
        "args": {"owner": "alice"},
        "format": "json"
      }' | jq .
```

The JS/TS `convex` client libraries work the same way — just
point them at `http://127.0.0.1:3210` and call
`api.mutations.create({ owner, text })`.

## How it works

1. `cargo run` builds this crate and pulls in `local_backend`.
2. `local_backend::make_app(...)` constructs the `Application`.
   Inside it calls `NativeFunctionRunner::from_inventory()`, which
   walks linker sections populated by `inventory::submit!` calls
   the `#[convex::*]` macros emitted at compile time.
3. `make_app` installs the native registry as the global resolver
   `udf::validation` consults at the HTTP / WebSocket / sync
   entry points. Without this step every pure-native deployment
   would 404 with "Could not find public function — run `npx
   convex dev`" because the backend would only look in the
   `_modules` system table (which a Rust-only deployment never
   writes). The bridge lives in
   `convex_native_backend::install_native_resolver`. See
   `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md` for the
   diagnosis.
4. `make_app` also wraps the native runner with V8 inside a
   `CompositeFunctionRunner`. Requests whose function path is a
   native-registered handler short-circuit to Rust at dispatch;
   everything else falls through to V8 (there is no V8 code
   here, so the JS side is effectively idle).
5. `publish_native_schema` submits the inventory-declared
   `DatabaseSchema` (including `#[convex(index(...))]` indexes)
   as pending, waits for the in-process `SchemaWorker` to
   validate it and for every index to finish backfilling, then
   activates the schema + enables the indexes. Boot blocks on
   this so the readiness probe (HTTP) flips to Ready only once
   indexes are live — queries that rely on indexes never see
   "index backfilling" errors on a fresh deployment.
6. The HTTP service starts on `--port` with the same router the
   upstream binary uses.

No upstream modifications — this crate depends on `local_backend`
exactly as shipped.

## Running in distributed mode

The distributed topology splits the single process above into two
roles that talk to each other over gRPC:

```
                  ┌──────────────────────────────────────┐
 Client ─HTTPS/WS─┤  agnostic backend                    │◀── admin CLI
                  │  (no deployer code; admission + HTTP)│    (loopback)
                  └─────────────┬────────────────────────┘
                                │ gRPC ·5678
                                │ (WorkerAdmissionService,
                                │  FunctionExecutionService,
                                │  BackendCallbackService)
                     ┌──────────┴─────────┐
                     ▼                    ▼
             ┌───────────────┐   ┌───────────────┐
             │ standalone_   │   │ standalone_   │   … scales
             │ todo_app      │   │ todo_app      │     horizontally
             │ (CONVEX_MODE= │   │ (same image)  │
             │  worker)      │   │               │
             └───────────────┘   └───────────────┘
```

Two binaries, built independently, rolled on independent
schedules. The agnostic backend is the long-lived image; workers
roll whenever you change handler code.

### Step 1 — build the agnostic backend

The **agnostic backend** comes from the `convex_native` crate's
`convex-backend` binary, which links the framework + backend
machinery but **zero `#[convex::*]` registrations**. It's called
"agnostic" because it has no knowledge of deployer handlers; it
only speaks admission + coordinates OCC + serves HTTP to clients.
Deployer worker images provide the handler inventory at boot via
gRPC registration.

```sh
# Run this from the repository root (not from this example's dir —
# workspace binaries land in the workspace-level `target/`).
cd $(git rev-parse --show-toplevel)
cargo build --release -p convex_native --bin convex-backend
# Binary: ./target/release/convex-backend
```

> Heads up: the repo also ships a `convex-local-backend` binary
> (the monolith from `STANDALONE.md`). It's a different binary —
> the distributed topology uses `convex-backend` (no `-local-`).
> If you see `./target/release/convex-backend: No such file or
> directory`, you either skipped the build step above or you're
> running the command from the example's subdirectory; check
> that `pwd` matches `git rev-parse --show-toplevel`.

For a container image, use the template in
`convex-native/examples/deploy/docker/Dockerfile.backend`:

```sh
docker build \
  -f convex-native/examples/deploy/docker/Dockerfile.backend \
  -t getconvex/convex-backend:dev .
```

`CONVEX_REFUSE_NATIVE_HANDLERS=1` on the backend process enforces
agnosticism at boot: if any `#[convex::*]` macro code somehow got
linked into the binary, it refuses to start rather than silently
shadowing the worker pool's handlers.

### Step 2 — boot the agnostic backend

```sh
# Terminal A — backend on :3210 (HTTP) + :5678 (admission) + :9090 (admin HTTP).
CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678 \
  CONVEX_ADMIN_BIND_ADDR=127.0.0.1:9090 \
  CONVEX_REFUSE_NATIVE_HANDLERS=1 \
  ./target/release/convex-backend \
    --port 3210 \
    --instance-name mydeploy \
    --instance-secret 0000000000000000000000000000000000000000000000000000000000000000 \
    --db sqlite \
    --local-storage ./_run/backend-storage
```

The backend logs `CONVEX_ADMISSION_BIND_ADDR=... — spawning
WorkerAdmissionService` on startup. Until a worker joins, query
dispatches return a "no workers available" gRPC error.

### Step 3 — boot this example as a worker

Same `standalone_todo_app` crate, different env. The worker dials
the backend's admission port, sends its `RegistrationEnvelope`
(native inventory + `registry_version`), and stays connected for
its lifetime. Closing the stream retires the worker.

`convex_native::run()` is the all-in-one bootstrap — even in
worker mode it still opens a local `Database<Rt>` and binds the
full HTTP + site-proxy surface on top of the worker gRPC server.
The two ports you need to move off the backend's defaults are
`--port` (default `3210`) and `--site-proxy-port` (default
`3211`); pick free ports or pass `0` to let the OS assign them.
Forgetting `--site-proxy-port` is the usual "address already in
use" cause when backend and worker run on the same host.

```sh
# Terminal B — worker on :4567, from the repo root.
cd $(git rev-parse --show-toplevel)
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
  CONVEX_BACKEND_ENDPOINT=http://127.0.0.1:5678 \
  cargo run --release -p standalone_todo_app -- \
    --port 0 \
    --site-proxy-port 0 \
    --instance-name mydeploy-worker \
    --instance-secret 0000000000000000000000000000000000000000000000000000000000000000 \
    --db sqlite \
    --local-storage ./_run/worker-storage
```

> **Why two `--local-storage` dirs?** The worker process opens
> its own `Database<Rt>` at `begin_timestamp`s the backend
> supplies on each request. In this split-binary demo both
> processes talk to their own sqlite; in production both sides
> point at the same Postgres/MySQL cluster.

### Step 4 — verify from the operator surface

```sh
# Pool snapshot — one worker, registry version = this crate's pkg version.
curl -s http://127.0.0.1:9090/admin/pool | jq .

# Scale by booting more workers on different ports; the backend
# load-balances across them (Power-of-2-Choices).
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=0.0.0.0:4568 \
  CONVEX_BACKEND_ENDPOINT=http://127.0.0.1:5678 \
  cargo run --release -p standalone_todo_app -- \
    --port 0 --site-proxy-port 0 ...

# Rolling update: bump the pool floor so only v2 workers receive
# new dispatches.
curl -X POST http://127.0.0.1:9090/admin/pool/floor \
  -d '{"min_registry_version":"0.2.0"}'

# Drain a specific worker before killing it.
curl -X POST http://127.0.0.1:9090/admin/pool/drain \
  -d '{"worker_id":3,"reason":"v1 retirement"}'
```

### Step 5 — call a function

Exactly like the monolith path — clients don't observe the split:

```sh
curl -sS http://127.0.0.1:3210/api/mutation \
  -H 'Content-Type: application/json' \
  -d '{
        "path": "mutations:create",
        "args": {"owner": "alice", "text": "shipped it"},
        "format": "json"
      }' | jq .
```

The backend routes `mutations:create` to an available worker,
collects the `FinalTxSummary` (reads + writes), commits on its
own `Committer` (OCC + subscription invalidation stay on the
backend), and returns the result. See
`convex-native/DISTRIBUTED_PLAN.md` §6 for the exact protocol.

### Kubernetes

For a real cluster, the manifests in
`convex-native/examples/deploy/kubernetes/` cover both roles:

- `backend-deployment.yaml` — single-replica backend + PVC +
  Services for HTTP and admission.
- `worker-deployment.yaml` — multi-replica worker Deployment +
  headless Service, dialing the backend via the cluster DNS name.

```sh
kubectl apply -f convex-native/examples/deploy/kubernetes/
```

See `../HOW_TO_RUN.md` §3 for the cargo-only flow and
`convex-native/DISTRIBUTED_PLAN.md` cover-to-cover for the
architecture rationale.

## Gotchas

- **`0` registered functions** — `use standalone_todo_app as _;`
  missing or the app module not actually reachable from `lib.rs`.
  Fix: keep the `as _` import and make sure every module carrying
  `#[convex::*]` is in the `pub mod` chain.
- **Port 3210 already in use** — another `convex-local-backend`
  instance is running. `lsof -i :3210` / `kill`.
- **V8 build fails on first run** — run `rush install` in
  `npm-packages/` at the repo root.
- **SQLite file locked** — the default `--db sqlite` writes to
  `./convex_local_backend.sqlite3` (positional `DB_SPEC` arg).
  Two instances can't share a DB; use a distinct `--local-storage`
  + DB file per process.

## Cross-references

- `../HOW_TO_RUN.md` — top-level runbook with three paths.
- `../../STANDALONE.md` — the walkthrough this crate instantiates.
- `../../QUICKSTART.md` — developer-surface tour (derives, ctx).
- `../../USAGE.md` — full feature reference.
