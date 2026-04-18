# How to run convex-native locally

A zero-to-running walkthrough. Three paths in order of speed:

| Path | Command surface | What you exercise |
|------|-----------------|-------------------|
| 1. Introspection smoke test | `cargo run --example tiny_app` | derives + schema + registry (no server) |
| 2. Worker-only | `cargo run --example todo_worker` | `FunctionExecutionService` gRPC |
| 3. End-to-end (backend + worker) | `convex-backend` + worker | full client traffic → OCC commit |

All three run from a fresh clone with no external services.

---

## 1. Introspection smoke test

Fastest sanity check that your toolchain compiles the derives and
proc-macros. No ports bound, no V8, exits after printing the
collected registry as JSON.

```sh
cargo run -p convex_native --example tiny_app
```

Expected output: a JSON document listing a `users` table + three
functions (`get_by_email`, `create`, `send_welcome`) plus a cron
and an HTTP route. If this succeeds, the developer surface works.

Source: [`crates/convex_native/examples/tiny_app.rs`](../../crates/convex_native/examples/tiny_app.rs).

---

## 2. Worker-only — real gRPC server, real handlers

The `todo_worker` example links a `Todo` schema and a
`list_for_owner` / `create` / `mark_done` / `summarise` handler
set, then boots `FunctionExecutionService` on a TCP port. This is
what a deployer's worker image runs in production; the example
skips the `Database` wiring so query dispatch returns
`Code::Unimplemented`, which is enough to exercise registration,
the gRPC transport, health probes, and graceful shutdown.

### Run

```sh
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
  cargo run -p convex_native_distributed --example todo_worker
```

Expected startup:

```
todo_worker: listening on 127.0.0.1:4567, 4 native function(s) registered:
  - list_for_owner
  - create
  - mark_done
  - summarise
```

### Probe it

gRPC-health check from another shell (install once:
`brew install grpcurl`):

```sh
grpcurl -plaintext 127.0.0.1:4567 list
# → convex.function_execution.FunctionExecutionService
#   grpc.health.v1.Health
```

Ship an `Execute` request:

```sh
grpcurl -plaintext \
  -d '{"function":{"name":"list_for_owner"},"args":"{\"owner\":\"alice\"}"}' \
  127.0.0.1:4567 \
  convex.function_execution.FunctionExecutionService/Execute
```

You'll get `Code::Unimplemented` back — expected without a
`Database`. Swap this example for a real database wiring via
`serve_worker_with_database(...)` once you have persistence
(`standalone_todo_app` does this for you under `CONVEX_MODE=worker`).

### Shut down

Ctrl-C. The server's graceful shutdown drains in-flight RPCs,
then exits.

Source: [`crates/convex_native_distributed/examples/todo_worker.rs`](../../crates/convex_native_distributed/examples/todo_worker.rs).

---

## 3. End-to-end — backend + worker over gRPC

This is the real thing: backend serves HTTP+WS to clients and
coordinates OCC; worker executes native handlers; transactions
commit on the backend. See `DISTRIBUTED_PLAN.md` for the full
protocol.

### Prereqs

```sh
# V8 build deps for the backend binary.
cd npm-packages && rush install && cd -
```

### Terminal A — agnostic backend

```sh
CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678 \
  cargo run -p convex_native --bin convex-backend -- \
    --port 3210 \
    --instance-name mydeploy \
    --instance-secret 0000000000000000000000000000000000000000000000000000000000000000 \
    --db sqlite \
    --local-storage ./_run/mydeploy_storage
```

Wait for `Listening on 0.0.0.0:3210`. The `convex-backend` binary
carries zero `#[convex::*]` registrations — handlers arrive from
the worker pool.

Omit `CONVEX_ADMISSION_BIND_ADDR` to boot the same binary as a
standalone backend with an empty native registry (useful for
testing the wire protocol without a worker).

### Terminal B — deployer worker

Build a deployer crate that links its handlers and boots via
`convex_native::run()` in worker mode. The shipped example is
`convex-native/examples/standalone_todo_app` — launch it as:

```sh
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
  CONVEX_BACKEND_ENDPOINT=http://127.0.0.1:5678 \
  cargo run --release -p standalone_todo_app -- \
    --port 0 \
    --site-proxy-port 0 \
    --instance-name mydeploy-worker \
    --instance-secret 0000000000000000000000000000000000000000000000000000000000000000 \
    --db sqlite \
    --local-storage ./_run/worker_storage
```

`convex_native::run()` is all-in-one: even in worker mode it
brings up the full HTTP + site-proxy surface alongside the worker
gRPC server. `--port 0 --site-proxy-port 0` asks the OS to pick
free ports so the worker doesn't collide with Terminal A's
`:3210` / `:3211` defaults. Forgetting `--site-proxy-port` is the
usual "address already in use" cause when both processes share a
host.

The worker dials the backend, sends a `RegistrationEnvelope` with
its native-function inventory + `registry_version`, stays
connected for its lifetime. Closing the stream retires the worker.

### Terminal C — call a function

The Convex JS/TS client library talks to `http://127.0.0.1:3210`
the same way it talks to production. From the CLI you can hit the
HTTP surface directly:

```sh
curl -sS 'http://127.0.0.1:3210/api/query' \
  -H 'Content-Type: application/json' \
  -d '{"path":"listForOwner","args":{"owner":"alice"},"format":"json"}' | jq .
```

### Operator probes

```sh
# Pool snapshot (set CONVEX_ADMIN_BIND_ADDR=127.0.0.1:9090).
curl -s http://127.0.0.1:9090/admin/pool | jq .

# Bump the floor version (rolling-update gate).
curl -X POST http://127.0.0.1:9090/admin/pool/floor \
  -d '{"min_registry_version":"v2"}'

# Drain a specific worker.
curl -X POST http://127.0.0.1:9090/admin/pool/drain \
  -d '{"worker_id":3,"reason":"v1 retirement"}'
```

---

## Bringing your own handlers

For a non-example project, follow `STANDALONE.md` §1–6. In
summary:

1. Create a new cargo crate that depends on `convex_native` +
   `local_backend`.
2. Declare your `#[convex::query/mutation/action]` functions +
   `#[derive(ConvexDocument)]` types in modules reachable from
   `lib.rs`.
3. Copy `crates/local_backend/src/main.rs` into your `main.rs` and
   add `#[allow(unused_imports)] use my_convex_app as _;` to pull
   the registrations into the link.
4. `cargo run -- --port 3210 ...` — you have a Convex backend with
   your handlers served natively.

Swap to the distributed topology by setting
`CONVEX_ADMISSION_BIND_ADDR` on the backend and rolling out
separate worker binaries built from the same crate.

---

## Docker / Kubernetes

See [`README.md`](README.md) in this directory — Dockerfile
templates (`deploy/docker/Dockerfile.{backend,worker}`), a k8s
bundle (`deploy/kubernetes/{backend,worker}-deployment.yaml`),
and a systemd unit (`deploy/systemd/convex-worker.service`) are
all ready to crib from. The HOW_TO_RUN story above is "everything
works when run as a cargo binary"; once that's comfortable, the
deploy artifacts are the same binaries packaged with the right
env vars.

---

## Cross-references

- [`QUICKSTART.md`](../QUICKSTART.md) — developer-surface tour
  (derives, ctx, scheduler).
- [`STANDALONE.md`](../STANDALONE.md) — monolith topology recipe.
- [`DEPLOYMENT.md`](../DEPLOYMENT.md) — operational reference
  (env-var matrix, rolling updates, observability).
- [`DISTRIBUTED_PLAN.md`](../DISTRIBUTED_PLAN.md) — architecture
  + phased delivery.
- [`STATUS.md`](../STATUS.md) — what's shipped vs outstanding.
