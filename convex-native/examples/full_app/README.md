# full_app — fully self-contained convex-native reference app

Real, workspace-member Cargo crate showing every surface the
framework exposes: derived schema with standard + text indexes,
queries, mutations, actions (including an `internal` variant),
an HTTP action, a cron, builder-side introspection, and a
distributed worker entry point — all buildable with a single
`cargo run`.

## Layout

```
full_app/
├── Cargo.toml                   workspace member
├── Dockerfile                   multi-stage build
├── deploy/
│   ├── docker-compose.yml       2-worker stack
│   └── kubernetes.yaml          Deployment + headless Service
└── src/
    ├── main.rs                  CONVEX_MODE-switched entry
    ├── lib.rs                   module tree + re-exports
    ├── schema.rs                User + Profile + Tier + Message
    ├── queries.rs               get_by_email, recent_messages_by, count_users
    ├── mutations.rs             create_user, send_message, promote
    ├── actions.rs               send_welcome, nightly_cleanup (internal)
    ├── http.rs                  POST /api/signup
    └── crons.rs                 daily_cleanup at 03:00 UTC
```

## Build

```sh
cargo build -p convex_full_app_example
```

No special tooling. This crate doesn't depend on the V8-backed
`isolate` stack, so you don't need `rush install`.

## Run — standalone (introspection print)

```sh
cargo run -q -p convex_full_app_example
```

Prints `BuiltBackend::describe_pretty()` — every function, table,
index, validator, route, and cron this binary knows about, in the
stable introspection envelope. Expected output summary:

```
convex_full_app: 8 function(s), 2 table(s), 1 route(s), 1 cron(s)
{ "version": 1, ... }
```

Use this as a CI gate: diff the JSON against the last deployed
version to catch accidental schema / registration changes.

## Run — worker mode

```sh
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
  cargo run -p convex_full_app_example
```

Boots the tonic `FunctionExecutionService` with every function
registered. Verify from a second terminal:

```sh
CONVEX_MODE=conductor \
  CONVEX_WORKER_ENDPOINTS=http://127.0.0.1:4567 \
  cargo run -p convex_native_distributed --example conductor
```

Expected:
```
http://127.0.0.1:4567 => v="0.1.0" traffic=true fns=8 in_flight=0
examples/conductor: probe complete — 1 healthy, 0 failed
```

## Deploy — Docker

```sh
docker build -t convex_full_app:latest \
  -f convex-native/examples/full_app/Dockerfile .
docker run --rm -p 4567:4567 convex_full_app:latest
```

## Deploy — Docker Compose (2 workers)

```sh
docker compose \
  -f convex-native/examples/full_app/deploy/docker-compose.yml \
  up --build
```

## Deploy — Kubernetes

Replace `IMAGE_TAG` in `deploy/kubernetes.yaml`, then:

```sh
kubectl apply -f convex-native/examples/full_app/deploy/kubernetes.yaml
```

The Deployment uses `maxSurge: 1 / maxUnavailable: 1` with
`terminationGracePeriodSeconds: 60` so rollouts respect the
worker's drain semantics. The headless Service exposes the pool
for the conductor's DNS-based resolution.

## How it's wired

Every `#[convex::query]` / `#[convex::mutation]` / `#[convex::action]`
/ `#[convex::http_action]` / `#[convex::cron]` / `#[derive(ConvexDocument)]`
expansion emits an `inventory::submit!` call. The linker only
keeps those submissions if the module is compiled into the final
binary — that's what the `use convex_full_app_example as _;`
line in `main.rs` forces. `NativeFunctionRunner::from_inventory()`
then walks every submission and builds the registry the worker
dispatches through.

## Use as a starting point

If you want this exact structure for your own app outside this
repo:

1. Copy `src/`, `Cargo.toml`, `Dockerfile`, `deploy/` to a new
   standalone directory.
2. Rename the crate (`convex_full_app_example` →
   whatever-you-want) and update `use ... as _;` in `main.rs`.
3. Point `convex_native` / `convex_native_distributed` at a git
   rev of this repo (see `../../STANDALONE.md` §2 for the
   `Cargo.toml` template).
4. Add your business logic modules alongside the sample ones.

To link the same module tree into the full standalone backend
(`convex-local-backend` with HTTP + WebSocket + the composite
runner), see `convex-native/STANDALONE.md` — that path also needs
`rush install` in `npm-packages/` for the V8 build step.

## Cross-refs

- `convex-native/USAGE.md` — per-feature reference.
- `convex-native/DEPLOYMENT.md` — operational guide for all three
  topologies.
- `convex-native/STANDALONE.md` — library-mode recipe for
  topology 1 (full HTTP/WebSocket backend).
- `convex-native/COMPOSITE_RUNNER.md` — how the composite runner
  dispatches native vs. JS.
