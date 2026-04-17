# standalone_todo_app — runnable monolith example

A complete deployer crate that defines a `Todo` schema with
query / mutation / action handlers and boots the full Convex
backend in a single process via the batteries-included
`convex_native::run()` entry point. Use this as the starting
template when you follow `convex-native/STANDALONE.md`.

## Layout

```
standalone_todo_app/
├── Cargo.toml          4 deps: convex_native + convex_native_core
│                        + tokio + anyhow.
├── README.md           this file
└── src/
    ├── main.rs         7 lines: #[tokio::main] → convex_native::run()
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
    --db-spec sqlite \
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
3. `make_app` wraps the native runner with V8 inside a
   `CompositeFunctionRunner`. Requests whose function path is a
   native-registered handler short-circuit to Rust; everything
   else falls through to V8 (there is no V8 code here, so the JS
   side is effectively idle).
4. The HTTP service starts on `--port` with the same router the
   upstream binary uses.

No upstream modifications — this crate depends on `local_backend`
exactly as shipped.

## Switching to the distributed topology

Set `CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678` on the backend
binary you run here, then boot a second instance of this same
crate as a worker with:

```sh
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
  CONVEX_BACKEND_ENDPOINT=http://127.0.0.1:5678 \
  cargo run --release -- \
    --port 0 \
    --instance-name todo-worker \
    --instance-secret 0000000000000000000000000000000000000000000000000000000000000000 \
    --db-spec sqlite \
    --local-storage ./_run/worker-storage
```

See `../HOW_TO_RUN.md` §3 and `convex-native/DISTRIBUTED_PLAN.md`
for the full split-topology walk-through.

## Gotchas

- **`0` registered functions** — `use standalone_todo_app as _;`
  missing or the app module not actually reachable from `lib.rs`.
  Fix: keep the `as _` import and make sure every module carrying
  `#[convex::*]` is in the `pub mod` chain.
- **Port 3210 already in use** — another `convex-local-backend`
  instance is running. `lsof -i :3210` / `kill`.
- **V8 build fails on first run** — run `rush install` in
  `npm-packages/` at the repo root.
- **SQLite file locked** — the default `--db-spec sqlite` writes
  to `./convex_local_backend.sqlite3`. Two instances can't share
  a DB; use a distinct `--local-storage` + DB file per process.

## Cross-references

- `../HOW_TO_RUN.md` — top-level runbook with three paths.
- `../../STANDALONE.md` — the walkthrough this crate instantiates.
- `../../QUICKSTART.md` — developer-surface tour (derives, ctx).
- `../../USAGE.md` — full feature reference.
