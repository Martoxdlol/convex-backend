# Convex Native

Framework for writing Convex server functions (queries, mutations,
actions, HTTP actions, crons) in native Rust. Developers compile
their handlers into a worker binary; the backend coordinates
transactions, subscriptions, and reactivity.

## Status — all phases shipped in source

The `DISTRIBUTED_PLAN.md` rewrite is land-complete in the Rust
source tree. Every phase (1 wire contract → 7 operator
tooling) has shipped; the distributed topology runs end-to-end
with OCC + subscription invalidation intact because the backend
owns the Committer and workers only return transaction
summaries over gRPC.

`STATUS.md` is the authoritative shipped-vs-outstanding
tracker. The remaining four items (2.8b + 4.6b live-DB test
assertions, 5.3 release pipeline, 6.3 reference JS worker
binary) are each blocked on infrastructure outside the plan's
in-source scope — DB test fixtures, CI/release tooling, and a
separate JS worker implementation respectively. None block the
production topology.

The framework surface (derives, ctx, registry, schema
reflection) is unchanged by the distributed work and continues
to be the stable developer entry point.

## Target architecture (summary)

```
Client ──WebSocket/HTTPS──► Backend ──gRPC──► Worker pool
                              │
                              ▼
                          Persistence
```

- **Backend** is one binary, prebuilt by the Convex project and
  published as a container image. It carries no deployer code.
  It owns the `Database`, the `Committer`, the `SubscriptionManager`,
  the sync protocol, scheduler + cron workers, and the public
  HTTP / WebSocket surface.
- **Workers** carry the deployer's code — every `#[convex::*]`
  registration, the derived schema, HTTP routes, crons. They
  register their inventory with the backend at startup. A "deploy"
  is "roll the worker pool to a new image." The backend never
  rebuilds.
- **Client traffic lands on the backend only.** Workers are
  internal gRPC. See `DISTRIBUTED_PLAN.md` §3 for why.
- **Pool is dynamic.** Workers auto-scale. Backend tolerates
  membership changes at any time.
- **JS and Rust workers coexist** in the same pool (`WorkerKind`
  in the admission envelope).

Read `DISTRIBUTED_PLAN.md` cover to cover before contributing.

## Docs map

| File | Read when you want… |
|------|---------------------|
| `DISTRIBUTED_PLAN.md` | **The target architecture.** Phases, protocols, decisions. |
| `STATUS.md` | What's present in the tree right now + which phase is active. |
| `USAGE.md` | Per-feature reference for the developer surface (ctx, derives, etc.) — unchanged by the replan. |
| `MIGRATION.md` | JS ↔ Rust cheatsheet. |
| `STANDALONE.md` | Monolith alternative (`local_backend` library mode). Not the recommended topology going forward, but it works today. |
| `COMPOSITE_RUNNER.md` | How the monolith dispatch path works inside `local_backend`. Implementation reference for the alternative topology. |
| `DEPLOYMENT.md` | Operational guide — kept, will evolve per-phase. |
| `IMPLEMENTATION_PLAN.md` | Superseded. Historical. |
| `native-rust-functions.md` | Original design doc. Framework sections still accurate; distributed sections are superseded by `DISTRIBUTED_PLAN.md`. |

## Crate map

```
crates/convex_native/            framework: derives, ctx, registry,
                                 schema reflection. Compiled into
                                 workers.

crates/convex_macro/             proc macros backing the derives
                                 and attribute macros.

crates/convex_native_backend/    monolith-topology adapter.
                                 CompositeFunctionRunner.
                                 Plugs native into `local_backend`
                                 (STANDALONE.md path).

crates/convex_native_distributed/ distributed-topology plumbing.
                                 All phases of DISTRIBUTED_PLAN.md
                                 shipped: wire contract, worker
                                 FunctionRunner impl, dynamic
                                 WorkerPool + admission service,
                                 action sub-call callbacks,
                                 prebuilt backend image + k8s
                                 manifests, WorkerKind (JS
                                 interop hooks), operator
                                 tooling (snapshot/floor/drain/
                                 kind_preference admin HTTP).
```

## At a glance (developer surface — unchanged)

```rust
use convex_native::prelude::*;

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User {
    pub email: String,
    pub display_name: String,
}

#[convex::query]
pub async fn get_by_email(
    ctx: &mut QueryCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<Option<User>> {
    ctx.db().query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, email)?
        .unique()
        .await
}
```

Same code runs under either topology: linked into `local_backend`
(monolith, STANDALONE.md) or packaged into a worker image against
the future published backend (DISTRIBUTED_PLAN.md).

## Development

```sh
cargo check -p convex_native -p convex_macro
cargo test  -p convex_native
cargo +nightly fmt -p convex_native -p convex_macro
```

`STATUS.md` has current test tallies.

## For agents iterating here

Priority order when docs drift:

1. **`DISTRIBUTED_PLAN.md`** — the source of truth for the target
   architecture. Phase boundaries, protocol shapes, decision
   rationale.
2. **`STATUS.md`** — what actually landed, which phase is active.
3. **`USAGE.md`** — per-feature reference for the surface a
   deployer's worker crate will see.
4. **This README** — landing page, stays short.

Code-wise: every phase of `DISTRIBUTED_PLAN.md` has landed in
source. New work plugs into the stable surface
(`WorkerPool`, `PoolFunctionRunner`, `BackendCallbackService`,
`admin_http::router`); `STATUS.md` is authoritative for what's
wire-proven vs. blocked on out-of-source prerequisites.
