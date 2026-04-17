# Standalone as a library

Run the full Convex backend in your own binary with your
`#[convex::query]` / `#[convex::mutation]` / `#[convex::action]`
code linked in — **without modifying this repo**. The
`local_backend` crate is library-shaped; you depend on it the
same way you depend on any other crate.

This is the right path when:
- You want one process that serves HTTP / WebSocket + dispatches
  native functions (the "standalone" topology from `DEPLOYMENT.md`
  §1.A).
- You can't (or won't) fork this repo and add a module inside it.
- You're OK with the V8 build dep (the standalone path pulls in
  the `isolate` crate; if you want to skip V8 entirely, use the
  distributed topology in `DEPLOYMENT.md` §1.C).

For the path where you *do* fork and link a module inside
`crates/local_backend/`, see the section at the bottom.

---

## 1. Your project layout

```
my_convex_app/                 ← your project, lives outside this repo
├── Cargo.toml
└── src/
    ├── main.rs                ← thin shim that calls into local_backend
    ├── lib.rs                 ← declares the app module tree
    └── app/
        ├── mod.rs
        ├── schema.rs          ← #[derive(ConvexDocument)] types
        ├── queries.rs         ← #[convex::query] functions
        ├── mutations.rs       ← #[convex::mutation] functions
        ├── actions.rs         ← #[convex::action] functions
        ├── http.rs            ← #[convex::http_action] functions
        └── crons.rs           ← #[convex::cron] registrations
```

Nothing special about the `app/` layout — the only hard
requirement is that every module containing `#[convex::*]` or
`#[derive(ConvexDocument)]` is actually compiled into your
binary. `inventory::submit!` puts the registrations into linker
sections; if the crate isn't linked, they don't exist.

---

## 2. `Cargo.toml`

```toml
[package]
name = "my_convex_app"
version = "0.1.0"
edition = "2024"

[dependencies]
# Pin to a specific git rev (or published crates.io versions once
# those exist). The `convex_native::VERSION` constant ends up
# driving the `registry_version` the worker reports.
convex_native = { git = "https://github.com/get-convex/convex-backend", rev = "PIN_ME" }
local_backend = { git = "https://github.com/get-convex/convex-backend", rev = "PIN_ME" }

# Everything below is re-exported by local_backend's own deps,
# but each is listed here because main.rs calls them directly.
common       = { git = "https://github.com/get-convex/convex-backend", rev = "PIN_ME" }
runtime      = { git = "https://github.com/get-convex/convex-backend", rev = "PIN_ME" }
db_connection = { git = "https://github.com/get-convex/convex-backend", rev = "PIN_ME" }
cmd_util     = { git = "https://github.com/get-convex/convex-backend", rev = "PIN_ME" }

anyhow         = "1"
async-broadcast = "0.7"
clap           = { version = "4", features = ["derive"] }
futures        = "0.3"
tokio          = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
tracing        = "0.1"
```

> **Pinning.** Until `convex_native` is on crates.io, pin to a
> specific git rev so `convex_native::VERSION` is stable across
> builds. Rolling-update routing (Phase 4.7) uses that string as
> the `registry_version` the worker advertises.

---

## 3. `src/lib.rs`

Declares every module containing your functions so the linker
pulls them into the binary. Without this the `inventory::submit!`
calls emitted by `#[convex::query/...]` never run — the
`NativeFunctionRunner` then sees an empty registry at startup.

```rust
// src/lib.rs
pub mod app {
    pub mod schema;
    pub mod queries;
    pub mod mutations;
    pub mod actions;
    pub mod http;
    pub mod crons;
}
```

If a module has no public items your `main.rs` calls, add
`#[allow(unused_imports)] use crate::app as _;` in main.rs — the
`as _` import is enough to force linkage.

---

## 4. `src/main.rs`

This is a *verbatim adaptation* of `crates/local_backend/src/main.rs`
in this repo — same shutdown semantics, same CLI, but it's in
**your** crate and it `use`s your app module so your functions
get linked. Copy-paste and keep in sync with upstream when you
bump the rev.

```rust
use std::time::Duration;

use clap::Parser;
use cmd_util::env::config_service;
use common::{
    errors::MainError,
    http::ConvexHttpService,
    knobs::HTTP_SERVER_TIMEOUT_DURATION,
    runtime::Runtime,
    shutdown::ShutdownSignal,
    version::SERVER_VERSION_STR,
};
use db_connection::{connect_persistence, ConnectPersistenceFlags};
use futures::{future::{self, Either}, FutureExt};
use local_backend::{
    config::LocalConfig,
    make_app,
    proxy::dev_site_proxy,
    router::router,
    HttpActionRouteMapper,
    MAX_CONCURRENT_REQUESTS,
};
use runtime::prod::ProdRuntime;
use tokio::{signal, sync::oneshot};

// Force the linker to pull in every `#[convex::*]` /
// `#[derive(ConvexDocument)]` registration from your app.
// Without this `use`, `inventory` has no entries to collect.
#[allow(unused_imports)]
use my_convex_app as _;

fn main() -> Result<(), MainError> {
    let _guard = config_service();
    let config = LocalConfig::parse();
    tracing::info!(
        "Starting a Convex backend with {} native function(s)",
        convex_native::NativeFunctionRunner::from_inventory()
            .map(|r| r.len())
            .unwrap_or(0),
    );
    let tokio = ProdRuntime::init_tokio()?;
    let runtime = ProdRuntime::new(&tokio);
    let runtime_ = runtime.clone();
    runtime.block_on("main", async move { run_server(runtime_, config).await })
}

async fn run_server(runtime: ProdRuntime, config: LocalConfig) -> anyhow::Result<()> {
    let (preempt_tx, preempt_rx) = oneshot::channel();
    let preempt_signal = ShutdownSignal::new(preempt_tx);
    let (shutdown_tx, shutdown_rx) = async_broadcast::broadcast(1);
    let persistence = connect_persistence(
        config.db,
        &config.db_spec,
        ConnectPersistenceFlags {
            require_ssl: !config.do_not_require_ssl,
            allow_read_only: false,
            skip_index_creation: false,
        },
        &config.name(),
        runtime.clone(),
        preempt_signal.clone(),
    )
    .await?;
    let st = make_app(
        runtime.clone(),
        config.clone(),
        persistence,
        shutdown_rx.clone(),
        preempt_signal.clone(),
    )
    .await?;
    let router = router(st.clone());
    let mut shutdown_rx_ = shutdown_rx.clone();
    let http_service = ConvexHttpService::new(
        router,
        "backend",
        SERVER_VERSION_STR.to_string(),
        MAX_CONCURRENT_REQUESTS,
        *HTTP_SERVER_TIMEOUT_DURATION,
        HttpActionRouteMapper,
    );
    let serve_http_future = http_service.serve(
        config.http_bind_address(),
        async move { let _ = shutdown_rx_.recv().await; },
    );
    let proxy_future = dev_site_proxy(
        config.site_bind_address(),
        config.site_forward_prefix(),
        shutdown_rx,
    );
    let serve_future = future::try_join(serve_http_future, proxy_future).fuse();
    futures::pin_mut!(serve_future);

    let mut force_exit_duration = None;
    futures::select! {
        r = serve_future => { r?; panic!("serve stopped unexpectedly") },
        _err = preempt_rx.fuse() => {
            force_exit_duration = Some(Duration::from_secs(0));
            let _: Result<_, _> = shutdown_tx.broadcast(()).await;
        },
        r = signal::ctrl_c().fuse() => {
            r?;
            let _: Result<_, _> = shutdown_tx.broadcast(()).await;
        },
    }

    let shutdown = async move {
        serve_future.await?;
        st.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }
    .fuse();
    futures::pin_mut!(shutdown);
    let mut force_exit_future = match force_exit_duration {
        Some(d) => Either::Left(runtime.wait(d)),
        None => Either::Right(std::future::pending()),
    }
    .fuse();
    loop {
        futures::select! {
            r = shutdown => { r?; if force_exit_duration.is_none() { break; } },
            _ = force_exit_future => break,
            r = signal::ctrl_c().fuse() => { r?; break; },
        }
    }
    Ok(())
}
```

That's all the `main.rs` needs to be. The `use my_convex_app as _`
is the only load-bearing addition over the upstream `main.rs` —
it forces the linker to include your functions so `inventory`
sees them when `make_app` builds the composite runner.

---

## 5. Example `src/app/queries.rs`

```rust
use convex_native::{convex, prelude::*, QueryCtx, Rt};
use crate::app::schema::{User, UserField, UserIndex};

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

You can nest modules as deeply as you like — `inventory` is
linker-section-based, so it doesn't care about module paths. The
only thing that matters is that *some* path from `main.rs` pulls
the file into the compilation.

---

## 6. Building + running

```sh
# One-time on this repo: V8 build prerequisites. `local_backend`
# pulls in `isolate`, which needs rush-managed JS deps even
# though your own code is Rust-only.
cd path/to/convex-backend/npm-packages
rush install
cd -

# Build and run your binary. Flags are the standard
# local_backend ones (LocalConfig).
cd path/to/my_convex_app
cargo run -- \
    --port 3210 \
    --instance-name mydeploy \
    --instance-secret 0000000000000000000000000000000000000000000000000000000000000000 \
    --db-spec sqlite \
    --local-storage ./mydeploy_storage
```

Verify your functions registered:

```sh
# The backend's HTTP surface includes introspection you can
# probe. For local assertions, also run the convex_native
# example (doesn't boot the server):
cargo run -p convex_native --example tiny_app
```

Your function names should appear in the `"functions"` array of
the `describe_pretty()` output.

---

## 7. What happens under the hood

When `make_app()` runs:

1. It constructs a JS `InProcessFunctionRunner` (the V8 side).
2. It calls `convex_native::NativeFunctionRunner::from_inventory()`.
   `inventory::iter::<NativeFunctionRegistration>` walks the
   linker sections and returns *every* function registered by
   any crate linked into your binary — yours + the empty set in
   the canonical `convex-native` test crate.
3. It wraps both in a `CompositeFunctionRunner`: native names
   short-circuit to your handlers, everything else falls through
   to V8.
4. It hands the composite runner to `Application::new` and builds
   the HTTP + WebSocket surface on top.

No code change is needed in this repo — the wiring in
`crates/local_backend/src/lib.rs:214` already consults the
global inventory table, which is populated by your binary at
compile time.

---

## 8. Alternative: forking the repo

If you'd rather keep your functions inside `crates/local_backend/`
itself (e.g. because you want the same CI pipeline), you can add
your module as a sub-module of that crate:

```rust
// crates/local_backend/src/lib.rs (in your fork)
mod my_app;
```

This works with a one-line change, but it entangles your
business logic with the fork's upstream sync. The library-crate
path above keeps your code strictly external.

---

## 9. Alternative: skip V8 entirely

If your app is pure-native (no JS functions), you don't need
`local_backend` / `isolate` at all. Build a worker binary on top
of `convex_native_distributed`:

- Build cost: no V8 = no `rush install` required.
- Runtime cost: you run a separate conductor process that
  dispatches over gRPC (see `DEPLOYMENT.md` §1.C).

The trade-off is topology complexity vs. toolchain complexity —
pick whichever is cheaper for your deployment. A single-process
standalone-with-native path that bypasses `isolate` altogether
would require more plumbing inside `local_backend` than is
currently exposed as a library surface; if you want that shape,
track the `STATUS.md` outstanding items.

---

## 10. Gotchas

- **Missing `use my_convex_app as _`** — the #1 source of "my
  functions don't show up." Without it, `cargo` optimizes away
  the linkage and `inventory` sees nothing.
- **Version drift between pinned rev and generated code** — the
  derive macros emit absolute paths like
  `::convex_native::__private::...`. If your pinned
  `convex_native` rev differs from the `local_backend` rev, you
  may hit `unresolved import` errors inside generated code. Pin
  all `convex-backend`-repo deps to the same rev.
- **Schema evaluation skipped for JS modules** — `convex dev`
  code generation isn't part of the native flow. Run
  `cargo run -p convex_native --example tiny_app` (or write a
  small binary that calls `describe_pretty()`) as your schema
  check instead.
- **`LocalConfig::parse()` vs your own CLI** — if you want a
  different CLI surface, copy `LocalConfig`'s fields into your
  own struct; or construct `LocalConfig` programmatically from
  whatever config source you want. The `make_app` signature
  takes it by value.
