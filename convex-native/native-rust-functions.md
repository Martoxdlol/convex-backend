# Design Doc: Native Rust Functions for Convex

**Author:** Tomás Cichero  
**Date:** 2026-04-15  
**Status:** Draft  

---

## Table of Contents

1. [Overview](#1-overview)
2. [Goals and Non-Goals](#2-goals-and-non-goals)
3. [Background: Current Architecture](#3-background-current-architecture)
4. [Proposed Architecture](#4-proposed-architecture)
5. [Rust Schema DSL and Type System](#5-rust-schema-dsl-and-type-system)
6. [Type-Safe Document API](#6-type-safe-document-api)
7. [Developer Experience: Proc Macros and Function Registry](#7-developer-experience-proc-macros-and-function-registry)
8. [Type-Safe Context Wrapper API](#8-type-safe-context-wrapper-api)
9. [NativeFunctionRunner: The Local Execution Engine](#9-nativefunctionrunner-the-local-execution-engine)
10. [Distributed Execution](#10-distributed-execution)
11. [Network Protocol and Serialization](#11-network-protocol-and-serialization)
12. [Deployment, Scaling, and Operations](#12-deployment-scaling-and-operations)
13. [Trade-offs, Risks, and Alternatives](#13-trade-offs-risks-and-alternatives)
14. [Rust Components](#14-rust-components)
15. [Implementation Phases](#15-implementation-phases)

---

## 0. Implementation notes

**Read `QUICKSTART.md` for the current API.** The code in this design
doc is the original proposal; where the shipped API diverges, the
quickstart is authoritative. Notable differences:

- **Typed sub-calls use PascalCase markers.** The design example has
  `ctx.run_query(get_user_by_email, ...)` where
  `get_user_by_email` is both the fn and the call reference. Rust
  forbids a fn and a struct sharing a name in one scope, so the macro
  emits a PascalCase marker struct: `ctx.run_query(GetUserByEmail,
  GetUserByEmailArgs { .. }).await`.
- **Pinned to `ProdRuntime` for native handlers.** `inventory` can't
  hold generic fn pointers, so native handlers are monomorphic over
  one runtime — aliased as `convex_native::Rt`. Developer code writes
  `ctx: &mut QueryCtx<'_, Rt>` (elidable in type position).
- **Backend integration is a separate crate.** The surface in this
  design lives in `convex_native`; the adapter that implements the
  full `function_runner::FunctionRunner` trait by wrapping the V8
  runner + the native dispatcher lives in a planned future
  `convex_native_backend` crate. See `COMPOSITE_RUNNER.md` for the
  reference implementation.

## 1. Overview

This document proposes adding support for writing Convex server functions
(queries, mutations, and actions) in native Rust. Functions are defined using
proc macro attributes (`#[convex::query]`, `#[convex::mutation]`,
`#[convex::action]`), compiled into the developer's own backend binary, and
executed without V8 or any intermediate runtime.

This repository provides the **framework crates** (`convex_native`,
`convex_macro`, `convex_native_distributed`) that developers consume as
dependencies — either from **crates.io** or directly from **git**. Application
code (schemas, functions, components) lives in the developer's own project and
repository, not in this one.

The design supports both **single-node** and **distributed multi-node**
execution, where a pool of identical worker binaries can execute functions in
parallel, with a central conductor coordinating transaction commits and
client sync.

### Motivating Example

The following shows what a developer's project looks like when using the
`convex_native` crate as a dependency:

```rust
// In the developer's own project (not this repo)
use convex_native::prelude::*;

// ── Schema: define tables as Rust structs ──────────────────────

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
#[convex(index(name = "by_created", fields = ["created_at"]))]
pub struct User {
    pub name: String,
    pub email: String,
    pub avatar_url: Option<String>,
    pub created_at: f64,
}

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "messages")]
#[convex(index(name = "by_channel", fields = ["channel", "created_at"]))]
pub struct Message {
    pub author: Id<User>,     // ← phantom-typed foreign key
    pub body: String,
    pub channel: String,
    pub created_at: f64,
}

// ── Queries: fully typed, compile-time checked ─────────────────

#[convex::query]
async fn list_users(ctx: &mut QueryCtx) -> Result<Vec<User>> {
    ctx.db().query::<User>()
        .with_index(UserIndex::ByCreated)
        .order(Order::Desc)
        .collect()
        .await
}

#[convex::query]
async fn get_user_by_email(ctx: &mut QueryCtx, email: String) -> Result<Option<User>> {
    ctx.db().query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, &email)   // ← compile-time field check
        .first()
        .await
}

// ── Mutations: typed inserts, patches, deletes ─────────────────

#[convex::mutation]
async fn create_user(
    ctx: &mut MutationCtx,
    name: String,
    email: String,
) -> Result<Id<User>> {
    let id = ctx.db().insert(User {
        name,
        email,
        avatar_url: None,
        created_at: ctx.unix_timestamp().as_secs_f64(),
    }).await?;

    ctx.scheduler().run_after(
        Duration::ZERO,
        send_welcome_email,    // ← function reference, not a string
        SendWelcomeEmailArgs { user_id: id.clone() },
    ).await?;

    Ok(id)
}

#[convex::mutation]
async fn update_user_name(
    ctx: &mut MutationCtx,
    user_id: Id<User>,          // ← cannot accidentally pass Id<Message>
    new_name: String,
) -> Result<User> {
    ctx.db().patch(user_id, UserPatch {
        name: Some(new_name),
        ..Default::default()    // only touch the fields you set
    }).await
}

// ── Actions: side effects with typed sub-calls ─────────────────

#[convex::action]
async fn send_welcome_email(ctx: &mut ActionCtx, user_id: Id<User>) -> Result<()> {
    let user: User = ctx.run_query(get_user_by_email, GetUserByEmailArgs {
        email: "...".into()
    }).await?.ok_or_else(|| anyhow!("user not found"))?;

    reqwest::Client::new()
        .post("https://api.sendgrid.com/v3/mail/send")
        .bearer_auth(std::env::var("SENDGRID_API_KEY")?)
        .json(&serde_json::json!({
            "to": user.email,
            "subject": format!("Welcome, {}!", user.name),
        }))
        .send()
        .await?;

    Ok(())
}

// ── Entry point (in the developer's own project) ──────────────

fn main() {
    ConvexBackend::new()
        .with_schema::<(User, Message)>()   // registers schema from structs
        .with_native_functions()             // auto-discovers #[convex::*] fns
        .run();
}
```

---

## 2. Goals and Non-Goals

### Goals

- **G1:** Allow developers to write queries, mutations, and actions in Rust
  with an ergonomic proc-macro-based API.
- **G2:** Define schemas as Rust structs with derive macros, generating
  `DatabaseSchema`, typed document structs, typed IDs, typed indexes, and
  typed patch structs — all checked at compile time.
- **G3:** Provide maximum type safety: phantom-typed `Id<T>` prevents
  cross-table ID confusion, typed query builders prevent querying with
  wrong index/field names, and typed insert/patch prevents field mismatches.
- **G4:** Publish framework crates (`convex_native`, `convex_macro`) to
  crates.io (and support git dependencies) so that developers can build their
  own backend binary — runnable as a standalone server or distributed worker.
- **G5:** Support horizontal scaling by running multiple identical worker
  nodes behind a load balancer.
- **G6:** Reuse the existing `FunctionRunner` trait as the distribution
  boundary — no changes to the database, sync, or commit layers.
- **G7:** Maintain compatibility with the reactive subscription system —
  native Rust queries participate in real-time sync just like JS queries.
- **G8:** Provide a Rust context API that maps 1:1 to the existing syscall
  interface, so native functions have the same capabilities as JS functions.

### Non-Goals

- **NG1:** Hot-reloading of Rust functions (requires recompile and redeploy).
- **NG2:** Running untrusted/user-submitted Rust code (no sandboxing — this
  is for first-party backend code).
- **NG3:** Replacing the JS/TS UDF system — both can coexist via the
  `FunctionRouter`.
- **NG4:** WASM compilation — this design targets native execution. A WASM
  path is a valid future extension but out of scope.

### Consumption Model

This repository **does not contain application code**. It provides the
framework crates that developers depend on from their own projects.

**Published crates (this repo):**

| Crate | Description | Phase |
|-------|-------------|-------|
| `convex_native` | Core type system, derive macros, context wrappers, `NativeFunctionRunner`, `ConvexBackend` builder | 1 |
| `convex_macro` | Proc macros (`#[convex::query]`, `#[convex::mutation]`, `#[convex::action]`, `#[derive(ConvexDocument)]`, etc.) | 1 |
| `convex_native_distributed` | gRPC worker/conductor support for multi-node deployments | 3 |

**Developer's project (separate repo):**

```toml
# my-backend/Cargo.toml
[package]
name = "my-backend"
version = "0.1.0"
edition = "2021"

[dependencies]
# From crates.io (after publish):
convex_native = "0.1"

# Or from git (before publish / for bleeding edge):
# convex_native = { git = "https://github.com/nicorp/convex-backend", branch = "main" }

# For distributed mode (Phase 3):
# convex_native_distributed = "0.1"

# Reusable components are also just crate dependencies:
# convex-rate-limiter = "1.0"
```

```
my-backend/
├── Cargo.toml
├── src/
│   ├── main.rs           # ConvexBackend::new()...run()
│   ├── schema.rs          # #[derive(ConvexDocument)] structs
│   └── functions/
│       ├── mod.rs
│       ├── users.rs       # #[convex::query], #[convex::mutation]
│       └── messages.rs
```

The developer runs `cargo build` in their own project to produce their
backend binary. This repo never contains or compiles application-specific
schemas, functions, or business logic.

---

## 3. Background: Current Architecture

### 3.1 How Functions Execute Today

```
┌─────────────────────────────────────────────────────────────┐
│                    local_backend binary                      │
│                                                             │
│  ┌──────────┐    ┌─────────────────────┐    ┌────────────┐ │
│  │  HTTP /   │    │ ApplicationFunction │    │  Database   │ │
│  │  WebSocket│───►│ Runner              │───►│  (commit,   │ │
│  │  Server   │    │                     │    │   subscribe)│ │
│  └──────────┘    └────────┬────────────┘    └────────────┘ │
│                           │                                 │
│                  ┌────────▼────────────┐                    │
│                  │   FunctionRouter    │                    │
│                  │                     │                    │
│                  │  ┌───────────────┐  │                    │
│                  │  │ FunctionRunner│  │  (trait object)    │
│                  │  └───────┬───────┘  │                    │
│                  └──────────┼──────────┘                    │
│                             │                               │
│               ┌─────────────┼──────────────┐                │
│               ▼                            ▼                │
│  ┌────────────────────┐      ┌─────────────────────┐       │
│  │ InProcessFunction  │      │ FunrunClient         │       │
│  │ Runner             │      │ (cloud only,         │       │
│  │                    │      │  not in OSS)         │       │
│  │ ┌────────────────┐ │      └─────────┬───────────┘       │
│  │ │FunctionRunner  │ │                │ gRPC              │
│  │ │Core            │ │                ▼                    │
│  │ │ ┌────────────┐ │ │      ┌─────────────────────┐       │
│  │ │ │IsolateClient│ │ │     │ Funrun cluster       │       │
│  │ │ │(V8 pool)   │ │ │      │ (N nodes, each with  │       │
│  │ │ └────────────┘ │ │      │  FunctionRunnerCore) │       │
│  │ └────────────────┘ │      └─────────────────────┘       │
│  └────────────────────┘                                     │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 The FunctionRunner Trait

The `FunctionRunner` trait (`crates/function_runner/src/lib.rs:84`) is the
central abstraction. All function execution flows through it:

```rust
#[async_trait]
pub trait FunctionRunner<RT: Runtime>: Send + Sync + 'static {
    async fn run_function(
        &self,
        udf_type: UdfType,            // Query | Mutation | Action | HttpAction
        identity: Identity,            // authenticated user
        ts: RepeatableTimestamp,       // snapshot timestamp
        existing_writes: FunctionWrites,
        log_line_sender: ...,
        function_metadata: Option<FunctionMetadata>,  // path + args + journal
        http_action_metadata: Option<HttpActionMetadata>,
        default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
        context: ExecutionContext,
    ) -> anyhow::Result<(
        Option<FunctionFinalTransaction>,  // reads + writes
        FunctionOutcome,                   // return value + logs + traces
        FunctionUsageStats,                // bandwidth, compute, etc.
    )>;

    async fn analyze(...) -> ...;
    async fn evaluate_schema(...) -> ...;
    async fn evaluate_auth_config(...) -> ...;
    // ... other evaluation methods
}
```

The return type `FunctionFinalTransaction` carries the transaction's read set
and write set back to the caller, where it is validated and committed by the
`Database`:

```rust
pub struct FunctionFinalTransaction {
    pub begin_timestamp: Timestamp,
    pub reads: FunctionReads,      // ReadSet + interval count + size
    pub writes: FunctionWrites,    // Vec<DocumentUpdateWithPrevTs>
    pub rows_read_by_tablet: BTreeMap<TabletId, u64>,
}
```

### 3.3 Key Insight: Execute Remotely, Commit Locally

The current architecture already separates **execution** from **commit**.
`FunctionRunnerCore` (in `crates/function_runner/src/server.rs`) creates a
`Transaction`, runs the function, then returns the reads/writes. The caller
(`ApplicationFunctionRunner`) validates retention and commits. This means the
function execution can happen anywhere — the commit always happens on the
conductor.

### 3.4 Existing Syscall Interface

Functions interact with the backend through ~20 syscalls, grouped by category:

| Category | Syscalls |
|----------|----------|
| **Database reads** | `1.0/get`, `1.0/queryStream`, `1.0/queryStreamNext`, `1.0/queryPage`, `1.0/count` |
| **Database writes** | `1.0/insert`, `1.0/replace`, `1.0/shallowMerge`, `1.0/remove` |
| **Identity** | `1.0/getUserIdentity` |
| **Scheduling** | `1.0/schedule` |
| **Storage** | `1.0/storageGenerateUploadUrl`, `1.0/storageGetUrl`, `1.0/storageDelete`, `1.0/storageGetMetadata` |
| **Metadata** | `1.0/db/normalizeId`, `1.0/getTransactionMetrics`, `1.0/getFunctionMetadata` |
| **Cross-component** | `1.0/runUdf`, `1.0/createFunctionHandle` |
| **Query lifecycle** | `1.0/queryCleanup` |

In the current system, these are dispatched as JSON through V8. In the native
Rust design, they become direct method calls on the context wrapper types.

---

## 4. Proposed Architecture

### 4.1 Standalone Mode (Single Node)

The developer's binary (built in their own project using `convex_native` as a
dependency) runs everything in a single process:

```
┌──────────────────────────────────────────────────────────────┐
│          Developer's Binary (their project)                   │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐     │
│  │  Native Function Registry                            │     │
│  │                                                     │     │
│  │  "list_users"    → fn(QueryCtx)    → Result<Value>  │     │
│  │  "create_user"   → fn(MutationCtx) → Result<Value>  │     │
│  │  "send_email"    → fn(ActionCtx)   → Result<Value>  │     │
│  └────────────────────────┬────────────────────────────┘     │
│                           │                                   │
│  ┌──────────┐    ┌────────▼────────────┐    ┌────────────┐   │
│  │  HTTP /   │    │ ApplicationFunction │    │  Database   │   │
│  │  WebSocket│───►│ Runner              │───►│            │   │
│  │  Server   │    └────────┬────────────┘    └────────────┘   │
│  └──────────┘             │                                   │
│                  ┌────────▼────────────┐                      │
│                  │   FunctionRouter    │                      │
│                  │                     │                      │
│                  │  Routes to either:  │                      │
│                  │  • NativeFunction   │                      │
│                  │    Runner (Rust)    │                      │
│                  │  • InProcessFunction│                      │
│                  │    Runner (V8/JS)   │                      │
│                  └─────────────────────┘                      │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  NativeFunctionRunner                                 │    │
│  │                                                      │    │
│  │  1. Look up function in registry by path             │    │
│  │  2. Create Transaction from DB snapshot              │    │
│  │  3. Build QueryCtx / MutationCtx / ActionCtx        │    │
│  │  4. Call native Rust function directly               │    │
│  │  5. Extract reads/writes from Transaction            │    │
│  │  6. Return FunctionFinalTransaction + FunctionOutcome│    │
│  └──────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌──────────────────────┐                                    │
│  │  Persistence Layer   │  (Postgres / MySQL / SQLite)       │
│  └──────────────────────┘                                    │
└──────────────────────────────────────────────────────────────┘
```

In standalone mode, the developer's binary runs everything: HTTP server,
WebSocket sync, database, and function execution. The `NativeFunctionRunner`
(provided by `convex_native`) replaces or supplements the
`InProcessFunctionRunner`.

### 4.2 Distributed Mode (Multi-Node)

In distributed mode, the developer deploys the same binary (from their
project) with different mode flags. The `convex_native_distributed` crate
provides the gRPC conductor/worker infrastructure:

```
┌─────────────────────────────────────────────────────────────────┐
│                        CONDUCTOR NODE                            │
│                  (developer's binary, mode=conductor)            │
│                                                                 │
│  ┌──────────┐    ┌─────────────────────┐    ┌────────────────┐  │
│  │  HTTP /   │    │ ApplicationFunction │    │  Database       │  │
│  │  WebSocket│───►│ Runner              │───►│  (owns commit, │  │
│  │  Server   │    └────────┬────────────┘    │   subscriptions)│  │
│  └──────────┘             │                 └────────────────┘  │
│                  ┌────────▼────────────┐                        │
│                  │   FunctionRouter    │                        │
│                  └────────┬────────────┘                        │
│                           │                                     │
│                  ┌────────▼────────────────┐                    │
│                  │ DistributedFunction     │                    │
│                  │ Runner (gRPC client)    │                    │
│                  │                         │                    │
│                  │ • Service discovery     │                    │
│                  │ • Load balancing        │                    │
│                  │ • Retry on overload     │                    │
│                  │ • Circuit breaking      │                    │
│                  └────────┬───────────────┘                    │
│                           │                                     │
└───────────────────────────┼─────────────────────────────────────┘
                            │
                            │ gRPC (FunctionExecutionService)
                            │
          ┌─────────────────┼─────────────────────┐
          │                 │                     │
          ▼                 ▼                     ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│   WORKER NODE 1  │ │   WORKER NODE 2  │ │   WORKER NODE N  │
│ (dev's binary,   │ │ (dev's binary,   │ │ (dev's binary,   │
│  mode=worker)    │ │  mode=worker)    │ │  mode=worker)    │
│                  │ │                  │ │                  │
│ ┌──────────────┐ │ │ ┌──────────────┐ │ │ ┌──────────────┐ │
│ │gRPC Server   │ │ │ │gRPC Server   │ │ │ │gRPC Server   │ │
│ │(FunctionExec │ │ │ │(FunctionExec │ │ │ │(FunctionExec │ │
│ │ Service)     │ │ │ │ Service)     │ │ │ │ Service)     │ │
│ └──────┬───────┘ │ │ └──────┬───────┘ │ │ └──────┬───────┘ │
│        │         │ │        │         │ │        │         │
│ ┌──────▼───────┐ │ │ ┌──────▼───────┐ │ │ ┌──────▼───────┐ │
│ │NativeFunction│ │ │ │NativeFunction│ │ │ │NativeFunction│ │
│ │Runner        │ │ │ │Runner        │ │ │ │Runner        │ │
│ │              │ │ │ │              │ │ │ │              │ │
│ │• Function    │ │ │ │• Function    │ │ │ │• Function    │ │
│ │  Registry    │ │ │ │  Registry    │ │ │ │  Registry    │ │
│ │• Index Cache │ │ │ │• Index Cache │ │ │ │• Index Cache │ │
│ └──────┬───────┘ │ │ └──────┬───────┘ │ │ └──────┬───────┘ │
│        │         │ │        │         │ │        │         │
│ ┌──────▼───────┐ │ │ ┌──────▼───────┐ │ │ ┌──────▼───────┐ │
│ │Persistence   │ │ │ │Persistence   │ │ │ │Persistence   │ │
│ │Reader        │ │ │ │Reader        │ │ │ │Reader        │ │
│ │(DB conn pool)│ │ │ │(DB conn pool)│ │ │ │(DB conn pool)│ │
│ └──────┬───────┘ │ │ └──────┬───────┘ │ │ └──────┬───────┘ │
│        │         │ │        │         │ │        │         │
└────────┼─────────┘ └────────┼─────────┘ └────────┼─────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   Shared Database  │
                    │ (Postgres / MySQL) │
                    └───────────────────┘
```

### 4.3 Request Lifecycle: Distributed Query Execution

```
 Client            Conductor                Worker              Database
   │                  │                       │                    │
   │ subscribe(       │                       │                    │
   │  "list_users")   │                       │                    │
   │─────────────────►│                       │                    │
   │                  │                       │                    │
   │                  │ 1. Validate path,     │                    │
   │                  │    resolve component  │                    │
   │                  │                       │                    │
   │                  │ 2. Acquire semaphore  │                    │
   │                  │    (query limiter)    │                    │
   │                  │                       │                    │
   │                  │ 3. Pick worker via    │                    │
   │                  │    load balancer      │                    │
   │                  │                       │                    │
   │                  │ ExecuteFunction {     │                    │
   │                  │   path: "list_users"  │                    │
   │                  │   ts: 1713200000      │                    │
   │                  │   identity: {...}     │                    │
   │                  │   args: {}            │                    │
   │                  │ }                     │                    │
   │                  │──────────────────────►│                    │
   │                  │                       │                    │
   │                  │                       │ 4. Look up         │
   │                  │                       │    "list_users"    │
   │                  │                       │    in registry     │
   │                  │                       │                    │
   │                  │                       │ 5. Create TX       │
   │                  │                       │    at ts           │
   │                  │                       │───────────────────►│
   │                  │                       │◄───────────────────│
   │                  │                       │                    │
   │                  │                       │ 6. Build QueryCtx  │
   │                  │                       │    with TX         │
   │                  │                       │                    │
   │                  │                       │ 7. Call native fn: │
   │                  │                       │    list_users(ctx) │
   │                  │                       │                    │
   │                  │                       │    ctx.db().query  │
   │                  │                       │    ("users")       │
   │                  │                       │───────────────────►│
   │                  │                       │◄───────────────────│
   │                  │                       │                    │
   │                  │                       │ 8. Extract reads/  │
   │                  │                       │    writes from TX  │
   │                  │                       │                    │
   │                  │  FunctionResponse {   │                    │
   │                  │    outcome: Ok([...]) │                    │
   │                  │    reads: ReadSet{..} │                    │
   │                  │    writes: []         │                    │
   │                  │    usage: {...}       │                    │
   │                  │  }                    │                    │
   │                  │◄──────────────────────│                    │
   │                  │                       │                    │
   │                  │ 9. Validate retention │                    │
   │                  │                       │                    │
   │                  │ 10. Register          │                    │
   │                  │     subscription with │                    │
   │                  │     ReadSet           │                    │
   │                  │                       │                    │
   │ { result: [...] }│                       │                    │
   │◄─────────────────│                       │                    │
   │                  │                       │                    │
```

### 4.4 Request Lifecycle: Distributed Mutation Execution

```
 Client            Conductor                Worker              Database
   │                  │                       │                    │
   │ mutate(          │                       │                    │
   │  "create_user",  │                       │                    │
   │  {name, email})  │                       │                    │
   │─────────────────►│                       │                    │
   │                  │                       │                    │
   │                  │ 1-3. Same as query    │                    │
   │                  │      (validate, pick  │                    │
   │                  │       worker)         │                    │
   │                  │                       │                    │
   │                  │ ExecuteFunction {     │                    │
   │                  │   type: Mutation      │                    │
   │                  │   path: "create_user" │                    │
   │                  │   args: {name, email} │                    │
   │                  │   ts: 1713200000      │                    │
   │                  │ }                     │                    │
   │                  │──────────────────────►│                    │
   │                  │                       │                    │
   │                  │                       │ 4-6. Same as query │
   │                  │                       │                    │
   │                  │                       │ 7. Call native fn: │
   │                  │                       │    create_user(ctx)│
   │                  │                       │                    │
   │                  │                       │    ctx.db().insert │
   │                  │                       │    ("users", doc)  │
   │                  │                       │     (buffered in   │
   │                  │                       │      Transaction)  │
   │                  │                       │                    │
   │                  │                       │ 8. Extract reads + │
   │                  │                       │    WRITES from TX  │
   │                  │                       │                    │
   │                  │  FunctionResponse {   │                    │
   │                  │    outcome: Ok(id)    │                    │
   │                  │    reads: ReadSet{..} │                    │
   │                  │    writes: [Insert{   │                    │
   │                  │      table: "users",  │                    │
   │                  │      doc: {...}       │                    │
   │                  │    }]                 │                    │
   │                  │  }                    │                    │
   │                  │◄──────────────────────│                    │
   │                  │                       │                    │
   │                  │ 9. Validate retention │                    │
   │                  │                       │                    │
   │                  │ 10. OCC: Check read   │                    │
   │                  │     set conflicts     │                    │
   │                  │                       │                    │
   │                  │ 11. COMMIT writes ────│───────────────────►│
   │                  │                       │                    │
   │                  │ 12. Notify sync layer │                    │
   │                  │     of affected       │                    │
   │                  │     subscriptions     │                    │
   │                  │                       │                    │
   │ { result: id }   │                       │                    │
   │◄─────────────────│                       │                    │
   │                  │                       │                    │
   │ (re-run affected │                       │                    │
   │  query subs      │                       │                    │
   │  automatically)  │                       │                    │
   │◄─────────────────│                       │                    │
```

### 4.5 Reactive Re-execution Flow

When a mutation commits, the sync layer detects which query subscriptions are
affected by comparing the mutation's write set against each subscription's
read set. Affected queries are re-executed — potentially on different worker
nodes:

```
 Conductor                                  Workers
   │                                          │
   │ Mutation committed at ts=1001            │
   │ Write set: {users/doc123}                │
   │                                          │
   │ Subscription scan:                       │
   │  sub_A (list_users) reads {users/*} ──── AFFECTED
   │  sub_B (get_config) reads {config/*} ─── not affected
   │                                          │
   │ Re-execute list_users at ts=1001         │
   │──────────────────────────────────────────►│ Worker 3
   │                                          │ (may differ from
   │◄──────────────────────────────────────────│  original worker)
   │                                          │
   │ Push updated result to sub_A clients     │
   │                                          │
```

This reactive loop works identically for native Rust functions because it
depends only on the `ReadSet` returned by the `FunctionRunner`, not on how
the function was executed.

---

## 5. Rust Schema DSL and Type System

The schema is the foundation of type safety. Every table is defined as a Rust
struct with `#[derive(ConvexDocument)]`. The derive macro generates all the
associated types (IDs, indexes, field enums, patch structs) and registers the
table in the global schema.

### 5.1 The `ConvexDocument` Derive Macro

```rust
// Defined by the developer:

#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
#[convex(index(name = "by_created", fields = ["created_at"]))]
pub struct User {
    pub name: String,
    pub email: String,
    pub avatar_url: Option<String>,
    pub role: UserRole,
    pub created_at: f64,
}

#[derive(ConvexEnum, Debug, Clone, PartialEq)]
pub enum UserRole {
    Admin,
    Member,
    Guest,
}
```

### 5.2 What the Derive Macro Generates

For each `#[derive(ConvexDocument)]` struct, the macro generates the
following companion types:

```
    #[derive(ConvexDocument)]
    struct User { ... }
         │
         │ generates:
         │
         ├──► Id<User>              Phantom-typed document ID
         ├──► UserField             Enum of field names (for query filters)
         ├──► UserIndex             Enum of index names (for query builders)
         ├──► UserPatch             Optional-field struct (for partial updates)
         ├──► UserWithId            User + its Id<User> (what queries return)
         ├──► impl ConvexDocument   Trait impl: serialization, table name, schema
         └──► inventory::submit!    Registers table in global schema registry
```

#### 5.2.1 Generated Code (Expanded)

```rust
// ─── Id<User>: phantom-typed document ID ───────────────────────
// (Id<T> is generic, defined once in convex_native)

pub struct Id<T: ConvexDocument> {
    inner: DeveloperDocumentId,
    _phantom: PhantomData<T>,
}

impl<T: ConvexDocument> Id<T> {
    /// Get the raw string representation (e.g., "k57a9c3...")
    pub fn to_string(&self) -> String { ... }

    /// Construct from a raw string (validated at runtime)
    pub fn from_str(s: &str) -> Result<Self> { ... }
}

// Compile-time safety: cannot pass Id<User> where Id<Message> is expected.
// fn delete_message(ctx: &mut MutationCtx, id: Id<Message>) { ... }
// delete_message(ctx, user_id)  // ← COMPILE ERROR: expected Id<Message>, got Id<User>


// ─── UserField: enum of field names ────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserField {
    Name,
    Email,
    AvatarUrl,
    Role,
    CreatedAt,
}

impl UserField {
    /// Returns the Convex field path string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Email => "email",
            Self::AvatarUrl => "avatar_url",
            Self::Role => "role",
            Self::CreatedAt => "created_at",
        }
    }
}

impl FieldReference for UserField {
    type Document = User;
}


// ─── UserIndex: enum of index names ────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserIndex {
    ByEmail,
    ByCreated,
}

impl UserIndex {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ByEmail => "by_email",
            Self::ByCreated => "by_created",
        }
    }

    /// Returns the fields this index covers (for compile-time validation)
    pub fn fields(&self) -> &'static [UserField] {
        match self {
            Self::ByEmail => &[UserField::Email],
            Self::ByCreated => &[UserField::CreatedAt],
        }
    }
}

impl IndexReference for UserIndex {
    type Document = User;
}


// ─── UserPatch: partial update struct ──────────────────────────

#[derive(Default, Debug, Clone)]
pub struct UserPatch {
    pub name: Option<String>,
    pub email: Option<String>,
    pub avatar_url: Option<Option<String>>,  // Option<Option<T>> for nullable fields:
                                              //   None = don't touch
                                              //   Some(None) = set to null
                                              //   Some(Some(v)) = set to v
    pub role: Option<UserRole>,
    pub created_at: Option<f64>,
}

impl ConvexPatch for UserPatch {
    type Document = User;

    fn to_object(&self) -> Result<ConvexObject> {
        let mut fields = BTreeMap::new();
        if let Some(ref name) = self.name {
            fields.insert("name".into(), name.to_convex()?);
        }
        if let Some(ref email) = self.email {
            fields.insert("email".into(), email.to_convex()?);
        }
        // ... for each field
        ConvexObject::try_from(fields)
    }
}


// ─── UserWithId: document + ID bundle ──────────────────────────

#[derive(Debug, Clone)]
pub struct UserWithId {
    pub id: Id<User>,
    pub doc: User,
}

impl std::ops::Deref for UserWithId {
    type Target = User;
    fn deref(&self) -> &User { &self.doc }
}


// ─── ConvexDocument trait impl ─────────────────────────────────

impl ConvexDocument for User {
    type Id = Id<User>;
    type Field = UserField;
    type Index = UserIndex;
    type Patch = UserPatch;
    type WithId = UserWithId;

    fn table_name() -> &'static str { "users" }

    fn to_convex_object(&self) -> Result<ConvexObject> {
        let mut fields = BTreeMap::new();
        fields.insert("name".into(), self.name.to_convex()?);
        fields.insert("email".into(), self.email.to_convex()?);
        fields.insert("avatar_url".into(), self.avatar_url.to_convex()?);
        fields.insert("role".into(), self.role.to_convex()?);
        fields.insert("created_at".into(), self.created_at.to_convex()?);
        ConvexObject::try_from(fields)
    }

    fn from_convex_object(obj: &ConvexObject) -> Result<Self> {
        Ok(Self {
            name: String::from_convex(obj.get("name"))?,
            email: String::from_convex(obj.get("email"))?,
            avatar_url: Option::<String>::from_convex(obj.get("avatar_url"))?,
            role: UserRole::from_convex(obj.get("role"))?,
            created_at: f64::from_convex(obj.get("created_at"))?,
        })
    }

    fn table_definition() -> TableDefinition {
        TableDefinition {
            table_name: "users".parse().unwrap(),
            indexes: btreemap! {
                "by_email".parse().unwrap() => IndexSchema {
                    index_descriptor: "by_email".parse().unwrap(),
                    fields: vec!["email".parse().unwrap()].try_into().unwrap(),
                },
                "by_created".parse().unwrap() => IndexSchema {
                    index_descriptor: "by_created".parse().unwrap(),
                    fields: vec!["created_at".parse().unwrap()].try_into().unwrap(),
                },
            },
            document_type: Some(DocumentSchema::Union(vec![
                // Generated from struct field types
            ])),
            ..Default::default()
        }
    }
}

// ─── Schema registration ───────────────────────────────────────

inventory::submit! {
    TableRegistration {
        table_name: "users",
        definition_fn: || User::table_definition(),
    }
}
```

### 5.3 Convex Type Mapping

The derive macro maps Rust types to Convex's type system
(`ConvexValue` variants as defined in `crates/value/src/lib.rs:123`):

```
┌──────────────────────────────────────────────────────────────┐
│              Rust Type → Convex Type Mapping                  │
├──────────────────────┬───────────────────────────────────────┤
│  Rust Type           │  ConvexValue Variant                  │
├──────────────────────┼───────────────────────────────────────┤
│  String              │  ConvexValue::String(ConvexString)    │
│  i64                 │  ConvexValue::Int64(i64)              │
│  f64                 │  ConvexValue::Float64(f64)            │
│  bool                │  ConvexValue::Boolean(bool)           │
│  Vec<u8> / Bytes     │  ConvexValue::Bytes(ConvexBytes)      │
│  Vec<T>              │  ConvexValue::Array(ConvexArray)       │
│  Option<T>           │  T or ConvexValue::Null               │
│  Id<T>               │  ConvexValue::String (encoded ID)     │
│  #[derive(ConvexEnum)]│ ConvexValue::String (variant name)   │
│  nested struct       │  ConvexValue::Object(ConvexObject)    │
│  BTreeMap<String, V> │  ConvexValue::Object(ConvexObject)    │
│  serde_json::Value   │  Dynamic (any ConvexValue)            │
└──────────────────────┴───────────────────────────────────────┘
```

The conversion is implemented via two traits:

```rust
/// Convert a Rust value into a ConvexValue
pub trait ToConvex {
    fn to_convex(&self) -> Result<ConvexValue>;
}

/// Convert a ConvexValue into a Rust value
pub trait FromConvex: Sized {
    fn from_convex(value: Option<&ConvexValue>) -> Result<Self>;
}

// Blanket implementations for standard types:
impl ToConvex for String { ... }
impl ToConvex for i64 { ... }
impl ToConvex for f64 { ... }
impl ToConvex for bool { ... }
impl<T: ToConvex> ToConvex for Vec<T> { ... }
impl<T: ToConvex> ToConvex for Option<T> { ... }
impl<T: ConvexDocument> ToConvex for Id<T> { ... }
// etc.
```

### 5.4 Nested Documents and Enums

#### Nested Structs

```rust
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "orders")]
pub struct Order {
    pub customer: Id<User>,
    pub items: Vec<OrderItem>,       // ← nested struct in array
    pub shipping: ShippingAddress,   // ← nested struct as object
    pub total_cents: i64,
}

#[derive(ConvexNested, Debug, Clone)]
pub struct OrderItem {
    pub product_name: String,
    pub quantity: i64,
    pub price_cents: i64,
}

#[derive(ConvexNested, Debug, Clone)]
pub struct ShippingAddress {
    pub street: String,
    pub city: String,
    pub country: String,
    pub zip: String,
}
```

`#[derive(ConvexNested)]` generates `ToConvex`/`FromConvex` impls that
serialize to/from `ConvexObject`, but does NOT register a table (nested
structs are embedded, not top-level tables).

#### Enums (Tagged Unions)

```rust
#[derive(ConvexEnum, Debug, Clone)]
pub enum PaymentStatus {
    Pending,
    Charged,
    Refunded,
    Failed,
}
// Serialized as: ConvexValue::String("pending"), "charged", etc.

#[derive(ConvexUnion, Debug, Clone)]
#[convex(tag = "type")]
pub enum NotificationChannel {
    Email { address: String },
    Sms { phone: String },
    Push { device_token: String },
}
// Serialized as: { "type": "email", "address": "..." }
// Uses a discriminant field for tagged union encoding.
```

### 5.5 Schema Registration and Evaluation

All `#[derive(ConvexDocument)]` structs register their table definitions via
`inventory`. At startup, the schema is assembled:

```rust
pub struct NativeSchema;

impl NativeSchema {
    /// Collect all registered tables into a DatabaseSchema.
    /// Called by NativeFunctionRunner::evaluate_schema().
    pub fn collect() -> DatabaseSchema {
        let mut tables = BTreeMap::new();
        for registration in inventory::iter::<TableRegistration> {
            let definition = (registration.definition_fn)();
            tables.insert(definition.table_name.clone(), definition);
        }
        DatabaseSchema {
            tables,
            schema_validation: true,
        }
    }
}
```

This maps directly to the existing `DatabaseSchema` struct
(`crates/common/src/schemas/mod.rs:144`), which contains
`BTreeMap<TableName, TableDefinition>`. The `TableDefinition`
(`crates/common/src/schemas/mod.rs:447`) includes indexes, text indexes,
vector indexes, and the document schema validator — all generated from the
derive macro attributes.

### 5.6 Schema Visualization

```
    Developer writes:                    Macro generates:
    ─────────────────                    ────────────────

    #[derive(ConvexDocument)]      ──►   DatabaseSchema
    #[convex(table = "users")]           ├── tables:
    #[convex(index(...))]                │   ├── "users" → TableDefinition
    struct User {                        │   │   ├── indexes: {by_email, by_created}
        name: String,                    │   │   ├── document_type: DocumentSchema::Union([
        email: String,                   │   │   │     {name: String, email: String, ...}
        ...                              │   │   │   ])
    }                                    │   │   └── (text/vector indexes if declared)
                                         │   │
    #[derive(ConvexDocument)]            │   └── "messages" → TableDefinition
    #[convex(table = "messages")]        │       ├── indexes: {by_channel}
    struct Message {                     │       └── document_type: ...
        author: Id<User>,               │
        body: String,                    └── schema_validation: true
        ...
    }
```

---

## 6. Type-Safe Document API

### 6.1 The Core Traits

```rust
/// Marker trait for any type that represents a Convex table.
/// Implemented by #[derive(ConvexDocument)].
pub trait ConvexDocument: Sized + Send + Sync + 'static {
    /// The phantom-typed ID for this table
    type Id;
    /// Enum of field names
    type Field: FieldReference<Document = Self>;
    /// Enum of index names
    type Index: IndexReference<Document = Self>;
    /// Partial-update struct
    type Patch: ConvexPatch<Document = Self>;
    /// Document + ID bundle
    type WithId;

    /// The Convex table name (e.g., "users")
    fn table_name() -> &'static str;

    /// Serialize this struct to a ConvexObject
    fn to_convex_object(&self) -> Result<ConvexObject>;

    /// Deserialize from a ConvexObject
    fn from_convex_object(obj: &ConvexObject) -> Result<Self>;

    /// Returns the full TableDefinition for schema registration
    fn table_definition() -> TableDefinition;
}

/// Marker trait linking a field enum to its document type.
pub trait FieldReference: Copy + Send + Sync + 'static {
    type Document: ConvexDocument;
    fn as_str(&self) -> &'static str;
}

/// Marker trait linking an index enum to its document type.
pub trait IndexReference: Copy + Send + Sync + 'static {
    type Document: ConvexDocument;
    fn as_str(&self) -> &'static str;
}

/// A partial update for a document type.
pub trait ConvexPatch: Default + Send + Sync + 'static {
    type Document: ConvexDocument;
    fn to_object(&self) -> Result<ConvexObject>;
}
```

### 6.2 Type-Safe Database Operations

#### Typed Reads

```rust
impl<'a, RT: Runtime> QueryDb<'a, RT> {
    /// Get a typed document by its phantom-typed ID.
    ///
    /// get(user_id)     → Ok(Some(User))     ✓
    /// get(message_id)  → COMPILE ERROR       ✗ (wrong Id type)
    pub async fn get<T: ConvexDocument>(
        &mut self,
        id: Id<T>,
    ) -> Result<Option<T::WithId>> {
        let raw = self.tx.get(id.inner()).await?;
        match raw {
            Some(doc) => Ok(Some(T::WithId::from_raw(id, T::from_convex_object(&doc)?))),
            None => Ok(None),
        }
    }

    /// Start a typed query. The returned builder only accepts
    /// indexes and fields belonging to type T.
    ///
    /// query::<User>().with_index(UserIndex::ByEmail)      ✓
    /// query::<User>().with_index(MessageIndex::ByChannel)  COMPILE ERROR
    pub fn query<T: ConvexDocument>(&mut self) -> TypedQueryBuilder<'_, RT, T> {
        TypedQueryBuilder::new(self.tx, T::table_name())
    }
}
```

#### Typed Writes

```rust
impl<'a, RT: Runtime> MutationDb<'a, RT> {
    /// Insert a typed document. Returns a phantom-typed Id<T>.
    ///
    /// insert(User { name: "Alice", ... })    → Ok(Id<User>)
    /// insert(Message { body: "hi", ... })    → Ok(Id<Message>)
    pub async fn insert<T: ConvexDocument>(
        &mut self,
        document: T,
    ) -> Result<Id<T>> {
        let obj = document.to_convex_object()?;
        let raw_id = self.tx.insert(T::table_name(), obj).await?;
        Ok(Id::from_raw(raw_id))
    }

    /// Replace a document entirely. Type-checked: the Id and document
    /// must belong to the same table.
    ///
    /// replace(user_id, User { ... })       ✓
    /// replace(user_id, Message { ... })    COMPILE ERROR
    pub async fn replace<T: ConvexDocument>(
        &mut self,
        id: Id<T>,
        document: T,
    ) -> Result<T::WithId> {
        let obj = document.to_convex_object()?;
        let raw = self.tx.replace(id.inner(), obj).await?;
        Ok(T::WithId::from_raw(id, T::from_convex_object(&raw)?))
    }

    /// Patch a document using the typed patch struct.
    /// Only fields set to Some(...) are updated.
    ///
    /// patch(user_id, UserPatch { name: Some("Bob"), ..Default::default() })   ✓
    /// patch(user_id, MessagePatch { ... })    COMPILE ERROR (wrong patch type)
    pub async fn patch<T: ConvexDocument>(
        &mut self,
        id: Id<T>,
        patch: T::Patch,
    ) -> Result<T::WithId> {
        let obj = patch.to_object()?;
        let raw = self.tx.patch(id.inner(), obj).await?;
        Ok(T::WithId::from_raw(id, T::from_convex_object(&raw)?))
    }

    /// Delete a document by its typed ID.
    ///
    /// delete(user_id: Id<User>)       ✓
    /// delete(message_id: Id<Message>)  ✓ (but different type)
    pub async fn delete<T: ConvexDocument>(
        &mut self,
        id: Id<T>,
    ) -> Result<()> {
        self.tx.delete(id.inner()).await
    }
}
```

### 6.3 Type-Safe Query Builder

```rust
pub struct TypedQueryBuilder<'a, RT: Runtime, T: ConvexDocument> {
    tx: &'a mut Transaction<RT>,
    table: &'static str,
    index: Option<String>,
    filters: Vec<Filter>,
    order: Order,
    limit: Option<usize>,
    _phantom: PhantomData<T>,
}

impl<'a, RT: Runtime, T: ConvexDocument> TypedQueryBuilder<'a, RT, T> {
    /// Set the index to query. Only accepts indexes defined on T.
    ///
    /// .with_index(UserIndex::ByEmail)        ✓ (if T = User)
    /// .with_index(MessageIndex::ByChannel)   COMPILE ERROR (if T = User)
    pub fn with_index(mut self, index: T::Index) -> Self {
        self.index = Some(index.as_str().to_string());
        self
    }

    /// Add an equality filter. Only accepts fields defined on T.
    ///
    /// .eq(UserField::Email, "alice@example.com")    ✓
    /// .eq(MessageField::Body, "hello")              COMPILE ERROR (if T = User)
    pub fn eq(mut self, field: T::Field, value: impl ToConvex) -> Self {
        self.filters.push(Filter::Eq(field.as_str().to_string(), value.to_convex().unwrap()));
        self
    }

    /// Comparison filters
    pub fn gt(mut self, field: T::Field, value: impl ToConvex) -> Self { ... }
    pub fn gte(mut self, field: T::Field, value: impl ToConvex) -> Self { ... }
    pub fn lt(mut self, field: T::Field, value: impl ToConvex) -> Self { ... }
    pub fn lte(mut self, field: T::Field, value: impl ToConvex) -> Self { ... }

    /// Set ordering
    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    /// Limit results
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Execute and collect all results as Vec<T::WithId>
    pub async fn collect(self) -> Result<Vec<T::WithId>> {
        let raw_docs = self.execute_raw().await?;
        raw_docs.into_iter()
            .map(|(id, obj)| {
                let doc = T::from_convex_object(&obj)?;
                Ok(T::WithId::from_raw(Id::from_raw(id), doc))
            })
            .collect()
    }

    /// Execute and return only the first result
    pub async fn first(self) -> Result<Option<T::WithId>> {
        Ok(self.limit(1).collect().await?.into_iter().next())
    }

    /// Execute with pagination
    pub async fn page(
        self,
        cursor: Option<Cursor>,
        page_size: usize,
    ) -> Result<TypedPage<T>> {
        // ...
    }
}

pub struct TypedPage<T: ConvexDocument> {
    pub items: Vec<T::WithId>,
    pub cursor: Option<Cursor>,
    pub is_done: bool,
}
```

### 6.4 Type Safety Guarantees at a Glance

```
┌──────────────────────────────────────────────────────────────────┐
│              What the Compiler Catches                            │
├─────────────────────────────────────┬────────────────────────────┤
│  Mistake                            │  Error                     │
├─────────────────────────────────────┼────────────────────────────┤
│  Pass Id<User> to delete_message()  │  Expected Id<Message>,     │
│                                     │  found Id<User>            │
├─────────────────────────────────────┼────────────────────────────┤
│  Query users with MessageIndex      │  Expected UserIndex,       │
│                                     │  found MessageIndex        │
├─────────────────────────────────────┼────────────────────────────┤
│  Filter users by MessageField       │  Expected UserField,       │
│                                     │  found MessageField        │
├─────────────────────────────────────┼────────────────────────────┤
│  Insert Message where User expected │  Expected User,            │
│                                     │  found Message             │
├─────────────────────────────────────┼────────────────────────────┤
│  Patch user with MessagePatch       │  Expected UserPatch,       │
│                                     │  found MessagePatch        │
├─────────────────────────────────────┼────────────────────────────┤
│  Missing required field in struct   │  Missing field `email`     │
│                                     │  in struct `User`          │
├─────────────────────────────────────┼────────────────────────────┤
│  Wrong type for field               │  Expected String,          │
│                                     │  found i64                 │
├─────────────────────────────────────┼────────────────────────────┤
│  Access field that doesn't exist    │  No variant `Foo` in       │
│                                     │  enum `UserField`          │
├─────────────────────────────────────┼────────────────────────────┤
│  Return wrong type from query fn    │  Expected Vec<User>,       │
│                                     │  found Vec<Message>        │
└─────────────────────────────────────┴────────────────────────────┘
```

### 6.5 Untyped Escape Hatch

For dynamic use cases (migrations, generic utilities), the untyped API
remains available:

```rust
// Typed (preferred):
let users: Vec<UserWithId> = ctx.db().query::<User>().collect().await?;

// Untyped (escape hatch):
let docs: Vec<Document> = ctx.db().query_raw("users").collect().await?;
let value: ConvexValue = docs[0].get("name").cloned().unwrap();
```

### 6.6 Type-Safe Action Sub-calls

Actions calling queries/mutations are also typed:

```rust
#[convex::action]
async fn notify_user(ctx: &mut ActionCtx, user_id: Id<User>) -> Result<()> {
    // Type-safe: compiler knows get_user_by_email returns Option<User>
    let user: Option<User> = ctx.run_query(
        get_user_by_email,                    // function reference, not string
        GetUserByEmailArgs { email: "...".into() },
    ).await?;

    // vs. untyped (still available):
    let raw: Value = ctx.run_query_raw("get_user_by_email", args! {
        "email" => "..."
    }).await?;

    Ok(())
}
```

The typed `run_query` / `run_mutation` uses the proc-macro-generated
args struct and return type to enforce correctness at compile time:

```rust
// Generated by #[convex::query] for get_user_by_email:
pub struct GetUserByEmailArgs {
    pub email: String,
}

// The ActionCtx method:
impl ActionCtx {
    pub async fn run_query<F, A, R>(
        &self,
        _func: F,        // only used for type inference, not called directly
        args: A,
    ) -> Result<R>
    where
        F: ConvexQueryFunction<Args = A, Output = R>,
        A: ConvexArgs,
        R: FromConvex,
    { ... }
}
```

---

## 7. Developer Experience: Proc Macros and Function Registry

### 7.1 Proc Macro Attributes

Three proc macros are introduced, each validating the function signature and
generating a registry entry:

```rust
// crates/convex_macro/src/lib.rs (extended)

#[convex::query]    // ctx: &mut QueryCtx,    no side effects
#[convex::mutation] // ctx: &mut MutationCtx, can write to DB
#[convex::action]   // ctx: &mut ActionCtx,   can call external APIs
```

#### Signature Requirements

| Attribute | First parameter | Return type | Restrictions |
|-----------|-----------------|-------------|--------------|
| `#[convex::query]` | `ctx: &mut QueryCtx` | `Result<Value>` | No writes, no external I/O |
| `#[convex::mutation]` | `ctx: &mut MutationCtx` | `Result<Value>` | No external I/O |
| `#[convex::action]` | `ctx: &mut ActionCtx` | `Result<Value>` | Full access |

Additional parameters after `ctx` are extracted from the function's `args`
object. The proc macro generates deserialization code for each parameter.

#### Example Expansion

Given:

```rust
#[convex::mutation]
async fn create_user(
    ctx: &mut MutationCtx,
    name: String,
    email: String,
) -> Result<Value> {
    let id = ctx.db().insert("users", convex_object! {
        "name" => name,
        "email" => email,
    }).await?;
    Ok(id.into())
}
```

The macro expands to:

```rust
// The original function, renamed
async fn __create_user_impl(
    ctx: &mut MutationCtx,
    name: String,
    email: String,
) -> Result<Value> {
    let id = ctx.db().insert("users", convex_object! {
        "name" => name,
        "email" => email,
    }).await?;
    Ok(id.into())
}

// Registry entry (using the `inventory` crate for static collection)
inventory::submit! {
    NativeFunctionRegistration {
        name: "create_user",
        udf_type: UdfType::Mutation,
        handler: |ctx_any: &mut dyn NativeCtx, args: ConvexObject| -> BoxFuture<Result<Value>> {
            Box::pin(async move {
                let ctx = ctx_any.as_mutation_ctx();
                let name: String = args.get("name")
                    .ok_or_else(|| anyhow!("missing argument: name"))?
                    .try_into()?;
                let email: String = args.get("email")
                    .ok_or_else(|| anyhow!("missing argument: email"))?
                    .try_into()?;
                __create_user_impl(ctx, name, email).await
            })
        },
        arg_names: &["name", "email"],
    }
}
```

### 7.2 Function Registry

The registry collects all `inventory::submit!` entries at program startup:

```rust
pub struct NativeFunctionRegistry {
    functions: HashMap<String, NativeFunctionRegistration>,
}

pub struct NativeFunctionRegistration {
    pub name: &'static str,
    pub udf_type: UdfType,
    pub handler: fn(&mut dyn NativeCtx, ConvexObject) -> BoxFuture<Result<Value>>,
    pub arg_names: &'static [&'static str],
}

impl NativeFunctionRegistry {
    /// Build from all statically-registered functions
    pub fn collect() -> Self {
        let mut functions = HashMap::new();
        for registration in inventory::iter::<NativeFunctionRegistration> {
            functions.insert(
                registration.name.to_string(),
                registration.clone(),
            );
        }
        Self { functions }
    }

    /// Look up a function by path
    pub fn get(&self, path: &str) -> Option<&NativeFunctionRegistration> {
        self.functions.get(path)
    }

    /// List all registered functions (used by analyze())
    pub fn list(&self) -> impl Iterator<Item = &NativeFunctionRegistration> {
        self.functions.values()
    }
}
```

### 7.3 Module Organization

Functions can be organized in modules that map to Convex's path conventions:

```rust
// src/functions/users.rs
#[convex::query]
async fn list(ctx: &mut QueryCtx) -> Result<Value> { ... }

#[convex::mutation]
async fn create(ctx: &mut MutationCtx, name: String) -> Result<Value> { ... }

// These register as "users:list" and "users:create" respectively.
// The module path is derived from the file path relative to src/functions/.
```

Alternatively, explicit path overrides:

```rust
#[convex::query(name = "users:listActive")]
async fn list_active_users(ctx: &mut QueryCtx) -> Result<Value> { ... }
```

### 7.4 Internal Functions

Convex distinguishes between **public** functions (callable by clients via
WebSocket/HTTP) and **internal** functions (only callable from other
server-side functions, cron jobs, and schedulers). This is enforced at
runtime via `FunctionCaller::allowed_visibility()` in
`crates/common/src/types/functions.rs:269`:

```
  FunctionCaller          allowed_visibility()
  ─────────────────────   ─────────────────────
  SyncWorker (client)  →  PublicOnly
  HttpApi (client)     →  PublicOnly
  HttpEndpoint         →  PublicOnly
  ─────────────────────   ─────────────────────
  Action (sub-call)    →  All (public + internal)
  Cron                 →  All
  Scheduler            →  All
  Tester (dashboard)   →  All
```

#### Internal Attribute

Internal functions use a `visibility` parameter on the proc macro:

```rust
/// Public: callable from clients.
#[convex::query]
async fn list_users(ctx: &mut QueryCtx) -> Result<Vec<UserWithId>> { ... }

/// Internal: only callable from other server-side functions,
/// cron jobs, or schedulers. Clients get a "function not found" error.
#[convex::query(internal)]
async fn list_users_admin(ctx: &mut QueryCtx) -> Result<Vec<UserWithId>> { ... }

/// Internal mutation — used by scheduled jobs.
#[convex::mutation(internal)]
async fn cleanup_expired_sessions(ctx: &mut MutationCtx) -> Result<()> { ... }

/// Internal action — called from other actions or scheduled.
#[convex::action(internal)]
async fn sync_to_external_api(ctx: &mut ActionCtx) -> Result<()> { ... }
```

#### Generated Registration

The `internal` flag is stored in the `NativeFunctionRegistration`:

```rust
pub struct NativeFunctionRegistration {
    pub name: &'static str,
    pub udf_type: UdfType,
    pub visibility: Visibility,    // ← Public or Internal
    pub handler: ...,
    pub arg_names: &'static [&'static str],
}

// Generated by #[convex::query(internal)]:
inventory::submit! {
    NativeFunctionRegistration {
        name: "list_users_admin",
        udf_type: UdfType::Query,
        visibility: Visibility::Internal,  // ← enforced at dispatch time
        handler: ...,
        arg_names: &[],
    }
}
```

#### Enforcement in NativeFunctionRunner

When `NativeFunctionRunner::run_function` dispatches a call, it checks
visibility against the caller:

```rust
// Inside NativeFunctionRunner::run_function:
let registration = self.registry.get(&function_path)?;

// Check visibility
if registration.visibility == Visibility::Internal
    && caller.allowed_visibility() == AllowedVisibility::PublicOnly
{
    return Err(JsError::missing_or_internal_error(function_path));
}
```

This matches the existing behavior in `crates/model/src/modules/` where
`Visibility::Internal` functions return a "function not found" error to
unauthorized callers, rather than "permission denied" (to avoid leaking
the function's existence).

#### Usage in Actions and Scheduling

Internal functions are typically called from:

```rust
// From an action — sub-calls have AllowedVisibility::All
#[convex::action]
async fn daily_sync(ctx: &mut ActionCtx) -> Result<()> {
    // Can call internal mutations/queries
    ctx.run_mutation(cleanup_expired_sessions, CleanupExpiredSessionsArgs {}).await?;
    ctx.run_action(sync_to_external_api, SyncToExternalApiArgs {}).await?;
    Ok(())
}

// Scheduled — Scheduler caller has AllowedVisibility::All
#[convex::mutation]
async fn schedule_daily_sync(ctx: &mut MutationCtx) -> Result<()> {
    ctx.scheduler().run_after(
        Duration::from_secs(86400),
        daily_sync,                   // internal action — OK from scheduler
        DailySyncArgs {},
    ).await
}
```

### 7.5 HTTP Actions

HTTP actions are a special function type that maps HTTP requests directly to
handler functions. Unlike regular actions (which are invoked by name with
JSON args), HTTP actions receive an HTTP request (method, path, headers,
body) and return an HTTP response (status, headers, streaming body).

#### How HTTP Actions Work in the Current Codebase

```
  Client HTTP request
       │
       ▼
  ┌──────────────┐     ┌──────────────────────────┐
  │ local_backend │     │ ApplicationFunctionRunner │
  │ HTTP server   │────►│ ::run_http_action()       │
  └──────────────┘     └──────────┬───────────────┘
                                  │
                       ┌──────────▼───────────────┐
                       │ HTTP Routing              │
                       │                          │
                       │ 1. Match method + path   │
                       │ 2. Resolve component     │
                       │ 3. Find handler in http.js│
                       └──────────┬───────────────┘
                                  │
                       ┌──────────▼───────────────┐
                       │ FunctionRunner            │
                       │ ::run_function(           │
                       │   UdfType::HttpAction,    │
                       │   http_action_metadata)   │
                       └──────────┬───────────────┘
                                  │
                       ┌──────────▼───────────────┐
                       │ Isolate (V8)              │
                       │                          │
                       │ Receives HttpActionRequest│
                       │ Streams back:            │
                       │  • HttpActionResponsePart │
                       │    ::Head(status, headers)│
                       │  • HttpActionResponsePart │
                       │    ::BodyChunk(bytes)     │
                       └──────────────────────────┘
```

Key types from `crates/udf/`:
- `HttpActionRequest` — contains `HttpActionRequestHead` (URL, method, headers) + body stream
- `HttpActionResponseStreamer` — sends `HttpActionResponsePart`s back (head then body chunks)
- `HttpActionResponsePart::Head(StatusCode, HeaderMap)` — the response header
- `HttpActionResponsePart::BodyChunk(Bytes)` — streaming body chunks

#### Native HTTP Action Macro

```rust
use convex_native::prelude::*;

/// Route: POST /api/webhooks/stripe
#[convex::http_action(method = "POST", path = "/api/webhooks/stripe")]
async fn stripe_webhook(ctx: &mut HttpActionCtx, req: HttpRequest) -> Result<HttpResponse> {
    let body = req.body_bytes().await?;
    let signature = req.header("Stripe-Signature")
        .ok_or_else(|| anyhow!("Missing signature"))?;

    // Verify webhook signature
    verify_stripe_signature(&body, signature)?;

    // Process the event — call internal mutation
    let event: StripeEvent = serde_json::from_slice(&body)?;
    ctx.run_mutation(process_stripe_event, ProcessStripeEventArgs {
        event_type: event.event_type,
        data: event.data,
    }).await?;

    Ok(HttpResponse::new(200))
}

/// Route: GET /api/health
#[convex::http_action(method = "GET", path = "/api/health")]
async fn health_check(_ctx: &mut HttpActionCtx, _req: HttpRequest) -> Result<HttpResponse> {
    Ok(HttpResponse::json(200, serde_json::json!({ "status": "ok" })))
}

/// Route: GET /api/files/:id — path parameters via wildcard
#[convex::http_action(method = "GET", path = "/api/files/*")]
async fn serve_file(ctx: &mut HttpActionCtx, req: HttpRequest) -> Result<HttpResponse> {
    let file_id = req.path_remainder();  // everything after /api/files/
    let url = ctx.storage().get_url(file_id.parse()?).await?
        .ok_or_else(|| anyhow!("File not found"))?;

    Ok(HttpResponse::redirect(302, &url))
}

/// Route: POST /api/upload — streaming response
#[convex::http_action(method = "POST", path = "/api/upload")]
async fn upload_file(ctx: &mut HttpActionCtx, req: HttpRequest) -> Result<HttpResponse> {
    let content_type = req.header("Content-Type").unwrap_or("application/octet-stream");
    let body = req.body_bytes().await?;

    let storage_id = ctx.storage().store(body, content_type).await?;

    Ok(HttpResponse::json(201, serde_json::json!({
        "storageId": storage_id.to_string(),
    })))
}
```

#### HttpActionCtx and Request/Response Types

```rust
/// Context for HTTP action handlers.
/// Has the same capabilities as ActionCtx (side effects allowed),
/// plus access to the raw HTTP request.
pub struct HttpActionCtx {
    inner: ActionCtx,
}

impl HttpActionCtx {
    // Delegates all ActionCtx methods:
    pub async fn run_query<F, A, R>(&self, func: F, args: A) -> Result<R> { ... }
    pub async fn run_mutation<F, A, R>(&self, func: F, args: A) -> Result<R> { ... }
    pub async fn run_action<F, A, R>(&self, func: F, args: A) -> Result<R> { ... }
    pub fn auth(&self) -> AuthInfo<'_> { ... }
    pub fn storage(&self) -> StorageCtx<'_> { ... }
}

/// Incoming HTTP request.
pub struct HttpRequest {
    pub method: http::Method,
    pub url: String,
    pub headers: http::HeaderMap,
    body: Option<Bytes>,
    /// The path portion after the matched route prefix
    routed_path: String,
}

impl HttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> { ... }
    pub fn path_remainder(&self) -> &str { &self.routed_path }
    pub async fn body_bytes(&self) -> Result<Bytes> { ... }
    pub fn body_text(&self) -> Result<String> { ... }
    pub fn body_json<T: DeserializeOwned>(&self) -> Result<T> { ... }
}

/// Outgoing HTTP response.
pub struct HttpResponse {
    pub status: u16,
    pub headers: http::HeaderMap,
    pub body: Option<Bytes>,
}

impl HttpResponse {
    pub fn new(status: u16) -> Self { ... }

    pub fn json(status: u16, value: serde_json::Value) -> Self {
        Self {
            status,
            headers: headers!{ "Content-Type" => "application/json" },
            body: Some(serde_json::to_vec(&value).unwrap().into()),
        }
    }

    pub fn text(status: u16, text: impl Into<String>) -> Self { ... }
    pub fn redirect(status: u16, url: &str) -> Self { ... }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.insert(name.parse().unwrap(), value.parse().unwrap());
        self
    }
}
```

#### HTTP Route Registration

The `#[convex::http_action]` macro generates a route registration entry
alongside the function registration:

```rust
// Generated by #[convex::http_action(method = "POST", path = "/api/webhooks/stripe")]
inventory::submit! {
    NativeFunctionRegistration {
        name: "http:POST:/api/webhooks/stripe",
        udf_type: UdfType::HttpAction,
        visibility: Visibility::Public,  // HTTP actions are always public
        handler: |ctx, req| Box::pin(stripe_webhook(ctx, req)),
        arg_names: &[],
    }
}

inventory::submit! {
    HttpRouteRegistration {
        method: RoutableMethod::Post,
        path: "/api/webhooks/stripe",
        handler_name: "http:POST:/api/webhooks/stripe",
    }
}
```

The `NativeHttpRouter` collects all routes at startup:

```rust
pub struct NativeHttpRouter {
    routes: Vec<HttpRouteRegistration>,
}

pub struct HttpRouteRegistration {
    pub method: RoutableMethod,
    pub path: &'static str,           // exact path or prefix with "*"
    pub handler_name: &'static str,   // references NativeFunctionRegistration
}

impl NativeHttpRouter {
    pub fn collect() -> Self {
        let routes = inventory::iter::<HttpRouteRegistration>()
            .cloned()
            .collect();
        Self { routes }
    }

    /// Match an incoming request to a registered handler.
    /// Follows the same priority as the existing JS router:
    /// 1. Exact match (highest priority)
    /// 2. Longest prefix match (for wildcard routes)
    pub fn match_route(
        &self,
        method: &RoutableMethod,
        path: &str,
    ) -> Option<(&HttpRouteRegistration, RoutedHttpPath)> {
        // First try exact match
        if let Some(route) = self.routes.iter()
            .find(|r| r.method == *method && r.path == path)
        {
            return Some((route, RoutedHttpPath::new(String::new())));
        }
        // Then try prefix match (routes ending with "*")
        let mut best_match: Option<(&HttpRouteRegistration, RoutedHttpPath)> = None;
        for route in &self.routes {
            if route.method != *method { continue; }
            if let Some(prefix) = route.path.strip_suffix('*') {
                if path.starts_with(prefix) {
                    let remainder = &path[prefix.len()..];
                    if best_match.as_ref().map_or(true, |(best, _)| prefix.len() > best.path.len()) {
                        best_match = Some((route, RoutedHttpPath::new(remainder.to_string())));
                    }
                }
            }
        }
        best_match
    }
}
```

#### Integration with the Response Streamer

In the current Convex architecture, HTTP actions support **streaming
responses** via `HttpActionResponseStreamer`. For native Rust HTTP actions,
we support both simple (buffered) and streaming responses:

```rust
// Simple response (most common): return HttpResponse directly
#[convex::http_action(method = "GET", path = "/api/data")]
async fn get_data(ctx: &mut HttpActionCtx, req: HttpRequest) -> Result<HttpResponse> {
    Ok(HttpResponse::json(200, serde_json::json!({ "ok": true })))
}

// Streaming response: return a stream of body chunks
#[convex::http_action(method = "GET", path = "/api/stream")]
async fn stream_data(
    ctx: &mut HttpActionCtx,
    req: HttpRequest,
    res: &mut HttpResponseStreamer,  // injected by the macro when present
) -> Result<()> {
    res.send_head(200, headers!{ "Content-Type" => "text/event-stream" }).await?;

    for i in 0..100 {
        let chunk = format!("data: {}\n\n", i);
        res.send_body_chunk(chunk.as_bytes()).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
```

The `NativeFunctionRunner` detects whether the handler signature includes
`HttpResponseStreamer` and routes accordingly:
- **Buffered**: calls the handler, converts `HttpResponse` into
  `Head` + `BodyChunk` parts sent through the existing
  `HttpActionResponseStreamer`
- **Streaming**: passes the `HttpActionResponseStreamer` directly to the
  handler, which sends parts incrementally

This maps directly onto the existing `HttpActionResponsePart` enum and the
streaming infrastructure in `crates/function_runner/src/in_process_function_runner.rs:139-199`.

#### HTTP Action Flow in Distributed Mode

```
  Client                  Conductor              Worker
    │                        │                     │
    │ POST /api/webhooks/... │                     │
    │───────────────────────►│                     │
    │                        │                     │
    │                        │ 1. NativeHttpRouter  │
    │                        │    .match_route()   │
    │                        │    → stripe_webhook │
    │                        │                     │
    │                        │ ExecuteFunction {   │
    │                        │   type: HttpAction  │
    │                        │   handler: "http:..."│
    │                        │   http_request: {   │
    │                        │     method, path,   │
    │                        │     headers, body   │
    │                        │   }                 │
    │                        │────────────────────►│
    │                        │                     │
    │                        │                     │ 2. Build
    │                        │                     │    HttpActionCtx
    │                        │                     │
    │                        │                     │ 3. Call native
    │                        │                     │    handler
    │                        │                     │
    │                        │  Stream response:   │
    │                        │◄─ Head(200, hdrs)   │
    │ HTTP 200 + headers     │◄─ BodyChunk(bytes)  │
    │◄───────────────────────│◄─ BodyChunk(bytes)  │
    │ body chunks            │                     │
    │◄───────────────────────│                     │
    │                        │                     │
```

In distributed mode, the HTTP request is serialized in the
`ExecuteFunctionRequest` protobuf, and response parts are streamed back via
the `ExecuteFunctionStream` RPC (defined in Section 11.1).

---

## 8. Type-Safe Context Wrapper API

### 8.1 Type Hierarchy

```
                    ┌───────────────┐
                    │  NativeCtx    │  (trait)
                    │               │
                    │  • db()       │  (read-only DB handle)
                    │  • auth()     │
                    │  • metadata() │
                    └───────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
      ┌───────▼──────┐ ┌───▼────────┐ ┌──▼──────────┐
      │  QueryCtx    │ │ MutationCtx│ │  ActionCtx   │
      │              │ │            │ │              │
      │  + db()      │ │ + db()     │ │ + run_query()│
      │    (read     │ │   (read +  │ │ + run_mut()  │
      │     only)    │ │    write)  │ │ + run_action│
      │              │ │ + scheduler│ │ + fetch()    │
      │              │ │            │ │ + storage()  │
      └──────────────┘ └────────────┘ └──────────────┘
              │             │             │
              │             │             │
              ▼             ▼             ▼
      ┌──────────────────────────────────────────┐
      │         Transaction<RT>                   │
      │                                          │
      │  The underlying Convex transaction that  │
      │  tracks all reads and writes. Returned   │
      │  as FunctionFinalTransaction when the    │
      │  function completes.                     │
      └──────────────────────────────────────────┘
```

### 8.2 QueryCtx

```rust
pub struct QueryCtx<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
    identity: Identity,
    rng_seed: [u8; 32],
    unix_timestamp: UnixTimestamp,
}

impl<'a, RT: Runtime> QueryCtx<'a, RT> {
    /// Access the read-only database handle
    pub fn db(&mut self) -> QueryDb<'_, RT> {
        QueryDb { tx: self.tx }
    }

    /// Get the authenticated user's identity
    pub fn auth(&self) -> AuthInfo<'_> {
        AuthInfo { identity: &self.identity }
    }

    /// Current unix timestamp (controlled for determinism)
    pub fn unix_timestamp(&self) -> UnixTimestamp {
        self.unix_timestamp
    }
}
```

### 8.3 QueryDb (Read-Only Database Handle)

```rust
pub struct QueryDb<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
}

impl<'a, RT: Runtime> QueryDb<'a, RT> {
    /// Get a single document by ID
    /// Maps to syscall: 1.0/get
    pub async fn get(&mut self, id: DocumentId) -> Result<Option<Document>> {
        self.tx.get(id).await
    }

    /// Start a query on a table
    /// Maps to syscalls: 1.0/queryStream, 1.0/queryStreamNext
    pub fn query(&mut self, table: &str) -> QueryBuilder<'_, RT> {
        QueryBuilder::new(self.tx, table)
    }

    /// Count documents in a table
    /// Maps to syscall: 1.0/count
    pub async fn count(&mut self, table: &str) -> Result<u64> {
        self.tx.count(table).await
    }

    /// Normalize a string ID
    /// Maps to syscall: 1.0/db/normalizeId
    pub fn normalize_id(&self, table: &str, id_str: &str) -> Result<Option<DocumentId>> {
        self.tx.normalize_id(table, id_str)
    }
}
```

### 8.4 MutationCtx

```rust
pub struct MutationCtx<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
    identity: Identity,
    rng_seed: [u8; 32],
    unix_timestamp: UnixTimestamp,
}

impl<'a, RT: Runtime> MutationCtx<'a, RT> {
    /// Access the read+write database handle
    pub fn db(&mut self) -> MutationDb<'_, RT> {
        MutationDb { tx: self.tx }
    }

    pub fn auth(&self) -> AuthInfo<'_> {
        AuthInfo { identity: &self.identity }
    }

    /// Schedule a function to run later.
    /// Maps to syscall: 1.0/schedule
    pub fn scheduler(&mut self) -> Scheduler<'_, RT> {
        Scheduler { tx: self.tx }
    }

    pub fn unix_timestamp(&self) -> UnixTimestamp {
        self.unix_timestamp
    }
}
```

### 8.4a Scheduler (Type-Safe Scheduling)

The scheduler API ensures at compile time that:
1. Only **mutations and actions** can be scheduled (not queries)
2. The args type **must match** the scheduled function's expected args
3. `cancel_job` is typed to the `ScheduledJobId` returned by `run_after`/`run_at`

```rust
pub struct Scheduler<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
}

/// Marker trait: only mutations and actions can be scheduled.
/// Queries do NOT implement this trait, so attempting to schedule
/// a query is a compile error.
///
///   ctx.scheduler().run_after(d, list_users, ...)
///   //                           ^^^^^^^^^^
///   //   ERROR: `list_users` does not implement `SchedulableFunction`
///
pub trait SchedulableFunction: Sized {
    type Args: ConvexArgs;
    fn function_path() -> &'static str;
    fn udf_type() -> UdfType;
}

// Generated by #[convex::mutation]:
impl SchedulableFunction for fn(create_user) {
    type Args = CreateUserArgs;
    fn function_path() -> &'static str { "create_user" }
    fn udf_type() -> UdfType { UdfType::Mutation }
}

// Generated by #[convex::action]:
impl SchedulableFunction for fn(send_welcome) {
    type Args = SendWelcomeArgs;
    fn function_path() -> &'static str { "send_welcome" }
    fn udf_type() -> UdfType { UdfType::Action }
}

// NOT generated by #[convex::query] — queries cannot be scheduled.

impl<'a, RT: Runtime> Scheduler<'a, RT> {
    /// Schedule a function to run after a delay.
    ///
    /// Type-safe: F determines both the function path and the required Args type.
    ///
    ///   // OK: send_welcome is an action, args match
    ///   ctx.scheduler().run_after(
    ///       Duration::from_secs(60),
    ///       send_welcome,
    ///       SendWelcomeArgs { user_id: id },
    ///   ).await?;
    ///
    ///   // COMPILE ERROR: list_users is a query, not schedulable
    ///   ctx.scheduler().run_after(Duration::ZERO, list_users, ...).await?;
    ///
    ///   // COMPILE ERROR: wrong args type for send_welcome
    ///   ctx.scheduler().run_after(
    ///       Duration::ZERO,
    ///       send_welcome,
    ///       CreateUserArgs { name: "...", email: "..." },
    ///   //  ^^^^^^^^^^^^^^ expected SendWelcomeArgs, found CreateUserArgs
    ///   ).await?;
    ///
    pub async fn run_after<F: SchedulableFunction>(
        &mut self,
        delay: Duration,
        _func: F,             // used only for type inference
        args: F::Args,
    ) -> Result<ScheduledJobId> {
        let scheduled_ts = UnixTimestamp::now() + delay;
        let path = F::function_path();
        let serialized_args = args.to_convex_args()?;
        let id = VirtualSchedulerModel::new(self.tx, ...)
            .schedule(path, serialized_args, scheduled_ts, ...)
            .await?;
        Ok(ScheduledJobId(id))
    }

    /// Schedule a function to run at an exact timestamp.
    pub async fn run_at<F: SchedulableFunction>(
        &mut self,
        timestamp: UnixTimestamp,
        _func: F,
        args: F::Args,
    ) -> Result<ScheduledJobId> { ... }

    /// Cancel a previously scheduled job.
    ///
    ///   let job_id = ctx.scheduler().run_after(...).await?;
    ///   ctx.scheduler().cancel(job_id).await?;
    ///
    pub async fn cancel(&mut self, job_id: ScheduledJobId) -> Result<()> {
        VirtualSchedulerModel::new(self.tx, ...)
            .cancel_job(job_id.0)
            .await
    }
}

/// Opaque ID for a scheduled job. Returned by run_after/run_at,
/// accepted by cancel.
#[derive(Debug, Clone)]
pub struct ScheduledJobId(DeveloperDocumentId);
```

#### Compile-Time Guarantees

```
┌───────────────────────────────────────────────────────────────┐
│              Scheduler Type Safety                             │
├──────────────────────────────────────┬────────────────────────┤
│  Mistake                             │  Compile Error          │
├──────────────────────────────────────┼────────────────────────┤
│  Schedule a query                    │  `list_users` does not │
│                                      │  implement             │
│                                      │  `SchedulableFunction` │
├──────────────────────────────────────┼────────────────────────┤
│  Wrong args for scheduled function   │  Expected              │
│                                      │  `SendWelcomeArgs`,    │
│                                      │  found `CreateUserArgs`│
├──────────────────────────────────────┼────────────────────────┤
│  Pass arbitrary string as job ID     │  Expected              │
│                                      │  `ScheduledJobId`,     │
│                                      │  found `String`        │
└──────────────────────────────────────┴────────────────────────┘
```
```

### 8.5 MutationDb (Read + Write Database Handle)

```rust
pub struct MutationDb<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
}

impl<'a, RT: Runtime> MutationDb<'a, RT> {
    // All QueryDb methods are available (get, query, count, normalize_id)
    // via Deref to QueryDb, plus:

    /// Insert a new document
    /// Maps to syscall: 1.0/insert
    pub async fn insert(
        &mut self,
        table: &str,
        document: ConvexObject,
    ) -> Result<DocumentId> {
        self.tx.insert(table, document).await
    }

    /// Replace a document entirely
    /// Maps to syscall: 1.0/replace
    pub async fn replace(
        &mut self,
        id: DocumentId,
        document: ConvexObject,
    ) -> Result<Document> {
        self.tx.replace(id, document).await
    }

    /// Patch a document (shallow merge)
    /// Maps to syscall: 1.0/shallowMerge
    pub async fn patch(
        &mut self,
        id: DocumentId,
        fields: ConvexObject,
    ) -> Result<Document> {
        self.tx.patch(id, fields).await
    }

    /// Delete a document
    /// Maps to syscall: 1.0/remove
    pub async fn delete(&mut self, id: DocumentId) -> Result<()> {
        self.tx.delete(id).await
    }
}
```

### 8.6 ActionCtx

```rust
pub struct ActionCtx<RT: Runtime> {
    identity: Identity,
    action_callbacks: Arc<dyn ActionCallbacks>,
    context: ExecutionContext,
}

impl<RT: Runtime> ActionCtx<RT> {
    /// Run a query from within an action
    /// Maps to ActionCallbacks::execute_query
    pub async fn run_query(
        &self,
        path: &str,
        args: ConvexObject,
    ) -> Result<Value> {
        self.action_callbacks.execute_query(
            self.identity.clone(),
            path.parse()?,
            args.into(),
            self.context.clone(),
        ).await
    }

    /// Run a mutation from within an action
    /// Maps to ActionCallbacks::execute_mutation
    pub async fn run_mutation(
        &self,
        path: &str,
        args: ConvexObject,
    ) -> Result<Value> {
        self.action_callbacks.execute_mutation(
            self.identity.clone(),
            path.parse()?,
            args.into(),
            self.context.clone(),
        ).await
    }

    /// Run another action from within an action
    pub async fn run_action(
        &self,
        path: &str,
        args: ConvexObject,
    ) -> Result<Value> {
        self.action_callbacks.execute_action(
            self.identity.clone(),
            path.parse()?,
            args.into(),
            self.context.clone(),
        ).await
    }

    /// Get the authenticated user's identity
    pub fn auth(&self) -> AuthInfo<'_> {
        AuthInfo { identity: &self.identity }
    }

    /// Access file storage operations
    pub fn storage(&self) -> StorageCtx<'_> {
        StorageCtx { callbacks: &self.action_callbacks, ... }
    }
}
```

### 8.7 Query Builder

```rust
pub struct QueryBuilder<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
    table: String,
    filters: Vec<Filter>,
    order: Order,
    index: Option<String>,
    limit: Option<usize>,
}

impl<'a, RT: Runtime> QueryBuilder<'a, RT> {
    pub fn filter(mut self, field: &str, op: FilterOp, value: Value) -> Self {
        self.filters.push(Filter { field, op, value });
        self
    }

    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    pub fn with_index(mut self, index_name: &str) -> Self {
        self.index = Some(index_name.to_string());
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Execute the query and collect all results
    pub async fn collect(self) -> Result<Vec<Document>> { ... }

    /// Execute the query and return a page
    pub async fn page(self, cursor: Option<Cursor>, page_size: usize)
        -> Result<Page> { ... }

    /// Execute the query and return only the first result
    pub async fn first(self) -> Result<Option<Document>> { ... }
}
```

### 8.8 Syscall-to-Rust-API Mapping (Complete)

| Syscall | Rust API | Context |
|---------|----------|---------|
| `1.0/get` | `ctx.db().get(id)` | Query, Mutation |
| `1.0/queryStream` + `queryStreamNext` | `ctx.db().query(table).collect()` | Query, Mutation |
| `1.0/queryPage` | `ctx.db().query(table).page(cursor, size)` | Query, Mutation |
| `1.0/count` | `ctx.db().count(table)` | Query, Mutation |
| `1.0/db/normalizeId` | `ctx.db().normalize_id(table, id_str)` | Query, Mutation |
| `1.0/insert` | `ctx.db().insert(table, doc)` | Mutation |
| `1.0/replace` | `ctx.db().replace(id, doc)` | Mutation |
| `1.0/shallowMerge` | `ctx.db().patch(id, fields)` | Mutation |
| `1.0/remove` | `ctx.db().delete(id)` | Mutation |
| `1.0/getUserIdentity` | `ctx.auth().user_identity()` | All |
| `1.0/schedule` | `ctx.scheduler().run_after(delay, fn_ref, typed_args)` | Mutation |
| `1.0/cancel_job` | `ctx.scheduler().cancel(job_id)` | Mutation |
| `1.0/storageGetUrl` | `ctx.storage().get_url(id)` | Action |
| `1.0/storageGenerateUploadUrl` | `ctx.storage().generate_upload_url()` | Action |
| `1.0/storageDelete` | `ctx.storage().delete(id)` | Action |
| `1.0/storageGetMetadata` | `ctx.storage().get_metadata(id)` | Action |
| `1.0/runUdf` | `ctx.run_query(path, args)` / `ctx.run_mutation(...)` | Action |
| `1.0/getTransactionMetrics` | `ctx.db().metrics()` | Query, Mutation |
| `1.0/getFunctionMetadata` | `ctx.metadata()` | All |

---

## 9. NativeFunctionRunner: The Local Execution Engine

### 9.1 Implementation

```rust
pub struct NativeFunctionRunner<RT: Runtime> {
    registry: Arc<NativeFunctionRegistry>,
    rt: RT,
    persistence_reader: Arc<dyn PersistenceReader>,
    database: Database<RT>,
    instance_name: String,
    key_broker: FunctionRunnerKeyBroker,
    convex_origin: ConvexOrigin,
    action_callbacks: Arc<RwLock<Option<Weak<dyn ActionCallbacks>>>>,
    fetch_client: Arc<dyn FetchClient>,
    index_cache: InMemoryIndexCache<RT>,
}
```

### 9.2 run_function Implementation

```rust
#[async_trait]
impl<RT: Runtime> FunctionRunner<RT> for NativeFunctionRunner<RT> {
    async fn run_function(
        &self,
        udf_type: UdfType,
        identity: Identity,
        ts: RepeatableTimestamp,
        existing_writes: FunctionWrites,
        log_line_sender: Option<mpsc::UnboundedSender<LogLine>>,
        function_metadata: Option<FunctionMetadata>,
        http_action_metadata: Option<HttpActionMetadata>,
        default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
        context: ExecutionContext,
    ) -> anyhow::Result<(
        Option<FunctionFinalTransaction>,
        FunctionOutcome,
        FunctionUsageStats,
    )> {
        let usage_tracker = FunctionUsageTracker::new();

        // 1. Resolve the function from the registry
        let metadata = function_metadata
            .context("Missing function metadata")?;
        let function_path = metadata.path_and_args.path().udf_path.to_string();
        let registration = self.registry.get(&function_path)
            .ok_or_else(|| anyhow!("Native function not found: {}", function_path))?;

        // 2. Verify UDF type matches registration
        anyhow::ensure!(
            registration.udf_type == udf_type,
            "Function {} registered as {:?} but called as {:?}",
            function_path, registration.udf_type, udf_type
        );

        // 3. Create a Transaction
        let snapshot = self.database.snapshot(ts)?;
        let table_count_snapshot = Arc::new(snapshot.table_summaries);
        let text_index_snapshot = Arc::new(TextIndexManagerSnapshot::new(
            snapshot.index_registry,
            snapshot.text_indexes,
            self.database.searcher.clone(),
            self.database.search_storage.clone(),
        ));

        let mut tx = self.index_cache.begin_tx(
            identity.clone(),
            ts,
            existing_writes,
            self.persistence_reader.clone(),
            self.instance_name.clone(),
            in_memory_index_last_modified,
            self.database.bootstrap_metadata.clone(),
            table_count_snapshot,
            text_index_snapshot,
            usage_tracker.clone(),
            Arc::new(NoopRetentionValidator {}),
            None,
        ).await?;

        // 4. Build context and execute
        let start_time = Instant::now();
        let rng_seed = self.rt.rng().random();
        let unix_timestamp = self.rt.unix_timestamp();

        let result = match udf_type {
            UdfType::Query => {
                let mut ctx = QueryCtx::new(&mut tx, identity, rng_seed, unix_timestamp);
                let args = metadata.path_and_args.args();
                (registration.handler)(&mut ctx, args).await
            },
            UdfType::Mutation => {
                let mut ctx = MutationCtx::new(&mut tx, identity, rng_seed, unix_timestamp);
                let args = metadata.path_and_args.args();
                (registration.handler)(&mut ctx, args).await
            },
            UdfType::Action => {
                let action_callbacks = self.action_callbacks.read()
                    .clone()
                    .context("Action callbacks not set")?
                    .upgrade()
                    .context("Backend shut down")?;
                let mut ctx = ActionCtx::new(
                    identity,
                    action_callbacks,
                    context.clone(),
                );
                let args = metadata.path_and_args.args();
                (registration.handler)(&mut ctx, args).await
            },
            _ => anyhow::bail!("Unsupported UDF type for native runner"),
        };

        let execution_time = start_time.elapsed();

        // 5. Build outcome
        let outcome = match result {
            Ok(value) => FunctionOutcome::from_success(udf_type, value, execution_time),
            Err(err) => FunctionOutcome::from_error(udf_type, err, execution_time),
        };

        // 6. Extract reads/writes from Transaction
        let final_tx = match udf_type {
            UdfType::Query | UdfType::Mutation => {
                Some(FunctionFinalTransaction::try_from(tx)?)
            },
            UdfType::Action => None,
        };

        Ok((final_tx, outcome, usage_tracker.gather_user_stats()))
    }

    async fn analyze(
        &self,
        _udf_config: UdfConfig,
        _modules: BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        _environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        _max_user_heap_size: usize,
    ) -> anyhow::Result<Result<BTreeMap<CanonicalizedModulePath, AnalyzedModule>, JsError>> {
        // For native functions, analyze() returns metadata from the registry
        // instead of parsing JS modules.
        let mut analyzed = BTreeMap::new();
        for registration in self.registry.list() {
            let module_path = registration.name.parse()?;
            analyzed.insert(module_path, AnalyzedModule {
                functions: vec![AnalyzedFunction {
                    name: registration.name.to_string(),
                    udf_type: registration.udf_type,
                    // ... other metadata
                }],
            });
        }
        Ok(Ok(analyzed))
    }

    // evaluate_schema and evaluate_auth_config would delegate to a
    // schema/auth config defined in Rust or fall back to the JS evaluator.
    // ...
}
```

### 9.3 Integration with FunctionRouter

The `FunctionRouter` decides where to send each function execution. With
native functions, it gains a third option:

```rust
impl<RT: Runtime> FunctionRouter<RT> {
    pub async fn run_function(&self, ...) -> Result<(...)> {
        let function_path = &metadata.path_and_args.path().udf_path;

        // Check if this function exists in the native registry
        if self.native_registry.get(function_path).is_some() {
            // Route to native runner
            self.native_runner.run_function(...).await
        } else {
            // Fall back to V8 runner (for JS functions)
            self.function_runner.run_function(...).await
        }
    }
}
```

This allows a **hybrid deployment** where some functions are native Rust and
others remain JavaScript, all coexisting in the same backend.

---

## 10. Distributed Execution

### 10.1 Node Roles

The same binary supports three modes, controlled by environment variable:

```
CONVEX_MODE=standalone   (default)
CONVEX_MODE=conductor
CONVEX_MODE=worker
```

| Role | Responsibilities | What it does NOT do |
|------|-----------------|---------------------|
| **Standalone** | Everything | — |
| **Conductor** | HTTP/WS server, sync, DB commits, subscription tracking, route function calls | Execute functions locally |
| **Worker** | Execute native functions, gRPC server, maintain index cache | Serve HTTP, manage subscriptions, commit |

### 10.2 DistributedFunctionRunner (Conductor Side)

```rust
/// Installed on the conductor. Implements FunctionRunner by forwarding
/// execution requests to remote worker nodes via gRPC.
pub struct DistributedFunctionRunner<RT: Runtime> {
    rt: RT,
    /// Pool of worker connections with load balancing
    worker_pool: WorkerPool,
    /// Database reference for creating snapshots
    database: Database<RT>,
}

pub struct WorkerPool {
    /// Active worker connections, discovered via service discovery
    workers: Arc<RwLock<Vec<WorkerConnection>>>,
    /// Round-robin counter or least-connections state
    load_balancer: LoadBalancer,
    /// Configuration
    max_requests_per_upstream: usize,  // from FUNRUN_CLIENT_MAX_REQUESTS_PER_UPSTREAM
    max_retries: usize,               // from FUNRUN_CLIENT_MAX_RETRIES
}

pub struct WorkerConnection {
    endpoint: String,
    client: FunctionExecutionServiceClient<tonic::transport::Channel>,
    /// Semaphore limiting concurrent requests to this worker
    concurrency: Arc<Semaphore>,
    /// Track in-flight requests for health checking
    in_flight: AtomicUsize,
}
```

#### Load Balancing Strategy

```
         DistributedFunctionRunner
                    │
                    │  pick_worker()
                    ▼
    ┌───────────────────────────────┐
    │        Load Balancer          │
    │                               │
    │  Strategy: Power of Two       │
    │  Choices (P2C)                │
    │                               │
    │  1. Pick 2 random workers     │
    │  2. Choose the one with       │
    │     fewer in-flight requests  │
    │  3. Check concurrency limit   │
    │  4. If both full, wait or     │
    │     return overloaded error   │
    └───────────────┬───────────────┘
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
    Worker 1    Worker 2    Worker 3
    (3 req)     (1 req)     (5 req)
                  ▲
                  │
              Selected
              (fewest)
```

#### Retry Logic

```
         Execute request
              │
              ▼
       ┌──────────────┐     Success
       │  Send to      │────────────────► Return result
       │  Worker N     │
       └──────┬───────┘
              │ Error
              ▼
       ┌──────────────┐
       │  Error type?  │
       └──────┬───────┘
              │
    ┌─────────┼──────────────┐
    │         │              │
    ▼         ▼              ▼
 Overloaded  Network       Other
 (retryable) (retryable)   (not retryable)
    │         │              │
    ▼         ▼              ▼
 Pick new   Pick new     Return error
 worker     worker       immediately
    │         │
    ▼         ▼
 Retry ≤ max_retries?
    │
    ├── Yes → Go to "Send to Worker N"
    │
    └── No → Return overloaded error
```

### 10.3 Worker gRPC Server

```rust
/// Runs on each worker node. Accepts function execution requests from
/// the conductor and runs them using the NativeFunctionRunner.
pub struct FunctionExecutionServer<RT: Runtime> {
    native_runner: NativeFunctionRunner<RT>,
    /// Semaphore limiting total concurrent executions
    concurrency_limit: Arc<Semaphore>,
}

#[tonic::async_trait]
impl FunctionExecutionService for FunctionExecutionServer<RT> {
    async fn execute_function(
        &self,
        request: Request<ExecuteFunctionRequest>,
    ) -> Result<Response<ExecuteFunctionResponse>, Status> {
        // Acquire concurrency permit
        let _permit = self.concurrency_limit
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("Worker overloaded"))?;

        let req = request.into_inner();

        // Deserialize request fields
        let udf_type = req.udf_type.try_into()?;
        let identity = Identity::try_from(req.identity)?;
        let ts = RepeatableTimestamp::from(req.timestamp);
        let function_metadata = FunctionMetadata::try_from(req.function_metadata)?;
        // ... deserialize remaining fields

        // Execute via NativeFunctionRunner
        let (final_tx, outcome, usage) = self.native_runner.run_function(
            udf_type,
            identity,
            ts,
            FunctionWrites::default(), // existing_writes from request
            None,                       // log_line_sender (streamed back)
            Some(function_metadata),
            None,
            req.system_env_vars,
            req.in_memory_index_last_modified,
            req.context,
        ).await
        .map_err(|e| Status::internal(e.to_string()))?;

        // Serialize response
        Ok(Response::new(ExecuteFunctionResponse {
            transaction: final_tx.map(|tx| tx.into()),
            outcome: outcome.into(),
            usage: usage.into(),
        }))
    }
}
```

### 10.4 Data Access Architecture

Workers need read access to the database to execute functions. There are
three supported patterns:

#### Pattern A: Direct Database Access (Recommended)

```
    Conductor                     Workers                    Database
    ┌────────────┐                                          ┌─────────┐
    │ Commits    │───────────────────────────────────────────►│         │
    │ writes     │                                          │         │
    └────────────┘                                          │         │
                                                            │ Postgres│
    ┌────────────┐     ┌────────────┐     ┌────────────┐    │ / MySQL │
    │ Worker 1   │────►│            │     │            │◄───│         │
    │ reads at   │     │ Connection │     │ Connection │    │         │
    │ ts=X       │◄────│ Pool       │     │ Pool       │───►│         │
    └────────────┘     └────────────┘     └────────────┘    └─────────┘
    ┌────────────┐                        ┌────────────┐
    │ Worker 2   │────────────────────────│ reads at   │
    │            │◄───────────────────────│ ts=X       │
    └────────────┘                        └────────────┘
```

Each worker holds a `PersistenceReader` connected to the same database. Since
functions read at a specific snapshot timestamp (`ts`), this is safe —
workers see a consistent view regardless of concurrent writes.

**Configuration:**

```env
# Worker node environment
PERSISTENCE_URL=postgres://user:pass@db-host:5432/convex
FUNRUN_INDEX_CACHE_SIZE=50000000     # 50 MB per worker
```

#### Pattern B: Read Replicas

For high read throughput, workers can read from database replicas:

```
    Conductor ──writes──► Primary DB ──replication──► Replica 1
                                                      Replica 2
    Worker 1 ──reads───────────────────────────────► Replica 1
    Worker 2 ──reads───────────────────────────────► Replica 2
```

Since reads are at a specific snapshot timestamp, replicas only need to be
caught up to that timestamp. The retention validation step on the conductor
ensures this.

#### Pattern C: Index Reader Proxy (For Stateless Workers)

The existing `index_reader_override` field in `RunRequestArgs` supports
routing index reads back to the conductor:

```
    Worker ──index read──► Conductor ──read──► Database
```

This allows workers to be completely stateless at the cost of an extra
network hop per index read. Useful for burst scaling where you want to add
workers quickly without configuring database access.

### 10.5 Service Discovery

Workers register themselves with a service discovery mechanism. The conductor
discovers workers by cluster name:

```
    ┌───────────────────────────────────────────────┐
    │              Service Discovery                 │
    │     (DNS / Consul / K8s Service / etc.)       │
    │                                               │
    │  cluster: "native-funrun-default"             │
    │                                               │
    │  ┌─────────────────────────────────────┐      │
    │  │ worker-1.funrun.svc  :50051  healthy│      │
    │  │ worker-2.funrun.svc  :50051  healthy│      │
    │  │ worker-3.funrun.svc  :50051  healthy│      │
    │  └─────────────────────────────────────┘      │
    └───────────────────────────────────────────────┘
                         │
                         │ resolve
                         ▼
    ┌────────────────────────────────────────┐
    │  DistributedFunctionRunner             │
    │                                        │
    │  FUNRUN_CLUSTER_NAME=native-funrun-... │
    │                                        │
    │  Periodically refreshes worker list    │
    │  from service discovery                │
    └────────────────────────────────────────┘
```

---

## 11. Network Protocol and Serialization

### 11.1 Protobuf Service Definition

```protobuf
syntax = "proto3";

package convex.funrun.v1;

import "common.proto";
import "outcome.proto";
import "usage.proto";
import "convex_identity.proto";
import "convex_query_journal.proto";

service FunctionExecutionService {
    // Execute a single function (query, mutation, or action)
    rpc ExecuteFunction(ExecuteFunctionRequest) returns (ExecuteFunctionResponse);

    // Execute a function with streaming log lines
    rpc ExecuteFunctionStream(ExecuteFunctionRequest)
        returns (stream ExecuteFunctionStreamResponse);

    // Health check
    rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}

message ExecuteFunctionRequest {
    // What to execute
    UdfType udf_type = 1;
    string function_path = 2;
    bytes serialized_args = 3;          // ConvexObject, serialized

    // Execution context
    convex_identity.Identity identity = 4;
    uint64 timestamp = 5;               // RepeatableTimestamp
    convex_query_journal.QueryJournal journal = 6;
    ExecutionContext context = 7;

    // Pre-existing state
    repeated DocumentUpdate existing_writes = 8;
    map<uint32, uint64> in_memory_index_last_modified = 9;

    // Environment
    map<string, string> system_env_vars = 10;
}

message ExecuteFunctionResponse {
    // Result
    outcome.FunctionOutcome outcome = 1;
    usage.FunctionUsageStats usage = 2;

    // Transaction state (empty for actions)
    optional FunctionFinalTransaction transaction = 3;
}

message FunctionFinalTransaction {
    uint64 begin_timestamp = 1;
    ReadSet reads = 2;
    repeated DocumentUpdate writes = 3;
    map<uint32, uint64> rows_read_by_tablet = 4;
}

message ReadSet {
    // Serialized read intervals
    repeated ReadInterval intervals = 1;
}

message ReadInterval {
    bytes start = 1;       // Index key range start
    bytes end = 2;         // Index key range end
}

message DocumentUpdate {
    bytes id = 1;
    optional bytes old_document = 2;   // Previous value (for conflict detection)
    optional bytes new_document = 3;   // New value (None for deletes)
    uint64 prev_ts = 4;
}

// Streaming response variant
message ExecuteFunctionStreamResponse {
    oneof message {
        // Intermediate: log lines as they're produced
        outcome.LogLine log_line = 1;
        // Final: the execution result
        ExecuteFunctionResponse result = 2;
    }
}

message HealthCheckRequest {}

message HealthCheckResponse {
    uint32 in_flight_requests = 1;
    uint32 max_capacity = 2;
    repeated string registered_functions = 3;
}
```

### 11.2 Serialization Strategy

| Data | Format | Rationale |
|------|--------|-----------|
| Function args/results | ConvexValue binary encoding | Native format, no conversion overhead |
| Transaction reads/writes | Protobuf | Already has proto definitions in `crates/pb/` |
| Identity | Protobuf | Existing `convex_identity.proto` |
| Query journal | Protobuf | Existing `convex_query_journal.proto` |
| Function outcome | Protobuf | Existing `outcome.proto` |
| Usage stats | Protobuf | Existing `usage.proto` |
| Log lines | Protobuf (streamed) | Existing `LogLine` in `outcome.proto` |

### 11.3 Existing Protobuf Definitions That Can Be Reused

The codebase already defines protobuf messages for most of the data that
crosses the function runner boundary:

```
crates/pb/protos/
├── common.proto              ── FunctionResult, DocumentUpdate
├── outcome.proto             ── FunctionOutcome, UdfOutcome, ActionOutcome
├── convex_identity.proto     ── Identity
├── convex_query_journal.proto ── QueryJournal
├── convex_actions.proto      ── Action-related types
├── usage.proto               ── Usage tracking
├── errors.proto              ── Error metadata
└── storage.proto             ── Storage types
```

The primary new protobuf definitions needed are:
- `ExecuteFunctionRequest` (assembles existing types into a request envelope)
- `ExecuteFunctionResponse` (assembles existing types into a response envelope)
- `ReadSet` serialization (the read interval format)
- `FunctionFinalTransaction` (wraps reads + writes)

---

## 12. Deployment, Scaling, and Operations

### 12.1 Deployment Topologies

All topologies use the binary produced by `cargo build` in the **developer's
own project** (which depends on the `convex_native` crate from this repo).

#### Topology A: Single Node (Development / Small Apps)

```
┌─────────────────────────────────┐
│    Developer's Binary            │
│     CONVEX_MODE=standalone       │
│                                 │
│  HTTP + WS + DB + Functions     │
│                                 │
│  Good for: development, small   │
│  apps, self-hosted single-node  │
└────────────────┬────────────────┘
                 │
         ┌───────▼───────┐
         │   SQLite /    │
         │   Postgres    │
         └───────────────┘
```

#### Topology B: Conductor + Workers (Production)

```
                 ┌────────────────────┐
                 │   Load Balancer    │
                 │   (HTTP/WS)       │
                 └────────┬──────────┘
                          │
              ┌───────────▼────────────┐
              │  Conductor (1 node)    │
              │  CONVEX_MODE=conductor │
              │                        │
              │  • HTTP/WS server      │
              │  • Sync layer          │
              │  • Transaction commits │
              └───────────┬────────────┘
                          │ gRPC
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
     ┌────────────┐┌────────────┐┌────────────┐
     │  Worker 1  ││  Worker 2  ││  Worker 3  │
     │  mode=     ││  mode=     ││  mode=     │
     │  worker    ││  worker    ││  worker    │
     └──────┬─────┘└──────┬─────┘└──────┬─────┘
            │             │             │
            └─────────────┼─────────────┘
                          │
                  ┌───────▼───────┐
                  │   Postgres    │
                  └───────────────┘
```

#### Topology C: HA Conductor + Auto-Scaling Workers

```
                 ┌────────────────────┐
                 │   Load Balancer    │
                 └────────┬──────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼                           ▼
     ┌────────────┐              ┌────────────┐
     │ Conductor  │              │ Conductor  │
     │ (active)   │◄────────────►│ (standby)  │
     └──────┬─────┘   failover  └────────────┘
            │ gRPC
            │
     ┌──────▼──────────────────────────────────┐
     │            Worker Pool                   │
     │         (auto-scaled 2-20 nodes)        │
     │                                         │
     │  Scaling signals:                       │
     │  • CPU utilization > 70%                │
     │  • In-flight requests > threshold       │
     │  • P99 latency > SLA                    │
     │                                         │
     │  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐   │
     │  │ W1 │ │ W2 │ │ W3 │ │ W4 │ │ W5 │   │
     │  └────┘ └────┘ └────┘ └────┘ └────┘   │
     └─────────────────────────────────────────┘
```

### 12.2 Configuration Knobs

| Knob | Default | Description |
|------|---------|-------------|
| `CONVEX_MODE` | `standalone` | Node role: standalone, conductor, worker |
| `NATIVE_FUNRUN_CLUSTER_NAME` | `native-funrun-default` | Service discovery name for worker pool |
| `NATIVE_FUNRUN_WORKER_PORT` | `50051` | gRPC port for worker nodes |
| `NATIVE_FUNRUN_MAX_REQUESTS_PER_UPSTREAM` | `15` | Max concurrent requests per worker |
| `NATIVE_FUNRUN_MAX_RETRIES` | `4` | Max retries on overloaded workers |
| `NATIVE_FUNRUN_INDEX_CACHE_SIZE` | `50000000` | Index cache size per worker (bytes) |
| `NATIVE_FUNRUN_CONCURRENCY_LIMIT` | `128` | Max concurrent function executions per worker |
| `NATIVE_FUNRUN_HEALTH_CHECK_INTERVAL` | `5000` | Health check interval (ms) |

### 12.3 Monitoring and Observability

```
    ┌─────────────────────────────────────────────────┐
    │                 Metrics                           │
    │                                                 │
    │  Conductor:                                     │
    │  • native_funrun_requests_total{worker, type}   │
    │  • native_funrun_request_duration_seconds       │
    │  • native_funrun_errors_total{type, retryable}  │
    │  • native_funrun_worker_pool_size               │
    │  • native_funrun_in_flight_per_worker           │
    │                                                 │
    │  Worker:                                        │
    │  • native_function_execution_seconds{fn_name}   │
    │  • native_function_errors_total{fn_name}        │
    │  • native_index_cache_hit_rate                  │
    │  • native_worker_concurrency_utilization        │
    │  • native_db_read_latency_seconds               │
    │                                                 │
    │  Distributed tracing:                           │
    │  • Fastrace spans propagated via gRPC metadata  │
    │  • Full trace: client → conductor → worker → DB │
    └─────────────────────────────────────────────────┘
```

### 12.4 Graceful Shutdown and Rolling Updates

Since all workers run the same binary (built from the developer's project)
with the same functions compiled in, rolling updates require restarting
workers with a new build of that binary:

```
    Rolling update sequence:
    
    1. Developer builds new binary with updated functions
    2. Start new workers (v2) alongside existing (v1)
    
         ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐
         │v1  │ │v1  │ │v1  │ │v2  │ │v2  │
         │ W1 │ │ W2 │ │ W3 │ │ W4 │ │ W5 │
         └────┘ └────┘ └────┘ └────┘ └────┘
    
    3. Drain old workers:
       a. Remove from service discovery
       b. Wait for in-flight requests to complete
       c. Terminate
    
         ┌────┐ ┌────┐          ┌────┐ ┌────┐
         │v1  │ │v1  │          │v2  │ │v2  │
         │ W2 │ │ W3 │          │ W4 │ │ W5 │
         │drain│ │drain│         └────┘ └────┘
         └────┘ └────┘
    
    4. All workers updated:
    
                          ┌────┐ ┌────┐ ┌────┐
                          │v2  │ │v2  │ │v2  │
                          │ W4 │ │ W5 │ │ W6 │
                          └────┘ └────┘ └────┘
```

**Important:** During rolling updates, the conductor must handle the case
where a function exists in v2 but not in v1 (new function) or vice versa
(removed function). The conductor should:
- Route new functions only to v2 workers
- Route removed functions only to remaining v1 workers
- The `HealthCheckResponse.registered_functions` field enables this

---

## 13. Trade-offs, Risks, and Alternatives

### 13.1 Trade-offs

| Aspect | Native Rust | JS/TS (current) |
|--------|-------------|-----------------|
| **Performance** | No V8 overhead, no GC, no serialization boundary | V8 JIT is fast but has startup + GC pauses |
| **Hot reload** | Requires recompile + redeploy | Push JS bundles without restart |
| **Safety** | Type-checked at compile time, but no sandbox | V8 sandbox: memory limits, CPU time limits |
| **Language ecosystem** | Full Rust ecosystem in actions | npm ecosystem |
| **Determinism** | Developer responsibility | V8 controls RNG, time |
| **Onboarding** | Rust expertise required | JS/TS developers are common |
| **Distribution** | Trivial — same binary everywhere, no module sync | Requires module cache, code cache per worker |
| **Warm-up time** | Zero — functions are compiled in | V8 isolate creation + JIT warmup |
| **Memory per worker** | Minimal — just index cache | V8 heap per isolate + module cache + code cache |

### 13.2 Risks and Mitigations

#### Risk 1: No Sandboxing

**Risk:** A native Rust function can access the filesystem, network, and
memory without limits. A bug could corrupt state or consume unbounded
resources.

**Mitigation:**
- This is for first-party code, not untrusted user code.
- Queries and mutations run with a `Transaction` that enforces read/write
  limits (same as JS UDFs).
- Add optional resource limits: execution timeout via `tokio::time::timeout`,
  memory monitoring via allocator hooks.
- For stronger isolation, future work can compile to WASM instead.

#### Risk 2: Determinism Not Enforced

**Risk:** Queries and mutations should produce the same result for the same
inputs to ensure the reactive system works correctly. Native Rust does not
enforce this.

**Mitigation:**
- The reactive system uses **read-set tracking**, not pure replay. A query
  is re-executed when its read set is invalidated by a mutation — it doesn't
  need to produce identical results from cached inputs.
- Provide controlled `rng_seed` and `unix_timestamp` through the context
  (same as V8 does), so functions that use time/randomness via the context
  API are deterministic.
- Document that queries/mutations should not use `std::time::Instant`,
  `rand::thread_rng()`, or other non-deterministic sources directly. Provide
  lint rules via clippy to flag these.

#### Risk 3: Schema Evaluation

**Risk:** The current `evaluate_schema()` and `evaluate_app_definitions()`
methods run JS code. Native Rust functions need schemas too.

**Mitigation:** Solved by design — the `#[derive(ConvexDocument)]` macro
(Section 5) generates `TableDefinition` structs that are collected into a
`DatabaseSchema` at startup via `NativeSchema::collect()`. The
`NativeFunctionRunner::evaluate_schema()` method returns this compiled-in
schema directly, with no JS evaluation needed. This is part of Phase 1.

#### Risk 4: Version Skew During Rolling Updates

**Risk:** During a rolling update, conductor may route a request to a worker
that doesn't have the requested function (or has an old version).

**Mitigation:**
- Workers report registered functions in their health check response.
- Conductor maintains a map of function → compatible workers.
- On routing failure, retry on a different worker.
- Abort rolling update if no workers can serve a required function.

### 13.3 Alternatives Considered

#### Alternative A: WASM Functions

Compile Rust to `wasm32-wasi`, run via Wasmtime.

| | Pro | Con |
|-|-----|-----|
| Sandboxing | Memory + CPU limits enforced | ~10-30% performance overhead |
| Determinism | WASM is deterministic by design | More complex toolchain |
| Distribution | Can distribute `.wasm` blobs | Module loading infra needed |
| Hot reload | Possible (load new `.wasm`) | Slower than JS push |

**Decision:** WASM is a valid future extension. This design starts with
native execution for maximum performance and simplicity, with the
`FunctionRunner` trait making a future `WasmFunctionRunner` straightforward
to add.

#### Alternative B: Shared Library Plugins (.so/.dylib)

Load functions as dynamic libraries at runtime.

**Rejected because:** Dynamic loading adds complexity (ABI stability, symbol
resolution, platform differences) without significant benefit. Rust's
compile-link model is fast enough, and a single binary is operationally
simpler.

#### Alternative C: gRPC-Based Custom Runtime

Define a generic "custom runtime" protocol where any process can register as
a function executor.

**Decision:** This is essentially what the distributed mode already provides.
The gRPC `FunctionExecutionService` could be implemented by any process, not
just the Rust binary. This is a natural extension point for supporting other
languages in the future.

---

## 14. Rust Components

### 14.1 Background: How Convex Components Work Today

Convex components are a packaging mechanism for reusable backend modules. In
the JS/TS world, a component is an npm package (e.g., `@convex-dev/rate-limiter`,
`@convex-dev/aggregate`) that an app installs and mounts in its `convex/`
directory.

The component system is built on these core concepts:

```
┌──────────────────────────────────────────────────────────────┐
│                    Component Hierarchy                        │
│                                                              │
│  App (root component)                                        │
│  ├── convex/schema.ts        ← app schema                   │
│  ├── convex/functions/       ← app functions                 │
│  │                                                           │
│  ├── rateLimiter: RateLimiterComponent    ← child component  │
│  │   ├── own schema (isolated tables)                        │
│  │   ├── own functions (namespaced)                          │
│  │   └── exports: { check, limit, reset }                    │
│  │                                                           │
│  └── aggregate: AggregateComponent       ← child component  │
│      ├── own schema (isolated tables)                        │
│      ├── own functions (namespaced)                          │
│      └── exports: { insert, get, count }                     │
│                                                              │
│  Key properties:                                             │
│  • Each component has its OWN tables (isolated namespace)    │
│  • Components export functions/values via an exports tree    │
│  • Parent passes args to child on instantiation              │
│  • Cross-component calls go through the export boundary      │
│  • Auth does NOT propagate across component boundaries       │
└──────────────────────────────────────────────────────────────┘
```

Internally, the backend represents this with:
- `ComponentDefinitionMetadata` — declares child components, exports, args
  (`crates/common/src/bootstrap_model/components/definition.rs:31`)
- `ComponentExport::Branch` / `ComponentExport::Leaf` — tree of exported symbols
- `Resource::Function(path)` / `Resource::Value(v)` — what exports resolve to
- `ComponentId` — runtime identity (`Root` or `Child(DocumentId)`)
- `ComponentPath` — hierarchical name path (e.g., `["rateLimiter"]`)

### 14.2 Rust Component Design

A Rust component is a self-contained module that bundles:
1. **Schema** — its own tables (via `#[derive(ConvexDocument)]`)
2. **Functions** — queries, mutations, actions operating on those tables
3. **Exports** — the public API other components/apps can call
4. **Args** — configuration parameters provided by the parent at install time

```rust
// ══════════════════════════════════════════════════════════════
// crate: convex-rate-limiter (published to crates.io)
// ══════════════════════════════════════════════════════════════

use convex_native::prelude::*;
use convex_native::component::*;

// ── Schema (component-private tables) ──────────────────────

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "rate_limit_state")]
#[convex(index(name = "by_key", fields = ["key"]))]
pub struct RateLimitState {
    pub key: String,
    pub tokens: f64,
    pub last_refill: f64,
}

// ── Component args (provided by parent at install time) ────

#[derive(ConvexComponentArgs, Debug, Clone)]
pub struct RateLimiterArgs {
    /// Maximum number of requests per window
    pub max_requests: i64,
    /// Window size in seconds
    pub window_secs: f64,
}

// ── Exported functions (the public API) ────────────────────

/// Check if a key is rate limited without consuming a token.
#[convex::query(export)]
async fn check(
    ctx: &mut QueryCtx,
    args: &RateLimiterArgs,     // ← component args injected
    key: String,
) -> Result<RateLimitStatus> { ... }

/// Consume a token for the given key. Returns whether allowed.
#[convex::mutation(export)]
async fn limit(
    ctx: &mut MutationCtx,
    args: &RateLimiterArgs,
    key: String,
) -> Result<RateLimitResult> { ... }

/// Reset the rate limit for a key.
#[convex::mutation(export)]
async fn reset(
    ctx: &mut MutationCtx,
    args: &RateLimiterArgs,
    key: String,
) -> Result<()> { ... }

// ── Internal helpers (not exported) ────────────────────────

#[convex::mutation(internal)]
async fn refill_tokens(
    ctx: &mut MutationCtx,
    args: &RateLimiterArgs,
    key: String,
) -> Result<f64> { ... }

// ── Component definition ───────────────────────────────────

#[convex::component]
pub struct RateLimiter;

impl ConvexComponent for RateLimiter {
    type Args = RateLimiterArgs;
    type Schema = (RateLimitState,);

    // Exports are auto-discovered from #[convex::*(export)] functions,
    // but can also be declared explicitly:
    // type Exports = (check, limit, reset);
}
```

### 14.3 Installing Components in an App

The app (root component) installs Rust components using the `component!` macro
or the builder API:

```rust
// ══════════════════════════════════════════════════════════════
// The application that uses the rate limiter component
// ══════════════════════════════════════════════════════════════

use convex_native::prelude::*;
use convex_rate_limiter::{RateLimiter, RateLimiterArgs};
use convex_aggregate::{Aggregate, AggregateArgs};

// ── App schema ─────────────────────────────────────────────

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
pub struct User {
    pub name: String,
    pub email: String,
}

// ── Install components ─────────────────────────────────────

// Typed component handle — knows the component's exports
static RATE_LIMITER: ComponentHandle<RateLimiter> = component!(
    RateLimiter,
    name = "rateLimiter",
    args = RateLimiterArgs {
        max_requests: 100,
        window_secs: 60.0,
    },
);

static AGGREGATE: ComponentHandle<Aggregate> = component!(
    Aggregate,
    name = "userAggregate",
    args = AggregateArgs {
        table: "users",
    },
);

// ── Use components from app functions ──────────────────────

#[convex::mutation]
async fn create_user(ctx: &mut MutationCtx, name: String, email: String) -> Result<Id<User>> {
    // Call the rate limiter component — type-safe!
    let result = ctx.component(&RATE_LIMITER)
        .call(rate_limiter::limit, rate_limiter::LimitArgs {
            key: email.clone(),
        })
        .await?;

    if !result.allowed {
        anyhow::bail!("Rate limited");
    }

    let id = ctx.db().insert(User { name, email }).await?;

    // Update the aggregate component
    ctx.component(&AGGREGATE)
        .call(aggregate::insert, aggregate::InsertArgs {
            id: id.clone().into(),
        })
        .await?;

    Ok(id)
}

#[convex::query]
async fn user_count(ctx: &mut QueryCtx) -> Result<i64> {
    // Query the aggregate component
    ctx.component(&AGGREGATE)
        .call(aggregate::count, aggregate::CountArgs {})
        .await
}
```

### 14.4 How It Maps to the Existing Component Model

```
    Rust concept                  Maps to backend type
    ────────────────────────      ──────────────────────────────────────
    #[convex::component]          ComponentDefinitionMetadata
    struct RateLimiter             .definition_type = ChildComponent

    ConvexComponent::Args         ComponentArgument + ComponentArgumentValidator

    ConvexComponent::Schema       TableDefinition (in component's namespace)

    #[convex::*(export)]          ComponentExport::Leaf(Reference::Function(...))

    component!(..., name="x")     ComponentInstantiation { name, path, args }

    ComponentHandle<T>            ComponentId (at runtime)

    ctx.component(&HANDLE)        CanonicalizedComponentFunctionPath
      .call(fn, args)              with the component's ComponentPath prefix
```

#### Table Isolation

Each component's tables are namespaced. When the `RateLimiter` component
defines a table `rate_limit_state`, it's stored internally as
`[rateLimiter].rate_limit_state` — completely isolated from the app's tables.
The `ComponentId` in the `Transaction` ensures reads/writes are scoped:

```
    App tables:          users, messages, ...
    rateLimiter tables:  [rateLimiter].rate_limit_state
    userAggregate tables:[userAggregate].aggregated_data
```

This is already how the existing component system works — each `ComponentId`
gets its own table namespace in the database.

#### Export Resolution

When the app calls `ctx.component(&RATE_LIMITER).call(rate_limiter::limit, ...)`,
this resolves to:

```
CanonicalizedComponentFunctionPath {
    component: ComponentPath(["rateLimiter"]),
    udf_path: "limit",
}
```

The `NativeFunctionRunner` resolves this by:
1. Looking up `"rateLimiter"` → `ComponentId::Child(doc_id)` in the component table
2. Finding the function registration for `limit` in the `RateLimiter` component's registry
3. Creating a `Transaction` scoped to that `ComponentId` (so `ctx.db()` only sees the component's tables)
4. Passing the component's `args` to the handler

### 14.5 Component Type Safety

```
┌───────────────────────────────────────────────────────────────┐
│              Component Type Safety Guarantees                  │
├──────────────────────────────────────┬────────────────────────┤
│  Mistake                             │  Compile Error          │
├──────────────────────────────────────┼────────────────────────┤
│  Pass wrong args type when           │  Expected              │
│  installing component                │  RateLimiterArgs,      │
│                                      │  found AggregateArgs   │
├──────────────────────────────────────┼────────────────────────┤
│  Call non-exported function on       │  `refill_tokens` is    │
│  component handle                    │  not an exported       │
│                                      │  function of           │
│                                      │  RateLimiter           │
├──────────────────────────────────────┼────────────────────────┤
│  Call function with wrong args       │  Expected LimitArgs,   │
│  through component handle            │  found CheckArgs       │
├──────────────────────────────────────┼────────────────────────┤
│  Access component's internal tables  │  RateLimitState is     │
│  from app code                       │  not in app's schema   │
├──────────────────────────────────────┼────────────────────────┤
│  Use wrong ComponentHandle           │  Expected              │
│                                      │  ComponentHandle<      │
│                                      │  RateLimiter>,         │
│                                      │  found ComponentHandle<│
│                                      │  Aggregate>            │
└──────────────────────────────────────┴────────────────────────┘
```

### 14.6 Component Context: Scoped Transactions

When a component function executes, its `QueryCtx` / `MutationCtx` is scoped
to that component's tables. This is enforced by the `Transaction`, which is
created with the component's `ComponentId`:

```rust
// Inside NativeFunctionRunner, when dispatching a component function:

let component_id = resolve_component_id(&self.database, &component_path)?;

// Transaction is scoped to this component — ctx.db() only sees
// the component's own tables.
let mut tx = self.begin_component_tx(
    component_id,   // ← scopes all reads/writes to this component
    identity,
    ts,
    ...
).await?;

// Inject component args
let component_args = self.resolve_component_args(component_id, &tx)?;

// Execute with scoped context
let mut ctx = MutationCtx::new_with_component_args(
    &mut tx,
    identity,
    &component_args,   // ← accessible via `args` parameter in handler
    ...
);
```

The component function has **no way** to access the parent app's tables or
other components' tables — the `Transaction` enforces namespace isolation.

### 14.7 Component Lifecycle

```
    Build time                        Runtime
    ──────────                        ───────

    1. Developer writes               4. App binary starts
       RateLimiter component             │
       as a Rust crate                   ▼
           │                          5. NativeSchema::collect()
           ▼                             collects ALL tables (app +
    2. App adds dependency               component tables, namespaced)
       convex-rate-limiter               │
       to Cargo.toml                     ▼
           │                          6. component!() macros register
           ▼                             ComponentInstantiations
    3. App uses component!()             │
       macro in code                     ▼
       to install with args           7. NativeFunctionRunner resolves
                                         component tree at startup:
    cargo build compiles                 - Creates ComponentIds
    everything into one binary           - Validates args against validators
                                         - Builds per-component function registries
                                         - Builds per-component schema namespaces
                                         │
                                         ▼
                                      8. Cross-component calls work via
                                         ctx.component(&HANDLE).call(fn, args)
                                         which routes through NativeFunctionRunner
                                         with the correct ComponentId scope
```

### 14.8 Publishing Components

Rust components are published as regular Rust crates:

```toml
# Cargo.toml of the rate limiter component
[package]
name = "convex-rate-limiter"
version = "1.0.0"
description = "Rate limiting component for Convex native Rust backends"

[dependencies]
convex_native = { version = "0.1" }
```

The crate exports:
- The component struct (`RateLimiter`)
- The args type (`RateLimiterArgs`)
- The exported function references and their args types (for typed calling)
- No internal implementation details leak — `#[convex::mutation(internal)]` functions and schema structs can be `pub(crate)`

```rust
// What the component crate's lib.rs exposes:
pub use component::{RateLimiter, RateLimiterArgs};
pub use exports::{
    check, Check, CheckArgs,       // function ref + return type + args
    limit, Limit, LimitArgs,
    reset, Reset, ResetArgs,
};
// Internal tables, helpers are NOT pub — they're implementation details
```

### 14.9 HTTP Mounts for Components

Components can also serve HTTP routes, mounted at a path prefix by the parent:

```rust
// In the component:
#[convex::http_action(export, method = "POST", path = "/webhook")]
async fn handle_webhook(ctx: &mut HttpActionCtx, req: HttpRequest) -> Result<HttpResponse> {
    // ...
}

// In the app:
static STRIPE: ComponentHandle<StripeComponent> = component!(
    StripeComponent,
    name = "stripe",
    args = StripeArgs { ... },
    http_mount = "/integrations/stripe/",  // ← all component HTTP routes
                                            //   are prefixed with this path
);

// Result: POST /integrations/stripe/webhook → stripe component's handle_webhook
```

This maps to `ComponentDefinitionMetadata::http_mounts` in the existing model.

---

## 15. Implementation Phases

### Phase 1: Schema, Type System, and Single-Node Queries/Mutations (MVP)

**Goal:** Define schemas in Rust, get full type safety, and run native
queries and mutations in a single-node backend.

```
┌──────────────────────────────────────────────────────────┐
│ Deliverables:                                             │
│                                                          │
│ 1. convex_native crate — core type system                │
│    ├── ConvexDocument trait + derive macro                │
│    │   ├── Id<T> (phantom-typed document IDs)            │
│    │   ├── Field enums, Index enums, Patch structs       │
│    │   └── TableDefinition generation                    │
│    ├── ConvexNested derive macro (embedded objects)       │
│    ├── ConvexEnum / ConvexUnion derive macros             │
│    ├── ToConvex / FromConvex traits + std impls          │
│    └── NativeSchema::collect() → DatabaseSchema          │
│                                                          │
│ 2. Typed context wrappers                                │
│    ├── QueryCtx + QueryDb (typed get, query, count)      │
│    ├── MutationCtx + MutationDb (typed insert, patch,    │
│    │   replace, delete)                                  │
│    └── TypedQueryBuilder<T> (typed index, field filters) │
│                                                          │
│ 3. convex_macro extensions                               │
│    ├── #[convex::query]  (signature validation + registry)│
│    └── #[convex::mutation] (same)                        │
│                                                          │
│ 4. NativeFunctionRunner                                  │
│    ├── Implements FunctionRunner<RT>                      │
│    ├── NativeFunctionRegistry (inventory-based)          │
│    ├── Dispatches to typed native functions               │
│    ├── evaluate_schema() from NativeSchema::collect()    │
│    └── Returns FunctionFinalTransaction + FunctionOutcome │
│                                                          │
│ 5. Integration with local_backend                        │
│    └── FunctionRouter routes to native or V8             │
│                                                          │
│ 6. Crate publishing / git consumption                    │
│    ├── convex_native and convex_macro publishable to     │
│    │   crates.io with stable public API surface          │
│    ├── Also consumable via git dependency                │
│    └── Example project (separate repo) as smoke test     │
│                                                          │
│ Key: Schema + types are foundational — everything else   │
│ builds on them. Doing this first means Phase 2-4 code    │
│ is type-safe from the start.                             │
│                                                          │
│ Dependencies: None (builds on existing crates)           │
│ Estimated scope: ~5-6 weeks                              │
└──────────────────────────────────────────────────────────┘
```

### Phase 2: Actions, Scheduling, and Typed Sub-calls

**Goal:** Add ActionCtx, storage, scheduling, cross-component calls, and
typed function references for sub-calls.

```
┌──────────────────────────────────────────────────────────┐
│ Deliverables:                                             │
│                                                          │
│ 1. ActionCtx                                             │
│    ├── run_query(fn_ref, TypedArgs) → TypedResult        │
│    ├── run_mutation(fn_ref, TypedArgs) → TypedResult      │
│    ├── run_action(fn_ref, TypedArgs) → TypedResult        │
│    ├── storage() — upload, download, delete               │
│    └── Full ActionCallbacks integration                  │
│                                                          │
│ 2. Typed function references                             │
│    ├── Generated Args structs per function               │
│    ├── ConvexQueryFunction / ConvexMutationFunction /     │
│    │   ConvexActionFunction traits                       │
│    └── Compile-time checked sub-calls from actions       │
│                                                          │
│ 3. #[convex::action] proc macro                          │
│                                                          │
│ 4. Scheduler API in MutationCtx                          │
│    └── ctx.scheduler().run_after(delay, fn_ref, args)    │
│        (typed: function reference, not string)           │
│                                                          │
│ 5. HTTP action support                                   │
│    └── #[convex::http_action(method = "POST", path = ...)]│
│                                                          │
│ Dependencies: Phase 1                                    │
│ Estimated scope: ~3-4 weeks                              │
└──────────────────────────────────────────────────────────┘
```

### Phase 3: Distributed Execution

**Goal:** Run functions across multiple worker nodes.

```
┌──────────────────────────────────────────────────────────┐
│ Deliverables:                                             │
│                                                          │
│ 1. Protobuf service definition                           │
│    └── FunctionExecutionService (execute, health check)  │
│                                                          │
│ 2. DistributedFunctionRunner (conductor side)            │
│    ├── WorkerPool with P2C load balancing                │
│    ├── Retry logic for overloaded workers                │
│    └── Service discovery integration                     │
│                                                          │
│ 3. FunctionExecutionServer (worker side)                 │
│    ├── gRPC server wrapping NativeFunctionRunner         │
│    ├── Concurrency limiting                              │
│    └── Health check endpoint                             │
│                                                          │
│ 4. Binary mode switching                                 │
│    └── CONVEX_MODE={standalone,conductor,worker}         │
│                                                          │
│ 5. Serialization for all request/response types          │
│    └── Extend crates/pb/ with new proto definitions      │
│                                                          │
│ Dependencies: Phase 2                                    │
│ Estimated scope: ~4-5 weeks                              │
└──────────────────────────────────────────────────────────┘
```

### Phase 4: Production Hardening

**Goal:** Make it production-ready with observability, graceful operations,
and resource management.

```
┌──────────────────────────────────────────────────────────┐
│ Deliverables:                                             │
│                                                          │
│ 1. Metrics and distributed tracing                       │
│    ├── Fastrace span propagation over gRPC               │
│    ├── Per-function latency/error metrics                 │
│    └── Worker pool health dashboards                     │
│                                                          │
│ 2. Graceful shutdown and rolling updates                 │
│    ├── Drain protocol for workers                        │
│    ├── Function-aware routing during version skew        │
│    └── Health-check-based readiness gates                │
│                                                          │
│ 3. Resource limits                                       │
│    ├── Per-function execution timeouts                   │
│    ├── Memory monitoring / OOM protection                │
│    └── Circuit breaker for unhealthy workers             │
│                                                          │
│ 4. Index cache warming and invalidation                  │
│    └── Pre-warm caches on worker startup                 │
│                                                          │
│ Dependencies: Phase 3                                    │
│ Estimated scope: ~3-4 weeks                              │
└──────────────────────────────────────────────────────────┘
```

### Phase 5: Advanced Type Features (Optional)

**Goal:** Push the type system further with compile-time schema validation,
migration tooling, and codegen.

```
┌──────────────────────────────────────────────────────────┐
│ Deliverables:                                             │
│                                                          │
│ 1. Schema migration tooling                              │
│    ├── Diff old schema ↔ new schema at compile time      │
│    ├── Generate migration functions for breaking changes │
│    └── Validate backwards compatibility                  │
│                                                          │
│ 2. Compile-time index field validation                   │
│    ├── Verify index fields exist on the struct           │
│    └── Verify filter field matches index field order     │
│                                                          │
│ 3. Typed vector/text search indexes                      │
│    ├── #[convex(vector_index(...))] attribute             │
│    ├── #[convex(text_index(...))] attribute               │
│    └── Typed search query builders                       │
│                                                          │
│ 4. Relationship helpers                                  │
│    ├── ctx.db().get_related::<Message>(user_id)          │
│    │   (auto-discovers foreign key via Id<User> field)   │
│    └── Compile-time relationship graph                   │
│                                                          │
│ Dependencies: Phase 1                                    │
│ Estimated scope: ~4-5 weeks                              │
└──────────────────────────────────────────────────────────┘
```

### Summary Timeline

```
    Week  1───2───3───4───5───6───7───8───9───10──11──12──13──14──15──16──17──18──19
          │                 │           │              │              │
          ├─────────────────┤           │              │              │
          │    Phase 1      │           │              │              │
          │  (Schema +      │           │              │              │
          │   Types + MVP)  │           │              │              │
          │                 ├───────────┤              │              │
          │                 │  Phase 2  │              │              │
          │                 │ (Actions, │              │              │
          │                 │  typed    │              │              │
          │                 │  subcalls)│              │              │
          │                 │           ├──────────────┤              │
          │                 │           │   Phase 3    │              │
          │                 │           │ (Distributed)│              │
          │                 │           │              ├──────────────┤
          │                 │           │              │   Phase 4    │
          │                 │           │              │ (Hardening)  │
          │                 │           │              │              │
          │     Phase 5 (Advanced Types) can run in parallel ────────┤
```

---

## Appendix A: Crate Dependency Map

```
    ┌─────────────────────────────────────────────────────────┐
    │  THIS REPO — framework crates (published to crates.io   │
    │  or consumed via git dependency)                        │
    │                                                         │
    │  convex_native (new)                convex_macro (ext.) │
    │      │                                   │              │
    │      ├── common                          │              │
    │      ├── database (Transaction)          │              │
    │      ├── function_runner (FunctionRunner) │              │
    │      ├── model (ModuleConfig, etc.)      │              │
    │      ├── value (ConvexValue, ConvexObject)│              │
    │      ├── udf (FunctionOutcome, ActionCallbacks)         │
    │      ├── keybroker (Identity)            │              │
    │      ├── usage_tracking                  │              │
    │      ├── indexing (InMemoryIndexCache)   │              │
    │      └── pb (protobuf, for Phase 3)     │              │
    │                                          │              │
    │  convex_native_distributed (new, Phase 3)│              │
    │      │                                   │              │
    │      ├── convex_native                   │              │
    │      ├── tonic (gRPC)                    │              │
    │      ├── pb (extended protos)            │              │
    │      └── common (knobs, service discovery)              │
    └─────────────────────────────────────────────────────────┘
                         │
                  cargo dependency
                         │
    ┌────────────────────▼────────────────────────────────────┐
    │  DEVELOPER'S PROJECT (separate repo)                    │
    │                                                         │
    │  my-backend                                             │
    │      ├── convex_native (from crates.io or git)          │
    │      ├── convex_native_distributed (optional, Phase 3)  │
    │      └── any component crates (e.g. convex-rate-limiter)│
    └─────────────────────────────────────────────────────────┘
```

## Appendix B: Full Example Application

This is a complete example of the **developer's own project** (a separate
repository that depends on the `convex_native` crate from this repo).

```toml
# Cargo.toml (in the developer's project)
[package]
name = "my-chat-backend"
version = "0.1.0"
edition = "2021"

[dependencies]
convex_native = "0.1"       # from crates.io
# convex_native = { git = "https://github.com/nicorp/convex-backend" }  # or from git
anyhow = "1"
```

```rust
// ══════════════════════════════════════════════════════════════
// src/schema.rs — Define your data model
// ══════════════════════════════════════════════════════════════
use convex_native::prelude::*;

#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
#[convex(index(name = "by_created", fields = ["created_at"]))]
pub struct User {
    pub name: String,
    pub email: String,
    pub avatar_url: Option<String>,
    pub role: UserRole,
    pub created_at: f64,
}

#[derive(ConvexEnum, Debug, Clone, PartialEq)]
pub enum UserRole {
    Admin,
    Member,
    Guest,
}

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "messages")]
#[convex(index(name = "by_channel", fields = ["channel", "created_at"]))]
pub struct Message {
    pub author: Id<User>,
    pub body: String,
    pub channel: String,
    pub created_at: f64,
}


// ══════════════════════════════════════════════════════════════
// src/functions/users.rs — Queries and mutations
// ══════════════════════════════════════════════════════════════
use convex_native::prelude::*;
use crate::schema::*;

/// List all users, newest first.
#[convex::query]
async fn list(ctx: &mut QueryCtx) -> Result<Vec<UserWithId>> {
    ctx.db().query::<User>()
        .with_index(UserIndex::ByCreated)
        .order(Order::Desc)
        .collect()
        .await
}

/// Look up a user by their email address.
#[convex::query]
async fn get_by_email(ctx: &mut QueryCtx, email: String) -> Result<Option<UserWithId>> {
    ctx.db().query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, &email)
        .first()
        .await
}

/// Create a new user. Returns the typed Id<User>.
#[convex::mutation]
async fn create(
    ctx: &mut MutationCtx,
    name: String,
    email: String,
) -> Result<Id<User>> {
    // Check for existing user — compile-time checked index + field
    let existing = ctx.db().query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, &email)
        .first()
        .await?;

    if existing.is_some() {
        anyhow::bail!("User with email {} already exists", email);
    }

    // Insert a typed struct — compiler ensures all required fields are set
    let id = ctx.db().insert(User {
        name,
        email,
        avatar_url: None,
        role: UserRole::Member,
        created_at: ctx.unix_timestamp().as_secs_f64(),
    }).await?;

    // Schedule a welcome email — function reference, not a string
    ctx.scheduler().run_after(
        Duration::ZERO,
        send_welcome,
        SendWelcomeArgs { user_id: id.clone() },
    ).await?;

    Ok(id)
}

/// Update only the user's name. Other fields are untouched.
#[convex::mutation]
async fn update_name(
    ctx: &mut MutationCtx,
    user_id: Id<User>,       // ← can't accidentally pass Id<Message>
    new_name: String,
) -> Result<UserWithId> {
    ctx.db().patch(user_id, UserPatch {
        name: Some(new_name),
        ..Default::default()
    }).await
}

/// Delete a user by ID.
#[convex::mutation]
async fn remove(ctx: &mut MutationCtx, user_id: Id<User>) -> Result<()> {
    ctx.db().delete(user_id).await
}


// ══════════════════════════════════════════════════════════════
// src/functions/messages.rs — Message queries
// ══════════════════════════════════════════════════════════════
use convex_native::prelude::*;
use crate::schema::*;

/// Get messages in a channel, paginated.
#[convex::query]
async fn list_by_channel(
    ctx: &mut QueryCtx,
    channel: String,
    cursor: Option<Cursor>,
) -> Result<TypedPage<Message>> {
    ctx.db().query::<Message>()
        .with_index(MessageIndex::ByChannel)
        .eq(MessageField::Channel, &channel)
        .order(Order::Desc)
        .page(cursor, 50)
        .await
}

/// Send a message to a channel.
#[convex::mutation]
async fn send(
    ctx: &mut MutationCtx,
    author: Id<User>,
    channel: String,
    body: String,
) -> Result<Id<Message>> {
    // Verify the author exists — get() returns Option<UserWithId>
    let _user = ctx.db().get(author.clone()).await?
        .ok_or_else(|| anyhow!("User not found"))?;

    ctx.db().insert(Message {
        author,              // ← Id<User>, compile-time foreign key safety
        body,
        channel,
        created_at: ctx.unix_timestamp().as_secs_f64(),
    }).await
}


// ══════════════════════════════════════════════════════════════
// src/functions/actions/email.rs — Side-effect actions
// ══════════════════════════════════════════════════════════════
use convex_native::prelude::*;
use crate::schema::*;

/// Send a welcome email to a new user.
#[convex::action]
async fn send_welcome(ctx: &mut ActionCtx, user_id: Id<User>) -> Result<()> {
    // Type-safe sub-call: compiler knows get_by_email returns Option<UserWithId>
    let user = ctx.run_query(
        get_by_email,
        GetByEmailArgs { email: "...".into() },
    ).await?.ok_or_else(|| anyhow!("User not found"))?;

    // Full Rust ecosystem available in actions
    reqwest::Client::new()
        .post("https://api.sendgrid.com/v3/mail/send")
        .bearer_auth(std::env::var("SENDGRID_API_KEY")?)
        .json(&serde_json::json!({
            "personalizations": [{"to": [{"email": user.email}]}],
            "subject": format!("Welcome, {}!", user.name),
            "content": [{"type": "text/plain", "value": "Welcome to our app!"}],
        }))
        .send()
        .await?;

    Ok(())
}


// ══════════════════════════════════════════════════════════════
// src/main.rs — Entry point (in the developer's project)
// ══════════════════════════════════════════════════════════════
use convex_native::prelude::*;

mod schema;
mod functions;

fn main() {
    ConvexBackend::new()
        .with_schema::<(schema::User, schema::Message)>()
        .with_native_functions()
        .with_persistence("postgres://localhost:5432/myapp")
        .run();
}
```

Build and run:
```bash
# In the developer's project directory (not this repo)
cargo build --release
./target/release/my-chat-backend  # standalone mode
```
