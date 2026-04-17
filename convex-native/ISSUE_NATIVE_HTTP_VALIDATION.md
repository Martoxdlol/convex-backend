# Issue: native handlers are invisible to the HTTP surface

**Status:** resolved 2026-04-17 via
`fix(udf): teach ValidatedPathAndArgs about the native function
registry` (commit `9521b91cf`). This file is retained as a postmortem;
the follow-up work items in "Open questions before we implement" and
"Immediate next steps" are tracked separately below.
**Originally discovered:** 2026-04-17 while smoke-testing
`convex-native/examples/standalone_todo_app`.
**Severity:** blocks every pure-native deployment from answering
client traffic, across both the monolith (`STANDALONE.md`) and the
distributed (`DISTRIBUTED_PLAN.md`) topologies.
**Affects:** `/api/mutation`, `/api/query`, `/api/action`, and any
sync-protocol flow that goes through `ValidatedPathAndArgs`.

---

## Observed symptom

Starting the shipped example:

```sh
cargo run -p standalone_todo_app -- \
    --port 3210 --instance-name mydeploy \
    --instance-secret 0000000000000000000000000000000000000000000000000000000000000000 \
    --db sqlite --local-storage ./_run/storage
```

Log confirms registration:

```
INFO convex_native: Starting Convex backend with 5 native function(s) registered
INFO common::http: backend listening on 0.0.0.0:3210
```

The functions are `list_for_owner`, `count_pending`, `create`,
`mark_done`, `summarise`. Calling any one over HTTP:

```sh
curl -sS http://127.0.0.1:3210/api/mutation \
  -H 'Content-Type: application/json' \
  -d '{"path":"mutations:create","args":{"owner":"alice","text":"x"},"format":"json"}'
```

Response:

```json
{
  "status": "error",
  "errorMessage": "[Request ID: ...] Server Error\nCould not find public function for 'mutations:create'. Did you forget to run `npx convex dev`?\n"
}
```

The same happens for bare names (`"create"`), for
`actions:summarise`, `queries:list_for_owner`, etc. The error
text is baked into `udf::validation::missing_or_internal_error`.

---

## Why this happens: two registries, one validation path

Convex resolves function calls against **two independent
sources of truth** that are never reconciled:

### 1. `inventory` table — populated by the `#[convex::*]` macros

Each `#[convex::query] pub async fn list_for_owner(...)` expands to
an `inventory::submit!` entry for a
`NativeFunctionRegistration`. The key is the bare Rust identifier
(`list_for_owner`), and the value carries the typed handler fn
pointer and arg-name slice.

The runtime consumer is
`crates/convex_native_core/src/registry.rs:104`:

```rust
inventory::collect!(NativeFunctionRegistration);
```

Collected into `NativeFunctionRegistry { by_name: HashMap<&str, &NativeFunctionRegistration> }`
at backend boot by `local_backend::lib::make_app` →
`NativeFunctionRunner::from_inventory()` at
`crates/local_backend/src/lib.rs:257`.

### 2. `_modules` / `_module_versions` rows in the database — populated by `npx convex dev`

The Convex CLI pushes bundled JS, the `analyze` isolate pre-parses
each module, and `Application::apply_config` writes a row per
module containing an `AnalyzedModule { functions:
Vec<AnalyzedFunction> }`. Each `AnalyzedFunction` carries:

- `name` (the exported JS symbol, e.g. `create`)
- `udf_type` (query/mutation/action/httpAction)
- `visibility: Option<Visibility>` (`Public` / `Internal`)
- `args: ArgsValidator` / `returns: ReturnsValidator`
- `params`, `source_mapped`, etc.

Look up happens via
`ModuleModel::get_analyzed_function(&CanonicalizedComponentFunctionPath)`
at `crates/model/src/modules/mod.rs:389`.

A pure-native deployer **never calls `apply_config`**. The table
stays empty. Nothing ever writes an `AnalyzedFunction` row for
`create`, `list_for_owner`, or any other
`#[convex::mutation]` / `#[convex::query]` handler.

### The validation path only checks #2

Every client entry point — `api/mutation`, `api/query`,
`api/action`, the WebSocket sync protocol — funnels through
`ValidatedPathAndArgs::new` in
`crates/udf/src/validation.rs:467`. The load-bearing lines are at
`validation.rs:492-499`:

```rust
let Ok(analyzed_function) = ModuleModel::new(tx)
    .get_analyzed_function_by_id(&path)
    .await?
else {
    return Ok(Err(JsError::from_message(missing_or_internal_error(
        public_path,
    )?)));
};
```

`get_analyzed_function_by_id` reads from the `_modules` system
table. A missing row produces the
`"Could not find public function for ..."` JsError (assembled by
`missing_or_internal_error` at `validation.rs:298-305`). Only
after a successful lookup does `ValidatedPathAndArgs::new_inner`
run, and only after `ValidatedPathAndArgs` does the
`FunctionRunner` ever get invoked. Native registry lookup never
enters this code path.

---

## Why `CompositeFunctionRunner` does not save us

`crates/convex_native_backend/src/composite_runner.rs:196` *does*
know about the native registry — `requested_function_name` pulls
`path.udf_path.function_name()` out of the metadata, and
`composite_runner.rs:556` short-circuits
`FunctionRunner::run_function` via
`self.native.get(name).is_some()`.

The catch: `FunctionRunner::run_function` is strictly downstream
of validation. Control flow:

```
HTTP handler  (crates/local_backend/src/http_actions.rs)
    ↓
Application::mutation / query / action
    ↓  (opens a tx)
Application::_validate_args  ──→  ValidatedPathAndArgs::new  ──→  FAIL here if no AnalyzedFunction
    ↓  (only if validation passes)
ApplicationFunctionRunner::run_mutation_sync_compile
    ↓
FunctionRunner::run_function         ←  CompositeFunctionRunner checks native registry HERE
```

The composite runner is correct and well-tested, it just never
gets asked the question.

---

## Why the regression was not caught

Four independent holes coincided:

1. **No HTTP-path integration tests for native handlers.** Every
   test in `crates/convex_native_core/tests/*.rs` and
   `crates/convex_native_backend/tests/*.rs` drives
   `NativeFunctionRunner::run_query` /
   `run_mutation` / `run_action_with_callbacks` directly. They
   skip `Application`, `ValidatedPathAndArgs`, and the HTTP
   router entirely.
2. **`convex_native_distributed`'s e2e tests** use
   `FunctionExecutionService` gRPC, which has its own bypass
   (the worker calls handlers inline, no `ValidatedPathAndArgs`).
3. **The `standalone_todo_app` README** documented curl commands
   that were never actually exercised end-to-end — the crate was
   only checked for compile + the backend was smoke-tested for
   boot, not for answering requests.
4. **The `convex_native::run()` helper just panicked on start
   until 197e0e4a5** (today's fix), so no one ever got far
   enough to hit the HTTP path against a native-only backend.

---

## Scope

The gap hits **every topology** that relies on the HTTP /
WebSocket entry points:

| Topology | Affected? | Notes |
|----------|-----------|-------|
| Monolith (`STANDALONE.md`) | **Yes** | Exactly what `standalone_todo_app` demonstrates. `Application` + HTTP router in one process, native registry loaded via `inventory`, DB empty of modules. |
| Distributed (`DISTRIBUTED_PLAN.md`) | **Yes** | Backend runs `ValidatedPathAndArgs` before dispatch even when the dispatcher is `PoolFunctionRunner`. Workers carry `NativeFunctionRegistry`; backend DB does not. |
| Legacy conductor | **Yes** | Same validation path. |
| Monolith with JS **and** native | **No** | JS side writes `_modules` rows so the validation passes; composite runner then intercepts native names at dispatch time. |

The "mixed JS + native" case is the one that has always worked —
which is why none of the existing tests caught this. They were
written for users who run `npx convex dev` at least once.

---

## What a fix looks like

Two viable approaches. Both must preserve the existing JS path —
any deployer who also pushes JS modules must continue to see
their functions validated the way they are today.

### Option A — synthesize `AnalyzedFunction` rows at boot

**Idea:** during `Application::new` (or a dedicated
`install_native_modules` step), walk `NativeFunctionRegistry` and
write one synthetic module row per native function.

Sketch:

```rust
async fn publish_native_modules<RT: Runtime>(
    db: &Database<RT>,
    native: &NativeFunctionRegistry,
) -> anyhow::Result<()> {
    let mut tx = db.begin_system().await?;
    for reg in native.iter() {
        let module_path = module_path_for(reg);           // e.g. "native/create.js"
        let analyzed = AnalyzedFunction {
            name: reg.name.parse()?,
            udf_type: reg.udf_type(),
            visibility: Some(if reg.is_internal {
                Visibility::Internal
            } else {
                Visibility::Public
            }),
            args: args_validator_from_registration(reg),
            returns: ReturnsValidator::Unvalidated,
            // is_native: true — new field? or reuse source_mapped?
            ..Default::default()
        };
        ModuleModel::new(&mut tx).put_analyzed_for_native(module_path, analyzed).await?;
    }
    db.commit(tx).await?;
    Ok(())
}
```

**Pros**
- The HTTP validation path does not change. Everything
  downstream (scheduling, cron validation, visibility checks,
  `UdfOutcome` assembly) keeps working unmodified.
- Native handlers appear in dashboards / introspection exactly
  like JS ones — same module browser, same function list.
- Clean story for `CompositeFunctionRunner`: JS rows live
  alongside synthetic native rows, name collisions are caught by
  a boot-time check.

**Cons / open questions**
- Native registrations only record arg **names**, not full
  validators. Either extend `#[convex::*]` to emit arg-validator
  metadata (substantial macro work) or use
  `ArgsValidator::Unvalidated` and accept that native functions
  lose runtime arg validation.
- Migration: pre-existing deployments may have stale rows for
  renamed or removed handlers — need a reconciliation pass on
  each boot, not just a one-shot insert.
- Cost: small write-per-handler on every startup; fine for tens,
  potentially rough for tens of thousands.
- Where to stamp `udf_version` / `server_version`? Native code
  does not ship through the SDK.
- Deciding the canonical `ModulePath` shape for native handlers
  (`native/<name>.js`? `__native__:<name>`? namespaced?) —
  whatever we pick is user-visible.

### Option B — teach `ValidatedPathAndArgs` about the native registry

**Idea:** inject `Arc<NativeFunctionRegistry>` into
`Application`, and in
`ValidatedPathAndArgs::new` short-circuit before
`get_analyzed_function_by_id` if the registry already has the
name.

Sketch:

```rust
if let Some(reg) = native_registry.get(path.udf_path.function_name()) {
    let analyzed = synthesize_analyzed_function_from_native(reg);
    return ValidatedPathAndArgs::new_inner(
        allowed_visibility,
        tx,
        path,
        args,
        expected_udf_type,
        analyzed,
        // …
    );
}
```

**Pros**
- No database writes at boot; no migration problem.
- Registry is the single source of truth for what a native
  function is — same lookup at validate-time and dispatch-time.
- Easier to reason about visibility: native registration already
  carries `is_internal`, so we do not have to re-encode it into a
  synthetic row.

**Cons / open questions**
- Couples `crates/udf/src/validation.rs` (currently DB-only) to
  the native registry. New dependency from `udf` crate to
  `convex_native_core`, or an abstracted `NativeLookup` trait
  passed in as config.
- Dashboards + any other surface that reads `_modules` still see
  an empty backend — user-confusing.
- Scheduling validation
  (`validate_schedule_args` at `validation.rs:143`) has its own
  `ModuleModel::get_metadata_for_function` call; every such site
  needs the same short-circuit. Easy to miss one.

### Hybrid

Option A for the persistent surfaces (dashboards, scheduling,
introspection tooling) plus a **registry-backed fast path** that
answers validation lookups without a DB roundtrip when the name
is known-native. Most engineering of Option A, fewer cache
misses, preserves a single source of truth (the registry).

---

## Workarounds available today

Until the fix lands, pure-native deployers can:

1. **Still drive handlers from Rust tests.** The
   `NativeFunctionRunner::run_query` / `run_mutation` /
   `run_action_with_callbacks` paths work. Useful for CI but not
   for clients.
2. **Mix with a tiny JS shim.** Push an otherwise empty Convex
   project with one JS file that re-exports dummies sharing the
   Rust function names. `CompositeFunctionRunner` will intercept
   the request at dispatch time and route to the native handler
   before any JS runs. Ugly, but unblocks clients.
3. **Skip the HTTP layer.** The gRPC
   `FunctionExecutionService` that the distributed worker
   already exposes can drive handlers without touching
   `ValidatedPathAndArgs`. Not a fit for browser clients.

---

## Test plan for the fix

Regardless of option, the fix must land with at least these
tests, all of which are missing today:

- **Monolith e2e.** Boot `standalone_todo_app` in-process, POST
  `/api/mutation`, POST `/api/query`, POST `/api/action`, assert
  200s and round-tripped values. Run without `npx convex dev`
  ever having executed against the DB.
- **Monolith mixed.** Same setup with one JS mutation pushed
  through `apply_config`. Assert JS and native both reachable.
- **Distributed e2e.** Boot backend + one worker in-process,
  same curl assertions against the backend's `:3210`.
- **Visibility.** A `#[convex::mutation(internal)]` handler
  returns the same `missing_or_internal_error` to a non-admin
  caller as an internal JS mutation does today.
- **Scheduling.**
  `ctx.scheduler().schedule::<NativeMutation>(...)` resolves
  without hitting the "nonexistent path" branch at
  `validation.rs:180-186`.
- **Dashboard / introspection.** If Option A, the function
  browser lists native entries; if Option B, document that it
  does not, and open a follow-up.

---

## Open questions before we implement

1. Which option (A / B / hybrid) aligns with where
   `convex_native` is supposed to sit long-term? B couples the
   validation layer to a new dep; A normalizes native into the
   existing module machinery at the cost of synthetic rows.
2. Arg validators: is it worth extending `#[convex::*]` to emit a
   full `ArgsValidator`, or is `Unvalidated` acceptable for v1?
   (Trade-off: type-safety at the boundary vs. macro complexity.)
3. Canonical path format for native functions
   (`native/<name>.js` vs. bare name vs. a new reserved prefix)?
   This becomes part of the observable protocol, so the answer
   propagates to dashboards, logs, error messages, and the
   `function_path` field in `FunctionExecutionRequest`.
4. Does the `CONVEX_REFUSE_NATIVE_HANDLERS` flag (agnostic
   backend enforcement) need an inverse —
   `CONVEX_REFUSE_JS_HANDLERS` — so a native-only deployer can
   reject a misconfigured JS module push at boot? Relevant mainly
   if Option A lands and we care about keeping the module table
   pure.
5. The failure today is loud but misleading — it asks the user to
   run `npx convex dev` even though the relevant project has no
   JS. Should we adjust `missing_or_internal_error` to mention
   the native path (`"not found in native registry; run the
   worker with the handler linked"`) when the backend is in
   native mode?

---

## Resolution (2026-04-17)

Shipped as **Option B — registry-backed fast path in validation**:

- New trait `udf::validation::NativeFunctionResolver` + global
  `install_native_function_resolver` install hook. Avoids a
  circular `udf → convex_native_core` dep — the `udf` crate stays
  storage-agnostic and the bridge lives in
  `convex_native_backend::native_resolver`.
- `ValidatedPathAndArgs::new_with_returns_validator` falls back
  to the native resolver **only when the JS-side module lookup
  would fail** — either UdfConfig is absent (pure-native
  deployment) or the named function is missing from the loaded
  `_modules` rows. Mixed JS + native deployments with a matching
  JS export keep using the JS-side `AnalyzedFunction` so the
  stricter JS validators win over the native registry's
  `Unvalidated` default. On a native hit the short-circuit
  synthesizes `AnalyzedFunction { args_str: None, returns_str:
  None, visibility derived from is_internal, udf_type from the
  handler kind }` and runs `new_inner` with `npm_version: None`.
- `validate_schedule_args` gets the same short-circuit — native
  mutations can be `ctx.scheduler().schedule(...)`'d.
- `missing_or_internal_error` now switches its hint based on
  whether a native resolver is installed. Answers question 5
  ("adjust the error text") from the original issue.
- `local_backend::make_app` calls
  `convex_native_backend::install_native_resolver` after the
  native registry is collected. JS-only deployments still behave
  exactly as before — the resolver is consulted only when the
  name is actually in the native inventory.

**Answers to the original open questions:**

1. **Which option?** Option B. The monolith never writes
   `_modules` rows for native handlers, so Option A's synthetic
   rows would either force storage+migration churn or diverge
   silently from the live registry. Option B reads from the
   single source of truth (the `inventory` table) at validation
   time and costs one `HashMap` lookup per request. The decision
   remains reviewable — the synthesis helper is tight enough that
   swapping in a persistent-module approach later would be a
   localized change.
2. **Arg validators.** `Unvalidated` is accepted for v1. Extending
   the `#[convex::*]` macros to emit full `ArgsValidator` metadata
   is a follow-up; the short-circuit code path is already ready to
   carry it through (replace `args_str: None` with the emitted
   JSON).
3. **Canonical path format.** Irrelevant for Option B — the
   registry keys on the bare function name and the validation
   path accepts any module segment the client sends. Native
   handlers do not appear in `_modules` or in the dashboard
   module browser; that's documented as a known limitation of
   Option B. Option A would have had to answer this question.
4. **`CONVEX_REFUSE_JS_HANDLERS`.** Not needed for the
   validation fix. Revisit if module-table hygiene becomes a
   concern in mixed deployments.
5. **Error message.** Adjusted (see above). When a native
   resolver is installed, the hint steers users toward
   `#[convex::*]` spelling + linking rather than `npx convex
   dev`.

**Test coverage shipped with the fix:**

- `crates/udf/src/validation.rs` `native_resolver_tests::*` —
  6 unit tests pinning the synthesizer + trait contract.
- `crates/convex_native_backend/src/native_resolver.rs` —
  `empty_runner_reports_none` pins the empty-registry path.

**Follow-up work still outstanding:**

- **HTTP-path integration test.** A real monolith e2e test that
  boots `standalone_todo_app` in-process and POSTs `/api/mutation`
  is blocked on the same `Database<Rt>` test fixture as substeps
  2.8b and 4.6b in `STATUS.md`. A thin smoke harness will land
  once that fixture exists. Until then, the fix has been
  validated by running the example directly against its shipped
  binary.
- **Arg validators on the macro.** Extending `#[convex::*]` to
  emit a `ConvexTypeOf`-backed `ArgsValidator` would close
  question 2 above. Separate PR.
- **Dashboard / introspection.** Option B leaves native handlers
  invisible to `_modules`-backed browsers. If that becomes a UX
  problem, revisit with a hybrid approach that publishes
  synthetic module rows.


---

## Cross-references

- `crates/udf/src/validation.rs:467-499` — the guard that rejects
  native handlers.
- `crates/udf/src/validation.rs:298-305` — error message text.
- `crates/convex_native_core/src/registry.rs:104-144` —
  `NativeFunctionRegistry`, the canonical native-side index.
- `crates/convex_native_backend/src/composite_runner.rs:556-620`
  — the composite runner's native short-circuit that never gets
  reached.
- `crates/local_backend/src/lib.rs:257-347` — wiring that loads
  the native registry into `make_app`.
- `convex-native/DISTRIBUTED_PLAN.md` — distributed topology
  rationale; any fix has to keep the backend-side validation
  working when the handler lives on a remote worker.
- `convex-native/STATUS.md` — track the fix here once a phase
  assignment is made.
