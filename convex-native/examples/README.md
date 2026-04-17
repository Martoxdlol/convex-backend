# convex-native examples

Reference snippets a deployer can crib from when building their own
binary against `convex_native` / `convex_native_distributed`. These
are *reading samples*, not a built Cargo workspace — they demonstrate
the shape of a deployer's own project rather than duplicating the
in-tree `convex_native_distributed/examples/{worker,conductor}`
binaries (which *are* runnable).

## What's here

| Path | Description |
|------|-------------|
| `minimal_app/` | Smallest runnable shape of a deployer's project — schema, functions, HTTP, crons — wired to a `ConvexBackend` builder. |

## Running the in-tree examples

The `convex_native_distributed` crate ships two *runnable* example
binaries a deployer can use as-is for smoke tests:

```sh
# Worker: listens on 127.0.0.1:4567 by default.
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
  cargo run -p convex_native_distributed --example worker

# Conductor: probes each worker's health, prints a summary, exits.
CONVEX_MODE=conductor \
  CONVEX_WORKER_ENDPOINTS="http://127.0.0.1:4567" \
  cargo run -p convex_native_distributed --example conductor
```

See `DEPLOYMENT.md` for the full deployment story.
