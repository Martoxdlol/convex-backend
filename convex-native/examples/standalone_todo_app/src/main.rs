//! Deployer entry point. `use standalone_todo_app as _;` is the
//! one load-bearing line — it forces the linker to include our
//! `#[convex::*]` registrations so `inventory` sees them at
//! startup. Drop that line and the backend boots with an empty
//! registry.
#[allow(unused_imports)]
use standalone_todo_app as _;

fn main() -> anyhow::Result<()> {
    convex_native::run()
}
