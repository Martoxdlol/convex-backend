//! Cron registrations — attach 5-field cron expressions to
//! existing mutations / actions.
//!
//! The target string must match a name declared by `#[convex::mutation]`
//! or `#[convex::action]` somewhere in the registry. `target_kind`
//! picks which of the two it's expected to be — `ConvexBackend::validate()`
//! cross-checks at startup, so a misconfigured cron crashes the
//! binary rather than silently skipping.

use convex_native::convex;

/// Daily nightly cleanup at 03:00 UTC. `target = "nightly_cleanup"`
/// points at the `#[convex::action(internal)]` in `actions.rs`.
#[convex::cron(
    name = "daily_cleanup",
    schedule = "0 3 * * *",
    target = "nightly_cleanup",
    target_kind = "action",
)]
#[allow(dead_code)]
fn _daily_cleanup() {}
