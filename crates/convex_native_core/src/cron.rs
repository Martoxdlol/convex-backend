//! Cron-scheduled recurring jobs.
//!
//! `#[convex::cron(name = "...", schedule = "...")]` attaches a cron
//! expression (5-field Unix style) to a mutation or action. At startup
//! the backend adapter inspects `CronRegistry::collect()` and installs
//! a scheduler thread for each entry — executing happens outside this
//! crate (same boundary as storage and callbacks).
//!
//! This module just collects the registrations via `inventory`; the
//! parsing of the schedule expression is validated at macro time using
//! `saffron` — the same crate the JS-side `cron.ts` uses.

/// One entry per `#[convex::cron(...)]`.
pub struct CronRegistration {
    /// Human-readable name (stable identifier for this cron).
    pub name: &'static str,
    /// 5-field cron expression (minute hour dom month dow).
    pub schedule: &'static str,
    /// Target function name — must match a registered
    /// `#[convex::mutation]` or `#[convex::action]`.
    pub target: &'static str,
    /// Whether the target is a mutation or an action. Stored as
    /// `"mutation"` / `"action"` because this struct needs to be
    /// monomorphic for `inventory`.
    pub target_kind: &'static str,
}

inventory::collect!(CronRegistration);

/// Aggregated registry over collected cron jobs.
pub struct CronRegistry {
    entries: Vec<&'static CronRegistration>,
}

impl CronRegistry {
    pub fn collect() -> anyhow::Result<Self> {
        let mut seen: std::collections::BTreeSet<&'static str> = std::collections::BTreeSet::new();
        let mut entries = Vec::new();
        for entry in inventory::iter::<CronRegistration> {
            if !seen.insert(entry.name) {
                anyhow::bail!("duplicate cron registration for {}", entry.name);
            }
            entries.push(entry);
        }
        Ok(Self { entries })
    }

    pub fn iter(&self) -> impl Iterator<Item = &'static CronRegistration> + '_ {
        self.entries.iter().copied()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn lookup(&self, name: &str) -> Option<&'static CronRegistration> {
        self.entries.iter().copied().find(|e| e.name == name)
    }
}
