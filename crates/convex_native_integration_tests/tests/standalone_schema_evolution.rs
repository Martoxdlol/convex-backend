//! Coverage for schema-evolution + warmup-plan helpers.
//!
//! Both features are wire-independent — they consume the
//! `DatabaseSchema` produced by `NativeSchema::collect()`.

use convex_native_core::{
    diff_schemas,
    plan_warmup,
    warmup::WarmupEntry,
    NativeSchema,
    SchemaChange,
};

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

#[test]
fn warmup_plan_includes_every_fixture_index() -> anyhow::Result<()> {
    let schema = NativeSchema::collect()?;
    let plan = plan_warmup(&schema);
    // Fixture declares `todos.by_owner`, `todos.by_owner_done`,
    // `messages.by_channel`. Every entry should be a `DbIndex`
    // (no text/vector in the baseline fixture).
    let mut saw_by_owner = false;
    let mut saw_by_owner_done = false;
    let mut saw_by_channel = false;
    for entry in &plan {
        match entry {
            WarmupEntry::DbIndex { table, descriptor } => {
                let t = table.to_string();
                let d = descriptor.to_string();
                if t == "todos" && d == "by_owner" {
                    saw_by_owner = true;
                } else if t == "todos" && d == "by_owner_done" {
                    saw_by_owner_done = true;
                } else if t == "messages" && d == "by_channel" {
                    saw_by_channel = true;
                }
            },
            WarmupEntry::TextIndex { .. } | WarmupEntry::VectorIndex { .. } => {
                panic!("fixture doesn't declare text/vector indexes; got {entry:?}");
            },
        }
    }
    assert!(saw_by_owner, "todos.by_owner missing from {plan:?}");
    assert!(
        saw_by_owner_done,
        "todos.by_owner_done missing from {plan:?}",
    );
    assert!(saw_by_channel, "messages.by_channel missing from {plan:?}");
    Ok(())
}

#[test]
fn diff_against_self_is_empty() -> anyhow::Result<()> {
    let schema = NativeSchema::collect()?;
    let changes = diff_schemas(&schema, &schema);
    assert!(
        changes.is_empty(),
        "a schema diffed against itself has no changes; got {changes:?}",
    );
    Ok(())
}

#[test]
fn diff_against_empty_reports_every_table_as_added() -> anyhow::Result<()> {
    use std::collections::BTreeMap;

    use common::schemas::DatabaseSchema;
    let empty = DatabaseSchema {
        tables: BTreeMap::new(),
        schema_validation: true,
    };
    let current = NativeSchema::collect()?;
    let changes = diff_schemas(&empty, &current);
    // At least two `TableAdded` entries: `todos` and `messages`.
    let added: Vec<_> = changes
        .iter()
        .filter_map(|c| match c {
            SchemaChange::TableAdded(t) => Some(t.to_string()),
            _ => None,
        })
        .collect();
    assert!(
        added.iter().any(|t| t == "todos"),
        "expected todos in TableAdded; got {added:?}",
    );
    assert!(
        added.iter().any(|t| t == "messages"),
        "expected messages in TableAdded; got {added:?}",
    );
    // None of these should be destructive.
    assert!(
        changes.iter().all(|c| !c.is_destructive()),
        "adding tables is non-destructive; got {changes:?}",
    );
    Ok(())
}

#[test]
fn diff_reports_table_removed_as_destructive() -> anyhow::Result<()> {
    // Symmetric to diff_against_empty_reports_every_table_as_added:
    // diffing the fixture schema → empty reports `TableRemoved`
    // entries that `is_destructive()` flags as dangerous. Real
    // migrations consult that flag to refuse accidental drops.
    use std::collections::BTreeMap;

    use common::schemas::DatabaseSchema;
    let empty = DatabaseSchema {
        tables: BTreeMap::new(),
        schema_validation: true,
    };
    let current = NativeSchema::collect()?;
    let changes = diff_schemas(&current, &empty);
    let removed: Vec<_> = changes
        .iter()
        .filter_map(|c| match c {
            SchemaChange::TableRemoved(t) => Some(t.to_string()),
            _ => None,
        })
        .collect();
    assert!(
        removed.iter().any(|t| t == "todos"),
        "expected todos in TableRemoved; got {removed:?}",
    );
    assert!(
        removed.iter().any(|t| t == "messages"),
        "expected messages in TableRemoved; got {removed:?}",
    );
    assert!(
        changes.iter().any(|c| c.is_destructive()),
        "at least one TableRemoved must be flagged destructive; got {changes:?}",
    );
    Ok(())
}
