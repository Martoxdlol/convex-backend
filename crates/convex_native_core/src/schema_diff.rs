//! Schema-to-schema diff for migration planning.
//!
//! Compare two `DatabaseSchema` values (typically the currently-deployed
//! one vs the one the current binary's `#[derive(ConvexDocument)]` set
//! would produce) and emit a list of [`SchemaChange`]s. Intended use is
//! dev-time tooling: dump the diff, flag additive vs destructive
//! changes, feed it into a migration generator.
//!
//! This module is purely descriptive — it doesn't apply anything.

use std::collections::BTreeMap;

use common::{
    schemas::{
        DatabaseSchema,
        IndexSchema,
        TableDefinition,
    },
    types::IndexDescriptor,
};
use value::TableName;

/// One unit of change between two schemas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChange {
    /// Table present in the new schema but not in the old.
    TableAdded(TableName),
    /// Table present in the old schema but not in the new. Flag
    /// destructive: dropping a table means dropping its data.
    TableRemoved(TableName),
    /// Index present only in the new schema.
    IndexAdded {
        table: TableName,
        index: IndexDescriptor,
    },
    /// Index present only in the old schema.
    IndexRemoved {
        table: TableName,
        index: IndexDescriptor,
    },
    /// Same index descriptor, different field list.
    IndexFieldsChanged {
        table: TableName,
        index: IndexDescriptor,
        old_fields: Vec<String>,
        new_fields: Vec<String>,
    },
}

impl SchemaChange {
    /// `true` for changes that may drop data or break existing queries.
    /// Useful for CI gating: "no destructive migrations without explicit
    /// approval".
    pub fn is_destructive(&self) -> bool {
        matches!(
            self,
            SchemaChange::TableRemoved(_)
                | SchemaChange::IndexRemoved { .. }
                | SchemaChange::IndexFieldsChanged { .. }
        )
    }
}

/// Produce a diff from `old` to `new`. The ordering of returned
/// changes follows `BTreeMap` iteration so it's stable.
pub fn diff(old: &DatabaseSchema, new: &DatabaseSchema) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    let old_tables: &BTreeMap<TableName, TableDefinition> = &old.tables;
    let new_tables: &BTreeMap<TableName, TableDefinition> = &new.tables;

    // Tables only in the new schema (additions).
    for name in new_tables.keys() {
        if !old_tables.contains_key(name) {
            changes.push(SchemaChange::TableAdded(name.clone()));
        }
    }

    // Tables only in the old schema (removals).
    for name in old_tables.keys() {
        if !new_tables.contains_key(name) {
            changes.push(SchemaChange::TableRemoved(name.clone()));
        }
    }

    // Tables in both: compare indexes.
    for (name, new_def) in new_tables {
        if let Some(old_def) = old_tables.get(name) {
            diff_indexes(name, &old_def.indexes, &new_def.indexes, &mut changes);
        }
    }

    changes
}

fn diff_indexes(
    table: &TableName,
    old: &BTreeMap<IndexDescriptor, IndexSchema>,
    new: &BTreeMap<IndexDescriptor, IndexSchema>,
    out: &mut Vec<SchemaChange>,
) {
    // Additions.
    for desc in new.keys() {
        if !old.contains_key(desc) {
            out.push(SchemaChange::IndexAdded {
                table: table.clone(),
                index: desc.clone(),
            });
        }
    }
    // Removals.
    for desc in old.keys() {
        if !new.contains_key(desc) {
            out.push(SchemaChange::IndexRemoved {
                table: table.clone(),
                index: desc.clone(),
            });
        }
    }
    // Field-set changes.
    for (desc, new_schema) in new {
        if let Some(old_schema) = old.get(desc) {
            let old_fields: Vec<String> =
                old_schema.fields.iter().map(|f| format!("{f}")).collect();
            let new_fields: Vec<String> =
                new_schema.fields.iter().map(|f| format!("{f}")).collect();
            if old_fields != new_fields {
                out.push(SchemaChange::IndexFieldsChanged {
                    table: table.clone(),
                    index: desc.clone(),
                    old_fields,
                    new_fields,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_schema() -> DatabaseSchema {
        DatabaseSchema {
            tables: BTreeMap::new(),
            schema_validation: true,
        }
    }

    #[test]
    fn diff_of_empty_schemas_is_empty() {
        let changes = diff(&empty_schema(), &empty_schema());
        assert!(changes.is_empty());
    }

    #[test]
    fn added_and_removed_tables_are_flagged() {
        let mut old = empty_schema();
        let mut new = empty_schema();
        // "gone" only in old, "fresh" only in new.
        let gone: TableName = "gone".parse().unwrap();
        let fresh: TableName = "fresh".parse().unwrap();
        old.tables.insert(gone.clone(), empty_table(gone.clone()));
        new.tables.insert(fresh.clone(), empty_table(fresh.clone()));
        let changes = diff(&old, &new);
        assert!(changes.contains(&SchemaChange::TableAdded(fresh)));
        assert!(changes.contains(&SchemaChange::TableRemoved(gone)));
        assert!(changes.iter().any(|c| c.is_destructive()));
    }

    fn empty_table(name: TableName) -> TableDefinition {
        TableDefinition {
            table_name: name,
            indexes: BTreeMap::new(),
            staged_db_indexes: BTreeMap::new(),
            text_indexes: BTreeMap::new(),
            staged_text_indexes: BTreeMap::new(),
            vector_indexes: BTreeMap::new(),
            staged_vector_indexes: BTreeMap::new(),
            document_type: None,
        }
    }

    fn td(s: &str) -> TableName {
        s.parse().unwrap()
    }

    fn idx(s: &str) -> IndexDescriptor {
        IndexDescriptor::new(s.to_string()).unwrap()
    }

    #[test]
    fn is_destructive_classifies_every_variant() {
        // TableAdded + IndexAdded are additive. TableRemoved,
        // IndexRemoved, IndexFieldsChanged are destructive (drop
        // data or break existing queries). CI can gate on this
        // predicate — a silent reclassification (e.g. marking
        // IndexFieldsChanged as non-destructive) would let
        // data-losing migrations sneak through.
        assert!(!SchemaChange::TableAdded(td("users")).is_destructive());
        assert!(!SchemaChange::IndexAdded {
            table: td("users"),
            index: idx("by_email"),
        }
        .is_destructive());
        assert!(SchemaChange::TableRemoved(td("users")).is_destructive());
        assert!(SchemaChange::IndexRemoved {
            table: td("users"),
            index: idx("by_email"),
        }
        .is_destructive());
        assert!(SchemaChange::IndexFieldsChanged {
            table: td("users"),
            index: idx("by_email"),
            old_fields: vec!["email".into()],
            new_fields: vec!["normalized_email".into()],
        }
        .is_destructive());
    }

    #[test]
    fn diff_is_empty_when_schemas_are_identical() {
        // Two schemas with the same single table + no indexes should
        // produce no changes. Rules out spurious table-touch events
        // for a refactor that changes nothing.
        let mut a = empty_schema();
        let mut b = empty_schema();
        let users = td("users");
        a.tables.insert(users.clone(), empty_table(users.clone()));
        b.tables.insert(users.clone(), empty_table(users));
        assert!(diff(&a, &b).is_empty());
    }

    #[test]
    fn diff_ignores_tables_that_exist_in_both_with_same_indexes() {
        // Presence-in-both should only emit index-level changes, not
        // any table-level event. If `diff()` ever regressed to
        // emitting a synthetic `TableAdded` for matched tables, this
        // test flips.
        let mut a = empty_schema();
        let mut b = empty_schema();
        let t = td("posts");
        a.tables.insert(t.clone(), empty_table(t.clone()));
        b.tables.insert(t.clone(), empty_table(t.clone()));
        let changes = diff(&a, &b);
        for change in &changes {
            assert!(
                !matches!(
                    change,
                    SchemaChange::TableAdded(_) | SchemaChange::TableRemoved(_),
                ),
                "no table-level event for identical tables: {change:?}",
            );
        }
    }

    #[test]
    fn diff_emits_stable_ordering_via_btreemap_iteration() {
        // The docstring promises stable ordering via BTreeMap
        // iteration. Two runs of `diff()` on the same inputs must
        // produce the same sequence — if the implementation ever
        // switches to a HashMap-backed pass, iteration order would
        // drift across compilations and this test would catch it.
        let mut old = empty_schema();
        let mut new = empty_schema();
        for t in ["alpha", "beta", "gamma"] {
            old.tables.insert(td(t), empty_table(td(t)));
        }
        for t in ["beta", "gamma", "delta"] {
            new.tables.insert(td(t), empty_table(td(t)));
        }
        let first = diff(&old, &new);
        let second = diff(&old, &new);
        assert_eq!(first, second, "diff ordering is deterministic");
        // And both snapshots should agree on which tables are added
        // / removed — specifically "alpha" gone, "delta" fresh.
        assert!(first.contains(&SchemaChange::TableRemoved(td("alpha"))));
        assert!(first.contains(&SchemaChange::TableAdded(td("delta"))));
    }
}
