//! Startup warm-up surface.
//!
//! The backend adapter calls [`plan_warmup`] at startup to get a list
//! of every (table, index) pair the native schema declared. It then
//! loads each one into its in-memory index cache so the first request
//! after startup doesn't pay cold-cache latency. We return a pure
//! data structure — executing the warm-up belongs to the adapter.

use common::{
    schemas::DatabaseSchema,
    types::IndexDescriptor,
};
use value::TableName;

/// One entry per (table, index) pair the schema declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarmupEntry {
    /// Standard database index.
    DbIndex {
        table: TableName,
        descriptor: IndexDescriptor,
    },
    /// Text (search) index.
    TextIndex {
        table: TableName,
        descriptor: IndexDescriptor,
    },
    /// Vector index.
    VectorIndex {
        table: TableName,
        descriptor: IndexDescriptor,
    },
}

/// Produce a warm-up plan from a schema. Ordering is stable so the
/// adapter can parallelize across chunks deterministically.
pub fn plan_warmup(schema: &DatabaseSchema) -> Vec<WarmupEntry> {
    let mut out = Vec::new();
    for (table_name, table_def) in &schema.tables {
        for descriptor in table_def.indexes.keys() {
            out.push(WarmupEntry::DbIndex {
                table: table_name.clone(),
                descriptor: descriptor.clone(),
            });
        }
        for descriptor in table_def.text_indexes.keys() {
            out.push(WarmupEntry::TextIndex {
                table: table_name.clone(),
                descriptor: descriptor.clone(),
            });
        }
        for descriptor in table_def.vector_indexes.keys() {
            out.push(WarmupEntry::VectorIndex {
                table: table_name.clone(),
                descriptor: descriptor.clone(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use common::schemas::{
        DatabaseSchema,
        TableDefinition,
    };

    use super::*;

    #[test]
    fn empty_schema_produces_empty_plan() {
        let schema = DatabaseSchema {
            tables: BTreeMap::new(),
            schema_validation: true,
        };
        assert!(plan_warmup(&schema).is_empty());
    }

    #[test]
    fn empty_table_produces_empty_plan() {
        let mut schema = DatabaseSchema {
            tables: BTreeMap::new(),
            schema_validation: true,
        };
        schema
            .tables
            .insert("t".parse::<TableName>().unwrap(), empty_table("t"));
        assert!(plan_warmup(&schema).is_empty());
    }

    fn empty_table(name: &str) -> TableDefinition {
        TableDefinition {
            table_name: name.parse().unwrap(),
            indexes: BTreeMap::new(),
            staged_db_indexes: BTreeMap::new(),
            text_indexes: BTreeMap::new(),
            staged_text_indexes: BTreeMap::new(),
            vector_indexes: BTreeMap::new(),
            staged_vector_indexes: BTreeMap::new(),
            document_type: None,
        }
    }

    fn idx(s: &str) -> IndexDescriptor {
        IndexDescriptor::new(s.to_string()).unwrap()
    }

    fn single_field_db_index(name: &str, field: &str) -> common::schemas::IndexSchema {
        let descriptor = idx(name);
        let fp: common::paths::FieldPath = field.parse().unwrap();
        let fields: common::bootstrap_model::index::database_index::IndexedFields =
            vec![fp].try_into().unwrap();
        common::schemas::IndexSchema {
            index_descriptor: descriptor,
            fields,
        }
    }

    #[test]
    fn db_indexes_land_in_the_plan_as_db_entries() {
        // Register one db index on a table and confirm `plan_warmup`
        // emits exactly one `WarmupEntry::DbIndex` with the matching
        // table + descriptor.
        let mut schema = DatabaseSchema {
            tables: BTreeMap::new(),
            schema_validation: true,
        };
        let table: TableName = "users".parse().unwrap();
        let mut def = empty_table("users");
        def.indexes
            .insert(idx("by_email"), single_field_db_index("by_email", "email"));
        schema.tables.insert(table.clone(), def);

        let plan = plan_warmup(&schema);
        assert_eq!(plan.len(), 1);
        assert!(
            matches!(
                &plan[0],
                WarmupEntry::DbIndex { table: t, descriptor: d }
                    if t == &table && d.as_str() == "by_email",
            ),
            "{:?}",
            plan[0],
        );
    }

    #[test]
    fn plan_ordering_is_stable_across_btreemap_iteration() {
        // `plan_warmup` should emit entries in a deterministic order
        // so the adapter's parallel chunking is reproducible. With
        // two tables (sorted a/b) and two indexes on one of them
        // (sorted by descriptor), we lock the expected sequence.
        let mut schema = DatabaseSchema {
            tables: BTreeMap::new(),
            schema_validation: true,
        };
        let t_a: TableName = "a".parse().unwrap();
        let t_b: TableName = "b".parse().unwrap();
        let mut def_a = empty_table("a");
        def_a
            .indexes
            .insert(idx("by_x"), single_field_db_index("by_x", "x"));
        def_a
            .indexes
            .insert(idx("by_y"), single_field_db_index("by_y", "y"));
        schema.tables.insert(t_a.clone(), def_a);
        schema.tables.insert(t_b.clone(), empty_table("b"));

        let plan = plan_warmup(&schema);
        // BTreeMap iteration sorts tables alphabetically, then
        // indexes alphabetically within each. `b` has no indexes so
        // only `a`'s two show up, in `by_x`, `by_y` order.
        assert_eq!(plan.len(), 2);
        assert!(
            matches!(&plan[0], WarmupEntry::DbIndex { descriptor, .. } if descriptor.as_str() == "by_x"),
        );
        assert!(
            matches!(&plan[1], WarmupEntry::DbIndex { descriptor, .. } if descriptor.as_str() == "by_y"),
        );
    }

    #[test]
    fn warmup_entry_equality_and_clone() {
        // `#[derive(Debug, Clone, PartialEq, Eq)]` is load-bearing —
        // the adapter dedupes entries and clones them into worker
        // tasks. Pin the derives against a silent drop by exercising
        // them through the equality check.
        let table: TableName = "t".parse().unwrap();
        let a = WarmupEntry::DbIndex {
            table: table.clone(),
            descriptor: idx("by_x"),
        };
        let b = a.clone();
        assert_eq!(a, b);
        let different = WarmupEntry::TextIndex {
            table,
            descriptor: idx("by_x"),
        };
        assert_ne!(a, different, "variant discriminates equality");
    }
}
