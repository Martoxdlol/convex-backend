//! Startup warm-up surface.
//!
//! Per `IMPLEMENTATION_PLAN.md` Phase 4 step 4.6.
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
        let table: TableName = "t".parse().unwrap();
        schema.tables.insert(
            table.clone(),
            TableDefinition {
                table_name: table,
                indexes: BTreeMap::new(),
                staged_db_indexes: BTreeMap::new(),
                text_indexes: BTreeMap::new(),
                staged_text_indexes: BTreeMap::new(),
                vector_indexes: BTreeMap::new(),
                staged_vector_indexes: BTreeMap::new(),
                document_type: None,
            },
        );
        assert!(plan_warmup(&schema).is_empty());
    }
}
