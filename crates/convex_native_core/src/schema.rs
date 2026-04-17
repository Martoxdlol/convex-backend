//! Schema collection via `inventory`.
//!
//! Each `#[derive(ConvexDocument)]` emits one `inventory::submit!` of a
//! [`TableRegistration`]. At runtime the backend calls
//! [`NativeSchema::collect`] to gather every registered table into a
//! [`DatabaseSchema`].
//!
//! Why `inventory`: we want developers to declare their schema simply by
//! deriving the trait, without running any `fn main()`-level registration
//! boilerplate. `inventory` uses platform-specific linker sections to
//! collect the registrations — the collected set is fully populated by the
//! time `main` runs.

use std::collections::BTreeMap;

use common::schemas::{
    DatabaseSchema,
    TableDefinition,
};
use value::TableName;

/// One entry collected per `#[derive(ConvexDocument)]` type.
///
/// The constructor fn pointer defers the schema build until collection time,
/// which keeps the cost of the `inventory::submit!` macro itself trivial.
pub struct TableRegistration {
    /// Static table name (pre-parsed at collect time).
    pub table_name: &'static str,
    /// Produces the full `TableDefinition` for this table.
    pub build: fn() -> TableDefinition,
}

inventory::collect!(TableRegistration);

/// Entry point for assembling the registered schema.
pub struct NativeSchema;

impl NativeSchema {
    /// Walk every registered `TableRegistration` and build a
    /// [`DatabaseSchema`].
    ///
    /// When no tables are registered, returns an empty schema with
    /// validation enabled.
    pub fn collect() -> anyhow::Result<DatabaseSchema> {
        let mut tables: BTreeMap<TableName, TableDefinition> = BTreeMap::new();
        for registration in inventory::iter::<TableRegistration> {
            let definition = (registration.build)();
            let name: TableName = registration.table_name.parse()?;
            if tables.insert(name.clone(), definition).is_some() {
                anyhow::bail!(
                    "duplicate table registration for {}",
                    registration.table_name
                );
            }
        }
        Ok(DatabaseSchema {
            tables,
            schema_validation: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_schema() {
        let schema = NativeSchema::collect().expect("collect");
        // Running in isolation: no derives in this crate, so schema is empty.
        assert!(schema.tables.is_empty());
        assert!(schema.schema_validation);
    }
}
