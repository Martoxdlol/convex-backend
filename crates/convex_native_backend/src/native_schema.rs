//! Publish the native schema (and its indexes) at backend boot.
//!
//! Pure-native deployments never run `npx convex dev`, so
//! `Application::apply_config` — the normal path that writes
//! `_schemas` + `_indexes` rows — is never called. The HTTP
//! validation fix in `convex_native_backend::native_resolver`
//! lets client traffic reach the handlers, but native queries
//! that rely on indexes (e.g. `queries:list_for_owner` using
//! `todos.by_owner`) then fail downstream with
//! "Index todos.by_owner not found" because the index was
//! declared via `#[convex(index(...))]` but never installed
//! into the database.
//!
//! [`publish_native_schema`] closes that gap. At boot it:
//!
//! 1. Calls `convex_native_core::NativeSchema::collect()` to assemble the
//!    inventory-declared `DatabaseSchema`.
//! 2. If the schema is empty (JS-only deployment, or native deployment with no
//!    `#[derive(ConvexDocument)]`), returns early.
//! 3. Compares the collected schema to the current Active schema in the root
//!    component. If identical, returns early — the publish is idempotent across
//!    restarts.
//! 4. Otherwise, opens a system transaction, calls
//!    `IndexModel::prepare_new_and_mutated_indexes` +
//!    `SchemaModel::submit_pending`, and commits. The `SchemaWorker` running
//!    in-process then validates + activates the schema, which enables the
//!    indexes.
//!
//! This mirrors the work `prepare_schema_handler` in
//! `local_backend::schema` does on the JS push path — it just
//! runs automatically at boot against the native registry
//! instead of needing an HTTP admin call.

use std::{
    sync::Arc,
    time::Duration,
};

use common::{
    bootstrap_model::{
        index::IndexMetadata,
        schema::SchemaState,
    },
    document::ParsedDocument,
    runtime::Runtime,
    schemas::DatabaseSchema,
};
use convex_native_core::schema::NativeSchema;
use database::{
    Database,
    IndexModel,
    SchemaModel,
};
use value::{
    ResolvedDocumentId,
    TableName,
    TableNamespace,
};

/// Publish the native schema into the root component's `_schemas`
/// table. Idempotent — returns `false` when nothing was written
/// (empty native schema or the active schema already matches),
/// `true` when a new pending schema was submitted.
///
/// Errors are system-level (e.g. commit failures, tx failures) —
/// a missing-index application error is a caller concern.
pub async fn publish_native_schema<RT: Runtime>(database: &Database<RT>) -> anyhow::Result<bool> {
    let schema = NativeSchema::collect()?;
    publish_schema(database, schema).await
}

/// Publish an explicitly-provided `DatabaseSchema`. Used by the
/// admission server under the distributed topology to mirror a
/// worker's schema into the backend's `Database<RT>` (which owns
/// the committer + in-memory tablet registry). Without this the
/// backend can't commit writes the worker reports in its
/// `FinalTxSummary` — the tablet ids referenced by the summary
/// are unknown to the backend's `IndexRegistry` and the commit
/// bails with "Missing `by_id` index for table …".
pub async fn publish_schema<RT: Runtime>(
    database: &Database<RT>,
    schema: DatabaseSchema,
) -> anyhow::Result<bool> {
    if schema.tables.is_empty() {
        return Ok(false);
    }
    let namespace = TableNamespace::root_component();

    // Cheap check: if the active schema already matches byte-for-byte
    // we've nothing to do. Running `submit_pending` in that case is
    // also a no-op (it short-circuits on equality — see
    // `SchemaModel::submit_pending` at
    // `crates/database/src/bootstrap_model/schema/mod.rs:222`), but
    // skipping the extra transaction keeps boot clean.
    {
        let mut tx = database.begin_system().await?;
        if is_active_schema_equal(&mut tx, namespace, &schema).await? {
            return Ok(false);
        }
    }

    let mut tx = database.begin_system().await?;
    IndexModel::new(&mut tx)
        .prepare_new_and_mutated_indexes(namespace, &schema)
        .await?;
    let (schema_id, state) = SchemaModel::new(&mut tx, namespace)
        .submit_pending(schema.clone())
        .await?;
    database
        .commit_with_write_source(tx, "publish_native_schema")
        .await?;
    tracing::info!(
        "Published native schema with {} table(s) (state: {:?})",
        schema.tables.len(),
        state,
    );
    // Block boot until the schema is Active and every index has
    // finished backfilling. Accepting HTTP traffic before indexes
    // are enabled would give clients "index X is currently
    // backfilling and not available to query yet" errors — a
    // deployer reading that would reasonably think the deployment
    // is broken. Blocking here turns that race into a slower boot
    // (milliseconds on a fresh DB; longer only if there's data to
    // reindex). See
    // `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md` follow-ups.
    activate_when_ready(database, namespace, schema_id, schema).await?;
    Ok(true)
}

async fn activate_when_ready<RT: Runtime>(
    database: &Database<RT>,
    namespace: TableNamespace,
    schema_id: ResolvedDocumentId,
    schema: DatabaseSchema,
) -> anyhow::Result<()> {
    // Budget: 60s for validation + index backfill on a fresh boot.
    // Empty tables finish within a few ms in practice; the budget is
    // generous so a slow CI disk doesn't time us out.
    const POLL_INTERVAL: Duration = Duration::from_millis(200);
    const TIMEOUT: Duration = Duration::from_secs(60);
    let started = std::time::Instant::now();

    loop {
        if started.elapsed() > TIMEOUT {
            anyhow::bail!(
                "native schema activation timed out after {:?} waiting for pending → validated + \
                 indexes to finish backfilling",
                TIMEOUT,
            );
        }

        let ready = {
            let mut tx = database.begin_system().await?;
            let schema_state = SchemaModel::new(&mut tx, namespace)
                .get_by_state(SchemaState::Validated)
                .await?;
            let active_state = SchemaModel::new(&mut tx, namespace)
                .get_by_state(SchemaState::Active)
                .await?;
            // "Ready" = either the pending we submitted has moved to
            // Validated/Active, AND every index in that schema is no
            // longer backfilling.
            let schema_in_terminal_state = schema_state
                .as_ref()
                .map(|(id, _)| *id == schema_id)
                .unwrap_or(false)
                || active_state
                    .as_ref()
                    .map(|(id, _)| *id == schema_id)
                    .unwrap_or(false);

            let all_indexes_backfilled = if schema_in_terminal_state {
                let indexes = IndexModel::new(&mut tx)
                    .get_application_indexes(namespace)
                    .await?;
                !indexes.iter().any(is_still_backfilling)
            } else {
                false
            };

            schema_in_terminal_state && all_indexes_backfilled
        };

        if ready {
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    // If the schema is already Active (idempotent re-boot with
    // unchanged schema), the `submit_pending` at the caller already
    // short-circuited; we shouldn't get here. Safe to double-check.
    let mut tx = database.begin_system().await?;
    let already_active = SchemaModel::new(&mut tx, namespace)
        .get_by_state(SchemaState::Active)
        .await?
        .map(|(id, stored)| id == schema_id && *stored == schema)
        .unwrap_or(false);
    if already_active {
        return Ok(());
    }
    drop(tx);

    let mut tx = database.begin_system().await?;
    let (_schema_diff, next_schema) = SchemaModel::new(&mut tx, namespace)
        .apply(Some(schema_id))
        .await?;
    IndexModel::new(&mut tx)
        .apply(namespace, &next_schema)
        .await?;
    database
        .commit_with_write_source(tx, "activate_native_schema")
        .await?;
    tracing::info!("Activated native schema; indexes enabled");
    Ok(())
}

fn is_still_backfilling(doc: &ParsedDocument<IndexMetadata<TableName>>) -> bool {
    doc.config.is_backfilling()
}

async fn is_active_schema_equal<RT: Runtime>(
    tx: &mut database::Transaction<RT>,
    namespace: TableNamespace,
    schema: &DatabaseSchema,
) -> anyhow::Result<bool> {
    let existing: Option<(_, Arc<DatabaseSchema>)> = SchemaModel::new(tx, namespace)
        .get_by_state(SchemaState::Active)
        .await?;
    match existing {
        Some((_, active)) => Ok(&*active == schema),
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use convex_native_core::schema::NativeSchema;

    #[test]
    fn empty_inventory_collect() {
        // No `#[derive(ConvexDocument)]`s in this crate's test
        // binary, so `NativeSchema::collect` should return an
        // empty schema and the publisher's early-return path
        // would trigger. Pin the shape here so a future accident
        // (e.g. somebody adding a derive to this crate) fails
        // loud instead of changing boot semantics silently.
        let schema = NativeSchema::collect().expect("collect");
        assert!(schema.tables.is_empty());
    }
}
