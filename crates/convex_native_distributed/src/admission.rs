//! Phase 3 worker admission protocol — native-side helpers.
//!
//! Substep 3.2 of `convex-native/STATUS.md`. This module builds a
//! `pb::worker_admission::FunctionInventory` from the
//! `inventory::submit!`-collected native registries in
//! `convex_native` (functions, schema, HTTP routes, crons) and
//! computes the canonical SHA-256 the registration envelope
//! carries so workers on the same `registry_version` can be
//! enforced identical.
//!
//! Further Phase-3 work (admission server, worker-side
//! registration loop, dynamic `WorkerPool`) lands in sibling
//! modules that consume the types defined here.

use common::{
    schemas::{
        json::DatabaseSchemaJson,
        DatabaseSchema,
    },
    types::UdfType,
};
use convex_native::{
    cron::CronRegistry,
    http::HttpRouter,
    registry::NativeFunctionRegistry,
    schema::NativeSchema,
};
use pb::{
    common as pb_common,
    worker_admission as proto,
};
use sha2::{
    Digest,
    Sha256,
};

/// Collect every native registry exposed by the `convex_native`
/// framework and assemble the worker's `FunctionInventory` proto
/// plus a SHA-256 canonicalisation hash.
///
/// The hash is over the proto's serialised bytes (prost's
/// deterministic serialisation), not the developer's source —
/// so a no-op formatter change on the worker's source doesn't
/// bump the hash and trigger a needless rolling-update.
///
/// Called from the worker binary's startup path once
/// `NativeFunctionRunner::from_inventory()` has already
/// validated the registrations for uniqueness.
pub fn collect_inventory() -> anyhow::Result<(proto::FunctionInventory, [u8; 32])> {
    let functions = collect_functions()?;
    let schema_bytes = collect_schema_bytes()?;
    let routes = collect_routes()?;
    let crons = collect_crons()?;
    let inventory = proto::FunctionInventory {
        functions,
        schema: Some(proto::DatabaseSchema {
            schema_json: schema_bytes,
        }),
        routes,
        crons,
    };
    let hash = canonical_sha256(&inventory);
    Ok((inventory, hash))
}

/// Canonical hash over the full `FunctionInventory`. Uses prost's
/// serialisation which is **not** guaranteed canonical for maps
/// but is deterministic here because every field is a sorted
/// repeated list, not a map. If a map ever enters the proto,
/// switch to a hand-written canonical encoder.
pub fn canonical_sha256(inventory: &proto::FunctionInventory) -> [u8; 32] {
    use prost::Message;
    let mut buf = Vec::with_capacity(inventory.encoded_len());
    inventory
        .encode(&mut buf)
        .expect("prost encoding is infallible");
    let digest = Sha256::digest(&buf);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_slice());
    out
}

fn collect_functions() -> anyhow::Result<Vec<proto::FunctionRegistration>> {
    let registry = NativeFunctionRegistry::collect()?;
    let mut entries: Vec<proto::FunctionRegistration> = registry
        .iter()
        .map(|r| proto::FunctionRegistration {
            name: r.name.to_string(),
            udf_type: udf_type_to_i32(r.udf_type()),
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

fn collect_schema_bytes() -> anyhow::Result<Vec<u8>> {
    let schema: DatabaseSchema = NativeSchema::collect()?;
    let json: DatabaseSchemaJson = DatabaseSchemaJson::try_from(schema)?;
    Ok(serde_json::to_vec(&json)?)
}

fn collect_routes() -> anyhow::Result<Vec<proto::HttpRouteRegistration>> {
    let router = HttpRouter::collect()?;
    let mut entries: Vec<proto::HttpRouteRegistration> = router
        .iter()
        .map(|r| proto::HttpRouteRegistration {
            method: r.method.to_string(),
            path: r.path.to_string(),
            handler: r.name.to_string(),
        })
        .collect();
    entries.sort_by(|a, b| (&a.method, &a.path).cmp(&(&b.method, &b.path)));
    Ok(entries)
}

fn collect_crons() -> anyhow::Result<Vec<proto::CronRegistration>> {
    let registry = CronRegistry::collect()?;
    let mut entries: Vec<proto::CronRegistration> = registry
        .iter()
        .map(|c| proto::CronRegistration {
            name: c.name.to_string(),
            schedule: c.schedule.to_string(),
            handler: c.target.to_string(),
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

fn udf_type_to_i32(t: UdfType) -> i32 {
    match t {
        UdfType::Query => pb_common::UdfType::Query as i32,
        UdfType::Mutation => pb_common::UdfType::Mutation as i32,
        UdfType::Action => pb_common::UdfType::Action as i32,
        UdfType::HttpAction => pb_common::UdfType::HttpAction as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_worker_inventory_is_stable() {
        // Running the test binary doesn't register any
        // `#[convex::...]` handlers, so the inventory is empty —
        // every field is a zero-length repeated list and the
        // schema_json is the serialised empty DatabaseSchema.
        // Pin that the hash is stable across two successive
        // collections; any non-determinism in `collect_inventory`
        // would show up here.
        let (inv_a, hash_a) = collect_inventory().unwrap();
        let (inv_b, hash_b) = collect_inventory().unwrap();
        assert_eq!(hash_a, hash_b);
        assert_eq!(inv_a.functions, inv_b.functions);
        assert_eq!(inv_a.routes, inv_b.routes);
        assert_eq!(inv_a.crons, inv_b.crons);
        assert_eq!(
            inv_a.schema.as_ref().map(|s| &s.schema_json),
            inv_b.schema.as_ref().map(|s| &s.schema_json),
        );
    }

    #[test]
    fn empty_inventory_has_empty_registries() {
        let (inv, _) = collect_inventory().unwrap();
        assert!(inv.functions.is_empty());
        assert!(inv.routes.is_empty());
        assert!(inv.crons.is_empty());
        // Schema is always present (even when empty) — it's the
        // serialised form of an empty DatabaseSchema.
        let schema_json = inv.schema.expect("schema always present").schema_json;
        let json_str = std::str::from_utf8(&schema_json).unwrap();
        let json_value: serde_json::Value = serde_json::from_str(json_str).unwrap();
        let tables = json_value.get("tables").and_then(|v| v.as_array()).unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn canonical_sha256_is_stable_across_calls() {
        // `canonical_sha256` must be deterministic — same input
        // bytes → same output bytes. Guards against a future
        // edit accidentally hashing a non-canonical form.
        let inv = proto::FunctionInventory::default();
        assert_eq!(canonical_sha256(&inv), canonical_sha256(&inv));
    }

    #[test]
    fn canonical_sha256_changes_with_content() {
        let mut inv = proto::FunctionInventory::default();
        let before = canonical_sha256(&inv);
        inv.functions.push(proto::FunctionRegistration {
            name: "new_fn".to_string(),
            udf_type: pb_common::UdfType::Query as i32,
        });
        let after = canonical_sha256(&inv);
        assert_ne!(before, after);
    }
}
