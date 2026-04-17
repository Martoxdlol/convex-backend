//! Conversions between `pb::function_execution::*` (wire format)
//! and the tonic-free types in `convex_native::distributed`.
//!
//! The goal is a small, testable boundary: anything that needs to
//! read the proto goes through a `from_proto_*` function here;
//! anything that needs to write it goes through a `to_proto_*`.
//! The gRPC server/client implementations (Phase 3.3/3.4) then
//! become straightforward glue.

use std::time::Duration;

use convex_native::distributed::{
    ExecuteRequest,
    ExecuteResponse,
    FinalTxSummary,
    IndexReadsSummary,
    TxReadSize,
};
use pb::function_execution as proto;
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

/// Wire-format name for `TableNamespace::Global`. The backend and
/// worker agree on this literal so the round-trip preserves root
/// component semantics.
const NAMESPACE_GLOBAL: &str = "global";

/// Encode a `TableNamespace` as the wire-format string the proto
/// carries. `Global` becomes `"global"`; component-scoped namespaces
/// serialize through the component id's string form.
pub fn encode_namespace(ns: TableNamespace) -> String {
    match ns {
        TableNamespace::Global => NAMESPACE_GLOBAL.to_string(),
        TableNamespace::ByComponent(id) => format!("component:{id}"),
    }
}

/// Inverse of `encode_namespace`. Unknown/malformed strings error
/// rather than silently falling back, so a backend/worker version
/// skew surfaces loudly.
pub fn decode_namespace(s: &str) -> anyhow::Result<TableNamespace> {
    if s == NAMESPACE_GLOBAL {
        return Ok(TableNamespace::Global);
    }
    if let Some(rest) = s.strip_prefix("component:") {
        let id = rest
            .parse()
            .map_err(|e| anyhow::anyhow!("namespace {s:?}: malformed component id: {e}"))?;
        return Ok(TableNamespace::ByComponent(id));
    }
    anyhow::bail!("namespace {s:?}: expected \"global\" or \"component:<id>\"")
}

/// Serialize a `ConvexObject` as the single-object JSON blob the
/// proto carries in `args_json`. Native handlers always take one
/// object argument, so we encode it directly (no array wrapping —
/// that's the `SerializedArgs` shape used by the JS path).
pub fn encode_args(obj: &ConvexObject) -> anyhow::Result<Vec<u8>> {
    let v: ConvexValue = ConvexValue::Object(obj.clone());
    let json: serde_json::Value = v.into();
    Ok(serde_json::to_vec(&json)?)
}

/// Inverse of `encode_args`. Errors when the payload isn't a JSON
/// object (native handlers take one object; anything else is a
/// protocol bug).
pub fn decode_args(bytes: &[u8]) -> anyhow::Result<ConvexObject> {
    let json: serde_json::Value = serde_json::from_slice(bytes)?;
    let v: ConvexValue = json.try_into()?;
    match v {
        ConvexValue::Object(obj) => Ok(obj),
        other => anyhow::bail!(
            "ExecuteRequest.args_json must decode to a ConvexValue::Object, got {:?}",
            std::mem::discriminant(&other),
        ),
    }
}

/// Build a proto `ExecuteRequest` from the native shape plus the
/// `udf_type` (which is not carried on the native struct — it's
/// context the composite runner supplies).
pub fn to_proto_request(
    native: &ExecuteRequest,
    udf_type: common::types::UdfType,
) -> anyhow::Result<proto::ExecuteRequest> {
    // Substep 2.4: carry staged writes over the wire when the
    // backend has them. Empty vec → `existing_writes = None` so
    // the worker's "has the backend batched writes?" check is
    // cheap on the request-parse path.
    let existing_writes = if native.existing_writes.is_empty() {
        None
    } else {
        let updates: Vec<pb::common::DocumentUpdateWithPrevTs> = native
            .existing_writes
            .iter()
            .cloned()
            .map(pb::common::DocumentUpdateWithPrevTs::try_from)
            .collect::<anyhow::Result<_>>()?;
        Some(proto::ExistingWrites { updates })
    };
    Ok(proto::ExecuteRequest {
        name: native.name.clone(),
        udf_type: udf_type_to_i32(udf_type),
        namespace: encode_namespace(native.namespace),
        args_json: encode_args(&native.args)?,
        identity: None,
        timeout: native.timeout.map(duration_to_proto),
        execution_context: native.execution_context.clone().map(Into::into),
        min_registry_version: native.min_registry_version.clone(),
        begin_timestamp: native.begin_timestamp,
        existing_writes,
    })
}

/// Parse a proto `ExecuteRequest` into the native shape plus the
/// `UdfType` the worker should dispatch to. Identity isn't
/// reflected on the native type today — the worker reads that
/// directly off the proto — but `execution_context` is rebuilt
/// and carried through so request-id / execution-id / parent-job
/// chains span both processes.
pub fn from_proto_request(
    p: &proto::ExecuteRequest,
) -> anyhow::Result<(ExecuteRequest, common::types::UdfType)> {
    let udf_type = i32_to_udf_type(p.udf_type)?;
    let execution_context = p
        .execution_context
        .clone()
        .map(common::execution_context::ExecutionContext::try_from)
        .transpose()?;
    let existing_writes: Vec<common::document::DocumentUpdateWithPrevTs> = match &p.existing_writes
    {
        None => Vec::new(),
        Some(ew) => ew
            .updates
            .iter()
            .cloned()
            .map(common::document::DocumentUpdateWithPrevTs::try_from)
            .collect::<anyhow::Result<_>>()?,
    };
    let native = ExecuteRequest {
        name: p.name.clone(),
        namespace: decode_namespace(&p.namespace)?,
        args: decode_args(&p.args_json)?,
        timeout: p.timeout.as_ref().map(duration_from_proto),
        min_registry_version: p.min_registry_version.clone(),
        execution_context,
        begin_timestamp: p.begin_timestamp,
        existing_writes,
    };
    Ok((native, udf_type))
}

/// Encode an `ExecuteResponse` as a proto message. Errors on the
/// native side are stringified; the proto carries a
/// `common.FunctionResult` which is richer, but `ExecuteResponse`
/// itself already lossed the structure.
///
/// Returns a `Result` because the `final_tx` path encodes
/// `DocumentUpdateWithPrevTs` into its proto shape, which is
/// fallible (inherited from `common::document`'s conversion); see
/// substep 2.3. Well-formed worker transactions always round-trip
/// cleanly, so encoder failures indicate a code bug rather than a
/// user-level error — callers should propagate via `Status::internal`.
pub fn to_proto_response(native: &ExecuteResponse) -> anyhow::Result<proto::ExecuteResponse> {
    use pb::common;
    let result = match &native.result {
        Ok(v) => {
            let json: serde_json::Value = v.clone().into();
            common::FunctionResult {
                result: Some(common::function_result::Result::JsonPackedValue(
                    json.to_string(),
                )),
            }
        },
        Err(msg) => common::FunctionResult {
            result: Some(common::function_result::Result::JsError(common::JsError {
                message: Some(msg.clone()),
                custom_data: None,
                frames: None,
            })),
        },
    };
    let final_tx = native.final_tx.clone().map(final_tx_to_proto).transpose()?;
    Ok(proto::ExecuteResponse {
        result: Some(result),
        user_execution_time: None,
        served_by_version: None,
        log_lines: native.log_lines.clone(),
        final_tx,
    })
}

/// Decode a proto `ExecuteResponse` into the native shape. Missing
/// `result` is an error.
pub fn from_proto_response(p: &proto::ExecuteResponse) -> anyhow::Result<ExecuteResponse> {
    use pb::common::function_result::Result as R;
    let inner = p
        .result
        .as_ref()
        .and_then(|r| r.result.as_ref())
        .ok_or_else(|| anyhow::anyhow!("ExecuteResponse missing result"))?;
    let result = match inner {
        R::JsonPackedValue(s) => {
            let json: serde_json::Value = serde_json::from_str(s)?;
            let v: ConvexValue = json.try_into()?;
            Ok(v)
        },
        R::JsError(e) => Err(e.message.clone().unwrap_or_default()),
    };
    let final_tx = p.final_tx.as_ref().map(final_tx_from_proto).transpose()?;
    Ok(ExecuteResponse {
        result,
        log_lines: p.log_lines.clone(),
        final_tx,
    })
}

/// Encode a native `FinalTxSummary` as the wire message. Substep
/// 2.1 added the rows_read_by_tablet map; substep 2.3 added the
/// `writes` list (document updates) via the existing
/// `common.DocumentUpdateWithPrevTs` proto. Substep 2.2 (full
/// `FunctionReads` content) is still pending.
pub fn final_tx_to_proto(summary: FinalTxSummary) -> anyhow::Result<proto::DistributedFinalTx> {
    let writes: Vec<pb::common::DocumentUpdateWithPrevTs> = summary
        .writes
        .into_iter()
        .map(pb::common::DocumentUpdateWithPrevTs::try_from)
        .collect::<anyhow::Result<_>>()?;
    let index_reads: Vec<proto::DistributedIndexReads> = summary
        .index_reads
        .into_iter()
        .map(index_reads_to_proto)
        .collect();
    Ok(proto::DistributedFinalTx {
        begin_timestamp: summary.begin_timestamp,
        writes_count: summary.writes_count,
        reads_count: summary.reads_count,
        rows_read_by_tablet: summary.rows_read_by_tablet.into_iter().collect(),
        writes,
        user_tx_size: summary.user_tx_size.map(tx_read_size_to_proto),
        system_tx_size: summary.system_tx_size.map(tx_read_size_to_proto),
        index_reads,
    })
}

fn index_reads_to_proto(native: IndexReadsSummary) -> proto::DistributedIndexReads {
    let IndexReadsSummary {
        index_name,
        fields,
        intervals,
    } = native;
    let tablet_id = index_name.table().to_string();
    let index_descriptor = index_name.descriptor().as_str().to_string();
    let field_vec: Vec<value::FieldPath> = Vec::from(fields);
    let fields: Vec<pb::common::FieldPath> = field_vec
        .into_iter()
        .map(pb::common::FieldPath::from)
        .collect();
    let intervals: Vec<pb::common::Interval> = intervals.into();
    proto::DistributedIndexReads {
        tablet_id,
        index_descriptor,
        fields,
        intervals,
    }
}

fn index_reads_from_proto(p: proto::DistributedIndexReads) -> anyhow::Result<IndexReadsSummary> {
    use common::{
        bootstrap_model::index::database_index::IndexedFields,
        interval::IntervalSet,
        types::{
            IndexDescriptor,
            TabletIndexName,
        },
    };
    let tablet_id: value::TabletId = p
        .tablet_id
        .parse()
        .map_err(|e| anyhow::anyhow!("DistributedIndexReads.tablet_id {:?}: {e}", p.tablet_id))?;
    let descriptor = IndexDescriptor::new(p.index_descriptor.clone())?;
    let index_name = if descriptor.is_reserved() {
        TabletIndexName::new_reserved(tablet_id, descriptor)?
    } else {
        TabletIndexName::new(tablet_id, descriptor)?
    };
    let fields_vec: Vec<value::FieldPath> = p
        .fields
        .into_iter()
        .map(value::FieldPath::try_from)
        .collect::<anyhow::Result<_>>()?;
    let fields = IndexedFields::try_from(fields_vec)?;
    let intervals = IntervalSet::try_from(p.intervals)?;
    Ok(IndexReadsSummary {
        index_name,
        fields,
        intervals,
    })
}

fn tx_read_size_to_proto(native: TxReadSize) -> proto::DistributedTxReadSize {
    proto::DistributedTxReadSize {
        total_document_size: native.total_document_size,
        total_document_count: native.total_document_count,
    }
}

fn tx_read_size_from_proto(p: &proto::DistributedTxReadSize) -> TxReadSize {
    TxReadSize {
        total_document_size: p.total_document_size,
        total_document_count: p.total_document_count,
    }
}

/// Inverse of `final_tx_to_proto`. Backend-side callers invoke this
/// on the parsed `ExecuteResponse` so downstream code works off the
/// tonic-free native shape. Map lands in a `BTreeMap` so the native
/// shape has deterministic ordering for tests and diffs.
pub fn final_tx_from_proto(proto: &proto::DistributedFinalTx) -> anyhow::Result<FinalTxSummary> {
    let writes: Vec<common::document::DocumentUpdateWithPrevTs> = proto
        .writes
        .iter()
        .cloned()
        .map(common::document::DocumentUpdateWithPrevTs::try_from)
        .collect::<anyhow::Result<_>>()?;
    let index_reads: Vec<IndexReadsSummary> = proto
        .index_reads
        .iter()
        .cloned()
        .map(index_reads_from_proto)
        .collect::<anyhow::Result<_>>()?;
    Ok(FinalTxSummary {
        begin_timestamp: proto.begin_timestamp,
        writes_count: proto.writes_count,
        reads_count: proto.reads_count,
        rows_read_by_tablet: proto
            .rows_read_by_tablet
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect(),
        writes,
        user_tx_size: proto.user_tx_size.as_ref().map(tx_read_size_from_proto),
        system_tx_size: proto.system_tx_size.as_ref().map(tx_read_size_from_proto),
        index_reads,
    })
}

fn duration_to_proto(d: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

fn duration_from_proto(p: &prost_types::Duration) -> Duration {
    let secs = p.seconds.max(0) as u64;
    let nanos = p.nanos.max(0) as u32;
    Duration::new(secs, nanos)
}

fn udf_type_to_i32(t: common::types::UdfType) -> i32 {
    use common::types::UdfType::*;
    match t {
        Query => pb::common::UdfType::Query as i32,
        Mutation => pb::common::UdfType::Mutation as i32,
        Action => pb::common::UdfType::Action as i32,
        HttpAction => pb::common::UdfType::HttpAction as i32,
    }
}

fn i32_to_udf_type(v: i32) -> anyhow::Result<common::types::UdfType> {
    let proto = pb::common::UdfType::try_from(v)
        .map_err(|_| anyhow::anyhow!("unknown UdfType enum value {v}"))?;
    Ok(match proto {
        pb::common::UdfType::Query => common::types::UdfType::Query,
        pb::common::UdfType::Mutation => common::types::UdfType::Mutation,
        pb::common::UdfType::Action => common::types::UdfType::Action,
        pb::common::UdfType::HttpAction => common::types::UdfType::HttpAction,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use value::FieldName;

    use super::*;

    fn sample_args() -> ConvexObject {
        let mut f: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
        f.insert(
            "email".parse().unwrap(),
            ConvexValue::try_from("alice@example.com".to_string()).unwrap(),
        );
        f.insert("age".parse().unwrap(), ConvexValue::Int64(30));
        ConvexObject::try_from(f).unwrap()
    }

    #[test]
    fn namespace_roundtrips_global() {
        let s = encode_namespace(TableNamespace::Global);
        assert_eq!(s, "global");
        assert_eq!(decode_namespace(&s).unwrap(), TableNamespace::Global);
    }

    #[test]
    fn namespace_rejects_garbage() {
        assert!(decode_namespace("").is_err());
        assert!(decode_namespace("not-a-known-prefix:xyz").is_err());
    }

    #[test]
    fn args_roundtrip() {
        let obj = sample_args();
        let bytes = encode_args(&obj).unwrap();
        let decoded = decode_args(&bytes).unwrap();
        assert_eq!(obj, decoded);
    }

    #[test]
    fn decode_args_rejects_non_object() {
        // JSON number instead of object.
        assert!(decode_args(b"42").is_err());
        // JSON array instead of object.
        assert!(decode_args(b"[1,2,3]").is_err());
    }

    #[test]
    fn request_roundtrip_through_proto() {
        let native = ExecuteRequest {
            name: "get_user".to_string(),
            namespace: TableNamespace::Global,
            args: sample_args(),
            timeout: Some(Duration::from_millis(1500)),
            min_registry_version: None,
            execution_context: None,
            begin_timestamp: None,
            existing_writes: Vec::new(),
        };
        let proto_req = to_proto_request(&native, common::types::UdfType::Query).unwrap();
        let (decoded, udf_type) = from_proto_request(&proto_req).unwrap();
        assert_eq!(decoded.name, native.name);
        assert_eq!(decoded.namespace, native.namespace);
        assert_eq!(decoded.args, native.args);
        assert_eq!(decoded.timeout, native.timeout);
        assert_eq!(udf_type, common::types::UdfType::Query);
    }

    #[test]
    fn response_ok_roundtrips() {
        let native = ExecuteResponse::new(Ok(ConvexValue::Int64(42)));
        let p = to_proto_response(&native).unwrap();
        let decoded = from_proto_response(&p).unwrap();
        assert_eq!(decoded.result, Ok(ConvexValue::Int64(42)));
        assert!(decoded.log_lines.is_empty());
    }

    #[test]
    fn response_err_roundtrips() {
        let native = ExecuteResponse::new(Err("boom".to_string()));
        let p = to_proto_response(&native).unwrap();
        let decoded = from_proto_response(&p).unwrap();
        assert!(matches!(decoded.result, Err(ref m) if m.contains("boom")));
    }

    #[test]
    fn request_execution_context_roundtrips_through_proto() {
        use common::execution_context::{
            ExecutionContext,
            RequestId,
        };
        // A caller-supplied ExecutionContext must survive the
        // proto trip verbatim — request_id + is_root are the
        // observable bits the backend cares about for tracing.
        let request_id = RequestId::new();
        let ctx =
            ExecutionContext::new_from_parts(request_id.clone(), Default::default(), None, true);
        let native = ExecuteRequest {
            name: "get".into(),
            namespace: TableNamespace::Global,
            args: sample_args(),
            timeout: None,
            min_registry_version: None,
            execution_context: Some(ctx),
            begin_timestamp: None,
            existing_writes: Vec::new(),
        };
        let proto_req = to_proto_request(&native, common::types::UdfType::Query).unwrap();
        let (decoded, _) = from_proto_request(&proto_req).unwrap();
        let decoded_ctx = decoded
            .execution_context
            .expect("execution context survives round-trip");
        assert!(decoded_ctx.is_root());
    }

    #[test]
    fn response_log_lines_roundtrip_through_proto() {
        // `with_log_lines(...)` round-trips verbatim — the proto
        // field is the exact shape (repeated string) our encoder
        // writes and our decoder reads. Without this, log lines
        // would silently disappear at the wire.
        let native = ExecuteResponse::new(Ok(ConvexValue::Null))
            .with_log_lines(vec!["[INFO] one".to_string(), "[WARN] two".to_string()]);
        let p = to_proto_response(&native).unwrap();
        let decoded = from_proto_response(&p).unwrap();
        assert_eq!(decoded.log_lines, vec!["[INFO] one", "[WARN] two"]);
    }

    #[test]
    fn response_final_tx_roundtrips_through_proto() {
        // `FinalTxSummary` is the native side of the wire message the
        // backend's Phase-2 Committer consumes. Pin the three-field
        // round-trip so a future proto expansion can't silently drop
        // a field on the native-decoded side.
        let mut rows_read_by_tablet = std::collections::BTreeMap::new();
        rows_read_by_tablet.insert("tab1".to_string(), 11);
        rows_read_by_tablet.insert("tab2".to_string(), 22);
        let summary = FinalTxSummary {
            begin_timestamp: 42,
            writes_count: 3,
            reads_count: 7,
            rows_read_by_tablet: rows_read_by_tablet.clone(),
            // Substep 2.3 `writes` content is exercised by
            // `response_final_tx_writes_roundtrip` below.
            writes: Vec::new(),
            // Substep 2.2a tx-size content is exercised by
            // `response_final_tx_tx_size_roundtrip` below.
            user_tx_size: None,
            system_tx_size: None,
            // Substep 2.2b index_reads content is exercised by
            // `response_final_tx_index_reads_roundtrip` below.
            index_reads: Vec::new(),
        };
        let native = ExecuteResponse::new(Ok(ConvexValue::Null)).with_final_tx(summary.clone());
        let p = to_proto_response(&native).unwrap();
        let decoded_proto = p
            .final_tx
            .as_ref()
            .expect("native final_tx was set → proto field must be populated");
        assert_eq!(decoded_proto.begin_timestamp, 42);
        assert_eq!(decoded_proto.writes_count, 3);
        assert_eq!(decoded_proto.reads_count, 7);
        assert_eq!(decoded_proto.rows_read_by_tablet.get("tab1"), Some(&11));
        assert_eq!(decoded_proto.rows_read_by_tablet.get("tab2"), Some(&22));
        assert!(decoded_proto.writes.is_empty());
        let decoded_native = from_proto_response(&p).unwrap();
        // `FinalTxSummary` no longer implements `PartialEq` because
        // it embeds `IntervalSet` (substep 2.2b). Compare the
        // fields that this test case actually exercises.
        let decoded_summary = decoded_native.final_tx.expect("final_tx populated");
        assert_eq!(decoded_summary.begin_timestamp, summary.begin_timestamp);
        assert_eq!(decoded_summary.writes_count, summary.writes_count);
        assert_eq!(decoded_summary.reads_count, summary.reads_count);
        assert_eq!(decoded_summary.rows_read_by_tablet, rows_read_by_tablet);
        assert!(decoded_summary.writes.is_empty());
        assert!(decoded_summary.index_reads.is_empty());
    }

    #[test]
    fn request_existing_writes_roundtrip() {
        // Substep 2.4: the backend stages pending writes on the
        // `ExistingWrites` field so batched UDFs see earlier UDFs'
        // writes. Pin a one-entry round-trip here; server-side
        // merging into `tx.merge_writes(...)` is tested separately
        // once the handler-dispatch path lands.
        use common::document::DocumentUpdateWithPrevTs;
        use value::ResolvedDocumentId;
        let update = DocumentUpdateWithPrevTs {
            id: ResolvedDocumentId::MIN,
            old_document: None,
            new_document: None,
        };
        let native = ExecuteRequest {
            name: "whatever".into(),
            namespace: TableNamespace::Global,
            args: sample_args(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
            begin_timestamp: Some(12345),
            existing_writes: vec![update.clone()],
        };
        let p = to_proto_request(&native, common::types::UdfType::Mutation).unwrap();
        assert_eq!(p.begin_timestamp, Some(12345));
        assert_eq!(
            p.existing_writes.as_ref().unwrap().updates.len(),
            1,
            "ExistingWrites carries the single staged update",
        );
        let (decoded, _) = from_proto_request(&p).unwrap();
        assert_eq!(decoded.begin_timestamp, Some(12345));
        assert_eq!(decoded.existing_writes.len(), 1);
        assert_eq!(decoded.existing_writes[0].id, update.id);
    }

    #[test]
    fn request_empty_existing_writes_encodes_as_none() {
        // Empty `existing_writes` must encode as `None` — otherwise
        // the worker's fast-path "is this a batched request?"
        // check would false-positive on every single-UDF call.
        let native = ExecuteRequest {
            name: "whatever".into(),
            namespace: TableNamespace::Global,
            args: sample_args(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
            begin_timestamp: None,
            existing_writes: Vec::new(),
        };
        let p = to_proto_request(&native, common::types::UdfType::Query).unwrap();
        assert!(
            p.existing_writes.is_none(),
            "empty vec must not produce a present proto ExistingWrites",
        );
    }

    #[test]
    fn response_final_tx_writes_roundtrip() {
        // Substep 2.3: `FinalTxSummary.writes` carries the
        // coalesced document updates the handler produced. Pin the
        // round-trip with a minimal `DocumentUpdateWithPrevTs`
        // whose `old_document` / `new_document` are both `None` —
        // valid per the struct definition and exercises the proto
        // encoding without needing real ResolvedDocument plumbing.
        use common::document::DocumentUpdateWithPrevTs;
        use value::ResolvedDocumentId;
        let id = ResolvedDocumentId::MIN;
        let update = DocumentUpdateWithPrevTs {
            id,
            old_document: None,
            new_document: None,
        };
        let summary = FinalTxSummary {
            begin_timestamp: 5,
            writes_count: 1,
            reads_count: 0,
            rows_read_by_tablet: Default::default(),
            writes: vec![update.clone()],
            user_tx_size: None,
            system_tx_size: None,
            index_reads: Vec::new(),
        };
        let native = ExecuteResponse::new(Ok(ConvexValue::Null)).with_final_tx(summary.clone());
        let p = to_proto_response(&native).unwrap();
        let dft = p.final_tx.as_ref().expect("final_tx populated");
        assert_eq!(dft.writes.len(), 1);
        let decoded = from_proto_response(&p).unwrap();
        let dft_native = decoded.final_tx.expect("final_tx populated on decode");
        assert_eq!(dft_native.writes.len(), 1);
        assert_eq!(dft_native.writes[0].id, update.id);
        assert!(dft_native.writes[0].old_document.is_none());
        assert!(dft_native.writes[0].new_document.is_none());
    }

    #[test]
    fn response_final_tx_tx_size_roundtrip() {
        // Substep 2.2a: `user_tx_size` / `system_tx_size` carry
        // the scalar read-size counters the backend rolls into
        // usage tracking. Pin the round-trip so a future proto
        // field-number bump or a sloppy conversion can't silently
        // land zeros on one side.
        let user = TxReadSize {
            total_document_size: 4096,
            total_document_count: 3,
        };
        let system = TxReadSize {
            total_document_size: 128,
            total_document_count: 1,
        };
        let summary = FinalTxSummary {
            begin_timestamp: 9,
            writes_count: 0,
            reads_count: 2,
            rows_read_by_tablet: Default::default(),
            writes: Vec::new(),
            user_tx_size: Some(user),
            system_tx_size: Some(system),
            index_reads: Vec::new(),
        };
        let native = ExecuteResponse::new(Ok(ConvexValue::Null)).with_final_tx(summary.clone());
        let p = to_proto_response(&native).unwrap();
        let dft = p.final_tx.as_ref().expect("final_tx populated");
        assert_eq!(
            dft.user_tx_size.as_ref().map(|s| s.total_document_size),
            Some(4096)
        );
        assert_eq!(
            dft.system_tx_size.as_ref().map(|s| s.total_document_count),
            Some(1)
        );
        let decoded_native = from_proto_response(&p).unwrap().final_tx.unwrap();
        assert_eq!(decoded_native.user_tx_size, Some(user));
        assert_eq!(decoded_native.system_tx_size, Some(system));
    }

    #[test]
    fn response_final_tx_index_reads_roundtrip() {
        // Substep 2.2b: `index_reads` carries one entry per
        // (tablet, index) the handler read through, including the
        // indexed-fields list and the flat interval set. Pin an
        // "index over everything" round-trip — IntervalSet::All
        // has its own proto encoding and is the shape any
        // `collect()`-style handler emits.
        use common::{
            bootstrap_model::index::database_index::IndexedFields,
            interval::IntervalSet,
            types::{
                IndexDescriptor,
                TabletIndexName,
            },
        };
        use convex_native::distributed::IndexReadsSummary;
        let tablet_id = value::TabletId::MIN;
        let descriptor = IndexDescriptor::new("by_email").unwrap();
        let index_name = TabletIndexName::new(tablet_id, descriptor).unwrap();
        let fields: IndexedFields = vec!["email".parse::<value::FieldPath>().unwrap()]
            .try_into()
            .unwrap();
        let intervals = IntervalSet::All;
        let summary = FinalTxSummary {
            begin_timestamp: 1,
            writes_count: 0,
            reads_count: 1,
            rows_read_by_tablet: Default::default(),
            writes: Vec::new(),
            user_tx_size: None,
            system_tx_size: None,
            index_reads: vec![IndexReadsSummary {
                index_name: index_name.clone(),
                fields: fields.clone(),
                intervals,
            }],
        };
        let native = ExecuteResponse::new(Ok(ConvexValue::Null)).with_final_tx(summary);
        let p = to_proto_response(&native).unwrap();
        let dft = p.final_tx.as_ref().expect("final_tx populated");
        assert_eq!(dft.index_reads.len(), 1);
        assert_eq!(dft.index_reads[0].tablet_id, tablet_id.to_string());
        assert_eq!(dft.index_reads[0].index_descriptor, "by_email");
        assert_eq!(dft.index_reads[0].fields.len(), 1);
        assert_eq!(
            dft.index_reads[0].fields[0].fields,
            vec!["email".to_string()]
        );
        let decoded = from_proto_response(&p).unwrap();
        let summary = decoded.final_tx.unwrap();
        assert_eq!(summary.index_reads.len(), 1);
        assert_eq!(summary.index_reads[0].index_name, index_name);
        assert_eq!(summary.index_reads[0].fields, fields);
        // `IntervalSet::All` has no PartialEq; compare via its
        // proto round-trip (All → `ALL_INTERVAL_PROTO`).
        let encoded: Vec<pb::common::Interval> = summary.index_reads[0].intervals.clone().into();
        assert_eq!(encoded.len(), 1, "IntervalSet::All encodes to one interval");
    }

    #[test]
    fn response_without_final_tx_keeps_field_none() {
        // Handler errors + actions carry no tx → the proto's final_tx
        // stays None. Tests downstream (Phase 2 dispatch) rely on this
        // to decide whether to OCC-validate or just forward the result.
        let native = ExecuteResponse::new(Err("boom".to_string()));
        let p = to_proto_response(&native).unwrap();
        assert!(p.final_tx.is_none());
        let decoded = from_proto_response(&p).unwrap();
        assert!(decoded.final_tx.is_none());
    }

    #[test]
    fn decoding_response_without_result_fails() {
        let p = proto::ExecuteResponse {
            result: None,
            user_execution_time: None,
            served_by_version: None,
            log_lines: vec![],
            final_tx: None,
        };
        assert!(from_proto_response(&p).is_err());
    }
}
