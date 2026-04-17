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
};
use pb::function_execution as proto;
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

/// Wire-format name for `TableNamespace::Global`. The conductor and
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
/// rather than silently falling back, so a conductor/worker version
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
/// per-call fields not carried on the native struct (udf_type +
/// identity). The native `ExecuteRequest` deliberately omits those
/// because they're context the composite runner supplies.
pub fn to_proto_request(
    native: &ExecuteRequest,
    udf_type: common::types::UdfType,
) -> anyhow::Result<proto::ExecuteRequest> {
    Ok(proto::ExecuteRequest {
        name: native.name.clone(),
        udf_type: udf_type_to_i32(udf_type),
        namespace: encode_namespace(native.namespace),
        args_json: encode_args(&native.args)?,
        identity: None,
        timeout: native.timeout.map(duration_to_proto),
        execution_context: None,
        min_registry_version: native.min_registry_version.clone(),
    })
}

/// Parse a proto `ExecuteRequest` into the native shape plus the
/// `UdfType` the worker should dispatch to. Identity and execution
/// context aren't reflected in the native type — the worker reads
/// those directly off the proto when needed.
pub fn from_proto_request(
    p: &proto::ExecuteRequest,
) -> anyhow::Result<(ExecuteRequest, common::types::UdfType)> {
    let udf_type = i32_to_udf_type(p.udf_type)?;
    let native = ExecuteRequest {
        name: p.name.clone(),
        namespace: decode_namespace(&p.namespace)?,
        args: decode_args(&p.args_json)?,
        timeout: p.timeout.as_ref().map(duration_from_proto),
        min_registry_version: p.min_registry_version.clone(),
    };
    Ok((native, udf_type))
}

/// Encode an `ExecuteResponse` as a proto message. Errors on the
/// native side are stringified; the proto carries a
/// `common.FunctionResult` which is richer, but `ExecuteResponse`
/// itself already lossed the structure.
pub fn to_proto_response(native: &ExecuteResponse) -> proto::ExecuteResponse {
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
    proto::ExecuteResponse {
        result: Some(result),
        user_execution_time: None,
        served_by_version: None,
        log_lines: native.log_lines.clone(),
    }
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
    Ok(ExecuteResponse {
        result,
        log_lines: p.log_lines.clone(),
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
        let p = to_proto_response(&native);
        let decoded = from_proto_response(&p).unwrap();
        assert_eq!(decoded.result, Ok(ConvexValue::Int64(42)));
        assert!(decoded.log_lines.is_empty());
    }

    #[test]
    fn response_err_roundtrips() {
        let native = ExecuteResponse::new(Err("boom".to_string()));
        let p = to_proto_response(&native);
        let decoded = from_proto_response(&p).unwrap();
        assert!(matches!(decoded.result, Err(ref m) if m.contains("boom")));
    }

    #[test]
    fn response_log_lines_roundtrip_through_proto() {
        // `with_log_lines(...)` round-trips verbatim — the proto
        // field is the exact shape (repeated string) our encoder
        // writes and our decoder reads. Without this, log lines
        // would silently disappear at the wire.
        let native = ExecuteResponse::new(Ok(ConvexValue::Null))
            .with_log_lines(vec!["[INFO] one".to_string(), "[WARN] two".to_string()]);
        let p = to_proto_response(&native);
        let decoded = from_proto_response(&p).unwrap();
        assert_eq!(decoded.log_lines, vec!["[INFO] one", "[WARN] two"]);
    }

    #[test]
    fn decoding_response_without_result_fails() {
        let p = proto::ExecuteResponse {
            result: None,
            user_execution_time: None,
            served_by_version: None,
            log_lines: vec![],
        };
        assert!(from_proto_response(&p).is_err());
    }
}
