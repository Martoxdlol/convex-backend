//! Two-way conversion traits between Rust types and [`ConvexValue`].
//!
//! `ToConvex` converts a Rust value into a `ConvexValue`. `FromConvex` parses
//! a `ConvexValue` into a Rust value. Together they allow the rest of the
//! crate to move values across the boundary of generated proc-macro code and
//! the Convex value representation without juggling multiple trait impls.
//!
//! These wrap `value`'s existing `TryFrom`/`TryInto` impls for the common
//! primitives and add first-class support for the container types the design
//! uses pervasively (`Option<T>`, `Vec<T>`, `BTreeMap<String, V>`).

use std::collections::BTreeMap;

use anyhow::{
    anyhow,
    Context,
};
use value::{
    ConvexArray,
    ConvexObject,
    ConvexValue,
    FieldName,
};

/// Convert a Rust value into a [`ConvexValue`].
pub trait ToConvex {
    fn to_convex(self) -> anyhow::Result<ConvexValue>;
}

/// Parse a [`ConvexValue`] into a Rust value.
pub trait FromConvex: Sized {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self>;
}

// ── Primitives ────────────────────────────────────────────────────

impl ToConvex for ConvexValue {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        Ok(self)
    }
}

impl FromConvex for ConvexValue {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        Ok(value)
    }
}

impl ToConvex for String {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        ConvexValue::try_from(self)
    }
}

impl FromConvex for String {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        String::try_from(value)
    }
}

impl ToConvex for i64 {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        Ok(ConvexValue::Int64(self))
    }
}

impl FromConvex for i64 {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        i64::try_from(value)
    }
}

impl ToConvex for f64 {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        Ok(ConvexValue::Float64(self))
    }
}

impl FromConvex for f64 {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        match value {
            ConvexValue::Float64(f) => Ok(f),
            other => Err(anyhow!("expected Float64, got {}", other.type_name())),
        }
    }
}

impl ToConvex for bool {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        Ok(ConvexValue::Boolean(self))
    }
}

impl FromConvex for bool {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        bool::try_from(value)
    }
}

impl ToConvex for Vec<u8> {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        ConvexValue::try_from(self)
    }
}

impl FromConvex for Vec<u8> {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        match value {
            ConvexValue::Bytes(b) => Ok(b.into()),
            other => Err(anyhow!("expected Bytes, got {}", other.type_name())),
        }
    }
}

// ── Containers ────────────────────────────────────────────────────

impl<T: ToConvex> ToConvex for Vec<T> {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        let items: Vec<ConvexValue> = self
            .into_iter()
            .map(|v| v.to_convex())
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(ConvexValue::Array(ConvexArray::try_from(items)?))
    }
}

impl<T: FromConvex> FromConvex for Vec<T> {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        match value {
            ConvexValue::Array(arr) => Vec::<ConvexValue>::from(arr)
                .into_iter()
                .map(T::from_convex)
                .collect(),
            other => Err(anyhow!("expected Array, got {}", other.type_name())),
        }
    }
}

/// `Option::None` round-trips as `ConvexValue::Null`.
///
/// Note: this is *lossy* in one direction — `Option<Option<T>>::Some(None)`
/// and `Option<Option<T>>::None` both serialize to `Null`. When a truly
/// nullable-vs-missing semantic is needed, use the generated `XxxPatch`
/// types which distinguish the two via separate field presence.
impl<T: ToConvex> ToConvex for Option<T> {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        match self {
            None => Ok(ConvexValue::Null),
            Some(v) => v.to_convex(),
        }
    }
}

impl<T: FromConvex> FromConvex for Option<T> {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        match value {
            ConvexValue::Null => Ok(None),
            v => Ok(Some(T::from_convex(v)?)),
        }
    }
}

impl<V: ToConvex> ToConvex for BTreeMap<String, V> {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        let mut fields: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
        for (k, v) in self {
            let name: FieldName = k.parse().context("invalid field name")?;
            fields.insert(name, v.to_convex()?);
        }
        Ok(ConvexValue::Object(ConvexObject::try_from(fields)?))
    }
}

impl<V: FromConvex> FromConvex for BTreeMap<String, V> {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        match value {
            ConvexValue::Object(obj) => {
                let fields: BTreeMap<FieldName, ConvexValue> = obj.into();
                fields
                    .into_iter()
                    .map(|(k, v)| Ok((k.to_string(), V::from_convex(v)?)))
                    .collect()
            },
            other => Err(anyhow!("expected Object, got {}", other.type_name())),
        }
    }
}

// ── ConvexObject passthrough (used by generated code) ────────────

impl ToConvex for ConvexObject {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        Ok(ConvexValue::Object(self))
    }
}

impl FromConvex for ConvexObject {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        ConvexObject::try_from(value)
    }
}

// `ToConvex` / `FromConvex` impls for types produced by
// `#[derive(ConvexDocument)]` are emitted by the derive macro itself —
// we can't blanket-impl them here without `specialization`, which would
// conflict with the primitive impls above.

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<T: ToConvex + FromConvex + PartialEq + std::fmt::Debug + Clone>(v: T) {
        let cv = v.clone().to_convex().expect("to_convex");
        let back = T::from_convex(cv).expect("from_convex");
        assert_eq!(v, back);
    }

    #[test]
    fn roundtrip_primitives() {
        roundtrip(String::from("hello"));
        roundtrip(42_i64);
        roundtrip(3.14_f64);
        roundtrip(true);
        roundtrip(false);
        roundtrip(Vec::<u8>::from(b"bytes".as_slice()));
    }

    #[test]
    fn roundtrip_option() {
        roundtrip::<Option<i64>>(Some(5));
        roundtrip::<Option<i64>>(None);
        // None and Some(Null) collapse — documented behavior.
        let cv = Option::<i64>::None.to_convex().unwrap();
        assert!(matches!(cv, ConvexValue::Null));
    }

    #[test]
    fn roundtrip_vec() {
        roundtrip::<Vec<i64>>(vec![1, 2, 3]);
        roundtrip::<Vec<String>>(vec!["a".into(), "b".into()]);
    }

    #[test]
    fn roundtrip_map() {
        let mut m: BTreeMap<String, i64> = BTreeMap::new();
        m.insert("a".into(), 1);
        m.insert("b".into(), 2);
        roundtrip(m);
    }
}
