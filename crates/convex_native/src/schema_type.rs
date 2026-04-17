//! Runtime reflection from Rust types into Convex [`Validator`]s.
//!
//! `#[derive(ConvexDocument)]` used to emit `document_type: None`, which
//! told the database layer "every document in this table has shape
//! `Any`" — so shape-violating writes were only caught downstream at
//! `from_convex_object` read time. With this module in place, every
//! derived document and nested type also emits a
//! [`DocumentSchema::Union(vec![ObjectValidator(...)])`] built from
//! `ConvexSchema::validator()` on each field, and the database enforces
//! that shape on write.
//!
//! The trait is deliberately small: callers ask for a
//! [`Validator`] and that's it. Implementations:
//!
//! - Primitives ([`String`], [`i64`], [`f64`], [`bool`],
//!   [`value::ConvexValue`]).
//! - Containers ([`Vec<T>`], [`Option<T>`], [`BTreeMap<String, V>`]).
//! - [`Id<T>`] → `Validator::Id(T::table_name())`.
//! - `#[derive(ConvexDocument)]` / [`ConvexNested`] / [`ConvexEnum`] /
//!   [`ConvexUnion`] emit the impl themselves.
//!
//! Non-implementations: `Vec<u8>` is handled *inside the derive macro*
//! (it becomes `Validator::Bytes`) rather than through a
//! trait impl, because `u8` has no meaningful standalone
//! `ConvexSchema` impl — an `impl ConvexSchema for u8` would make
//! `Vec<u8>` round-trip as `Validator::Array(Int64)`, which is not
//! what developers expect. Keeping that special-case at the macro
//! level avoids coherence problems while preserving the "bytes stay
//! bytes" contract.

use std::collections::BTreeMap;

use common::schemas::validator::{
    FieldValidator,
    LiteralValidator,
    ObjectValidator,
    Validator,
};
use value::{
    ConvexValue,
    IdentifierFieldName,
};

use crate::{
    document::ConvexDocument,
    id::Id,
};

/// Reflect a Rust type into the [`Validator`] that accepts its
/// serialized `ConvexValue` form.
pub trait ConvexSchema {
    /// The validator accepting values produced by
    /// `<Self as ToConvex>::to_convex`.
    fn validator() -> Validator;

    /// True for `Option<T>` — the containing field is allowed to be
    /// absent. Defaults to `false` for every other type.
    ///
    /// Derived code uses this to pick between
    /// [`FieldValidator::required_field_type`] and
    /// [`FieldValidator::optional_field_type`].
    fn field_is_optional() -> bool {
        false
    }
}

// ── Primitives ─────────────────────────────────────────────────────

impl ConvexSchema for String {
    fn validator() -> Validator {
        Validator::String
    }
}

impl ConvexSchema for i64 {
    fn validator() -> Validator {
        Validator::Int64
    }
}

impl ConvexSchema for f64 {
    fn validator() -> Validator {
        Validator::Float64
    }
}

impl ConvexSchema for bool {
    fn validator() -> Validator {
        Validator::Boolean
    }
}

/// `ConvexValue` is the escape hatch: when a field is typed as
/// `ConvexValue`, any shape is allowed.
impl ConvexSchema for ConvexValue {
    fn validator() -> Validator {
        Validator::Any
    }
}

// ── Containers ─────────────────────────────────────────────────────

impl<T: ConvexSchema> ConvexSchema for Vec<T> {
    fn validator() -> Validator {
        Validator::Array(Box::new(T::validator()))
    }
}

impl<T: ConvexSchema> ConvexSchema for Option<T> {
    fn validator() -> Validator {
        // `Option::None` serializes as `ConvexValue::Null`. When the
        // field IS present in the stored object, it must match either
        // `Null` or the inner type's validator. The absent-vs-present
        // choice is the `field_is_optional` signal; this validator
        // describes the allowed shape when the field *is* present.
        Validator::Union(vec![Validator::Null, T::validator()])
    }

    fn field_is_optional() -> bool {
        true
    }
}

impl<V: ConvexSchema> ConvexSchema for BTreeMap<String, V> {
    fn validator() -> Validator {
        Validator::Record(Box::new(Validator::String), Box::new(V::validator()))
    }
}

// ── Id<T> ──────────────────────────────────────────────────────────

impl<T: ConvexDocument> ConvexSchema for Id<T> {
    fn validator() -> Validator {
        Validator::Id(T::table_name())
    }
}

// ── Helpers for generated code ─────────────────────────────────────

/// Assemble a `FieldValidator` respecting `T::field_is_optional()`.
///
/// Used by the `#[derive(ConvexDocument)]` / `ConvexNested` macros so
/// each field picks the correct required/optional wrapper without the
/// generated code having to branch on the Rust type.
pub fn field_validator_for<T: ConvexSchema>() -> FieldValidator {
    let v = T::validator();
    if T::field_is_optional() {
        FieldValidator::optional_field_type(v)
    } else {
        FieldValidator::required_field_type(v)
    }
}

/// Parse `name` as an [`IdentifierFieldName`] — the key type
/// `ObjectValidator` uses. Each struct-field name emitted by the
/// derive must be a valid identifier (the derive rejects otherwise),
/// so this only fails when the derive was fed a pre-existing
/// non-identifier name.
pub fn identifier_field(name: &str) -> anyhow::Result<IdentifierFieldName> {
    name.parse().map_err(anyhow::Error::from)
}

/// Wrap a single-variant [`Validator::Literal`] string — used by the
/// `#[derive(ConvexEnum)]` / `ConvexUnion` macros to emit the tag-value
/// constraint as part of the union shape.
pub fn string_literal_validator(wire: &str) -> anyhow::Result<Validator> {
    let literal = LiteralValidator::String(wire.to_string().try_into()?);
    Ok(Validator::Literal(literal))
}

/// Build an `ObjectValidator` from (name, validator) pairs. Used by
/// derives that need to assemble variants or nested objects.
pub fn build_object_validator<I>(fields: I) -> anyhow::Result<ObjectValidator>
where
    I: IntoIterator<Item = (String, FieldValidator)>,
{
    let mut map: BTreeMap<IdentifierFieldName, FieldValidator> = BTreeMap::new();
    for (name, validator) in fields {
        let ident: IdentifierFieldName = name.parse()?;
        if map.insert(ident, validator).is_some() {
            anyhow::bail!("duplicate field {name:?} in derived validator");
        }
    }
    Ok(ObjectValidator(map))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitives_reflect_to_expected_validators() {
        assert_eq!(String::validator(), Validator::String);
        assert_eq!(i64::validator(), Validator::Int64);
        assert_eq!(f64::validator(), Validator::Float64);
        assert_eq!(bool::validator(), Validator::Boolean);
        assert_eq!(ConvexValue::validator(), Validator::Any);
    }

    #[test]
    fn option_reflects_as_nullable_union_and_marks_field_optional() {
        // `Option<i64>` becomes Union(Null, Int64) *and* the containing
        // field is marked optional. Both halves matter: without
        // `field_is_optional` the absent-field path fails; without the
        // Null in the union, a stored None (serialized as Null) fails.
        assert_eq!(
            <Option<i64>>::validator(),
            Validator::Union(vec![Validator::Null, Validator::Int64]),
        );
        assert!(<Option<i64>>::field_is_optional());
        assert!(!i64::field_is_optional(), "plain types are required");
    }

    #[test]
    fn vec_nests_element_validator() {
        assert_eq!(
            <Vec<String>>::validator(),
            Validator::Array(Box::new(Validator::String)),
        );
    }

    #[test]
    fn btreemap_string_value_is_record_string_to_value() {
        assert_eq!(
            <BTreeMap<String, i64>>::validator(),
            Validator::Record(Box::new(Validator::String), Box::new(Validator::Int64)),
        );
    }

    #[test]
    fn field_validator_for_wraps_required_vs_optional() {
        let req = field_validator_for::<i64>();
        assert_eq!(req.validator, Validator::Int64);
        assert!(!req.optional);

        let opt = field_validator_for::<Option<String>>();
        assert!(opt.optional, "Option<T> field is optional");
        assert_eq!(
            opt.validator,
            Validator::Union(vec![Validator::Null, Validator::String]),
        );
    }

    #[test]
    fn build_object_validator_rejects_duplicate_field_names() {
        let err = build_object_validator(vec![
            (
                "name".to_string(),
                FieldValidator::required_field_type(Validator::String),
            ),
            (
                "name".to_string(),
                FieldValidator::required_field_type(Validator::Int64),
            ),
        ])
        .expect_err("duplicate field name");
        assert!(format!("{err}").contains("duplicate field"));
    }

    #[test]
    fn string_literal_validator_wraps_string_into_literal() {
        let v = string_literal_validator("email").expect("string is a valid literal");
        match v {
            Validator::Literal(LiteralValidator::String(s)) => {
                assert_eq!(s.as_ref(), "email");
            },
            other => panic!("unexpected validator {other:?}"),
        }
    }
}
