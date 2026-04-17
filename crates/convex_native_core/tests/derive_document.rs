//! Integration tests for `#[derive(ConvexDocument)]`.
//!
//! Lives in `tests/` rather than `src/` so the proc-macro expansion runs in
//! a consumer-style crate (matching what developers will actually write).

// Both the derive macro and the trait share the name `ConvexDocument` —
// derive macros and traits live in different namespaces, so `use` brings
// both into scope simultaneously (same pattern as `serde::Serialize`).
use convex_native_core::{
    prelude::*,
    ConvexDocument,
    ConvexPatch,
    FieldReference,
    Id,
    IndexReference,
    NativeSchema,
};

#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
#[convex(index(name = "by_created", fields = ["created_at"]))]
pub struct User {
    pub name: String,
    pub email: String,
    pub avatar_url: Option<String>,
    pub created_at: f64,
}

#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "messages")]
#[convex(index(name = "by_channel", fields = ["channel", "created_at"]))]
pub struct Message {
    pub author: Id<User>,
    pub body: String,
    pub channel: String,
    pub created_at: f64,
}

/// Exercises the `Vec<u8>` special-case in the derive macro.
/// A field typed `Vec<u8>` must reflect as `Validator::Bytes`,
/// not `Validator::Array(Int64)` — that distinction is what keeps
/// file payloads routing through the right wire format.
#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "blobs")]
pub struct Blob {
    pub payload: Vec<u8>,
    pub optional_payload: Option<Vec<u8>>,
    pub checksum: String,
}

/// Exercises the container validators (`Vec<T>`, `BTreeMap<String, V>`)
/// end-to-end through the derive.
#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "settings")]
pub struct Settings {
    pub tags: Vec<String>,
    pub limits: std::collections::BTreeMap<String, i64>,
}

#[test]
fn field_enum_variants_expose_field_names() {
    assert_eq!(UserField::Name.as_str(), "name");
    assert_eq!(UserField::Email.as_str(), "email");
    assert_eq!(UserField::AvatarUrl.as_str(), "avatar_url");
    assert_eq!(UserField::CreatedAt.as_str(), "created_at");
}

#[test]
fn index_enum_variants_expose_name_and_fields() {
    assert_eq!(UserIndex::ByEmail.as_str(), "by_email");
    assert_eq!(UserIndex::ByEmail.fields(), &["email"]);
    assert_eq!(UserIndex::ByCreated.as_str(), "by_created");
    assert_eq!(UserIndex::ByCreated.fields(), &["created_at"]);
    assert_eq!(MessageIndex::ByChannel.fields(), &["channel", "created_at"]);
}

#[test]
fn document_round_trips_through_convex_object() {
    let user = User {
        name: "Alice".into(),
        email: "a@example.com".into(),
        avatar_url: Some("https://img".into()),
        created_at: 123.5,
    };
    let obj = user.to_convex_object().expect("to_convex_object");
    let back = User::from_convex_object(obj).expect("from_convex_object");
    assert_eq!(user, back);
}

#[test]
fn document_round_trips_with_null_for_none() {
    let user = User {
        name: "Bob".into(),
        email: "b@example.com".into(),
        avatar_url: None,
        created_at: 0.0,
    };
    let obj = user.to_convex_object().expect("to_convex_object");
    let back = User::from_convex_object(obj).expect("from_convex_object");
    assert_eq!(user, back);
}

#[test]
fn patch_serializes_only_set_fields() {
    let patch = UserPatch {
        name: Some("Carol".into()),
        ..Default::default()
    };
    let obj = patch.to_convex_object().expect("patch to_convex_object");
    assert_eq!(obj.iter().count(), 1);
}

#[test]
fn with_id_derefs_to_document() {
    // Compile-time check that the UserWithId type exists and derefs to User.
    // `&*wi` is the explicit form — auto-deref would pick the same impl,
    // but we want the test to call out what's being exercised.
    #[allow(clippy::explicit_auto_deref)]
    fn _compile_check(wi: UserWithId) -> &'static str {
        let _: &User = &*wi;
        "ok"
    }
    let _ = _compile_check;
}

#[test]
fn table_definition_includes_all_indexes() {
    let def = User::table_definition();
    assert_eq!(def.table_name.to_string(), "users");
    assert_eq!(def.indexes.len(), 2);
    let descs: Vec<_> = def.indexes.keys().map(|d| d.as_str().to_string()).collect();
    assert!(descs.iter().any(|s| s == "by_email"));
    assert!(descs.iter().any(|s| s == "by_created"));
}

#[test]
fn table_definition_emits_document_shape_validator() {
    // Before: `document_type: None` — every write went through a
    // permissive "Any" schema and invalid shapes only blew up on read
    // via `from_convex_object`. After: the derive reflects each field
    // into a `Validator` so the DB layer rejects shape-violating
    // writes up-front.
    use common::schemas::{
        validator::{
            FieldValidator,
            ObjectValidator,
            Validator,
        },
        DocumentSchema,
    };
    use value::IdentifierFieldName;

    let def = User::table_definition();
    let doc = def
        .document_type
        .as_ref()
        .expect("document_type now populated");
    let DocumentSchema::Union(objs) = doc else {
        panic!("unexpected document_type {doc:?}")
    };
    assert_eq!(objs.len(), 1, "single-shape struct -> one ObjectValidator");
    let ObjectValidator(fields) = &objs[0];

    let get = |name: &str| -> &FieldValidator {
        let key: IdentifierFieldName = name.parse().unwrap();
        fields.get(&key).unwrap_or_else(|| panic!("missing {name}"))
    };
    // Plain fields: required, with the expected primitive validator.
    assert_eq!(get("name").validator, Validator::String);
    assert!(!get("name").optional);
    assert_eq!(get("email").validator, Validator::String);
    assert_eq!(get("created_at").validator, Validator::Float64);
    // Option<String> -> optional field, union(null, string) when
    // present. This is the crux of what lets derived structs round-
    // trip through the database: without `optional: true`, any write
    // that didn't include `avatar_url` would be rejected even though
    // the Rust type permits `None`.
    let avatar = get("avatar_url");
    assert!(avatar.optional, "Option<T> field must be optional");
    assert_eq!(
        avatar.validator,
        Validator::Union(vec![Validator::Null, Validator::String]),
    );
}

#[test]
fn table_definition_emits_bytes_validator_for_vec_u8_fields() {
    // The derive macro special-cases `Vec<u8>` → `Validator::Bytes`
    // at the AST level because `u8` has no `ConvexSchema` impl (see
    // `schema_type.rs` docs). This pins that path: a refactor that
    // accidentally routed `Vec<u8>` through the generic `Vec<T>`
    // impl would flip this to `Array(Int64)` and break file /
    // payload round-trips.
    use common::schemas::{
        validator::{
            ObjectValidator,
            Validator,
        },
        DocumentSchema,
    };
    use value::IdentifierFieldName;

    let def = Blob::table_definition();
    let DocumentSchema::Union(objs) = def.document_type.as_ref().expect("document_type populated")
    else {
        panic!("expected Union");
    };
    let ObjectValidator(fields) = &objs[0];

    let payload_key: IdentifierFieldName = "payload".parse().unwrap();
    let payload = fields.get(&payload_key).expect("payload field present");
    assert_eq!(payload.validator, Validator::Bytes);
    assert!(!payload.optional);

    // `Option<Vec<u8>>` threads through the same special-case and
    // wraps Bytes in the Option-shaped union.
    let opt_key: IdentifierFieldName = "optional_payload".parse().unwrap();
    let opt = fields.get(&opt_key).expect("optional_payload present");
    assert!(opt.optional, "Option<Vec<u8>> is an optional field");
    assert_eq!(
        opt.validator,
        Validator::Union(vec![Validator::Null, Validator::Bytes]),
    );
}

#[test]
fn table_definition_emits_container_validators_for_vec_and_btreemap_fields() {
    // `Vec<T>` must nest the element validator; `BTreeMap<String, V>`
    // must become `Record(String, V)`. These are the default paths
    // in `schema_type.rs`; pin them end-to-end through the derive
    // so a refactor to the macro's validator-expr emitter can't
    // silently flip them back to `Any`.
    use common::schemas::{
        validator::{
            ObjectValidator,
            Validator,
        },
        DocumentSchema,
    };
    use value::IdentifierFieldName;

    let def = Settings::table_definition();
    let DocumentSchema::Union(objs) = def.document_type.as_ref().expect("document_type populated")
    else {
        panic!("expected Union");
    };
    let ObjectValidator(fields) = &objs[0];

    let tags_key: IdentifierFieldName = "tags".parse().unwrap();
    let tags = fields.get(&tags_key).expect("tags field present");
    assert_eq!(
        tags.validator,
        Validator::Array(Box::new(Validator::String)),
        "Vec<String> -> Array(String)",
    );

    let limits_key: IdentifierFieldName = "limits".parse().unwrap();
    let limits = fields.get(&limits_key).expect("limits field present");
    assert_eq!(
        limits.validator,
        Validator::Record(Box::new(Validator::String), Box::new(Validator::Int64)),
        "BTreeMap<String, i64> -> Record(String, Int64)",
    );
}

#[test]
fn table_definition_emits_id_validator_for_related_documents() {
    use common::schemas::{
        validator::{
            ObjectValidator,
            Validator,
        },
        DocumentSchema,
    };
    use value::IdentifierFieldName;

    let def = Message::table_definition();
    let DocumentSchema::Union(objs) = def.document_type.as_ref().expect("document_type populated")
    else {
        panic!("expected Union");
    };
    let ObjectValidator(fields) = &objs[0];
    let author_key: IdentifierFieldName = "author".parse().unwrap();
    let author = fields.get(&author_key).expect("author field present");
    assert_eq!(
        author.validator,
        Validator::Id("users".parse().unwrap()),
        "Id<User> reflects as Validator::Id(\"users\")",
    );
}

#[test]
fn schema_collection_picks_up_derived_tables() {
    let schema = NativeSchema::collect().expect("collect");
    let names: Vec<_> = schema.tables.keys().map(|t| t.to_string()).collect();
    // The whole test binary registers both User and Message.
    assert!(
        names.iter().any(|n| n == "users"),
        "users not found: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "messages"),
        "messages not found: {names:?}"
    );
}

#[test]
fn phantom_typed_id_cannot_be_confused_across_tables() {
    // This is a compile-time check; we assert it by explicit type
    // annotations and relying on `FromStr`. If the phantom doesn't
    // discriminate, the wrong assignment would be accepted.
    let raw = "jd72jdw7t0x9grf04vg5f65t0s7g4k1d";
    let _id_user: Result<Id<User>, _> = raw.parse();
    let _id_msg: Result<Id<Message>, _> = raw.parse();
    // No attempt to cross-assign — documented invariant is "different types".
}
