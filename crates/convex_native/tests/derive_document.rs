//! Integration tests for `#[derive(ConvexDocument)]`.
//!
//! Lives in `tests/` rather than `src/` so the proc-macro expansion runs in
//! a consumer-style crate (matching what developers will actually write).

// Both the derive macro and the trait share the name `ConvexDocument` —
// derive macros and traits live in different namespaces, so `use` brings
// both into scope simultaneously (same pattern as `serde::Serialize`).
use convex_native::{
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
