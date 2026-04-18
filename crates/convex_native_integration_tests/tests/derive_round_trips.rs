//! Round-trip coverage for the `#[derive(ConvexDocument)]` /
//! `#[derive(ConvexEnum)]` / `#[derive(ConvexNested)]` /
//! `#[derive(ConvexUnion)]` macros against the fixture app's
//! types.
//!
//! These assertions run entirely in-process (no database, no
//! tonic); they prove the generated `ToConvex` / `FromConvex`
//! impls are symmetric so the wire form survives every
//! dispatch-layer boundary (request args, response bodies,
//! storage envelopes).

use convex_native_core::{
    FromConvex,
    ToConvex,
};
use convex_native_integration_tests::fixture_app::{
    Metadata,
    Notification,
    Priority,
    Todo,
};
use value::{
    ConvexValue,
    FieldName,
};

#[test]
fn priority_enum_round_trips_through_convex_value() -> anyhow::Result<()> {
    for variant in [Priority::Low, Priority::Medium, Priority::High] {
        let v: ConvexValue = variant.clone().to_convex()?;
        let back: Priority = Priority::from_convex(v)?;
        assert_eq!(variant, back);
    }
    Ok(())
}

#[test]
fn priority_default_wire_form_is_lowercase_variant_name() -> anyhow::Result<()> {
    // The default rename rule in `#[derive(ConvexEnum)]` lower-cases
    // the variant name; a rename attribute would override it. The
    // fixture doesn't set one, so we pin the default shape.
    let low: ConvexValue = Priority::Low.to_convex()?;
    match low {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "low"),
        other => panic!("expected string for enum, got {other:?}"),
    }
    Ok(())
}

#[test]
fn metadata_nested_struct_round_trips() -> anyhow::Result<()> {
    let md = Metadata {
        source: "web".to_string(),
        priority: Priority::High,
    };
    let v = md.clone().to_convex()?;
    let back = Metadata::from_convex(v)?;
    assert_eq!(md, back);
    Ok(())
}

#[test]
fn notification_tagged_union_uses_kind_discriminator() -> anyhow::Result<()> {
    let email = Notification::Email {
        to: "a@b.com".to_string(),
    };
    let v = email.clone().to_convex()?;
    match &v {
        ConvexValue::Object(o) => {
            // The fixture uses `#[convex(tag = "kind")]`; default
            // would be "type". Pin the chosen tag.
            let kind_field: FieldName = "kind".parse()?;
            let kind = o
                .get(&kind_field)
                .expect("tag field present on tagged-union object");
            match kind {
                ConvexValue::String(s) => assert_eq!(s.to_string(), "email"),
                other => panic!("expected string tag, got {other:?}"),
            }
        },
        other => panic!("expected object, got {other:?}"),
    }
    let back = Notification::from_convex(v)?;
    assert_eq!(email, back);
    Ok(())
}

#[test]
fn todo_document_round_trips_with_optional_nested() -> anyhow::Result<()> {
    // Option<Metadata> should accept both None (serialised as
    // absent / null) and Some(value).
    for md in [
        None,
        Some(Metadata {
            source: "cli".to_string(),
            priority: Priority::Medium,
        }),
    ] {
        let todo = Todo {
            owner: "owner".to_string(),
            text: "text".to_string(),
            done: false,
            created_at: 0.0,
            metadata: md.clone(),
        };
        let v = todo.clone().to_convex()?;
        let back = Todo::from_convex(v)?;
        assert_eq!(back.owner, todo.owner);
        assert_eq!(back.text, todo.text);
        assert_eq!(back.done, todo.done);
        assert_eq!(back.metadata, md);
    }
    Ok(())
}
