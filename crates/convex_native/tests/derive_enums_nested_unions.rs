//! Tests `#[derive(ConvexEnum)]`, `#[derive(ConvexNested)]`, and
//! `#[derive(ConvexUnion)]`.

use convex_native::{
    prelude::*,
    ConvexDocument,
    FromConvex,
    ToConvex,
};
use value::{
    ConvexObject,
    ConvexValue,
};

// ── ConvexEnum ────────────────────────────────────────────────────

#[derive(ConvexEnum, Debug, Clone, PartialEq)]
pub enum Role {
    Admin,
    Member,
    #[convex(rename = "guest-account")]
    Guest,
}

#[test]
fn convex_enum_round_trips() {
    for role in [Role::Admin, Role::Member, Role::Guest] {
        let cv = role.clone().to_convex().expect("to_convex");
        let back = Role::from_convex(cv).expect("from_convex");
        assert_eq!(back, role);
    }
}

#[test]
fn convex_enum_rename_takes_effect() {
    let cv = Role::Guest.to_convex().unwrap();
    match cv {
        ConvexValue::String(s) => assert_eq!(s.as_ref(), "guest-account"),
        _ => panic!("expected String"),
    }
}

#[test]
fn convex_enum_rejects_unknown_tag() {
    let cv = ConvexValue::try_from("unknown".to_string()).unwrap();
    let err = Role::from_convex(cv).unwrap_err();
    assert!(err.to_string().contains("unknown Role variant"));
}

// ── ConvexNested ──────────────────────────────────────────────────

#[derive(ConvexNested, Debug, Clone, PartialEq)]
pub struct Address {
    pub street: String,
    pub city: String,
}

#[derive(ConvexDocument, Debug, Clone, PartialEq)]
#[convex(table = "accounts")]
pub struct Account {
    pub name: String,
    pub address: Address,
    pub role: Role,
}

#[test]
fn convex_nested_round_trips_inside_document() {
    let account = Account {
        name: "Dana".into(),
        address: Address {
            street: "1 Elm".into(),
            city: "Portland".into(),
        },
        role: Role::Admin,
    };
    let obj = account.clone().to_convex_object().unwrap();
    let back = Account::from_convex_object(obj).unwrap();
    assert_eq!(back, account);
}

// ── ConvexUnion ───────────────────────────────────────────────────

#[derive(ConvexUnion, Debug, Clone, PartialEq)]
#[convex(tag = "type")]
pub enum NotificationChannel {
    Email {
        address: String,
    },
    Sms {
        phone: String,
    },
    #[convex(rename = "push_notification")]
    Push {
        device_token: String,
    },
}

#[test]
fn convex_union_round_trips_each_variant() {
    let cases = vec![
        NotificationChannel::Email {
            address: "a@b.com".into(),
        },
        NotificationChannel::Sms { phone: "+1".into() },
        NotificationChannel::Push {
            device_token: "tok".into(),
        },
    ];
    for case in cases {
        let cv = case.clone().to_convex().unwrap();
        let back = NotificationChannel::from_convex(cv).unwrap();
        assert_eq!(back, case);
    }
}

#[test]
fn convex_union_emits_tag_field() {
    let email = NotificationChannel::Email {
        address: "x@y".into(),
    };
    let cv = email.to_convex().unwrap();
    let ConvexValue::Object(obj) = cv else {
        panic!("expected object")
    };
    let map: std::collections::BTreeMap<_, _> = obj.into();
    let tag = map
        .get(&"type".parse::<value::FieldName>().unwrap())
        .expect("tag field");
    match tag {
        ConvexValue::String(s) => assert_eq!(s.as_ref(), "email"),
        _ => panic!("expected String tag"),
    }
}

#[test]
fn convex_union_rejects_unknown_tag() {
    // Build a manual object with an unknown discriminant.
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "type".parse::<value::FieldName>().unwrap(),
        ConvexValue::try_from("carrier_pigeon".to_string()).unwrap(),
    );
    map.insert(
        "something".parse::<value::FieldName>().unwrap(),
        ConvexValue::try_from("value".to_string()).unwrap(),
    );
    let obj = ConvexObject::try_from(map).unwrap();
    let err = NotificationChannel::from_convex(ConvexValue::Object(obj)).unwrap_err();
    assert!(err.to_string().contains("unknown NotificationChannel"));
}

#[test]
fn convex_union_rejects_missing_tag() {
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "address".parse::<value::FieldName>().unwrap(),
        ConvexValue::try_from("x".to_string()).unwrap(),
    );
    let obj = ConvexObject::try_from(map).unwrap();
    let err = NotificationChannel::from_convex(ConvexValue::Object(obj)).unwrap_err();
    assert!(err.to_string().contains("missing discriminant"));
}
