//! Phantom-typed document IDs.
//!
//! `Id<User>` and `Id<Message>` are distinct types even though both wrap a
//! `DeveloperDocumentId` — the compiler rejects mixing them up. At the wire
//! level they serialize identically to the string form Convex uses for IDs.

use std::{
    fmt::{
        self,
        Debug,
        Display,
    },
    hash::{
        Hash,
        Hasher,
    },
    marker::PhantomData,
    str::FromStr,
};

use anyhow::anyhow;
use value::{
    ConvexValue,
    DeveloperDocumentId,
};

use crate::{
    convert::{
        FromConvex,
        ToConvex,
    },
    document::ConvexDocument,
};

/// A phantom-typed reference to a document of type `T`.
pub struct Id<T: ConvexDocument> {
    inner: DeveloperDocumentId,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: ConvexDocument> Id<T> {
    /// Wrap an existing [`DeveloperDocumentId`] with a phantom tag.
    pub fn new(inner: DeveloperDocumentId) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Unwrap the underlying [`DeveloperDocumentId`].
    pub fn into_developer_id(self) -> DeveloperDocumentId {
        self.inner
    }

    /// Borrow the underlying [`DeveloperDocumentId`].
    pub fn as_developer_id(&self) -> &DeveloperDocumentId {
        &self.inner
    }
}

impl<T: ConvexDocument> Copy for Id<T> {}

impl<T: ConvexDocument> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ConvexDocument> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T: ConvexDocument> Eq for Id<T> {}

impl<T: ConvexDocument> Hash for Id<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<T: ConvexDocument> Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Id<{}>({})",
            std::any::type_name::<T>(),
            self.inner.encode()
        )
    }
}

impl<T: ConvexDocument> Display for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner.encode())
    }
}

impl<T: ConvexDocument> FromStr for Id<T> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let inner = DeveloperDocumentId::decode(s).map_err(|e| anyhow!(e))?;
        Ok(Self::new(inner))
    }
}

impl<T: ConvexDocument> From<Id<T>> for DeveloperDocumentId {
    fn from(id: Id<T>) -> Self {
        id.inner
    }
}

impl<T: ConvexDocument> ToConvex for Id<T> {
    fn to_convex(self) -> anyhow::Result<ConvexValue> {
        Ok(self.inner.into())
    }
}

impl<T: ConvexDocument> FromConvex for Id<T> {
    fn from_convex(value: ConvexValue) -> anyhow::Result<Self> {
        let inner = DeveloperDocumentId::try_from(value)?;
        Ok(Self::new(inner))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        str::FromStr,
    };

    use common::{
        schemas::TableDefinition,
        types::TableName,
    };
    use value::ConvexObject;

    use super::*;
    use crate::document::{
        FieldReference,
        IndexReference,
    };

    /// Hand-rolled minimal `ConvexDocument` so `Id<User>` compiles
    /// without dragging the derive macro through the crate's own
    /// test build (the derive emits `::convex_native_core::...` which
    /// doesn't resolve from inside the crate under test).
    #[derive(Clone, Debug)]
    struct User;
    #[derive(Clone, Debug)]
    struct Message;

    #[derive(Copy, Clone, Debug)]
    #[allow(dead_code)]
    enum Noop {}
    impl FieldReference for Noop {
        fn as_str(&self) -> &'static str {
            match *self {}
        }
    }
    impl IndexReference for Noop {
        fn as_str(&self) -> &'static str {
            match *self {}
        }

        fn fields(&self) -> &'static [&'static str] {
            match *self {}
        }
    }

    macro_rules! impl_convex_doc {
        ($name:ident, $table:literal) => {
            impl ConvexDocument for $name {
                type Field = Noop;
                type Index = Noop;
                type Patch = ();

                fn table_name() -> TableName {
                    TableName::from_str($table).unwrap()
                }

                fn table_definition() -> TableDefinition {
                    TableDefinition {
                        table_name: Self::table_name(),
                        indexes: Default::default(),
                        staged_db_indexes: Default::default(),
                        text_indexes: Default::default(),
                        staged_text_indexes: Default::default(),
                        vector_indexes: Default::default(),
                        staged_vector_indexes: Default::default(),
                        document_type: None,
                    }
                }

                fn to_convex_object(&self) -> anyhow::Result<ConvexObject> {
                    ConvexObject::try_from(std::collections::BTreeMap::new())
                }

                fn from_convex_object(_obj: ConvexObject) -> anyhow::Result<Self> {
                    Ok(Self)
                }
            }
        };
    }
    impl_convex_doc!(User, "users");
    impl_convex_doc!(Message, "messages");

    fn hash_one<T: Hash>(t: &T) -> u64 {
        let mut h = DefaultHasher::new();
        t.hash(&mut h);
        h.finish()
    }

    #[test]
    fn new_wraps_and_as_developer_id_exposes_the_inner_value() {
        let inner = DeveloperDocumentId::MIN;
        let id: Id<User> = Id::new(inner);
        assert_eq!(*id.as_developer_id(), inner);
        assert_eq!(id.into_developer_id(), inner);
    }

    #[test]
    fn copy_and_clone_preserve_equality() {
        let id: Id<User> = Id::new(DeveloperDocumentId::MIN);
        let copied = id;
        let cloned = id;
        assert_eq!(id, copied);
        assert_eq!(id, cloned);
    }

    #[test]
    fn equal_ids_hash_to_the_same_value() {
        // Hash / PartialEq must be consistent; map keys rely on it.
        let a: Id<User> = Id::new(DeveloperDocumentId::MIN);
        let b: Id<User> = Id::new(DeveloperDocumentId::MIN);
        assert_eq!(a, b);
        assert_eq!(hash_one(&a), hash_one(&b));
    }

    #[test]
    fn display_and_from_str_round_trip() {
        let id: Id<User> = Id::new(DeveloperDocumentId::MIN);
        let rendered = id.to_string();
        let parsed: Id<User> = Id::from_str(&rendered).expect("parse");
        assert_eq!(id, parsed);
    }

    #[test]
    fn from_str_rejects_garbage() {
        // An invalid encoded id must fail to parse rather than
        // constructing a silently-bogus typed reference.
        let bad: Result<Id<User>, _> = Id::from_str("not-a-valid-convex-id");
        assert!(bad.is_err());
    }

    #[test]
    fn debug_includes_phantom_type_name() {
        // The Debug impl prefixes with `Id<T>(…)` using
        // `type_name::<T>()` so a logged `Id<User>` can be visually
        // distinguished from an `Id<Message>` carrying the same
        // underlying bytes. Relying on exact module path would be
        // brittle, so we just confirm the short type names appear.
        let u: Id<User> = Id::new(DeveloperDocumentId::MIN);
        let m: Id<Message> = Id::new(DeveloperDocumentId::MIN);
        assert!(format!("{u:?}").contains("User"));
        assert!(format!("{m:?}").contains("Message"));
    }

    #[test]
    fn into_developer_id_drops_the_phantom_tag() {
        // The escape hatch `Id<T> -> DeveloperDocumentId` must be
        // lossless — that's the contract `ToConvex` depends on.
        let inner = DeveloperDocumentId::MIN;
        let id: Id<User> = Id::new(inner);
        let plain: DeveloperDocumentId = id.into();
        assert_eq!(plain, inner);
    }

    #[test]
    fn to_convex_and_from_convex_round_trip() {
        let id: Id<User> = Id::new(DeveloperDocumentId::MIN);
        let value = id.to_convex().expect("encode");
        let parsed = <Id<User> as FromConvex>::from_convex(value).expect("decode");
        assert_eq!(id, parsed);
    }
}
