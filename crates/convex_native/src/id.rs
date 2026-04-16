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
