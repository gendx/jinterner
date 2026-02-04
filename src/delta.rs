#[cfg(feature = "get-size2")]
use get_size2::GetSize;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

/// Wrapper around a [`Jinterners`](crate::Jinterners) that uses delta encoding
/// to serialize it.
#[derive(Default, PartialEq, Eq)]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct DeltaEncoding<T> {
    pub(crate) inner: T,
}

impl<T> DeltaEncoding<T> {
    /// Creates a new wrapper around the given data.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    /// Extracts the inner data from this wrapper.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Deref for DeltaEncoding<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for DeltaEncoding<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> Debug for DeltaEncoding<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
