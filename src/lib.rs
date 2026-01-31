//! An efficient and concurrent interning library for JSON values.

#![forbid(missing_docs, unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod detail;

use blazinterner::{Arena, DeltaEncoding};
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};

/// An arena to store interned JSON values.
#[derive(Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct Jinterners {
    string: Arena<str, Box<str>>,
    iarray: DeltaEncoding<Arena<detail::IArray>, detail::IArrayAccumulator>,
    iobject: DeltaEncoding<Arena<detail::IObject>, detail::IObjectAccumulator>,
}

#[cfg(feature = "get-size2")]
impl Jinterners {
    /// Gets the size in bytes of the underlying string arena.
    pub fn get_size_strings(&self) -> usize {
        self.string.get_size()
    }

    /// Gets the size in bytes of the underlying array arena.
    pub fn get_size_arrays(&self) -> usize {
        self.iarray.get_size()
    }

    /// Gets the size in bytes of the underlying object arena.
    pub fn get_size_objects(&self) -> usize {
        self.iobject.get_size()
    }
}

#[cfg(feature = "debug")]
impl Jinterners {
    /// Prints a summary of the storage used by the underlying string arena to
    /// stdout.
    pub fn print_summary_strings(&self, prefix: &str, title: &str, total_bytes: usize) {
        self.string.print_summary(prefix, title, total_bytes);
    }

    /// Prints a summary of the storage used by the underlying array arena to
    /// stdout.
    pub fn print_summary_arrays(&self, prefix: &str, title: &str, total_bytes: usize) {
        self.iarray.print_summary(prefix, title, total_bytes);
    }

    /// Prints a summary of the storage used by the underlying object arena to
    /// stdout.
    pub fn print_summary_objects(&self, prefix: &str, title: &str, total_bytes: usize) {
        self.iobject.print_summary(prefix, title, total_bytes);
    }
}

/// An interned JSON value.
#[derive(Default, Debug, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct IValue(detail::IValue);

impl IValue {
    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    pub fn from(interners: &Jinterners, source: Value) -> Self {
        Self(detail::IValue::from(interners, source))
    }

    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    pub fn from_ref(interners: &Jinterners, source: &Value) -> Self {
        Self(detail::IValue::from_ref(interners, source))
    }

    /// Retrieves the corresponding [`serde_json::Value`] inside the given
    /// [`Jinterners`] arena.
    pub fn lookup(&self, interners: &Jinterners) -> Value {
        self.0.lookup(interners)
    }
}
