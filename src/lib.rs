//! An efficient and concurrent interning library for JSON values.

#![forbid(missing_docs, unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod detail;

use blazinterner::{Arena, DeltaEncoding};
use detail::{IArray, IArrayAccumulator, IObject, IObjectAccumulator};
pub use detail::{IValue, ValueRef};
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};

/// An arena to store interned JSON values.
#[derive(Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct Jinterners {
    string: Arena<str, Box<str>>,
    iarray: DeltaEncoding<Arena<IArray>, IArrayAccumulator>,
    iobject: DeltaEncoding<Arena<IObject>, IObjectAccumulator>,
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
