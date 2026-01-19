//! An efficient and concurrent interning library for JSON values.

#![forbid(missing_docs, unsafe_code)]

mod detail;

use blazinterner::Arena;
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};

/// An arena to store interned JSON values.
#[derive(Default)]
#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct Jinterners {
    string: Arena<str, Box<str>>,
    iarray: Arena<detail::IArray>,
    iobject: Arena<detail::IObject>,
}

/// An interned JSON value.
#[derive(Default, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct IValue(detail::IValue);

impl IValue {
    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    pub fn from(interners: &Jinterners, source: Value) -> Self {
        Self(detail::IValue::from(interners, source))
    }

    /// Retrieves the corresponding [`serde_json::Value`] inside the given
    /// [`Jinterners`] arena.
    pub fn lookup(&self, interners: &Jinterners) -> Value {
        self.0.lookup(interners)
    }
}
