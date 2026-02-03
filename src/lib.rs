//! An efficient and concurrent interning library for JSON values.

#![forbid(missing_docs, unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod detail;

use blazinterner::{Arena, ArenaSlice, DeltaEncoding, Interned, InternedSlice};
use detail::{IArrayAccumulator, IObjectAccumulator, InternedStrKey};
pub use detail::{IValue, Mapping, ValueRef};
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::ops::Deref;

/// An arena to store interned JSON values.
#[derive(Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct Jinterners {
    string: Arena<str, Box<str>>,
    iarray: DeltaEncoding<ArenaSlice<IValue>, IArrayAccumulator>,
    iobject: DeltaEncoding<ArenaSlice<(InternedStrKey, IValue)>, IObjectAccumulator>,
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

impl Jinterners {
    /// Returns an optimized version of this [`Jinterners`].
    ///
    /// [`IValue`]s rooted in this [`Jinterners`] need to be converted using the
    /// resulting [`Mapping`] to be used in the destination [`Jinterners`].
    pub fn optimize(&self) -> (Jinterners, Mapping) {
        let (string_rev, string) = self.optimized_mapping_strings();
        let (iarray_rev, iarray) = self.optimized_mapping_arrays();
        let (iobject_rev, iobject) = self.optimized_mapping_objects();

        let mapping = Mapping {
            string,
            iarray,
            iobject,
        };

        let mut jinterners = Jinterners {
            string: Arena::with_capacity(self.string.len()),
            iarray: DeltaEncoding::new(ArenaSlice::with_capacity(
                self.iarray.slices(),
                self.iarray.items(),
            )),
            iobject: DeltaEncoding::new(ArenaSlice::with_capacity(
                self.iobject.slices(),
                self.iobject.items(),
            )),
        };

        for i in string_rev {
            jinterners
                .string
                .push_mut(Interned::from_id(i).lookup_ref(&self.string).into());
        }
        for i in iarray_rev {
            let array = InternedSlice::from_id(i).lookup(&self.iarray);
            let array: Box<[_]> = array.iter().map(|ivalue| mapping.map(*ivalue)).collect();
            jinterners.iarray.push_mut(&array);
        }
        for i in iobject_rev {
            let object = InternedSlice::from_id(i).lookup(&self.iobject);
            let object: Box<[_]> = object
                .iter()
                .map(|(k, ivalue)| (mapping.map_str_key(*k), mapping.map(*ivalue)))
                .collect();
            jinterners.iobject.push_mut(&object);
        }

        (jinterners, mapping)
    }

    fn optimized_mapping_strings(&self) -> (Vec<u32>, Box<[u32]>) {
        let mut mapping: Vec<u32> = (0..self.string.len() as u32).collect();
        mapping.sort_by(|i, j| {
            let a: &str = Interned::from_id(*i).lookup_ref(&self.string);
            let b: &str = Interned::from_id(*j).lookup_ref(&self.string);
            a.len().cmp(&b.len()).then_with(|| a.cmp(b))
        });

        let reverse = Self::reverse(&mapping);
        (mapping, reverse)
    }

    fn optimized_mapping_arrays(&self) -> (Vec<u32>, Box<[u32]>) {
        let mut mapping: Vec<u32> = (0..self.iarray.slices() as u32).collect();
        mapping.sort_by(|i, j| {
            let a: &[IValue] = InternedSlice::from_id(*i).lookup(self.iarray.deref());
            let b: &[IValue] = InternedSlice::from_id(*j).lookup(self.iarray.deref());
            a.len().cmp(&b.len()).then_with(|| a.cmp(b))
        });

        let reverse = Self::reverse(&mapping);
        (mapping, reverse)
    }

    fn optimized_mapping_objects(&self) -> (Vec<u32>, Box<[u32]>) {
        let mut mapping: Vec<u32> = (0..self.iobject.slices() as u32).collect();
        mapping.sort_by(|i, j| {
            let a: &[(InternedStrKey, IValue)] =
                InternedSlice::from_id(*i).lookup(self.iobject.deref());
            let b: &[(InternedStrKey, IValue)] =
                InternedSlice::from_id(*j).lookup(self.iobject.deref());
            a.len().cmp(&b.len()).then_with(|| a.cmp(b))
        });

        let reverse = Self::reverse(&mapping);
        (mapping, reverse)
    }

    fn reverse(mapping: &[u32]) -> Box<[u32]> {
        let mut reverse = vec![0; mapping.len()];
        for i in 0..mapping.len() as u32 {
            reverse[mapping[i as usize] as usize] = i;
        }
        reverse.into_boxed_slice()
    }
}
