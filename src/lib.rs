//! An efficient and concurrent interning library for JSON values.

#![forbid(missing_docs, unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "delta")]
mod delta;
mod detail;

use blazinterner::{Arena, ArenaSlice, Interned, InternedSlice};
#[cfg(feature = "delta")]
pub use delta::DeltaEncoding;
use detail::InternedStrKey;
pub use detail::mapping::Mapping;
use detail::mapping::{MappingNoStrings, MappingStrings, RevMappingImpl};
pub use detail::{IValue, ValueRef};
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::cmp::Ordering;

/// An arena to store interned JSON values.
#[derive(Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct Jinterners {
    string: Arena<str, Box<str>>,
    iarray: ArenaSlice<IValue>,
    iobject: ArenaSlice<(InternedStrKey, IValue)>,
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
    /// Returns an optimized version of this [`Jinterners`], or [`None`] if the
    /// iteration `limit` is set to zero.
    ///
    /// [`IValue`]s rooted in this [`Jinterners`] need to be converted using the
    /// resulting [`Mapping`] to be used in the destination [`Jinterners`].
    pub fn optimize(&self, limit: Option<usize>) -> Option<(Jinterners, Mapping)> {
        if limit == Some(0) {
            return None;
        }

        let mut optimized = self.optimize_once_strings().map(|(jinterners, mapping)| {
            let mapping = mapping.promote(
                jinterners.iarray.slices() as u32,
                jinterners.iobject.slices() as u32,
            );
            (jinterners, mapping)
        });

        let mut i = 0;
        loop {
            if limit == Some(i) {
                break;
            }

            let jinterners = match optimized {
                None => self,
                Some((ref jinterners, _)) => jinterners,
            };
            let (jinterners, mapping) = match jinterners.optimize_once_no_strings() {
                None => break,
                Some((iarray, iobject, mapping_opt)) => match optimized {
                    None => {
                        let num_strings = self.string.len() as u32;
                        let mut string = Arena::with_capacity(self.string.len());
                        for i in 0..num_strings {
                            string.push_mut(Interned::from_id(i).lookup_ref(&self.string).into());
                        }

                        (
                            Jinterners {
                                string,
                                iarray,
                                iobject,
                            },
                            mapping_opt.promote(num_strings),
                        )
                    }
                    Some((mut jinterners, mapping)) => {
                        jinterners.iarray = iarray;
                        jinterners.iobject = iobject;
                        (jinterners, mapping.compose(mapping_opt))
                    }
                },
            };
            optimized = Some((jinterners, mapping));

            i = i.wrapping_add(1);
        }
        optimized
    }

    /// Returns a partially optimized version of this [`Jinterners`], or
    /// [`None`] if this instance was already optimized.
    ///
    /// This only runs one iteration of the optimization routine, so you may
    /// want to use [`optimize()`](Self::optimize) instead.
    ///
    /// [`IValue`]s rooted in this [`Jinterners`] need to be converted using the
    /// resulting [`Mapping`] to be used in the destination [`Jinterners`].
    pub fn optimize_once(&self) -> Option<(Jinterners, Mapping)> {
        let string_rev = self.optimized_mapping_strings();
        let iarray_rev = self.optimized_mapping_arrays();
        let iobject_rev = self.optimized_mapping_objects();

        let mapping = Mapping {
            string: string_rev.reverse(),
            iarray: iarray_rev.reverse(),
            iobject: iobject_rev.reverse(),
        };
        if mapping.is_identity() {
            return None;
        }

        let mut jinterners = Jinterners {
            string: Arena::with_capacity(self.string.len()),
            iarray: ArenaSlice::with_capacity(self.iarray.slices(), self.iarray.items()),
            iobject: ArenaSlice::with_capacity(self.iobject.slices(), self.iobject.items()),
        };

        for i in string_rev.iter() {
            jinterners
                .string
                .push_mut(Interned::from_id(i).lookup_ref(&self.string).into());
        }
        for i in iarray_rev.iter() {
            let array = InternedSlice::from_id(i).lookup(&self.iarray);
            let array: Box<[_]> = array.iter().map(|ivalue| mapping.map(*ivalue)).collect();
            jinterners.iarray.push_mut(&array);
        }
        for i in iobject_rev.iter() {
            let object = InternedSlice::from_id(i).lookup(&self.iobject);
            let mut object: Box<[_]> = object
                .iter()
                .map(|(k, ivalue)| (mapping.map_str_key(*k), mapping.map(*ivalue)))
                .collect();
            object.sort_unstable_by_key(|(k, _)| *k);
            jinterners.iobject.push_mut(&object);
        }

        Some((jinterners, mapping))
    }

    fn optimize_once_strings(&self) -> Option<(Jinterners, MappingStrings)> {
        let string_rev = self.optimized_mapping_strings();
        let mapping = MappingStrings {
            string: string_rev.reverse(),
        };

        if mapping.is_identity() {
            return None;
        }

        let mut jinterners = Jinterners {
            string: Arena::with_capacity(self.string.len()),
            iarray: ArenaSlice::with_capacity(self.iarray.slices(), self.iarray.items()),
            iobject: ArenaSlice::with_capacity(self.iobject.slices(), self.iobject.items()),
        };

        for i in string_rev.iter() {
            jinterners
                .string
                .push_mut(Interned::from_id(i).lookup_ref(&self.string).into());
        }
        for i in 0..self.iarray.slices() as u32 {
            let array = InternedSlice::from_id(i).lookup(&self.iarray);
            let array: Box<[_]> = array.iter().map(|ivalue| mapping.map(*ivalue)).collect();
            jinterners.iarray.push_mut(&array);
        }
        for i in 0..self.iobject.slices() as u32 {
            let object = InternedSlice::from_id(i).lookup(&self.iobject);
            let mut object: Box<[_]> = object
                .iter()
                .map(|(k, ivalue)| (mapping.map_str_key(*k), mapping.map(*ivalue)))
                .collect();
            object.sort_unstable_by_key(|(k, _)| *k);
            jinterners.iobject.push_mut(&object);
        }

        Some((jinterners, mapping))
    }

    #[expect(clippy::type_complexity)]
    fn optimize_once_no_strings(
        &self,
    ) -> Option<(
        ArenaSlice<IValue>,
        ArenaSlice<(InternedStrKey, IValue)>,
        MappingNoStrings,
    )> {
        let iarray_rev = self.optimized_mapping_arrays();
        let iobject_rev = self.optimized_mapping_objects();

        let mapping = MappingNoStrings {
            iarray: iarray_rev.reverse(),
            iobject: iobject_rev.reverse(),
        };
        if mapping.is_identity() {
            return None;
        }

        let mut iarray = ArenaSlice::with_capacity(self.iarray.slices(), self.iarray.items());
        for i in iarray_rev.iter() {
            let array = InternedSlice::from_id(i).lookup(&self.iarray);
            let array: Box<[_]> = array.iter().map(|ivalue| mapping.map(*ivalue)).collect();
            iarray.push_mut(&array);
        }

        let mut iobject = ArenaSlice::with_capacity(self.iobject.slices(), self.iobject.items());
        for i in iobject_rev.iter() {
            let object = InternedSlice::from_id(i).lookup(&self.iobject);
            let object: Box<[_]> = object
                .iter()
                .map(|(k, ivalue)| (*k, mapping.map(*ivalue)))
                .collect();
            iobject.push_mut(&object);
        }

        Some((iarray, iobject, mapping))
    }

    fn optimized_mapping_strings(&self) -> RevMappingImpl {
        let mut mapping: Vec<u32> = (0..self.string.len() as u32).collect();
        mapping
            .sort_by_cached_key(|i| CustomStrOrd(Interned::from_id(*i).lookup_ref(&self.string)));
        RevMappingImpl(mapping.into_boxed_slice())
    }

    fn optimized_mapping_arrays(&self) -> RevMappingImpl {
        let mut mapping: Vec<u32> = (0..self.iarray.slices() as u32).collect();
        mapping.sort_by_cached_key(|i| {
            CustomSliceOrd(InternedSlice::from_id(*i).lookup(&self.iarray))
        });
        RevMappingImpl(mapping.into_boxed_slice())
    }

    fn optimized_mapping_objects(&self) -> RevMappingImpl {
        let mut mapping: Vec<u32> = (0..self.iobject.slices() as u32).collect();
        mapping.sort_by_cached_key(|i| {
            CustomSliceOrd(InternedSlice::from_id(*i).lookup(&self.iobject))
        });
        RevMappingImpl(mapping.into_boxed_slice())
    }
}

#[derive(PartialEq, Eq)]
struct CustomStrOrd<'a>(&'a str);

impl PartialOrd for CustomStrOrd<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CustomStrOrd<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.cmp(other.0))
    }
}

#[derive(PartialEq, Eq)]
struct CustomSliceOrd<'a, T>(&'a [T]);

impl<T: Ord> PartialOrd for CustomSliceOrd<'_, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for CustomSliceOrd<'_, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.cmp(other.0))
    }
}
