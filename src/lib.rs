//! An efficient and concurrent interning library for JSON values.

#![forbid(
    missing_docs,
    unsafe_op_in_unsafe_fn,
    clippy::missing_safety_doc,
    clippy::multiple_unsafe_ops_per_block,
    clippy::undocumented_unsafe_blocks
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "delta")]
mod delta;
mod detail;

#[cfg(feature = "retain")]
use bit_set::BitSet;
use blazinterner::{ArenaSlice, ArenaStr, InternedSlice};
#[cfg(feature = "delta")]
pub use delta::DeltaEncoding;
pub use detail::mapping::Mapping;
use detail::mapping::{MappingNoStrings, MappingStrings};
pub use detail::{IValue, InternedStrKey, MapRef, ValueRef};
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
use serde_json::Value;
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};

/// An arena to store interned JSON values.
#[derive(Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct Jinterners {
    string: ArenaStr,
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
    /// Interns the given [`serde_json::Value`] into this arena.
    pub fn intern(&self, source: Value) -> IValue {
        IValue::from(self, source)
    }

    /// Interns the given [`serde_json::Value`] into this arena.
    pub fn intern_ref(&self, source: &Value) -> IValue {
        IValue::from_ref(self, source)
    }

    /// Interns the given [`serde_json::Value`] into this arena.
    pub fn intern_mut(&mut self, source: Value) -> IValue {
        IValue::from_mut(self, source)
    }

    /// Interns the given [`serde_json::Value`] into this arena.
    pub fn intern_ref_mut(&mut self, source: &Value) -> IValue {
        IValue::from_ref_mut(self, source)
    }

    /// Retrieves the object key associated to the given string, or [`None`] if
    /// no such key has been interned in this arena.
    ///
    /// This can be useful in combination with [`MapRef::get_by_key()`].
    pub fn find_key(&self, key: &str) -> Option<InternedStrKey> {
        self.string.find(key).map(InternedStrKey)
    }

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
                        let string_iter = self.string.iter();
                        let num_strings = string_iter.len();
                        let mut string = ArenaStr::with_capacity(num_strings, self.string.bytes());
                        for s in string_iter {
                            string.push_mut(s);
                        }

                        (
                            Jinterners {
                                string,
                                iarray,
                                iobject,
                            },
                            mapping_opt.promote(num_strings as u32),
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
        let string_map = self.string.sort();
        let iarray_map = self.iarray.sort();
        let iobject_map = self.iobject.sort();

        let mapping = Mapping {
            string: string_map.forward,
            iarray: iarray_map.forward,
            iobject: iobject_map.forward,
        };
        if mapping.is_identity() {
            return None;
        }

        let iobject_map_iter = iobject_map.reverse.iter();

        let mut jinterners = Jinterners {
            string: self.string.map(&string_map.reverse),
            iarray: self
                .iarray
                .map2(&iarray_map.reverse, |ivalue| mapping.map(*ivalue)),
            iobject: ArenaSlice::with_capacity(iobject_map_iter.len(), self.iobject.items()),
        };

        let mut buffer = Vec::new();
        for i in iobject_map_iter {
            let object = self.iobject.lookup(InternedSlice::from_id(i));
            buffer.extend(
                object
                    .iter()
                    .map(|(k, ivalue)| (mapping.map_str_key(*k), mapping.map(*ivalue))),
            );
            buffer.sort_unstable_by_key(|(k, _)| *k);
            jinterners.iobject.push_copy_mut(&buffer);
            buffer.clear();
        }

        Some((jinterners, mapping))
    }

    fn optimize_once_strings(&self) -> Option<(Jinterners, MappingStrings)> {
        let string_map = self.string.sort();
        let mapping = MappingStrings {
            string: string_map.forward,
        };

        if mapping.is_identity() {
            return None;
        }

        let iarray_iter = self.iarray.iter();
        let iobject_iter = self.iobject.iter();

        let mut jinterners = Jinterners {
            string: self.string.map(&string_map.reverse),
            iarray: ArenaSlice::with_capacity(iarray_iter.len(), self.iarray.items()),
            iobject: ArenaSlice::with_capacity(iobject_iter.len(), self.iobject.items()),
        };

        for array in iarray_iter {
            let iter = array.iter().map(|ivalue| mapping.map(*ivalue));
            // SAFETY: The iterator length is trusted, as it's a simple mapping on a slice
            // iterator.
            unsafe { jinterners.iarray.push_iter_mut(iter) };
        }

        let mut buffer = Vec::new();
        for object in iobject_iter {
            buffer.extend(
                object
                    .iter()
                    .map(|(k, ivalue)| (mapping.map_str_key(*k), mapping.map(*ivalue))),
            );
            buffer.sort_unstable_by_key(|(k, _)| *k);
            jinterners.iobject.push_copy_mut(&buffer);
            buffer.clear();
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
        let iarray_map = self.iarray.sort();
        let iobject_map = self.iobject.sort();

        let mapping = MappingNoStrings {
            iarray: iarray_map.forward,
            iobject: iobject_map.forward,
        };
        if mapping.is_identity() {
            return None;
        }

        let iarray = self
            .iarray
            .map2(&iarray_map.reverse, |ivalue| mapping.map(*ivalue));
        let iobject = self.iobject.map2(&iobject_map.reverse, |(k, ivalue)| {
            (*k, mapping.map(*ivalue))
        });
        Some((iarray, iobject, mapping))
    }

    /// Returns a [`Jinterners`] containing only the given [`IValue`]s of this
    /// arena, as well as all values transitively referenced by them.
    ///
    /// Returns [`None`] if everything contained in this [`Jinterners`] was
    /// retained.
    ///
    /// [`IValue`]s rooted in this [`Jinterners`] need to be converted using the
    /// resulting [`Mapping`] to be used in the destination [`Jinterners`].
    #[cfg(feature = "retain")]
    pub fn retain(&self, values: impl Iterator<Item = IValue>) -> Option<(Jinterners, Mapping)> {
        let mut retained_strings = BitSet::with_capacity(self.string.strings());
        let mut retained_arrays = BitSet::with_capacity(self.iarray.slices());
        let mut retained_objects = BitSet::with_capacity(self.iobject.slices());

        IValue::retain_all(
            values,
            &mut retained_strings,
            &mut retained_arrays,
            &mut retained_objects,
            &self.iarray,
            &self.iobject,
        );

        let string_map = self
            .string
            .retain(|i| retained_strings.contains(i.id() as usize));
        let iarray_map = self
            .iarray
            .retain(|i| retained_arrays.contains(i.id() as usize));
        let iobject_map = self
            .iobject
            .retain(|i| retained_objects.contains(i.id() as usize));

        let mapping = Mapping {
            string: string_map.forward,
            iarray: iarray_map.forward,
            iobject: iobject_map.forward,
        };
        if mapping.is_identity() {
            return None;
        }

        let jinterners = Jinterners {
            string: self.string.map(&string_map.reverse),
            iarray: self
                .iarray
                .map2(&iarray_map.reverse, |ivalue| mapping.map(*ivalue)),
            iobject: self.iobject.map2(&iobject_map.reverse, |(k, ivalue)| {
                // Retained keys are still in the same order, so we don't need to re-sort them.
                (mapping.map_str_key(*k), mapping.map(*ivalue))
            }),
        };

        Some((jinterners, mapping))
    }
}

#[cfg(test)]
mod test {
    #[cfg(feature = "retain")]
    use super::*;
    #[cfg(feature = "retain")]
    use serde_json::json;

    #[cfg(feature = "retain")]
    #[test]
    fn retain() {
        let interners = Jinterners::default();

        let john = interners.intern(json!({
            "name": "John",
            "surname": "Doe",
            "address": {
                "number": 42,
                "street": "Way",
                "city": "Big City",
            }
        }));
        let mary = interners.intern(json!({
            "name": "Mary",
            "surname": "Smith",
            "address": {
                "number": 123,
                "square": "Central Square",
                "city": "Small Town",
            }
        }));

        assert_eq!(
            mary.lookup(&interners),
            json!({
                "name": "Mary",
                "surname": "Smith",
                "address": {
                    "number": 123,
                    "square": "Central Square",
                    "city": "Small Town",
                }
            })
        );

        // Retaining everything doesn't change the arena.
        assert!(interners.retain([john, mary].into_iter()).is_none());

        let (filtered, mapping) = interners.retain([john].into_iter()).unwrap();
        let mapped_john = mapping.map(john);

        assert_eq!(
            mapped_john.lookup(&filtered),
            json!({
                "name": "John",
                "surname": "Doe",
                "address": {
                    "number": 42,
                    "street": "Way",
                    "city": "Big City",
                }
            })
        );
    }
}
