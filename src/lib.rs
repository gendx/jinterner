//! An efficient and concurrent interning library for JSON values.

#![forbid(
    missing_docs,
    unsafe_op_in_unsafe_fn,
    clippy::missing_safety_doc,
    clippy::multiple_unsafe_ops_per_block,
    clippy::undocumented_unsafe_blocks
)]
#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

#[cfg(feature = "delta")]
mod delta;
mod detail;
#[cfg(feature = "serde")]
mod util;

use alloc::vec::Vec;
#[cfg(feature = "std")]
pub use blazinterner::StdBuildHasher;
use blazinterner::{Arena, ArenaSlice, ArenaStr, InternedSlice, U32};
pub use blazinterner::{DefaultBuildHasher, HashbrownBuildHasher};
#[cfg(feature = "serde")]
use blazinterner::{
    ExtendArena, ExtendArenaSlice, ExtendArenaStr, Snapshot as SnapshotItem,
    SnapshotDiff as SnapshotItemDiff, SnapshotMark as SnapshotItemMark, SnapshotSlice,
    SnapshotSliceDiff, SnapshotSliceMark, SnapshotStr, SnapshotStrDiff, SnapshotStrMark,
};
#[cfg(feature = "retain")]
use blazinterner::{RetainBuilder as RetainItemBuilder, RetainSliceBuilder, RetainStrBuilder};
use core::fmt::Debug;
use core::hash::BuildHasher;
#[cfg(feature = "delta")]
pub use delta::DeltaEncoding;
use detail::Float64;
#[cfg(all(feature = "serde", feature = "sync"))]
pub use detail::InterningDeserializer;
pub use detail::mapping::Mapping;
use detail::mapping::{MappingNoScalars, MappingScalars};
#[cfg(feature = "serde")]
pub use detail::{BoundValue, InterningDeserializerMut};
pub use detail::{IValue, InternedStrKey, MapRef, ValueRef};
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
#[cfg(feature = "serde")]
use serde::de::{DeserializeSeed, Error, SeqAccess, Visitor};
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
#[cfg(feature = "serde")]
use util::Buffer;
#[cfg(feature = "serde")]
pub use util::BufferPool;

/// An arena to store interned JSON values.
pub struct Jinterners<H = DefaultBuildHasher> {
    uint64: Arena<u64, u64, H, U32>,
    int64: Arena<i64, i64, H, U32>,
    float64: Arena<Float64, Float64, H, U32>,
    string: ArenaStr<H, U32>,
    iarray: ArenaSlice<IValue<H>, H, U32>,
    iobject: ArenaSlice<(InternedStrKey<H>, IValue<H>), H, U32>,
}

impl<H> Default for Jinterners<H>
where
    H: Default,
{
    fn default() -> Self {
        Self {
            uint64: Default::default(),
            int64: Default::default(),
            float64: Default::default(),
            string: Default::default(),
            iarray: Default::default(),
            iobject: Default::default(),
        }
    }
}

impl<H> Debug for Jinterners<H> {
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt.debug_struct("Jinterners")
            .field("uint64", &self.uint64)
            .field("int64", &self.int64)
            .field("float64", &self.float64)
            .field("string", &self.string)
            .field("iarray", &self.iarray)
            .field("iobject", &self.iobject)
            .finish()
    }
}

impl<H> Clone for Jinterners<H>
where
    H: Default + BuildHasher,
{
    fn clone(&self) -> Self {
        Self {
            uint64: self.uint64.clone(),
            int64: self.int64.clone(),
            float64: self.float64.clone(),
            string: self.string.clone(),
            iarray: self.iarray.clone(),
            iobject: self.iobject.clone(),
        }
    }
}

impl<H> PartialEq for Jinterners<H> {
    fn eq(&self, other: &Self) -> bool {
        self.uint64 == other.uint64
            && self.int64 == other.int64
            && self.float64 == other.float64
            && self.string == other.string
            && self.iarray == other.iarray
            && self.iobject == other.iobject
    }
}

impl<H> Eq for Jinterners<H> {}

#[cfg(feature = "get-size2")]
impl<H> GetSize for Jinterners<H> {
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        let (size_uint64, tracker) = GetSize::get_heap_size_with_tracker(&self.uint64, tracker);
        let (size_int64, tracker) = GetSize::get_heap_size_with_tracker(&self.int64, tracker);
        let (size_float64, tracker) = GetSize::get_heap_size_with_tracker(&self.float64, tracker);
        let (size_string, tracker) = GetSize::get_heap_size_with_tracker(&self.string, tracker);
        let (size_iarray, tracker) = GetSize::get_heap_size_with_tracker(&self.iarray, tracker);
        let (size_iobject, tracker) = GetSize::get_heap_size_with_tracker(&self.iobject, tracker);
        (
            size_uint64 + size_int64 + size_float64 + size_string + size_iarray + size_iobject,
            tracker,
        )
    }
}

#[cfg(feature = "serde")]
impl<H> Serialize for Jinterners<H> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (
            &self.uint64,
            &self.int64,
            &self.float64,
            &self.string,
            &self.iarray,
            &self.iobject,
        )
            .serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, H> Deserialize<'de> for Jinterners<H>
where
    H: Default + BuildHasher,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (uint64, int64, float64, string, iarray, iobject) =
            Deserialize::deserialize(deserializer)?;
        Ok(Self {
            uint64,
            int64,
            float64,
            string,
            iarray,
            iobject,
        })
    }
}

#[cfg(feature = "get-size2")]
impl<H> Jinterners<H> {
    /// Gets the size in bytes of the underlying [`u64`] arena.
    pub fn get_size_u64s(&self) -> usize {
        self.uint64.get_size()
    }

    /// Gets the size in bytes of the underlying [`i64`] arena.
    pub fn get_size_i64s(&self) -> usize {
        self.int64.get_size()
    }

    /// Gets the size in bytes of the underlying [`f64`] arena.
    pub fn get_size_f64s(&self) -> usize {
        self.float64.get_size()
    }

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

#[cfg(all(feature = "debug", feature = "std"))]
impl<H> Jinterners<H> {
    /// Prints a summary of the storage used by the underlying [`u64`] arena to
    /// stdout.
    pub fn print_summary_u64s(&self, prefix: &str, title: &str, total_bytes: usize) {
        self.uint64.print_summary(prefix, title, total_bytes);
    }

    /// Prints a summary of the storage used by the underlying [`i64`] arena to
    /// stdout.
    pub fn print_summary_i64s(&self, prefix: &str, title: &str, total_bytes: usize) {
        self.int64.print_summary(prefix, title, total_bytes);
    }

    /// Prints a summary of the storage used by the underlying [`f64`] arena to
    /// stdout.
    pub fn print_summary_f64s(&self, prefix: &str, title: &str, total_bytes: usize) {
        self.float64.print_summary(prefix, title, total_bytes);
    }

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

/// Statistics about a [`Jinterners`] arena.
///
/// This struct is returned by the [`stats()`](Jinterners::stats) method.
pub struct Stats {
    /// Number of [`u64`]s in the arena.
    pub u64s: usize,
    /// Number of [`i64`]s in the arena.
    pub i64s: usize,
    /// Number of [`f64`]s in the arena.
    pub f64s: usize,
    /// Number of strings in the arena.
    pub strings: usize,
    /// Number of string bytes in the arena.
    pub string_bytes: usize,
    /// Number of arrays in the arena.
    pub arrays: usize,
    /// Number of array items in the arena.
    pub array_items: usize,
    /// Number of objects in the arena.
    pub objects: usize,
    /// Number of object key-value pairs in the arena.
    pub object_key_values: usize,
}

impl<H> Jinterners<H> {
    /// Returns statistics about this arena.
    ///
    /// Note that because [`Jinterners`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change if
    /// other threads are inserting values.
    pub fn stats(&self) -> Stats {
        Stats {
            u64s: self.uint64.len(),
            i64s: self.int64.len(),
            f64s: self.float64.len(),
            strings: self.string.strings(),
            string_bytes: self.string.bytes(),
            arrays: self.iarray.slices(),
            array_items: self.iarray.items(),
            objects: self.iobject.slices(),
            object_key_values: self.iobject.items(),
        }
    }

    /// Returns a snapshot of this arena.
    ///
    /// Note that because [`Jinterners`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread.
    ///
    /// Snapshots can be diffed into a [`SnapshotDiff`], which is useful to
    /// serialize an arena incrementally as values are added to it.
    #[cfg(feature = "serde")]
    pub fn snapshot(&self) -> Snapshot<'_, H> {
        Snapshot {
            uint64: self.uint64.snapshot(),
            int64: self.int64.snapshot(),
            float64: self.float64.snapshot(),
            string: self.string.snapshot(),
            iarray: self.iarray.snapshot(),
            iobject: self.iobject.snapshot(),
        }
    }
}

impl<H> Jinterners<H>
where
    H: BuildHasher,
{
    /// Interns the given [`serde_json::Value`] into this arena.
    ///
    /// See also [`intern_mut()`](Self::intern_mut), which is more efficient if
    /// you hold a mutable reference to this [`Jinterners`] arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern(&self, source: Value) -> IValue<H> {
        IValue::from(self, source)
    }

    /// Interns the given [`serde_json::Value`] into this arena.
    ///
    /// See also [`intern_ref_mut()`](Self::intern_ref_mut), which is more
    /// efficient if you hold a mutable reference to this [`Jinterners`] arena
    /// as it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern_ref(&self, source: &Value) -> IValue<H> {
        IValue::from_ref(self, source)
    }

    /// Interns the given [`serde_json::Value`] into this arena.
    ///
    /// Contrary to [`intern()`](Self::intern), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// this [`Jinterners`] arena.
    pub fn intern_mut(&mut self, source: Value) -> IValue<H> {
        IValue::from_mut(self, source)
    }

    /// Interns the given [`serde_json::Value`] into this arena.
    ///
    /// Contrary to [`intern_ref()`](Self::intern_ref), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to this [`Jinterners`] arena.
    pub fn intern_ref_mut(&mut self, source: &Value) -> IValue<H> {
        IValue::from_ref_mut(self, source)
    }

    /// Retrieves the given interned value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or a
    /// panic will happen.
    ///
    /// See also [`lookup_ref()`](Self::lookup_ref) if you only need a shallow
    /// view.
    pub fn lookup(&self, value: &IValue<H>) -> Value {
        value.lookup(self)
    }

    /// Retrieves the given interned value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or a
    /// panic will happen.
    ///
    /// Contrary to [`lookup()`](Self::lookup), this function doesn't create a
    /// deep copy of the value, and is therefore likely more efficient if you
    /// only need to query specific object field(s) or array element(s).
    pub fn lookup_ref(&self, value: &IValue<H>) -> ValueRef<'_, H> {
        value.lookup_ref(self)
    }

    /// Interns the given string as a map key into this arena.
    ///
    /// See also [`find_key`](Self::find_key) is you don't need to add the key
    /// to the arena but just use it to lookup values in JSON maps.
    ///
    /// See also [`intern_key_mut()`](Self::intern_key_mut), which is more
    /// efficient if you hold a mutable reference to this [`Jinterners`]
    /// arena as it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern_key(&self, key: &str) -> InternedStrKey<H> {
        InternedStrKey(self.string.intern(key))
    }

    /// Interns the given string as a map key into this arena.
    ///
    /// See also [`find_key_mut`](Self::find_key_mut) is you don't need to add
    /// the key to the arena but just use it to lookup values in JSON maps.
    ///
    /// Contrary to [`intern_key()`](Self::intern_key), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to this [`Jinterners`] arena.
    pub fn intern_key_mut(&mut self, key: &str) -> InternedStrKey<H> {
        InternedStrKey(self.string.intern_mut(key))
    }

    /// Retrieves the given interned key from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this key, otherwise an arbitrary string will be returned or a
    /// panic will happen.
    pub fn lookup_key(&self, key: InternedStrKey<H>) -> &str {
        self.string.lookup(key.0)
    }

    /// Retrieves the object key associated to the given string, or [`None`] if
    /// no such key has been interned in this arena.
    ///
    /// This can be useful in combination with [`MapRef::get_by_key()`].
    ///
    /// See also [`find_key_mut()`](Self::find_key_mut), which is more efficient
    /// if you hold a mutable reference to this [`Jinterners`] arena as it
    /// avoids acquiring locks.
    pub fn find_key(&self, key: &str) -> Option<InternedStrKey<H>> {
        self.string.find(key).map(InternedStrKey)
    }

    /// Retrieves the object key associated to the given string, or [`None`] if
    /// no such key has been interned in this arena.
    ///
    /// This can be useful in combination with [`MapRef::get_by_key()`].
    ///
    /// Contrary to [`find_key()`](Self::find_key), no locks are held internally
    /// because this function already takes an exclusive mutable reference
    /// to this [`Jinterners`] arena.
    pub fn find_key_mut(&mut self, key: &str) -> Option<InternedStrKey<H>> {
        self.string.find_mut(key).map(InternedStrKey)
    }
}

impl<H> Jinterners<H>
where
    H: Default + BuildHasher,
{
    /// Returns an optimized version of this [`Jinterners`], or [`None`] if this
    /// instance was already optimized or the iteration `limit` is set to zero.
    ///
    /// [`IValue`]s rooted in this [`Jinterners`] need to be converted using the
    /// resulting [`Mapping`] to be used in the destination [`Jinterners`].
    pub fn optimize(&self, limit: Option<usize>) -> Option<(Jinterners<H>, Mapping)> {
        if limit == Some(0) {
            return None;
        }

        let mut optimized = self.optimize_once_scalars().map(|(jinterners, mapping)| {
            let mapping = mapping.promote(jinterners.iarray.slices(), jinterners.iobject.slices());
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
            let (jinterners, mapping) = match jinterners.optimize_once_no_scalars() {
                None => break,
                Some((iarray, iobject, mapping_opt)) => match optimized {
                    None => {
                        let uint64 = self.uint64.clone();
                        let int64 = self.int64.clone();
                        let float64 = self.float64.clone();
                        let string = self.string.clone();

                        let num_uint64s = uint64.len();
                        let num_int64s = int64.len();
                        let num_float64s = float64.len();
                        let num_strings = string.strings();

                        (
                            Jinterners {
                                uint64,
                                int64,
                                float64,
                                string,
                                iarray,
                                iobject,
                            },
                            mapping_opt.promote(num_uint64s, num_int64s, num_float64s, num_strings),
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
    pub fn optimize_once(&self) -> Option<(Jinterners<H>, Mapping)> {
        let u64_map = self.uint64.sort();
        let i64_map = self.int64.sort();
        let f64_map = self.float64.sort();
        let string_map = self.string.sort();
        let iarray_map = self.iarray.sort();
        let iobject_map = self.iobject.sort();

        let mapping = Mapping {
            uint64: u64_map.forward,
            int64: i64_map.forward,
            float64: f64_map.forward,
            string: string_map.forward,
            iarray: iarray_map.forward,
            iobject: iobject_map.forward,
        };
        if mapping.is_identity() {
            return None;
        }

        let iobject_map_iter = iobject_map.reverse.iter();

        let mut jinterners = Jinterners {
            uint64: self.uint64.map(&u64_map.reverse),
            int64: self.int64.map(&i64_map.reverse),
            float64: self.float64.map(&f64_map.reverse),
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

    fn optimize_once_scalars(&self) -> Option<(Jinterners<H>, MappingScalars)> {
        let u64_map = self.uint64.sort();
        let i64_map = self.int64.sort();
        let f64_map = self.float64.sort();
        let string_map = self.string.sort();

        let mapping = MappingScalars {
            uint64: u64_map.forward,
            int64: i64_map.forward,
            float64: f64_map.forward,
            string: string_map.forward,
        };

        if mapping.is_identity() {
            return None;
        }

        let iarray_iter = self.iarray.iter();
        let iobject_iter = self.iobject.iter();

        let mut jinterners = Jinterners {
            uint64: self.uint64.map(&u64_map.reverse),
            int64: self.int64.map(&i64_map.reverse),
            float64: self.float64.map(&f64_map.reverse),
            string: self.string.map(&string_map.reverse),
            iarray: ArenaSlice::with_capacity(iarray_iter.len(), self.iarray.items()),
            iobject: ArenaSlice::with_capacity(iobject_iter.len(), self.iobject.items()),
        };

        for array in iarray_iter {
            let iter = array.iter().map(|ivalue| mapping.map(*ivalue));
            // SAFETY: The iterator length is trusted, as it's a simple mapping
            // on a slice iterator.
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
    fn optimize_once_no_scalars(
        &self,
    ) -> Option<(
        ArenaSlice<IValue<H>, H, U32>,
        ArenaSlice<(InternedStrKey<H>, IValue<H>), H, U32>,
        MappingNoScalars,
    )> {
        let iarray_map = self.iarray.sort();
        let iobject_map = self.iobject.sort();

        let mapping = MappingNoScalars {
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
    pub fn retain_values(
        &self,
        values: impl Iterator<Item = IValue<H>>,
    ) -> Option<(Jinterners<H>, Mapping)> {
        let mut builder = self.retain_builder();
        for v in values {
            builder.insert(v);
        }
        builder.build()
    }
}

impl<H> Jinterners<H> {
    /// Returns a builder allowing to select items to retain, and create a
    /// [`Jinterners`] arena containing only these.
    #[cfg(feature = "retain")]
    pub fn retain_builder(&self) -> RetainBuilder<'_, H> {
        RetainBuilder {
            jinterners: self,
            u64s: self.uint64.retain_builder(),
            i64s: self.int64.retain_builder(),
            f64s: self.float64.retain_builder(),
            strings: self.string.retain_builder(),
            arrays: self.iarray.retain_builder(),
            objects: self.iobject.retain_builder(),
            queue_arrays: Vec::new(),
            queue_objects: Vec::new(),
        }
    }
}

/// A builder to select items to retain in a [`Jinterners`] arena.
///
/// This struct is created by the
/// [`retain_builder()`](Jinterners::retain_builder) method on [`Jinterners`].
#[cfg(feature = "retain")]
#[expect(clippy::type_complexity)]
pub struct RetainBuilder<'a, H = DefaultBuildHasher> {
    jinterners: &'a Jinterners<H>,
    u64s: RetainItemBuilder<u64, u64, H, U32>,
    i64s: RetainItemBuilder<i64, i64, H, U32>,
    f64s: RetainItemBuilder<Float64, Float64, H, U32>,
    strings: RetainStrBuilder<H, U32>,
    arrays: RetainSliceBuilder<IValue<H>, H, U32>,
    objects: RetainSliceBuilder<(InternedStrKey<H>, IValue<H>), H, U32>,
    queue_arrays: Vec<InternedSlice<IValue<H>, H, U32>>,
    queue_objects: Vec<InternedSlice<(InternedStrKey<H>, IValue<H>), H, U32>>,
}

#[cfg(feature = "retain")]
impl<H> RetainBuilder<'_, H> {
    /// Marks the given value as retained.
    ///
    /// Returns [`true`] if the value is newly inserted and [`false`] if it was
    /// already inserted before or doesn't need interning (e.g. because it
    /// contains a simple value like an integer).
    pub fn insert(&mut self, value: IValue<H>) -> bool {
        value.retain(self)
    }
}

#[cfg(feature = "retain")]
impl<H> RetainBuilder<'_, H>
where
    H: Default + BuildHasher,
{
    /// Returns a [`Jinterners`] containing only the retained [`IValue`]s, as
    /// well as all values transitively referenced by them.
    ///
    /// Returns [`None`] if everything contained in the corresponding
    /// [`Jinterners`] was retained.
    ///
    /// [`IValue`]s rooted in the original [`Jinterners`] need to be converted
    /// using the resulting [`Mapping`] to be used in the destination
    /// [`Jinterners`].
    pub fn build(mut self) -> Option<(Jinterners<H>, Mapping)> {
        loop {
            if let Some(a) = self.queue_arrays.pop() {
                for v in self.jinterners.iarray.lookup(a) {
                    v.retain(&mut self);
                }
            } else if let Some(o) = self.queue_objects.pop() {
                for (k, v) in self.jinterners.iobject.lookup(o) {
                    self.strings.insert(k.0);
                    v.retain(&mut self);
                }
            } else {
                break;
            }
        }

        let u64_map = self.u64s.build();
        let i64_map = self.i64s.build();
        let f64_map = self.f64s.build();
        let string_map = self.strings.build();
        let iarray_map = self.arrays.build();
        let iobject_map = self.objects.build();

        let mapping = Mapping {
            uint64: u64_map.forward,
            int64: i64_map.forward,
            float64: f64_map.forward,
            string: string_map.forward,
            iarray: iarray_map.forward,
            iobject: iobject_map.forward,
        };
        if mapping.is_identity() {
            return None;
        }

        let jinterners = Jinterners {
            uint64: self.jinterners.uint64.map(&u64_map.reverse),
            int64: self.jinterners.int64.map(&i64_map.reverse),
            float64: self.jinterners.float64.map(&f64_map.reverse),
            string: self.jinterners.string.map(&string_map.reverse),
            iarray: self
                .jinterners
                .iarray
                .map2(&iarray_map.reverse, |ivalue| mapping.map(*ivalue)),
            iobject: self
                .jinterners
                .iobject
                .map2(&iobject_map.reverse, |(k, ivalue)| {
                    // Retained keys are still in the same order, so we don't
                    // need to re-sort them.
                    (mapping.map_str_key(*k), mapping.map(*ivalue))
                }),
        };

        Some((jinterners, mapping))
    }
}

/// A mark indicates the position of a [`Snapshot`] in a [`Jinterners`] arena.
///
/// This allows creating a difference between two snapshots, via
/// [`Snapshot::diff()`].
#[cfg(feature = "serde")]
pub struct SnapshotMark<H = DefaultBuildHasher> {
    uint64: SnapshotItemMark<u64, u64, H, U32>,
    int64: SnapshotItemMark<i64, i64, H, U32>,
    float64: SnapshotItemMark<Float64, Float64, H, U32>,
    string: SnapshotStrMark<H, U32>,
    iarray: SnapshotSliceMark<IValue<H>, H, U32>,
    iobject: SnapshotSliceMark<(InternedStrKey<H>, IValue<H>), H, U32>,
}

#[cfg(feature = "serde")]
impl<H> Default for SnapshotMark<H> {
    fn default() -> Self {
        Self {
            uint64: Default::default(),
            int64: Default::default(),
            float64: Default::default(),
            string: Default::default(),
            iarray: Default::default(),
            iobject: Default::default(),
        }
    }
}

#[cfg(feature = "serde")]
impl<H> Clone for SnapshotMark<H> {
    fn clone(&self) -> Self {
        *self
    }
}

#[cfg(feature = "serde")]
impl<H> Copy for SnapshotMark<H> {}

/// Snapshot of a [`Jinterners`] arena.
///
/// This struct is created by the [`snapshot()`](Jinterners::snapshot) function
/// on [`Jinterners`].
#[cfg(feature = "serde")]
pub struct Snapshot<'a, H = DefaultBuildHasher> {
    uint64: SnapshotItem<'a, u64, u64, H, U32>,
    int64: SnapshotItem<'a, i64, i64, H, U32>,
    float64: SnapshotItem<'a, Float64, Float64, H, U32>,
    string: SnapshotStr<'a, H, U32>,
    iarray: SnapshotSlice<'a, IValue<H>, H, U32>,
    iobject: SnapshotSlice<'a, (InternedStrKey<H>, IValue<H>), H, U32>,
}

#[cfg(feature = "serde")]
impl<H> Snapshot<'_, H> {
    /// Returns the position of this snapshot in the arena.
    pub fn mark(&self) -> SnapshotMark<H> {
        SnapshotMark {
            uint64: self.uint64.mark(),
            int64: self.int64.mark(),
            float64: self.float64.mark(),
            string: self.string.mark(),
            iarray: self.iarray.mark(),
            iobject: self.iobject.mark(),
        }
    }

    /// Returns the difference between this snapshot and a previous mark.
    pub fn diff(&self, start: SnapshotMark<H>) -> SnapshotDiff<'_, H> {
        SnapshotDiff {
            uint64: self.uint64.diff(start.uint64),
            int64: self.int64.diff(start.int64),
            float64: self.float64.diff(start.float64),
            string: self.string.diff(start.string),
            iarray: self.iarray.diff(start.iarray),
            iobject: self.iobject.diff(start.iobject),
        }
    }
}

/// Difference between two snapshots of a [`Jinterners`] arena.
///
/// This is useful to serialize an arena incrementally as more values are added
/// to it.
#[cfg(feature = "serde")]
pub struct SnapshotDiff<'a, H = DefaultBuildHasher> {
    uint64: SnapshotItemDiff<'a, u64, u64, H, U32>,
    int64: SnapshotItemDiff<'a, i64, i64, H, U32>,
    float64: SnapshotItemDiff<'a, Float64, Float64, H, U32>,
    string: SnapshotStrDiff<'a, H, U32>,
    iarray: SnapshotSliceDiff<'a, IValue<H>, H, U32>,
    iobject: SnapshotSliceDiff<'a, (InternedStrKey<H>, IValue<H>), H, U32>,
}

#[cfg(feature = "serde")]
impl<H> Serialize for SnapshotDiff<'_, H> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (
            &self.uint64,
            &self.int64,
            &self.float64,
            &self.string,
            &self.iarray,
            &self.iobject,
        )
            .serialize(serializer)
    }
}

/// Wrapper to extend a [`Jinterners`] arena incrementally, typically by
/// deserializing a sequence of serialized [`SnapshotDiff`].
#[cfg(feature = "serde")]
pub struct ExtendJinterners<'a, H = DefaultBuildHasher> {
    interners: &'a mut Jinterners<H>,
}

#[cfg(feature = "serde")]
impl<'a, H> ExtendJinterners<'a, H> {
    /// Wraps the given arena to deserialize into it.
    pub fn new(interners: &'a mut Jinterners<H>) -> Self {
        Self { interners }
    }
}

#[cfg(feature = "serde")]
impl<'de, H> DeserializeSeed<'de> for ExtendJinterners<'_, H>
where
    H: Default + BuildHasher,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(
            6,
            ExtendJinternersVisitor {
                interners: self.interners,
            },
        )
    }
}

#[cfg(feature = "serde")]
struct ExtendJinternersVisitor<'a, H> {
    interners: &'a mut Jinterners<H>,
}

#[cfg(feature = "serde")]
impl<'de, H> Visitor<'de> for ExtendJinternersVisitor<'_, H>
where
    H: Default + BuildHasher,
{
    type Value = ();

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("a tuple with 6 elements")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        seq.next_element_seed(ExtendArena::new(&mut self.interners.uint64))?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        seq.next_element_seed(ExtendArena::new(&mut self.interners.int64))?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;
        seq.next_element_seed(ExtendArena::new(&mut self.interners.float64))?
            .ok_or_else(|| A::Error::invalid_length(2, &self))?;
        seq.next_element_seed(ExtendArenaStr::new(&mut self.interners.string))?
            .ok_or_else(|| A::Error::invalid_length(3, &self))?;
        seq.next_element_seed(ExtendArenaSlice::new(&mut self.interners.iarray))?
            .ok_or_else(|| A::Error::invalid_length(4, &self))?;
        seq.next_element_seed(ExtendArenaSlice::new(&mut self.interners.iobject))?
            .ok_or_else(|| A::Error::invalid_length(5, &self))?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[cfg(feature = "retain")]
    #[test]
    fn retain() {
        let mut interners: Jinterners = Jinterners::default();

        let john = interners.intern_mut(json!({
            "name": "John",
            "surname": "Doe",
            "address": {
                "number": 42,
                "street": "Way",
                "city": "Big City",
            }
        }));
        let mary = interners.intern_mut(json!({
            "name": "Mary",
            "surname": "Smith",
            "address": {
                "number": 123,
                "square": "Central Square",
                "city": "Small Town",
            }
        }));

        assert_eq!(
            interners.lookup(&mary),
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
        assert!(interners.retain_values([john, mary].into_iter()).is_none());

        let (filtered, mapping) = interners.retain_values([john].into_iter()).unwrap();
        let mapped_john = mapping.map(john);

        assert_eq!(
            filtered.lookup(&mapped_john),
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

    #[test]
    fn test_optimize_strings() {
        let mut interners: Jinterners = Jinterners::default();

        let mary = interners.intern_mut(json!("Mary"));
        let john = interners.intern_mut(json!("John"));

        // No optimization happens with a limit of zero.
        assert!(interners.optimize(Some(0)).is_none());

        // Optimization sorts strings.
        let (interners, mapping) = interners.optimize(None).unwrap();
        let mary = mapping.map(mary);
        assert_eq!(interners.lookup(&mary), json!("Mary"));
        let john = mapping.map(john);
        assert_eq!(interners.lookup(&john), json!("John"));

        // Optimizing a second time is a no-op.
        assert!(interners.optimize(None).is_none());
    }

    #[test]
    fn test_optimize_once_strings() {
        let mut interners: Jinterners = Jinterners::default();

        let mary = interners.intern_mut(json!("Mary"));
        let john = interners.intern_mut(json!("John"));

        // Optimization sorts strings.
        let (interners, mapping) = interners.optimize_once().unwrap();
        let mary = mapping.map(mary);
        assert_eq!(interners.lookup(&mary), json!("Mary"));
        let john = mapping.map(john);
        assert_eq!(interners.lookup(&john), json!("John"));

        // Optimizing a second time is a no-op when the interner only contains
        // strings.
        assert!(interners.optimize_once().is_none());
    }
}
