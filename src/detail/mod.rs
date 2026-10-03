#[cfg(feature = "serde")]
mod de;
pub mod mapping;
#[cfg(feature = "serde")]
mod ser;

#[cfg(feature = "serde")]
use super::BufferPool;
use super::Jinterners;
#[cfg(feature = "retain")]
use super::RetainBuilder;
use alloc::boxed::Box;
#[cfg(feature = "serde")]
use alloc::string::String;
#[cfg(feature = "serde")]
use alloc::vec::Vec;
use blazinterner::{ArenaStr, DefaultBuildHasher, InternedSlice, InternedStr};
use core::cmp::Ordering;
use core::fmt::Debug;
use core::hash::{BuildHasher, Hash, Hasher};
#[cfg(feature = "serde")]
use core::marker::PhantomData;
#[cfg(feature = "serde")]
pub use de::InterningDeserializerMut;
#[cfg(feature = "serde")]
use de::ValueDeserializer;
#[cfg(all(feature = "serde", feature = "sync"))]
pub use de::sync::InterningDeserializer;
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
use ordered_float::OrderedFloat;
#[cfg(all(feature = "serde", feature = "sync"))]
use ser::ValueSerializer;
#[cfg(feature = "serde")]
use ser::ValueSerializerMut;
#[cfg(feature = "serde")]
use serde::de::{EnumAccess, VariantAccess, Visitor};
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(feature = "serde")]
use serde_json::Deserializer as SerdeJsonDeserializer;
use serde_json::{Number, Value};
#[cfg(all(feature = "serde", feature = "std"))]
use std::io::{Read, Write};

/// An [`IValue`] interned value together with a reference to the associated
/// [`Jinterners`] arena.
///
/// This is for example useful to serialize the value as plain JSON data.
#[cfg(feature = "serde")]
pub struct BoundValue<'a, H = DefaultBuildHasher> {
    value: &'a IValue<H>,
    interners: &'a Jinterners<H>,
}

#[cfg(feature = "serde")]
impl<'a, H> BoundValue<'a, H> {
    /// Binds the given [`IValue`] with its associated [`Jinterners`] arena.
    pub fn new(value: &'a IValue<H>, interners: &'a Jinterners<H>) -> Self {
        Self { value, interners }
    }
}

/// An interned key for JSON objects.
///
/// You can obtain a key with [`Jinterners::find_key()`] and use it to lookup
/// values in JSON objects with [`MapRef::get_by_key()`].
pub struct InternedStrKey<H = DefaultBuildHasher>(pub(crate) InternedStr<H>);

impl<H> Default for InternedStrKey<H> {
    fn default() -> Self {
        InternedStrKey(InternedStr::from_id(0))
    }
}

impl<H> Debug for InternedStrKey<H> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("InternedStrKey").field(&self.0).finish()
    }
}

impl<H> Clone for InternedStrKey<H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H> Copy for InternedStrKey<H> {}

impl<H> PartialEq for InternedStrKey<H> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<H> Eq for InternedStrKey<H> {}

impl<H> PartialOrd for InternedStrKey<H> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<H> Ord for InternedStrKey<H> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<H> Hash for InternedStrKey<H> {
    fn hash<G>(&self, state: &mut G)
    where
        G: Hasher,
    {
        self.0.hash(state);
    }
}

#[cfg(feature = "get-size2")]
impl<H> GetSize for InternedStrKey<H> {
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        GetSize::get_heap_size_with_tracker(&self.0, tracker)
    }
}

#[cfg(feature = "serde")]
impl<H> Serialize for InternedStrKey<H> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, H> Deserialize<'de> for InternedStrKey<H> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = Deserialize::deserialize(deserializer)?;
        Ok(Self(inner))
    }
}

/// An interned JSON value.
pub struct IValue<H = DefaultBuildHasher>(IValueImpl<H>);

impl<H> Default for IValue<H> {
    fn default() -> Self {
        IValue(Default::default())
    }
}

impl<H> Debug for IValue<H> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

impl<H> Clone for IValue<H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H> Copy for IValue<H> {}

impl<H> PartialEq for IValue<H> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<H> Eq for IValue<H> {}

impl<H> PartialOrd for IValue<H> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<H> Ord for IValue<H> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<H> Hash for IValue<H> {
    fn hash<G>(&self, state: &mut G)
    where
        G: Hasher,
    {
        self.0.hash(state);
    }
}

#[cfg(feature = "get-size2")]
impl<H> GetSize for IValue<H> {
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        GetSize::get_heap_size_with_tracker(&self.0, tracker)
    }
}

#[cfg(feature = "serde")]
impl<H> Serialize for IValue<H> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, H> Deserialize<'de> for IValue<H> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = Deserialize::deserialize(deserializer)?;
        Ok(Self(inner))
    }
}

impl<H> IValue<H> {
    /// Interns a null JSON value.
    pub fn null() -> Self {
        Self(IValueImpl::Null)
    }

    /// Interns a boolean JSON value.
    pub fn bool(b: bool) -> Self {
        Self(IValueImpl::Bool(b))
    }

    /// Interns an integer JSON value.
    pub fn u64(x: u64) -> Self {
        Self(IValueImpl::U64(x))
    }

    /// Interns an integer JSON value.
    pub fn i64(x: i64) -> Self {
        Self(IValueImpl::I64(x))
    }

    /// Interns a floating-point JSON value.
    pub fn f64(x: f64) -> Self {
        Self(IValueImpl::F64(Float64(OrderedFloat(x))))
    }
}

impl<H> IValue<H>
where
    H: BuildHasher,
{
    /// Interns a string JSON value.
    ///
    /// See also [`string_mut()`](Self::string_mut), which is more efficient if
    /// you hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn string(interners: &Jinterners<H>, s: &str) -> Self {
        Self(IValueImpl::String(interners.string.intern(s)))
    }

    /// Interns a string JSON value.
    ///
    /// Contrary to [`string()`](Self::string), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    pub fn string_mut(interners: &mut Jinterners<H>, s: &str) -> Self {
        Self(IValueImpl::String(interners.string.intern_mut(s)))
    }

    /// Interns a JSON array, whose items are already interned.
    ///
    /// See also [`array_mut()`](Self::array_mut), which is more efficient if
    /// you hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn array(interners: &Jinterners<H>, a: &[Self]) -> Self {
        Self(IValueImpl::Array(interners.iarray.intern_copy(a)))
    }

    /// Interns a JSON array, whose items are already interned.
    ///
    /// Contrary to [`array()`](Self::array), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    pub fn array_mut(interners: &mut Jinterners<H>, a: &[Self]) -> Self {
        Self(IValueImpl::Array(interners.iarray.intern_copy_mut(a)))
    }

    /// Interns a JSON object, whose values are already interned.
    ///
    /// See also [`object_mut()`](Self::object_mut), which is more efficient if
    /// you hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn object<'a>(interners: &Jinterners<H>, o: impl Iterator<Item = (&'a str, Self)>) -> Self {
        let mut io: Box<[_]> = o
            .map(|(k, v)| (InternedStrKey(interners.string.intern(k)), v))
            .collect();
        io.sort_unstable_by_key(|(k, _)| *k);
        Self(IValueImpl::Object(interners.iobject.intern_copy(&io)))
    }

    /// Interns a JSON object, whose values are already interned.
    ///
    /// Contrary to [`object()`](Self::object), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    pub fn object_mut<'a>(
        interners: &mut Jinterners<H>,
        o: impl Iterator<Item = (&'a str, Self)>,
    ) -> Self {
        let mut io: Box<[_]> = o
            .map(|(k, v)| (InternedStrKey(interners.string.intern_mut(k)), v))
            .collect();
        io.sort_unstable_by_key(|(k, _)| *k);
        Self(IValueImpl::Object(interners.iobject.intern_copy_mut(&io)))
    }

    /// Interns a JSON object, whose keys and values are already interned.
    ///
    /// See also [`object_from_keys_mut()`](Self::object_from_keys_mut), which
    /// is more efficient if you hold a mutable reference to the [`Jinterners`]
    /// arena as it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn object_from_keys(
        interners: &Jinterners<H>,
        o: impl Iterator<Item = (InternedStrKey<H>, Self)>,
    ) -> Self {
        let mut io: Box<[_]> = o.collect();
        io.sort_unstable_by_key(|(k, _)| *k);
        Self(IValueImpl::Object(interners.iobject.intern_copy(&io)))
    }

    /// Interns a JSON object, whose keys and values are already interned.
    ///
    /// Contrary to [`object_from_keys()`](Self::object_from_keys), no locks are
    /// held internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    pub fn object_from_keys_mut(
        interners: &mut Jinterners<H>,
        o: impl Iterator<Item = (InternedStrKey<H>, Self)>,
    ) -> Self {
        let mut io: Box<[_]> = o.collect();
        io.sort_unstable_by_key(|(k, _)| *k);
        Self(IValueImpl::Object(interners.iobject.intern_copy_mut(&io)))
    }

    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    #[cfg(feature = "sync")]
    pub(crate) fn from(interners: &Jinterners<H>, source: Value) -> Self {
        Self(IValueImpl::from(interners, source))
    }

    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    #[cfg(feature = "sync")]
    pub(crate) fn from_ref(interners: &Jinterners<H>, source: &Value) -> Self {
        Self(IValueImpl::from_ref(interners, source))
    }

    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    pub(crate) fn from_mut(interners: &mut Jinterners<H>, source: Value) -> Self {
        Self(IValueImpl::from_mut(interners, source))
    }

    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    pub(crate) fn from_ref_mut(interners: &mut Jinterners<H>, source: &Value) -> Self {
        Self(IValueImpl::from_ref_mut(interners, source))
    }

    /// Retrieves the corresponding [`serde_json::Value`] inside the given
    /// [`Jinterners`] arena.
    pub(crate) fn lookup(&self, interners: &Jinterners<H>) -> Value {
        self.0.lookup(interners)
    }

    /// Performs a shallow lookup of this value inside the given [`Jinterners`]
    /// arena.
    pub(crate) fn lookup_ref<'a>(&self, interners: &'a Jinterners<H>) -> ValueRef<'a, H> {
        self.0.lookup_ref(interners)
    }

    /// Convert an arbitrary type into an [`IValue`] using that type's
    /// [`Serialize`] implementation.
    ///
    /// See also [`from_value_mut()`](Self::from_value_mut), which is more
    /// efficient if you hold a mutable reference to the [`Jinterners`] arena as
    /// it avoids acquiring locks.
    #[cfg(all(feature = "serde", feature = "sync"))]
    pub fn from_value<T>(
        value: T,
        interners: &Jinterners<H>,
    ) -> Result<Self, serde_json::error::Error>
    where
        T: Serialize,
    {
        value
            .serialize(ValueSerializer {
                interners,
                buffers: &mut BufferPool::default(),
            })
            .map(IValue)
    }

    /// Convert an arbitrary type into an [`IValue`] using that type's
    /// [`Serialize`] implementation.
    ///
    /// Contrary to [`from_value()`](Self::from_value), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    #[cfg(feature = "serde")]
    pub fn from_value_mut<T>(
        value: T,
        interners: &mut Jinterners<H>,
    ) -> Result<Self, serde_json::error::Error>
    where
        T: Serialize,
    {
        value
            .serialize(ValueSerializerMut {
                interners,
                buffers: &mut BufferPool::default(),
            })
            .map(IValue)
    }

    /// Convert an [`IValue`] into an arbitrary type using that type's
    /// [`Deserialize`] implementation.
    #[cfg(feature = "serde")]
    pub fn to_value<'de, T>(
        &self,
        interners: &'de Jinterners<H>,
    ) -> Result<T, serde_json::error::Error>
    where
        T: Deserialize<'de>,
    {
        T::deserialize(ValueDeserializer {
            value: &self.0,
            interners,
        })
    }

    /// Convenience function to combine a [`serde_json::Deserializer`] with an
    /// [`InterningDeserializer`] to deserialize an interned value from a plain
    /// JSON slice, using the provided [`Jinterners`] arena.
    ///
    /// See also [`from_json_slice_mut()`](Self::from_json_slice_mut), which is
    /// more efficient if you hold a mutable reference to the [`Jinterners`]
    /// arena as it avoids acquiring locks.
    #[cfg(all(feature = "serde", feature = "sync"))]
    pub fn from_json_slice(
        json: &[u8],
        interners: &Jinterners<H>,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_slice(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializer::new(&mut json_de, interners, &mut buffers);
        let value = de.deserialize()?;
        json_de.end()?;
        Ok(value)
    }

    /// Convenience function to combine a [`serde_json::Deserializer`] with an
    /// [`InterningDeserializer`] to deserialize an interned value from a plain
    /// JSON string, using the provided [`Jinterners`] arena.
    ///
    /// See also [`from_json_str_mut()`](Self::from_json_str_mut), which is more
    /// efficient if you hold a mutable reference to the [`Jinterners`] arena as
    /// it avoids acquiring locks.
    #[cfg(all(feature = "serde", feature = "sync"))]
    pub fn from_json_str(
        json: &str,
        interners: &Jinterners<H>,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_str(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializer::new(&mut json_de, interners, &mut buffers);
        let value = de.deserialize()?;
        json_de.end()?;
        Ok(value)
    }

    /// Convenience function to combine a [`serde_json::Deserializer`] with an
    /// [`InterningDeserializer`] to deserialize an interned value from a plain
    /// JSON reader, using the provided [`Jinterners`] arena.
    ///
    /// See also [`from_json_reader_mut()`](Self::from_json_reader_mut), which
    /// is more efficient if you hold a mutable reference to the [`Jinterners`]
    /// arena as it avoids acquiring locks.
    #[cfg(all(feature = "serde", feature = "std", feature = "sync"))]
    pub fn from_json_reader<R: Read>(
        json: R,
        interners: &Jinterners<H>,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_reader(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializer::new(&mut json_de, interners, &mut buffers);
        let value = de.deserialize()?;
        json_de.end()?;
        Ok(value)
    }

    /// Convenience function to combine a [`serde_json::Deserializer`] with an
    /// [`InterningDeserializerMut`] to deserialize an interned value from a
    /// plain JSON slice, using the provided [`Jinterners`] arena.
    ///
    /// Contrary to [`from_json_slice()`](Self::from_json_slice), no locks are
    /// held internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    #[cfg(feature = "serde")]
    pub fn from_json_slice_mut(
        json: &[u8],
        interners: &mut Jinterners<H>,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_slice(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializerMut::new(&mut json_de, interners, &mut buffers);
        let value = de.deserialize()?;
        json_de.end()?;
        Ok(value)
    }

    /// Convenience function to combine a [`serde_json::Deserializer`] with an
    /// [`InterningDeserializerMut`] to deserialize an interned value from a
    /// plain JSON string, using the provided [`Jinterners`] arena.
    ///
    /// Contrary to [`from_json_str()`](Self::from_json_str), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    #[cfg(feature = "serde")]
    pub fn from_json_str_mut(
        json: &str,
        interners: &mut Jinterners<H>,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_str(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializerMut::new(&mut json_de, interners, &mut buffers);
        let value = de.deserialize()?;
        json_de.end()?;
        Ok(value)
    }

    /// Convenience function to combine a [`serde_json::Deserializer`] with an
    /// [`InterningDeserializerMut`] to deserialize an interned value from a
    /// plain JSON reader, using the provided [`Jinterners`] arena.
    ///
    /// Contrary to [`from_json_reader()`](Self::from_json_reader), no locks are
    /// held internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn from_json_reader_mut<R: Read>(
        json: R,
        interners: &mut Jinterners<H>,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_reader(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializerMut::new(&mut json_de, interners, &mut buffers);
        let value = de.deserialize()?;
        json_de.end()?;
        Ok(value)
    }

    /// Convenience function to call [`serde_json::to_vec()`] on a
    /// [`BoundValue`] combining this value with the given [`Jinterners`].
    #[cfg(feature = "serde")]
    pub fn to_json_bytes(
        &self,
        interners: &Jinterners<H>,
    ) -> Result<Vec<u8>, serde_json::error::Error> {
        serde_json::to_vec(&BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_vec_pretty()`] on a
    /// [`BoundValue`] combining this value with the given [`Jinterners`].
    #[cfg(feature = "serde")]
    pub fn to_json_bytes_pretty(
        &self,
        interners: &Jinterners<H>,
    ) -> Result<Vec<u8>, serde_json::error::Error> {
        serde_json::to_vec_pretty(&BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_string()`] on a
    /// [`BoundValue`] combining this value with the given [`Jinterners`].
    #[cfg(feature = "serde")]
    pub fn to_json_string(
        &self,
        interners: &Jinterners<H>,
    ) -> Result<String, serde_json::error::Error> {
        serde_json::to_string(&BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_string_pretty()`] on a
    /// [`BoundValue`] combining this value with the given [`Jinterners`].
    #[cfg(feature = "serde")]
    pub fn to_json_string_pretty(
        &self,
        interners: &Jinterners<H>,
    ) -> Result<String, serde_json::error::Error> {
        serde_json::to_string_pretty(&BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_writer()`] on a
    /// [`BoundValue`] combining this value with the given [`Jinterners`].
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn to_json_writer<W: Write>(
        &self,
        writer: W,
        interners: &Jinterners<H>,
    ) -> Result<(), serde_json::error::Error> {
        serde_json::to_writer(writer, &BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_writer_pretty()`] on a
    /// [`BoundValue`] combining this value with the given [`Jinterners`].
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn to_json_writer_pretty<W: Write>(
        &self,
        writer: W,
        interners: &Jinterners<H>,
    ) -> Result<(), serde_json::error::Error> {
        serde_json::to_writer_pretty(writer, &BoundValue::new(self, interners))
    }
}

impl<H> IValue<H> {
    #[cfg(feature = "retain")]
    pub(crate) fn retain(&self, builder: &mut RetainBuilder<H>) -> bool {
        match self.0 {
            IValueImpl::Null
            | IValueImpl::Bool(_)
            | IValueImpl::U64(_)
            | IValueImpl::I64(_)
            | IValueImpl::F64(_) => false,
            IValueImpl::String(s) => builder.strings.insert(s),
            IValueImpl::Array(a) => {
                if builder.arrays.insert(a) {
                    builder.queue_arrays.push(a);
                    true
                } else {
                    false
                }
            }
            IValueImpl::Object(o) => {
                if builder.objects.insert(o) {
                    builder.queue_objects.push(o);
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Default, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
struct Float64(OrderedFloat<f64>);

impl Debug for Float64 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.0.fmt(f)
    }
}

#[cfg(feature = "get-size2")]
impl GetSize for Float64 {
    // There is nothing on the heap, so the default implementation works out of
    // the box.
}

#[derive(Default)]
enum IValueImpl<H> {
    #[default]
    Null,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(Float64),
    String(InternedStr<H>),
    Array(InternedSlice<IValue<H>, H>),
    Object(InternedSlice<(InternedStrKey<H>, IValue<H>), H>),
}

impl<H> Debug for IValueImpl<H> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IValueImpl::Null => f.debug_tuple("IValue::Null").finish(),
            IValueImpl::Bool(x) => f.debug_tuple("IValue::Bool").field(x).finish(),
            IValueImpl::U64(x) => f.debug_tuple("IValue::U64").field(x).finish(),
            IValueImpl::I64(x) => f.debug_tuple("IValue::I64").field(x).finish(),
            IValueImpl::F64(Float64(OrderedFloat(x))) => {
                f.debug_tuple("IValue::F64").field(x).finish()
            }
            IValueImpl::String(s) => f.debug_tuple("IValue::String").field(s).finish(),
            IValueImpl::Array(a) => f.debug_tuple("IValue::Array").field(a).finish(),
            IValueImpl::Object(o) => f.debug_tuple("IValue::Object").field(o).finish(),
        }
    }
}

impl<H> Clone for IValueImpl<H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H> Copy for IValueImpl<H> {}

impl<H> PartialEq for IValueImpl<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (IValueImpl::Null, IValueImpl::Null) => true,
            (IValueImpl::Bool(a), IValueImpl::Bool(b)) => a == b,
            (IValueImpl::U64(a), IValueImpl::U64(b)) => a == b,
            (IValueImpl::I64(a), IValueImpl::I64(b)) => a == b,
            (IValueImpl::F64(a), IValueImpl::F64(b)) => a == b,
            (IValueImpl::String(a), IValueImpl::String(b)) => a == b,
            (IValueImpl::Array(a), IValueImpl::Array(b)) => a == b,
            (IValueImpl::Object(a), IValueImpl::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl<H> Eq for IValueImpl<H> {}

impl<H> PartialOrd for IValueImpl<H> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<H> Ord for IValueImpl<H> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.discriminant().cmp(&other.discriminant()) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => match (self, other) {
                (IValueImpl::Null, IValueImpl::Null) => Ordering::Equal,
                (IValueImpl::Bool(a), IValueImpl::Bool(b)) => a.cmp(b),
                (IValueImpl::U64(a), IValueImpl::U64(b)) => a.cmp(b),
                (IValueImpl::I64(a), IValueImpl::I64(b)) => a.cmp(b),
                (IValueImpl::F64(a), IValueImpl::F64(b)) => a.cmp(b),
                (IValueImpl::String(a), IValueImpl::String(b)) => a.cmp(b),
                (IValueImpl::Array(a), IValueImpl::Array(b)) => a.cmp(b),
                (IValueImpl::Object(a), IValueImpl::Object(b)) => a.cmp(b),
                _ => unreachable!(),
            },
        }
    }
}

impl<H> Hash for IValueImpl<H> {
    fn hash<G>(&self, state: &mut G)
    where
        G: Hasher,
    {
        core::mem::discriminant(self).hash(state);
        match self {
            IValueImpl::Null => (),
            IValueImpl::Bool(x) => x.hash(state),
            IValueImpl::U64(x) => x.hash(state),
            IValueImpl::I64(x) => x.hash(state),
            IValueImpl::F64(x) => x.hash(state),
            IValueImpl::String(s) => s.hash(state),
            IValueImpl::Array(a) => a.hash(state),
            IValueImpl::Object(o) => o.hash(state),
        }
    }
}

#[cfg(feature = "get-size2")]
impl<H> GetSize for IValueImpl<H> {
    // There is nothing on the heap, so the default implementation works out of
    // the box.
}

#[cfg(feature = "serde")]
impl<H> Serialize for IValueImpl<H> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            IValueImpl::Null => serializer.serialize_unit_variant("IValueImpl", 0, "Null"),
            IValueImpl::Bool(x) => serializer.serialize_newtype_variant("IValueImpl", 1, "Bool", x),
            IValueImpl::U64(x) => serializer.serialize_newtype_variant("IValueImpl", 2, "U64", x),
            IValueImpl::I64(x) => serializer.serialize_newtype_variant("IValueImpl", 3, "I64", x),
            IValueImpl::F64(x) => serializer.serialize_newtype_variant("IValueImpl", 4, "F64", x),
            IValueImpl::String(x) => {
                serializer.serialize_newtype_variant("IValueImpl", 5, "String", x)
            }
            IValueImpl::Array(x) => {
                serializer.serialize_newtype_variant("IValueImpl", 6, "Array", x)
            }
            IValueImpl::Object(x) => {
                serializer.serialize_newtype_variant("IValueImpl", 7, "Object", x)
            }
        }
    }
}

#[cfg(feature = "serde")]
impl<'de, H> Deserialize<'de> for IValueImpl<H> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_enum(
            "IValueImpl",
            &[
                "Null", "Bool", "U64", "I64", "F64", "String", "Array", "Object",
            ],
            IValueImplVisitor(PhantomData),
        )
    }
}

#[cfg(feature = "serde")]
struct IValueImplVisitor<H>(PhantomData<fn() -> IValueImpl<H>>);

#[cfg(feature = "serde")]
impl<'de, H> Visitor<'de> for IValueImplVisitor<H> {
    type Value = IValueImpl<H>;

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("an enumeration")
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: EnumAccess<'de>,
    {
        #[derive(Deserialize)]
        enum IValueImplVariant {
            Null,
            Bool,
            U64,
            I64,
            F64,
            String,
            Array,
            Object,
        }

        let (variant, data) = data.variant()?;
        Ok(match variant {
            IValueImplVariant::Null => {
                data.unit_variant()?;
                IValueImpl::Null
            }
            IValueImplVariant::Bool => IValueImpl::Bool(data.newtype_variant()?),
            IValueImplVariant::U64 => IValueImpl::U64(data.newtype_variant()?),
            IValueImplVariant::I64 => IValueImpl::I64(data.newtype_variant()?),
            IValueImplVariant::F64 => IValueImpl::F64(data.newtype_variant()?),
            IValueImplVariant::String => IValueImpl::String(data.newtype_variant()?),
            IValueImplVariant::Array => IValueImpl::Array(data.newtype_variant()?),
            IValueImplVariant::Object => IValueImpl::Object(data.newtype_variant()?),
        })
    }
}

impl<H> IValueImpl<H> {
    // This is needed because core::mem::Discriminant doesn't implement Ord.
    fn discriminant(&self) -> usize {
        match self {
            IValueImpl::Null => 0,
            IValueImpl::Bool(_) => 1,
            IValueImpl::U64(_) => 2,
            IValueImpl::I64(_) => 3,
            IValueImpl::F64(_) => 4,
            IValueImpl::String(_) => 5,
            IValueImpl::Array(_) => 6,
            IValueImpl::Object(_) => 7,
        }
    }
}

impl<H> IValueImpl<H>
where
    H: BuildHasher,
{
    #[cfg(feature = "sync")]
    fn from(interners: &Jinterners<H>, source: Value) -> Self {
        match source {
            Value::Null => IValueImpl::Null,
            Value::Bool(x) => IValueImpl::Bool(x),
            Value::Number(x) => {
                if x.is_u64() {
                    IValueImpl::U64(x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValueImpl::I64(x.as_i64().unwrap())
                } else {
                    IValueImpl::F64(Float64(OrderedFloat(x.as_f64().unwrap())))
                }
            }
            Value::String(s) => IValueImpl::String(interners.string.intern(&s)),
            Value::Array(a) => IValueImpl::Array(
                interners.iarray.intern_copy(
                    &a.into_iter()
                        .map(|v| interners.intern(v))
                        .collect::<Box<[_]>>(),
                ),
            ),
            Value::Object(o) => {
                let mut io: Box<[_]> = o
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            InternedStrKey(interners.string.intern(&k)),
                            interners.intern(v),
                        )
                    })
                    .collect();
                io.sort_unstable_by_key(|(k, _)| *k);
                IValueImpl::Object(interners.iobject.intern_copy(&io))
            }
        }
    }

    #[cfg(feature = "sync")]
    fn from_ref(interners: &Jinterners<H>, source: &Value) -> Self {
        match source {
            Value::Null => IValueImpl::Null,
            Value::Bool(x) => IValueImpl::Bool(*x),
            Value::Number(x) => {
                if x.is_u64() {
                    IValueImpl::U64(x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValueImpl::I64(x.as_i64().unwrap())
                } else {
                    IValueImpl::F64(Float64(OrderedFloat(x.as_f64().unwrap())))
                }
            }
            Value::String(s) => IValueImpl::String(interners.string.intern(s.as_str())),
            Value::Array(a) => IValueImpl::Array(
                interners.iarray.intern_copy(
                    &a.iter()
                        .map(|v| interners.intern_ref(v))
                        .collect::<Box<[_]>>(),
                ),
            ),
            Value::Object(o) => {
                let mut io: Box<[_]> = o
                    .iter()
                    .map(|(k, v)| {
                        (
                            InternedStrKey(interners.string.intern(k.as_str())),
                            interners.intern_ref(v),
                        )
                    })
                    .collect();
                io.sort_unstable_by_key(|(k, _)| *k);
                IValueImpl::Object(interners.iobject.intern_copy(&io))
            }
        }
    }

    fn from_mut(interners: &mut Jinterners<H>, source: Value) -> Self {
        match source {
            Value::Null => IValueImpl::Null,
            Value::Bool(x) => IValueImpl::Bool(x),
            Value::Number(x) => {
                if x.is_u64() {
                    IValueImpl::U64(x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValueImpl::I64(x.as_i64().unwrap())
                } else {
                    IValueImpl::F64(Float64(OrderedFloat(x.as_f64().unwrap())))
                }
            }
            Value::String(s) => IValueImpl::String(interners.string.intern_mut(&s)),
            Value::Array(a) => {
                let a = a
                    .into_iter()
                    .map(|v| interners.intern_mut(v))
                    .collect::<Box<[_]>>();
                IValueImpl::Array(interners.iarray.intern_copy_mut(&a))
            }
            Value::Object(o) => {
                let mut io: Box<[_]> = o
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            InternedStrKey(interners.string.intern_mut(&k)),
                            interners.intern_mut(v),
                        )
                    })
                    .collect();
                io.sort_unstable_by_key(|(k, _)| *k);
                IValueImpl::Object(interners.iobject.intern_copy_mut(&io))
            }
        }
    }

    fn from_ref_mut(interners: &mut Jinterners<H>, source: &Value) -> Self {
        match source {
            Value::Null => IValueImpl::Null,
            Value::Bool(x) => IValueImpl::Bool(*x),
            Value::Number(x) => {
                if x.is_u64() {
                    IValueImpl::U64(x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValueImpl::I64(x.as_i64().unwrap())
                } else {
                    IValueImpl::F64(Float64(OrderedFloat(x.as_f64().unwrap())))
                }
            }
            Value::String(s) => IValueImpl::String(interners.string.intern_mut(s.as_str())),
            Value::Array(a) => {
                let a = a
                    .iter()
                    .map(|v| interners.intern_ref_mut(v))
                    .collect::<Box<[_]>>();
                IValueImpl::Array(interners.iarray.intern_copy_mut(&a))
            }
            Value::Object(o) => {
                let mut io: Box<[_]> = o
                    .iter()
                    .map(|(k, v)| {
                        (
                            InternedStrKey(interners.string.intern_mut(k.as_str())),
                            interners.intern_ref_mut(v),
                        )
                    })
                    .collect();
                io.sort_unstable_by_key(|(k, _)| *k);
                IValueImpl::Object(interners.iobject.intern_copy_mut(&io))
            }
        }
    }

    fn lookup(&self, interners: &Jinterners<H>) -> Value {
        match self {
            IValueImpl::Null => Value::Null,
            IValueImpl::Bool(x) => Value::Bool(*x),
            IValueImpl::U64(x) => Value::Number(Number::from_u128(*x as u128).unwrap()),
            IValueImpl::I64(x) => Value::Number(Number::from_i128(*x as i128).unwrap()),
            IValueImpl::F64(Float64(OrderedFloat(x))) => {
                Value::Number(Number::from_f64(*x).unwrap())
            }
            IValueImpl::String(s) => Value::String(interners.string.lookup(*s).into()),
            IValueImpl::Array(a) => Value::Array(
                interners
                    .iarray
                    .lookup(*a)
                    .iter()
                    .map(|v| interners.lookup(v))
                    .collect(),
            ),
            IValueImpl::Object(o) => Value::Object(
                interners
                    .iobject
                    .lookup(*o)
                    .iter()
                    .map(|(k, v)| (interners.string.lookup(k.0).into(), interners.lookup(v)))
                    .collect(),
            ),
        }
    }

    fn lookup_ref<'a>(&self, interners: &'a Jinterners<H>) -> ValueRef<'a, H> {
        match self {
            IValueImpl::Null => ValueRef::Null,
            IValueImpl::Bool(x) => ValueRef::Bool(*x),
            IValueImpl::U64(x) => ValueRef::U64(*x),
            IValueImpl::I64(x) => ValueRef::I64(*x),
            IValueImpl::F64(Float64(OrderedFloat(x))) => ValueRef::F64(*x),
            IValueImpl::String(s) => ValueRef::String(interners.string.lookup(*s)),
            IValueImpl::Array(a) => ValueRef::Array(interners.iarray.lookup(*a)),
            IValueImpl::Object(o) => ValueRef::Object(MapRef {
                arena_str: &interners.string,
                map: interners.iobject.lookup(*o),
            }),
        }
    }
}

/// A shallow reference to a JSON value.
pub enum ValueRef<'a, H = DefaultBuildHasher> {
    /// JSON null value.
    Null,
    /// JSON boolean value.
    Bool(bool),
    /// JSON number that fits in a [`u64`].
    U64(u64),
    /// JSON number that fits in a [`i64`].
    I64(i64),
    /// JSON number that fits in a [`f64`].
    F64(f64),
    /// JSON string.
    String(&'a str),
    /// JSON array.
    Array(&'a [IValue<H>]),
    /// JSON object.
    Object(MapRef<'a, H>),
}

/// A shallow reference to a JSON map.
pub struct MapRef<'a, H = DefaultBuildHasher> {
    arena_str: &'a ArenaStr<H>,
    map: &'a [(InternedStrKey<H>, IValue<H>)],
}

impl<'a, H> MapRef<'a, H>
where
    H: BuildHasher,
{
    /// Returns the value associated to the given key, or [`None`] if there is
    /// no such key in this map.
    ///
    /// If you're repeatedly querying the same key, it's more efficient to cache
    /// it once with [`Jinterners::find_key()`] and then use
    /// [`get_by_key()`](Self::get_by_key).
    pub fn get(&self, key: &str) -> Option<&'a IValue<H>> {
        let k = InternedStrKey(self.arena_str.find(key)?);
        self.get_by_key(k)
    }
}

impl<'a, H> MapRef<'a, H> {
    /// Returns the value associated to the given key, or [`None`] if there is
    /// no such key in this map.
    pub fn get_by_key(&self, key: InternedStrKey<H>) -> Option<&'a IValue<H>> {
        let i = self.map.binary_search_by_key(&key, |entry| entry.0).ok()?;
        Some(&self.map[i].1)
    }

    /// Iterates over the key-value pairs in this JSON map, in arbitrary order.
    ///
    /// See also [`iter_keys()`](Self::iter_keys) which is more efficient if you
    /// only need to manipulate [`InternedStrKey`]s as it doesn't resolve them
    /// to strings.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&'a str, &'a IValue<H>)> {
        self.map
            .iter()
            .map(|(k, v)| (self.arena_str.lookup(k.0), v))
    }

    /// Iterates over the key-value pairs in this JSON map, in sorted order of
    /// keys.
    ///
    /// Note that keys are sorted using the ordering on the [`InternedStrKey`]
    /// type, i.e. the corresponding strings are in arbitrary order.
    pub fn iter_keys(&self) -> impl ExactSizeIterator<Item = (InternedStrKey<H>, &'a IValue<H>)> {
        self.map.iter().map(|(k, v)| (*k, v))
    }
}

#[cfg(all(feature = "delta", feature = "serde"))]
mod delta {
    use super::*;
    use crate::DeltaEncoding;
    use blazinterner::{Accumulator, ArenaSlice, DeltaEncoding as RawDeltaEncoding};
    use hashbrown::HashMap;
    use serde::de::{Error, SeqAccess, Visitor};
    use serde::ser::SerializeTuple;
    use serde::{Deserializer, Serializer};

    impl<H> Serialize for DeltaEncoding<Jinterners<H>> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut tuple = serializer.serialize_tuple(3)?;

            tuple.serialize_element(&self.inner.string)?;

            let iarray: RawDeltaEncoding<_, IArrayAccumulator<H>> =
                RawDeltaEncoding::new(&self.inner.iarray);
            tuple.serialize_element(&iarray)?;

            let iobject: RawDeltaEncoding<_, IObjectAccumulator<H>> =
                RawDeltaEncoding::new(&self.inner.iobject);
            tuple.serialize_element(&iobject)?;

            tuple.end()
        }
    }

    impl<'de, H> Deserialize<'de> for DeltaEncoding<Jinterners<H>>
    where
        H: Default + BuildHasher,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_tuple(3, DeltaJinternersVisitor(PhantomData))
        }
    }

    struct DeltaJinternersVisitor<H>(PhantomData<fn() -> DeltaEncoding<Jinterners<H>>>);

    impl<'de, H> Visitor<'de> for DeltaJinternersVisitor<H>
    where
        H: Default + BuildHasher,
    {
        type Value = DeltaEncoding<Jinterners<H>>;

        fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
            formatter.write_str("a tuple with 3 elements")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let string = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(0, &self))?;
            let iarray: RawDeltaEncoding<ArenaSlice<IValue<H>, H>, IArrayAccumulator<H>> = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(1, &self))?;
            #[expect(clippy::type_complexity)]
            let iobject: RawDeltaEncoding<
                ArenaSlice<(InternedStrKey<H>, IValue<H>), H>,
                IObjectAccumulator<H>,
            > = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(2, &self))?;

            Ok(DeltaEncoding::new(Jinterners {
                string,
                iarray: iarray.into_inner(),
                iobject: iobject.into_inner(),
            }))
        }
    }

    /// Difference between two JSON values, for better delta encoding
    /// serialization.
    #[derive(Serialize, Deserialize)]
    pub enum IValueDelta {
        Null,
        Bool(bool),
        U64(i64),
        I64(i64),
        F64(f64),
        String(i32),
        Array(i32),
        Object(i32),
    }

    struct IValueAccumulator<H> {
        b: bool,
        u: u64,
        i: i64,
        f: f64,
        s: u32,
        a: u32,
        o: u32,
        _phantom: PhantomData<fn() -> IValueImpl<H>>,
    }

    impl<H> Default for IValueAccumulator<H> {
        fn default() -> Self {
            Self {
                b: false,
                u: 0,
                i: 0,
                f: f64::from_bits(0),
                s: 0,
                a: 0,
                o: 0,
                _phantom: PhantomData,
            }
        }
    }

    impl<H> Accumulator for IValueAccumulator<H> {
        type Value = IValueImpl<H>;
        type Storage = IValueImpl<H>;
        type Delta = IValueDelta;
        type DeltaStorage = IValueDelta;

        fn fold(&mut self, v: &Self::Value) -> Self::DeltaStorage {
            match v {
                IValueImpl::Null => IValueDelta::Null,
                IValueImpl::Bool(x) => {
                    let diff = self.b ^ x;
                    self.b = *x;
                    IValueDelta::Bool(diff)
                }
                IValueImpl::U64(x) => {
                    let diff = x.wrapping_sub(self.u);
                    self.u = *x;
                    IValueDelta::U64(diff as i64)
                }
                IValueImpl::I64(x) => {
                    let diff = x.wrapping_sub(self.i);
                    self.i = *x;
                    IValueDelta::I64(diff)
                }
                IValueImpl::F64(x) => {
                    let diff = x.0.to_bits() ^ self.f.to_bits();
                    self.f = *x.0;
                    IValueDelta::F64(f64::from_bits(diff))
                }
                IValueImpl::String(x) => {
                    let diff = x.id().wrapping_sub(self.s);
                    self.s = x.id();
                    IValueDelta::String(diff as i32)
                }
                IValueImpl::Array(x) => {
                    let diff = x.id().wrapping_sub(self.a);
                    self.a = x.id();
                    IValueDelta::Array(diff as i32)
                }
                IValueImpl::Object(x) => {
                    let diff = x.id().wrapping_sub(self.o);
                    self.o = x.id();
                    IValueDelta::Object(diff as i32)
                }
            }
        }

        fn unfold(&mut self, d: &Self::Delta) -> Self::Storage {
            match d {
                IValueDelta::Null => IValueImpl::Null,
                IValueDelta::Bool(x) => {
                    let x = self.b ^ x;
                    self.b = x;
                    IValueImpl::Bool(x)
                }
                IValueDelta::U64(x) => {
                    let x = self.u.wrapping_add(*x as u64);
                    self.u = x;
                    IValueImpl::U64(x)
                }
                IValueDelta::I64(x) => {
                    let x = self.i.wrapping_add(*x);
                    self.i = x;
                    IValueImpl::I64(x)
                }
                IValueDelta::F64(x) => {
                    let x = f64::from_bits(self.f.to_bits() ^ x.to_bits());
                    self.f = x;
                    IValueImpl::F64(Float64(OrderedFloat(x)))
                }
                IValueDelta::String(x) => {
                    let x = self.s.wrapping_add(*x as u32);
                    self.s = x;
                    IValueImpl::String(InternedStr::from_id(x))
                }
                IValueDelta::Array(x) => {
                    let x = self.a.wrapping_add(*x as u32);
                    self.a = x;
                    IValueImpl::Array(InternedSlice::from_id(x))
                }
                IValueDelta::Object(x) => {
                    let x = self.o.wrapping_add(*x as u32);
                    self.o = x;
                    IValueImpl::Object(InternedSlice::from_id(x))
                }
            }
        }
    }

    struct IArrayAccumulator<H>(IValueAccumulator<H>);

    impl<H> Default for IArrayAccumulator<H> {
        fn default() -> Self {
            Self(Default::default())
        }
    }

    impl<H> Accumulator for IArrayAccumulator<H> {
        type Value = [IValue<H>];
        type Storage = Box<[IValue<H>]>;
        type Delta = [IValueDelta];
        type DeltaStorage = Box<[IValueDelta]>;

        fn fold(&mut self, v: &Self::Value) -> Self::DeltaStorage {
            v.iter().map(|x| self.0.fold(&x.0)).collect()
        }

        fn unfold(&mut self, d: &Self::Delta) -> Self::Storage {
            d.iter().map(|x| IValue(self.0.unfold(x))).collect()
        }
    }

    struct IObjectAccumulator<H> {
        map: HashMap<u32, IValueAccumulator<H>>,
    }

    impl<H> Default for IObjectAccumulator<H> {
        fn default() -> Self {
            Self {
                map: Default::default(),
            }
        }
    }

    impl<H> Accumulator for IObjectAccumulator<H> {
        type Value = [(InternedStrKey<H>, IValue<H>)];
        type Storage = Box<[(InternedStrKey<H>, IValue<H>)]>;
        type Delta = [(i32, IValueDelta)];
        type DeltaStorage = Box<[(i32, IValueDelta)]>;

        fn fold(&mut self, v: &Self::Value) -> Self::DeltaStorage {
            let mut key = 0;
            v.iter()
                .map(|(k, x)| {
                    let k = k.0.id();
                    let kdiff = k.wrapping_sub(key);
                    key = k;
                    let acc = self.map.entry(k).or_default();
                    let xdiff = acc.fold(&x.0);
                    (kdiff as i32, xdiff)
                })
                .collect()
        }

        fn unfold(&mut self, d: &Self::Delta) -> Self::Storage {
            let mut key = 0;
            d.iter()
                .map(|(kdiff, xdiff)| {
                    let k = (*kdiff as u32).wrapping_add(key);
                    key = k;
                    let acc = self.map.entry(k).or_default();
                    let x = IValue(acc.unfold(xdiff));
                    (InternedStrKey(InternedStr::from_id(k)), x)
                })
                .collect()
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod serde_test {
    use super::*;
    use hashbrown::HashMap;
    use serde_json::json;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Foo {
        a: bool,
        b: i32,
        c: u64,
        d: f32,
        e: Option<f64>,
        f: String,
        g: Vec<Bar>,
        h: HashMap<String, Bar>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Bar {
        First,
        Second(u32, i64),
        Third { i: String, j: [u8; 4] },
    }

    fn make_foo() -> Foo {
        Foo {
            a: true,
            b: -0x12345678,
            c: 0xfedcba98_76543210,
            d: core::f32::consts::PI,
            e: Some(core::f64::consts::E),
            f: "Hello world".into(),
            g: vec![
                Bar::First,
                Bar::Second(0x87654321, -0x12345678_9abcdef0),
                Bar::Third {
                    i: "Hello".into(),
                    j: [1, 2, 3, 4],
                },
            ],
            h: [
                ("Hello".to_string(), Bar::First),
                ("world".to_string(), Bar::Second(42, -123)),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct SmallFoo {
        a: bool,
        c: u64,
        f: String,
    }

    fn make_small_foo() -> SmallFoo {
        SmallFoo {
            a: true,
            c: 0xfedcba98_76543210,
            f: "Hello world".into(),
        }
    }

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    enum SimpleEnum {
        First,
        Second,
        Third,
    }

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    struct NewString(String);

    #[cfg(feature = "sync")]
    #[test]
    #[expect(clippy::approx_constant)]
    fn round_trip() {
        let interners: Jinterners = Jinterners::default();

        let original = make_foo();
        let ivalue = IValue::from_value(&original, &interners).expect("Failed to intern value");

        let json = ivalue.lookup(&interners);
        assert_eq!(
            json,
            json!({
                "a": true,
                "b": -0x12345678,
                "c": 0xfedcba98_76543210_u64,
                "d": 3.1415927410125732,
                "e": 2.718281828459045,
                "f": "Hello world",
                "g": [
                    "First",
                    {"Second": [0x87654321_u32, -0x12345678_9abcdef0_i64]},
                    {"Third": {"i": "Hello", "j": [1, 2, 3, 4]}},
                ],
                "h": {
                    "Hello": "First",
                    "world": {"Second": [42, -123]},
                }
            })
        );

        let foo: Foo = ivalue
            .to_value(&interners)
            .expect("Failed to convert to value");
        assert_eq!(foo, original);
    }

    #[test]
    #[expect(clippy::approx_constant)]
    fn round_trip_mut() {
        let mut interners: Jinterners = Jinterners::default();

        let original = make_foo();
        let ivalue =
            IValue::from_value_mut(&original, &mut interners).expect("Failed to intern value");

        let json = ivalue.lookup(&interners);
        assert_eq!(
            json,
            json!({
                "a": true,
                "b": -0x12345678,
                "c": 0xfedcba98_76543210_u64,
                "d": 3.1415927410125732,
                "e": 2.718281828459045,
                "f": "Hello world",
                "g": [
                    "First",
                    {"Second": [0x87654321_u32, -0x12345678_9abcdef0_i64]},
                    {"Third": {"i": "Hello", "j": [1, 2, 3, 4]}},
                ],
                "h": {
                    "Hello": "First",
                    "world": {"Second": [42, -123]},
                }
            })
        );

        let foo: Foo = ivalue
            .to_value(&interners)
            .expect("Failed to convert to value");
        assert_eq!(foo, original);
    }

    #[test]
    #[expect(clippy::approx_constant)]
    fn deserialize_smaller() {
        let mut interners: Jinterners = Jinterners::default();

        let json = json!({
            "a": true,
            "b": -0x12345678,
            "c": 0xfedcba98_76543210_u64,
            "d": 3.1415927410125732,
            "e": 2.718281828459045,
            "f": "Hello world",
            "g": [
                "First",
                {"Second": [0x87654321_u32, -0x12345678_9abcdef0_i64]},
                {"Third": {"i": "Hello", "j": [1, 2, 3, 4]}},
            ],
            "h": {
                "Hello": "First",
                "world": {"Second": [42, -123]},
            }
        });
        let ivalue = interners.intern_mut(json);

        let small_foo: SmallFoo = ivalue
            .to_value(&interners)
            .expect("Failed to convert to value");
        assert_eq!(small_foo, make_small_foo());
    }

    #[cfg(feature = "sync")]
    #[test]
    fn round_trip_map_key_enum() {
        let interners: Jinterners = Jinterners::default();

        let original: HashMap<SimpleEnum, u32> = [
            (SimpleEnum::First, 1),
            (SimpleEnum::Second, 2),
            (SimpleEnum::Third, 3),
        ]
        .into_iter()
        .collect();
        let ivalue = IValue::from_value(&original, &interners).expect("Failed to intern value");

        let json = ivalue.lookup(&interners);
        assert_eq!(
            json,
            json!({
                "First": 1,
                "Second": 2,
                "Third": 3,
            })
        );

        let deser: HashMap<SimpleEnum, u32> = ivalue
            .to_value(&interners)
            .expect("Failed to convert to value");
        assert_eq!(deser, original);
    }

    #[test]
    fn round_trip_mut_map_key_enum() {
        let mut interners: Jinterners = Jinterners::default();

        let original: HashMap<SimpleEnum, u32> = [
            (SimpleEnum::First, 1),
            (SimpleEnum::Second, 2),
            (SimpleEnum::Third, 3),
        ]
        .into_iter()
        .collect();
        let ivalue =
            IValue::from_value_mut(&original, &mut interners).expect("Failed to intern value");

        let json = ivalue.lookup(&interners);
        assert_eq!(
            json,
            json!({
                "First": 1,
                "Second": 2,
                "Third": 3,
            })
        );

        let deser: HashMap<SimpleEnum, u32> = ivalue
            .to_value(&interners)
            .expect("Failed to convert to value");
        assert_eq!(deser, original);
    }

    #[cfg(feature = "sync")]
    #[test]
    fn round_trip_map_key_newtype() {
        let interners: Jinterners = Jinterners::default();

        let original: HashMap<NewString, u32> = [
            (NewString("First".into()), 1),
            (NewString("Second".into()), 2),
            (NewString("Third".into()), 3),
        ]
        .into_iter()
        .collect();
        let ivalue = IValue::from_value(&original, &interners).expect("Failed to intern value");

        let json = ivalue.lookup(&interners);
        assert_eq!(
            json,
            json!({
                "First": 1,
                "Second": 2,
                "Third": 3,
            })
        );

        let deser: HashMap<NewString, u32> = ivalue
            .to_value(&interners)
            .expect("Failed to convert to value");
        assert_eq!(deser, original);
    }

    #[test]
    fn round_trip_mut_map_key_newtype() {
        let mut interners: Jinterners = Jinterners::default();

        let original: HashMap<NewString, u32> = [
            (NewString("First".into()), 1),
            (NewString("Second".into()), 2),
            (NewString("Third".into()), 3),
        ]
        .into_iter()
        .collect();
        let ivalue =
            IValue::from_value_mut(&original, &mut interners).expect("Failed to intern value");

        let json = ivalue.lookup(&interners);
        assert_eq!(
            json,
            json!({
                "First": 1,
                "Second": 2,
                "Third": 3,
            })
        );

        let deser: HashMap<NewString, u32> = ivalue
            .to_value(&interners)
            .expect("Failed to convert to value");
        assert_eq!(deser, original);
    }

    #[test]
    fn to_json_string() {
        let mut interners: Jinterners = Jinterners::default();

        let original = make_foo();
        let ivalue =
            IValue::from_value_mut(&original, &mut interners).expect("Failed to intern value");

        let json = ivalue.to_json_string(&interners);
        assert_eq!(
            json.unwrap(),
            r#"{"a":true,"b":-305419896,"c":18364758544493064720,"d":3.1415927410125732,"e":2.718281828459045,"f":"Hello world","g":["First",{"Second":[2271560481,-1311768467463790320]},{"Third":{"i":"Hello","j":[1,2,3,4]}}],"h":{"Hello":"First","world":{"Second":[42,-123]}}}"#
        );
    }
}
