#[cfg(feature = "std")]
mod compare;
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
use blazinterner::{
    ArenaStr, DefaultBuildHasher, Index, Interned, InternedSlice, InternedStr, U32,
};
#[cfg(feature = "std")]
use compare::Comparator;
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
#[cfg(feature = "postcard-schema")]
use postcard_schema::Schema;
#[cfg(feature = "postcard-schema")]
use postcard_schema::schema::{DataModelType, DataModelVariant, NamedType, NamedVariant};
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

/// Configuration to indicate whether array elements are logically ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayOrder {
    /// Array elements are logically ordered: the interner must preserve this
    /// order.
    Preserve,
    /// Array elements form a logically unordered set: the interner can reorder
    /// them to optimize the representation.
    Unordered,
}

/// An [`IValue`] interned value together with a reference to the associated
/// [`Jinterners`] arena.
///
/// This is for example useful to serialize the value as plain JSON data.
pub struct BoundValue<'a, H = DefaultBuildHasher> {
    value: &'a IValue<H>,
    interners: &'a Jinterners<H>,
}

impl<'a, H> BoundValue<'a, H> {
    /// Binds the given [`IValue`] with its associated [`Jinterners`] arena.
    pub fn new(value: &'a IValue<H>, interners: &'a Jinterners<H>) -> Self {
        Self { value, interners }
    }
}

impl<H> Debug for BoundValue<'_, H> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BoundValue")
            .field("value", self.value)
            .field("interners", &(self.interners as *const _))
            .finish()
    }
}

impl<H> Clone for BoundValue<'_, H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H> Copy for BoundValue<'_, H> {}

#[cfg(feature = "std")]
impl<'a, 'b, H, I> PartialEq<BoundValue<'b, I>> for BoundValue<'a, H>
where
    H: BuildHasher,
{
    fn eq(&self, other: &BoundValue<'b, I>) -> bool {
        if std::ptr::eq(
            self.interners as *const Jinterners<H> as *const (),
            other.interners as *const Jinterners<I> as *const (),
        ) {
            return match (self.value.0, other.value.0) {
                (IValueImpl::Null, IValueImpl::Null) => true,
                (IValueImpl::Bool(a), IValueImpl::Bool(b)) => a == b,
                (IValueImpl::U32(a), IValueImpl::U32(b)) => a == b,
                (IValueImpl::I32(a), IValueImpl::I32(b)) => a == b,
                (IValueImpl::U64(a), IValueImpl::U64(b)) => a.id() == b.id(),
                (IValueImpl::I64(a), IValueImpl::I64(b)) => a.id() == b.id(),
                (IValueImpl::F64(a), IValueImpl::F64(b)) => a.id() == b.id(),
                (IValueImpl::String(a), IValueImpl::String(b)) => a.id() == b.id(),
                (IValueImpl::Array(a), IValueImpl::Array(b)) => a.id() == b.id(),
                (IValueImpl::Set(a), IValueImpl::Set(b)) => a.id() == b.id(),
                (IValueImpl::Object(a), IValueImpl::Object(b)) => a.id() == b.id(),
                _ => false,
            };
        }

        match (self.value.0, other.value.0) {
            (IValueImpl::Null, IValueImpl::Null) => true,
            (IValueImpl::Bool(a), IValueImpl::Bool(b)) => a == b,
            (IValueImpl::U32(a), IValueImpl::U32(b)) => a == b,
            (IValueImpl::I32(a), IValueImpl::I32(b)) => a == b,
            (IValueImpl::U64(a), IValueImpl::U64(b)) => {
                self.interners.uint64.lookup(a) == other.interners.uint64.lookup(b)
            }
            (IValueImpl::I64(a), IValueImpl::I64(b)) => {
                self.interners.int64.lookup(a) == other.interners.int64.lookup(b)
            }
            (IValueImpl::F64(a), IValueImpl::F64(b)) => {
                self.interners.float64.lookup(a) == other.interners.float64.lookup(b)
            }
            (IValueImpl::String(a), IValueImpl::String(b)) => {
                self.interners.string.lookup(a) == other.interners.string.lookup(b)
            }
            (IValueImpl::Array(_), IValueImpl::Array(_))
            | (IValueImpl::Set(_), IValueImpl::Set(_))
            | (IValueImpl::Object(_), IValueImpl::Object(_)) => Comparator::eq(self, other),
            _ => false,
        }
    }
}

#[cfg(feature = "std")]
impl<H> Eq for BoundValue<'_, H> where H: BuildHasher {}

// TODO: use Iterator::eq_by when feature(iter_order_by) becomes stable.
fn iter_eq_by<T, U>(
    mut lhs: impl Iterator<Item = T>,
    mut rhs: impl Iterator<Item = U>,
    mut cmp: impl FnMut(T, U) -> bool,
) -> bool {
    loop {
        match (lhs.next(), rhs.next()) {
            (None, None) => return true,
            (Some(_), None) | (None, Some(_)) => return false,
            (Some(x), Some(y)) => {
                if !cmp(x, y) {
                    return false;
                }
            }
        }
    }
}

impl<H> PartialEq<Value> for BoundValue<'_, H> {
    fn eq(&self, other: &Value) -> bool {
        match (self.value.0, other) {
            (IValueImpl::Null, Value::Null) => true,
            (IValueImpl::Bool(a), Value::Bool(b)) => a == *b,
            (IValueImpl::U32(a), Value::Number(b)) => b
                .as_u64()
                .is_some_and(|b| u64::from(u32::from_ne_bytes(a)) == b),
            (IValueImpl::I32(a), Value::Number(b)) => b
                .as_i64()
                .is_some_and(|b| i64::from(i32::from_ne_bytes(a)) == b),
            (IValueImpl::U64(a), Value::Number(b)) => b
                .as_u64()
                .is_some_and(|b| self.interners.uint64.lookup(a) == b),
            (IValueImpl::I64(a), Value::Number(b)) => b
                .as_i64()
                .is_some_and(|b| self.interners.int64.lookup(a) == b),
            (IValueImpl::F64(a), Value::Number(b)) => b
                .as_f64()
                .is_some_and(|b| self.interners.float64.lookup(a).0.0 == b),
            (IValueImpl::String(a), Value::String(b)) => self.interners.string.lookup(a) == b,
            (IValueImpl::Array(a), Value::Array(b)) => {
                let a = self.interners.iarray.lookup(a);
                a.len() == b.len()
                    && iter_eq_by(
                        a.iter().map(|value| BoundValue {
                            value,
                            interners: self.interners,
                        }),
                        b.iter(),
                        |v, w| v == *w,
                    )
            }
            (IValueImpl::Set(_), Value::Array(_)) => todo!("Compare sets"),
            (IValueImpl::Object(a), Value::Object(b)) => {
                let a = self.interners.iobject.lookup(a);
                if a.len() != b.len() {
                    return false;
                }

                // The interner has its own order for InternedStrKey (based on
                // interned indices). We need to order them back as canonical
                // strings.
                //
                // If the `preserve_order` feature of Serde is enabled, we also
                // need to sort the Serde object. As there is no way to know if
                // the feature is enabled, we always sort.
                let mut a: Box<[_]> = a
                    .iter()
                    .map(|(k, v)| (self.interners.string.lookup(k.0), v))
                    .collect();
                let mut b: Box<[_]> = b.iter().collect();
                a.sort_unstable_by_key(|(k, _)| *k);
                b.sort_unstable_by_key(|(k, _)| *k);

                iter_eq_by(
                    a.into_iter().map(|(k, value)| {
                        (
                            k,
                            BoundValue {
                                value,
                                interners: self.interners,
                            },
                        )
                    }),
                    b.into_iter(),
                    |(k, v), (l, w)| k == l && v == *w,
                )
            }
            _ => false,
        }
    }
}

impl<H> PartialEq<BoundValue<'_, H>> for Value {
    fn eq(&self, other: &BoundValue<'_, H>) -> bool {
        other == self
    }
}

/// An interned key for JSON objects.
///
/// You can obtain a key with [`Jinterners::find_key()`] and use it to lookup
/// values in JSON objects with [`MapRef::get_by_key()`].
pub struct InternedStrKey<H = DefaultBuildHasher>(pub(crate) InternedStr<H, U32>);

impl<H> Default for InternedStrKey<H> {
    fn default() -> Self {
        InternedStrKey(InternedStr::from_id(U32::from_usize(0)))
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

#[cfg(feature = "postcard-schema")]
impl<H> Schema for InternedStrKey<H> {
    const SCHEMA: &'static NamedType = InternedStr::<H, U32>::SCHEMA;
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

impl<H> InternedStrKey<H>
where
    H: BuildHasher,
{
    #[cfg(feature = "sync")]
    pub(crate) fn new(interners: &Jinterners<H>, s: &str) -> Self {
        Self(interners.string.intern(s))
    }

    #[cfg(feature = "sync")]
    pub(crate) fn try_new(interners: &Jinterners<H>, s: &str) -> Option<Self> {
        interners.string.try_intern(s).map(Self)
    }

    pub(crate) fn new_mut(interners: &mut Jinterners<H>, s: &str) -> Self {
        Self(interners.string.intern_mut(s))
    }

    pub(crate) fn try_new_mut(interners: &mut Jinterners<H>, s: &str) -> Option<Self> {
        interners.string.try_intern_mut(s).map(Self)
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

#[cfg(feature = "postcard-schema")]
impl<H> Schema for IValue<H> {
    const SCHEMA: &'static NamedType = IValueImpl::<H>::SCHEMA;
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
    pub fn u32(x: u32) -> Self {
        Self(IValueImpl::U32(x.to_ne_bytes()))
    }

    /// Interns an integer JSON value.
    pub fn i32(x: i32) -> Self {
        Self(IValueImpl::I32(x.to_ne_bytes()))
    }
}

impl<H> IValue<H>
where
    H: BuildHasher,
{
    /// Interns an integer JSON value.
    ///
    /// See also [`u64_mut()`](Self::u64_mut), which is more efficient if you
    /// hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_u64()`](Self::try_u64) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn u64(interners: &Jinterners<H>, x: u64) -> Self {
        Self(IValueImpl::u64(interners, x))
    }

    /// Interns an integer JSON value.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also [`try_u64_mut()`](Self::try_u64_mut), which is more efficient
    /// if you hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn try_u64(interners: &Jinterners<H>, x: u64) -> Option<Self> {
        IValueImpl::try_u64(interners, x).map(Self)
    }

    /// Interns an integer JSON value.
    ///
    /// Contrary to [`u64()`](Self::u64), no locks are held internally because
    /// this function already takes an exclusive mutable reference to the
    /// [`Jinterners`] arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_u64_mut()`](Self::try_u64_mut) for a fallible API.
    pub fn u64_mut(interners: &mut Jinterners<H>, x: u64) -> Self {
        Self(IValueImpl::u64_mut(interners, x))
    }

    /// Interns an integer JSON value.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_u64()`](Self::try_u64), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    pub fn try_u64_mut(interners: &mut Jinterners<H>, x: u64) -> Option<Self> {
        IValueImpl::try_u64_mut(interners, x).map(Self)
    }

    /// Interns an integer JSON value.
    ///
    /// See also [`i64_mut()`](Self::i64_mut), which is more efficient if you
    /// hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_i64()`](Self::try_i64) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn i64(interners: &Jinterners<H>, x: i64) -> Self {
        Self(IValueImpl::i64(interners, x))
    }

    /// Interns an integer JSON value.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also [`try_i64_mut()`](Self::try_i64_mut), which is more efficient
    /// if you hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn try_i64(interners: &Jinterners<H>, x: i64) -> Option<Self> {
        IValueImpl::try_i64(interners, x).map(Self)
    }

    /// Interns an integer JSON value.
    ///
    /// Contrary to [`i64()`](Self::i64), no locks are held internally because
    /// this function already takes an exclusive mutable reference to the
    /// [`Jinterners`] arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_i64_mut()`](Self::try_i64_mut) for a fallible API.
    pub fn i64_mut(interners: &mut Jinterners<H>, x: i64) -> Self {
        Self(IValueImpl::i64_mut(interners, x))
    }

    /// Interns an integer JSON value.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_i64()`](Self::try_i64), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    pub fn try_i64_mut(interners: &mut Jinterners<H>, x: i64) -> Option<Self> {
        IValueImpl::try_i64_mut(interners, x).map(Self)
    }

    /// Interns a floating-point JSON value.
    ///
    /// See also [`f64_mut()`](Self::f64_mut), which is more efficient if you
    /// hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_f64_mut()`](Self::try_f64_mut) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn f64(interners: &Jinterners<H>, x: f64) -> Self {
        Self(IValueImpl::f64(interners, x))
    }

    /// Interns a floating-point JSON value.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also [`try_f64_mut()`](Self::try_f64_mut), which is more efficient
    /// if you hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn try_f64(interners: &Jinterners<H>, x: f64) -> Option<Self> {
        IValueImpl::try_f64(interners, x).map(Self)
    }

    /// Interns a floating-point JSON value.
    ///
    /// Contrary to [`f64()`](Self::f64), no locks are held internally because
    /// this function already takes an exclusive mutable reference to the
    /// [`Jinterners`] arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_f64_mut()`](Self::try_f64_mut) for a fallible API.
    pub fn f64_mut(interners: &mut Jinterners<H>, x: f64) -> Self {
        Self(IValueImpl::f64_mut(interners, x))
    }

    /// Interns a floating-point JSON value.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_f64()`](Self::try_f64), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    pub fn try_f64_mut(interners: &mut Jinterners<H>, x: f64) -> Option<Self> {
        IValueImpl::try_f64_mut(interners, x).map(Self)
    }

    /// Interns a string JSON value.
    ///
    /// See also [`string_mut()`](Self::string_mut), which is more efficient if
    /// you hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_string()`](Self::try_string) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn string(interners: &Jinterners<H>, s: &str) -> Self {
        Self(IValueImpl::string(interners, s))
    }

    /// Interns an string JSON value.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also [`try_string_mut()`](Self::try_string_mut), which is more
    /// efficient if you hold a mutable reference to the [`Jinterners`] arena as
    /// it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn try_string(interners: &Jinterners<H>, s: &str) -> Option<Self> {
        IValueImpl::try_string(interners, s).map(Self)
    }

    /// Interns a string JSON value.
    ///
    /// Contrary to [`string()`](Self::string), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_string_mut()`](Self::try_string_mut) for a fallible API.
    pub fn string_mut(interners: &mut Jinterners<H>, s: &str) -> Self {
        Self(IValueImpl::string_mut(interners, s))
    }

    /// Interns a string JSON value.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_string()`](Self::try_string), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    pub fn try_string_mut(interners: &mut Jinterners<H>, s: &str) -> Option<Self> {
        IValueImpl::try_string_mut(interners, s).map(Self)
    }

    /// Interns a JSON array, whose items are already interned.
    ///
    /// See also:
    /// - [`array_mut()`](Self::array_mut), which is more efficient if you hold
    ///   a mutable reference to the [`Jinterners`] arena as it avoids acquiring
    ///   locks,
    /// - [`seq()`](Self::seq) and [`unordered_set()`](Self::unordered_set) if
    ///   you know the [`ArrayOrder`] policy at compile time.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_array()`](Self::try_array) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn array(interners: &Jinterners<H>, a: &mut [Self], order: ArrayOrder) -> Self {
        Self(IValueImpl::array(interners, a, order))
    }

    /// Interns a JSON array, whose items are already interned.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also:
    /// - [`try_array_mut()`](Self::try_array_mut), which is more efficient if
    ///   you hold a mutable reference to the [`Jinterners`] arena as it avoids
    ///   acquiring locks,
    /// - [`try_seq()`](Self::try_seq) and
    ///   [`try_unordered_set()`](Self::try_unordered_set) if you know the
    ///   [`ArrayOrder`] policy at compile time.
    #[cfg(feature = "sync")]
    pub fn try_array(interners: &Jinterners<H>, a: &mut [Self], order: ArrayOrder) -> Option<Self> {
        IValueImpl::try_array(interners, a, order).map(Self)
    }

    /// Interns a JSON array, whose items are already interned.
    ///
    /// Contrary to [`array()`](Self::array), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    ///
    /// See also [`seq_mut()`](Self::seq_mut) and
    /// [`unordered_set_mut()`](Self::unordered_set_mut) if you know the
    /// [`ArrayOrder`] policy at compile time.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_array_mut()`](Self::try_array_mut) for a fallible API.
    pub fn array_mut(interners: &mut Jinterners<H>, a: &mut [Self], order: ArrayOrder) -> Self {
        Self(IValueImpl::array_mut(interners, a, order))
    }

    /// Interns a JSON array, whose items are already interned.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_array()`](Self::try_array), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    ///
    /// See also [`try_seq_mut()`](Self::try_seq_mut) and
    /// [`try_unordered_set_mut()`](Self::try_unordered_set_mut) if you know the
    /// [`ArrayOrder`] policy at compile time.
    pub fn try_array_mut(
        interners: &mut Jinterners<H>,
        a: &mut [Self],
        order: ArrayOrder,
    ) -> Option<Self> {
        IValueImpl::try_array_mut(interners, a, order).map(Self)
    }

    /// Interns a JSON array, preserving its sequential order.
    ///
    /// See also:
    /// - [`seq_mut()`](Self::seq_mut), which is more efficient if you hold a
    ///   mutable reference to the [`Jinterners`] arena as it avoids acquiring
    ///   locks,
    /// - [`array()`](Self::array) if you don't know the [`ArrayOrder`] policy
    ///   at compile time.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_seq()`](Self::try_seq) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn seq(interners: &Jinterners<H>, a: &[Self]) -> Self {
        Self(IValueImpl::seq(interners, a))
    }

    /// Interns a JSON array, preserving its sequential order.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also:
    /// - [`try_seq_mut()`](Self::try_seq_mut), which is more efficient if you
    ///   hold a mutable reference to the [`Jinterners`] arena as it avoids
    ///   acquiring locks,
    /// - [`try_array()`](Self::try_array) if you don't know the [`ArrayOrder`]
    ///   policy at compile time.
    #[cfg(feature = "sync")]
    pub fn try_seq(interners: &Jinterners<H>, a: &[Self]) -> Option<Self> {
        IValueImpl::try_seq(interners, a).map(Self)
    }

    /// Interns a JSON array, preserving its sequential order.
    ///
    /// Contrary to [`seq()`](Self::seq), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    ///
    /// See also [`array_mut()`](Self::array_mut) if you don't know the
    /// [`ArrayOrder`] policy at compile time.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_seq_mut()`](Self::try_seq_mut) for a fallible API.
    pub fn seq_mut(interners: &mut Jinterners<H>, a: &[Self]) -> Self {
        Self(IValueImpl::seq_mut(interners, a))
    }

    /// Interns a JSON array, preserving its sequential order.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_seq()`](Self::try_seq), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    ///
    /// See also [`try_array_mut()`](Self::try_array_mut) if you don't know the
    /// [`ArrayOrder`] policy at compile time.
    pub fn try_seq_mut(interners: &mut Jinterners<H>, a: &[Self]) -> Option<Self> {
        IValueImpl::try_seq_mut(interners, a).map(Self)
    }

    /// Interns a JSON array as an unordered set.
    ///
    /// See also:
    /// - [`unordered_set_mut()`](Self::unordered_set_mut), which is more
    ///   efficient if you hold a mutable reference to the [`Jinterners`] arena
    ///   as it avoids acquiring locks,
    /// - [`array()`](Self::array) if you don't know the [`ArrayOrder`] policy
    ///   at compile time.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_unordered_set()`](Self::try_unordered_set) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn unordered_set(interners: &Jinterners<H>, a: &mut [Self]) -> Self {
        Self(IValueImpl::unordered_set(interners, a))
    }

    /// Interns a JSON array as an unordered set.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also:
    /// - [`try_unordered_set_mut()`](Self::try_unordered_set_mut), which is
    ///   more efficient if you hold a mutable reference to the [`Jinterners`]
    ///   arena as it avoids acquiring locks,
    /// - [`try_array()`](Self::try_array) if you don't know the [`ArrayOrder`]
    ///   policy at compile time.
    #[cfg(feature = "sync")]
    pub fn try_unordered_set(interners: &Jinterners<H>, a: &mut [Self]) -> Option<Self> {
        IValueImpl::try_unordered_set(interners, a).map(Self)
    }

    /// Interns a JSON array in arbitrary order.
    ///
    /// Contrary to [`unordered_set()`](Self::unordered_set), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    ///
    /// See also [`array_mut()`](Self::array_mut) if you don't know the
    /// [`ArrayOrder`] policy at compile time.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_unordered_set_mut()`](Self::try_unordered_set_mut) for a fallible
    /// API.
    pub fn unordered_set_mut(interners: &mut Jinterners<H>, a: &mut [Self]) -> Self {
        Self(IValueImpl::unordered_set_mut(interners, a))
    }

    /// Interns a JSON array as an unordered set.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_unordered_set()`](Self::try_unordered_set), no locks
    /// are held internally because this function already takes an exclusive
    /// mutable reference to the [`Jinterners`] arena.
    ///
    /// See also [`try_array_mut()`](Self::try_array_mut) if you don't know the
    /// [`ArrayOrder`] policy at compile time.
    pub fn try_unordered_set_mut(interners: &mut Jinterners<H>, a: &mut [Self]) -> Option<Self> {
        IValueImpl::try_unordered_set_mut(interners, a).map(Self)
    }

    /// Interns a JSON object, whose values are already interned.
    ///
    /// See also [`object_mut()`](Self::object_mut), which is more efficient if
    /// you hold a mutable reference to the [`Jinterners`] arena as it avoids
    /// acquiring locks.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_object()`](Self::try_object) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn object<'a>(interners: &Jinterners<H>, o: impl Iterator<Item = (&'a str, Self)>) -> Self {
        Self(IValueImpl::object(
            interners,
            &mut o
                .map(|(k, v)| (InternedStrKey::new(interners, k), v))
                .collect::<Box<[_]>>(),
        ))
    }

    /// Interns a JSON object, whose values are already interned.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also [`try_object_mut()`](Self::try_object_mut), which is more
    /// efficient if you hold a mutable reference to the [`Jinterners`] arena as
    /// it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn try_object<'a>(
        interners: &Jinterners<H>,
        o: impl Iterator<Item = (&'a str, Self)>,
    ) -> Option<Self> {
        IValueImpl::try_object(
            interners,
            &mut o
                .map(|(k, v)| Some((InternedStrKey::try_new(interners, k)?, v)))
                .collect::<Option<Box<[_]>>>()?,
        )
        .map(Self)
    }

    /// Interns a JSON object, whose values are already interned.
    ///
    /// Contrary to [`object()`](Self::object), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// the [`Jinterners`] arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_object_mut()`](Self::try_object_mut) for a fallible API.
    pub fn object_mut<'a>(
        interners: &mut Jinterners<H>,
        o: impl Iterator<Item = (&'a str, Self)>,
    ) -> Self {
        let mut o = o
            .map(|(k, v)| (InternedStrKey::new_mut(interners, k), v))
            .collect::<Box<[_]>>();
        Self(IValueImpl::object_mut(interners, &mut o))
    }

    /// Interns a JSON object, whose values are already interned.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_object()`](Self::try_object), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    pub fn try_object_mut<'a>(
        interners: &mut Jinterners<H>,
        o: impl Iterator<Item = (&'a str, Self)>,
    ) -> Option<Self> {
        let mut o = o
            .map(|(k, v)| Some((InternedStrKey::try_new_mut(interners, k)?, v)))
            .collect::<Option<Box<[_]>>>()?;
        IValueImpl::try_object_mut(interners, &mut o).map(Self)
    }

    /// Interns a JSON object, whose keys and values are already interned.
    ///
    /// See also [`object_from_keys_mut()`](Self::object_from_keys_mut), which
    /// is more efficient if you hold a mutable reference to the [`Jinterners`]
    /// arena as it avoids acquiring locks.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_object_from_keys()`](Self::try_object_from_keys) for a fallible
    /// API.
    #[cfg(feature = "sync")]
    pub fn object_from_keys(
        interners: &Jinterners<H>,
        o: impl Iterator<Item = (InternedStrKey<H>, Self)>,
    ) -> Self {
        Self(IValueImpl::object(interners, &mut o.collect::<Box<[_]>>()))
    }

    /// Interns a JSON object, whose keys and values are already interned.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// See also [`try_object_from_keys_mut()`](Self::try_object_from_keys_mut),
    /// which is more efficient if you hold a mutable reference to the
    /// [`Jinterners`] arena as it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn try_object_from_keys(
        interners: &Jinterners<H>,
        o: impl Iterator<Item = (InternedStrKey<H>, Self)>,
    ) -> Option<Self> {
        IValueImpl::try_object(interners, &mut o.collect::<Box<[_]>>()).map(Self)
    }

    /// Interns a JSON object, whose keys and values are already interned.
    ///
    /// Contrary to [`object_from_keys()`](Self::object_from_keys), no locks are
    /// held internally because this function already takes an exclusive mutable
    /// reference to the [`Jinterners`] arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full. Use
    /// [`try_object_from_keys_mut()`](Self::try_object_from_keys_mut) for a
    /// fallible API.
    pub fn object_from_keys_mut(
        interners: &mut Jinterners<H>,
        o: impl Iterator<Item = (InternedStrKey<H>, Self)>,
    ) -> Self {
        Self(IValueImpl::object_mut(
            interners,
            &mut o.collect::<Box<[_]>>(),
        ))
    }

    /// Interns a JSON object, whose keys and values are already interned.
    ///
    /// This returns [`None`] if the arena is full.
    ///
    /// Contrary to [`try_object_from_keys()`](Self::try_object_from_keys), no
    /// locks are held internally because this function already takes an
    /// exclusive mutable reference to the [`Jinterners`] arena.
    pub fn try_object_from_keys_mut(
        interners: &mut Jinterners<H>,
        o: impl Iterator<Item = (InternedStrKey<H>, Self)>,
    ) -> Option<Self> {
        IValueImpl::try_object_mut(interners, &mut o.collect::<Box<[_]>>()).map(Self)
    }

    #[cfg(feature = "sync")]
    pub(crate) fn from(interners: &Jinterners<H>, source: &Value, array_order: ArrayOrder) -> Self {
        Self(IValueImpl::from(interners, source, array_order))
    }

    #[cfg(feature = "sync")]
    pub(crate) fn try_from(
        interners: &Jinterners<H>,
        source: &Value,
        array_order: ArrayOrder,
    ) -> Option<Self> {
        IValueImpl::try_from(interners, source, array_order).map(Self)
    }

    pub(crate) fn from_mut(
        interners: &mut Jinterners<H>,
        source: &Value,
        array_order: ArrayOrder,
    ) -> Self {
        Self(IValueImpl::from_mut(interners, source, array_order))
    }

    pub(crate) fn try_from_mut(
        interners: &mut Jinterners<H>,
        source: &Value,
        array_order: ArrayOrder,
    ) -> Option<Self> {
        IValueImpl::try_from_mut(interners, source, array_order).map(Self)
    }

    pub(crate) fn lookup(&self, interners: &Jinterners<H>) -> Value {
        self.0.lookup(interners)
    }

    pub(crate) fn try_lookup(&self, interners: &Jinterners<H>) -> Option<Value> {
        self.0.try_lookup(interners)
    }

    pub(crate) fn lookup_ref<'a>(&self, interners: &'a Jinterners<H>) -> ValueRef<'a, H> {
        self.0.lookup_ref(interners)
    }

    pub(crate) fn try_lookup_ref<'a>(
        &self,
        interners: &'a Jinterners<H>,
    ) -> Option<ValueRef<'a, H>> {
        self.0.try_lookup_ref(interners)
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
        array_order: ArrayOrder,
    ) -> Result<Self, serde_json::error::Error>
    where
        T: Serialize,
    {
        value
            .serialize(ValueSerializer {
                interners,
                buffers: &mut BufferPool::default(),
                array_order,
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
        array_order: ArrayOrder,
    ) -> Result<Self, serde_json::error::Error>
    where
        T: Serialize,
    {
        value
            .serialize(ValueSerializerMut {
                interners,
                buffers: &mut BufferPool::default(),
                array_order,
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
        array_order: ArrayOrder,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_slice(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializer::new(&mut json_de, interners, &mut buffers, array_order);
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
        array_order: ArrayOrder,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_str(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializer::new(&mut json_de, interners, &mut buffers, array_order);
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
        array_order: ArrayOrder,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_reader(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializer::new(&mut json_de, interners, &mut buffers, array_order);
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
        array_order: ArrayOrder,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_slice(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializerMut::new(&mut json_de, interners, &mut buffers, array_order);
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
        array_order: ArrayOrder,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_str(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializerMut::new(&mut json_de, interners, &mut buffers, array_order);
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
        array_order: ArrayOrder,
    ) -> Result<Self, serde_json::error::Error> {
        let mut json_de = SerdeJsonDeserializer::from_reader(json);
        let mut buffers = BufferPool::default();
        let de = InterningDeserializerMut::new(&mut json_de, interners, &mut buffers, array_order);
        let value = de.deserialize()?;
        json_de.end()?;
        Ok(value)
    }

    /// Convenience function to call [`serde_json::to_vec()`] on a
    /// [`BoundValue`] that combines this value with the given [`Jinterners`].
    #[cfg(feature = "serde")]
    pub fn to_json_bytes(
        &self,
        interners: &Jinterners<H>,
    ) -> Result<Vec<u8>, serde_json::error::Error> {
        serde_json::to_vec(&BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_vec_pretty()`] on a
    /// [`BoundValue`] that combines this value with the given [`Jinterners`].
    #[cfg(feature = "serde")]
    pub fn to_json_bytes_pretty(
        &self,
        interners: &Jinterners<H>,
    ) -> Result<Vec<u8>, serde_json::error::Error> {
        serde_json::to_vec_pretty(&BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_string()`] on a
    /// [`BoundValue`] that combines this value with the given [`Jinterners`].
    #[cfg(feature = "serde")]
    pub fn to_json_string(
        &self,
        interners: &Jinterners<H>,
    ) -> Result<String, serde_json::error::Error> {
        serde_json::to_string(&BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_string_pretty()`] on a
    /// [`BoundValue`] that combines this value with the given [`Jinterners`].
    #[cfg(feature = "serde")]
    pub fn to_json_string_pretty(
        &self,
        interners: &Jinterners<H>,
    ) -> Result<String, serde_json::error::Error> {
        serde_json::to_string_pretty(&BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_writer()`] on a
    /// [`BoundValue`] that combines this value with the given [`Jinterners`].
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn to_json_writer<W: Write>(
        &self,
        writer: W,
        interners: &Jinterners<H>,
    ) -> Result<(), serde_json::error::Error> {
        serde_json::to_writer(writer, &BoundValue::new(self, interners))
    }

    /// Convenience function to call [`serde_json::to_writer_pretty()`] on a
    /// [`BoundValue`] that combines this value with the given [`Jinterners`].
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
            IValueImpl::Null | IValueImpl::Bool(_) | IValueImpl::U32(_) | IValueImpl::I32(_) => {
                false
            }
            IValueImpl::U64(x) => builder.u64s.insert(x),
            IValueImpl::I64(x) => builder.i64s.insert(x),
            IValueImpl::F64(x) => builder.f64s.insert(x),
            IValueImpl::String(s) => builder.strings.insert(s),
            IValueImpl::Array(a) => {
                if builder.arrays.insert(a) {
                    builder.queue_arrays.push(a);
                    true
                } else {
                    false
                }
            }
            IValueImpl::Set(a) => {
                if builder.sets.insert(a) {
                    builder.queue_sets.push(a);
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
pub(crate) struct Float64(OrderedFloat<f64>);

impl Debug for Float64 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.0.fmt(f)
    }
}

#[cfg(feature = "postcard-schema")]
impl Schema for Float64 {
    const SCHEMA: &'static NamedType = f64::SCHEMA;
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
    U32([u8; 4]),
    I32([u8; 4]),
    U64(Interned<u64, u64, H, U32>),
    I64(Interned<i64, i64, H, U32>),
    F64(Interned<Float64, Float64, H, U32>),
    String(InternedStr<H, U32>),
    Array(InternedSlice<IValue<H>, H, U32>),
    Set(InternedSlice<IValue<H>, H, U32>),
    Object(InternedSlice<(InternedStrKey<H>, IValue<H>), H, U32>),
}

impl<H> Debug for IValueImpl<H> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IValueImpl::Null => f.debug_tuple("IValue::Null").finish(),
            IValueImpl::Bool(x) => f.debug_tuple("IValue::Bool").field(x).finish(),
            IValueImpl::U32(x) => f
                .debug_tuple("IValue::U32")
                .field(&u32::from_ne_bytes(*x))
                .finish(),
            IValueImpl::I32(x) => f
                .debug_tuple("IValue::I32")
                .field(&i32::from_ne_bytes(*x))
                .finish(),
            IValueImpl::U64(x) => f.debug_tuple("IValue::U64").field(x).finish(),
            IValueImpl::I64(x) => f.debug_tuple("IValue::I64").field(x).finish(),
            IValueImpl::F64(x) => f.debug_tuple("IValue::F64").field(x).finish(),
            IValueImpl::String(s) => f.debug_tuple("IValue::String").field(s).finish(),
            IValueImpl::Array(a) => f.debug_tuple("IValue::Array").field(a).finish(),
            IValueImpl::Set(a) => f.debug_tuple("IValue::Set").field(a).finish(),
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
            (IValueImpl::U32(a), IValueImpl::U32(b)) => a == b,
            (IValueImpl::I32(a), IValueImpl::I32(b)) => a == b,
            (IValueImpl::U64(a), IValueImpl::U64(b)) => a == b,
            (IValueImpl::I64(a), IValueImpl::I64(b)) => a == b,
            (IValueImpl::F64(a), IValueImpl::F64(b)) => a == b,
            (IValueImpl::String(a), IValueImpl::String(b)) => a == b,
            (IValueImpl::Array(a), IValueImpl::Array(b)) => a == b,
            (IValueImpl::Set(a), IValueImpl::Set(b)) => a == b,
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
                (IValueImpl::U32(a), IValueImpl::U32(b)) => {
                    u32::from_ne_bytes(*a).cmp(&u32::from_ne_bytes(*b))
                }
                (IValueImpl::I32(a), IValueImpl::I32(b)) => {
                    i32::from_ne_bytes(*a).cmp(&i32::from_ne_bytes(*b))
                }
                (IValueImpl::U64(a), IValueImpl::U64(b)) => a.cmp(b),
                (IValueImpl::I64(a), IValueImpl::I64(b)) => a.cmp(b),
                (IValueImpl::F64(a), IValueImpl::F64(b)) => a.cmp(b),
                (IValueImpl::String(a), IValueImpl::String(b)) => a.cmp(b),
                (IValueImpl::Array(a), IValueImpl::Array(b)) => a.cmp(b),
                (IValueImpl::Set(a), IValueImpl::Set(b)) => a.cmp(b),
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
            IValueImpl::U32(x) => x.hash(state),
            IValueImpl::I32(x) => x.hash(state),
            IValueImpl::U64(x) => x.hash(state),
            IValueImpl::I64(x) => x.hash(state),
            IValueImpl::F64(x) => x.hash(state),
            IValueImpl::String(s) => s.hash(state),
            IValueImpl::Array(a) => a.hash(state),
            IValueImpl::Set(a) => a.hash(state),
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
            IValueImpl::U32(x) => serializer.serialize_newtype_variant(
                "IValueImpl",
                2,
                "U32",
                &u32::from_ne_bytes(*x),
            ),
            IValueImpl::I32(x) => serializer.serialize_newtype_variant(
                "IValueImpl",
                3,
                "I32",
                &i32::from_ne_bytes(*x),
            ),
            IValueImpl::U64(x) => serializer.serialize_newtype_variant("IValueImpl", 4, "U64", x),
            IValueImpl::I64(x) => serializer.serialize_newtype_variant("IValueImpl", 5, "I64", x),
            IValueImpl::F64(x) => serializer.serialize_newtype_variant("IValueImpl", 6, "F64", x),
            IValueImpl::String(x) => {
                serializer.serialize_newtype_variant("IValueImpl", 7, "String", x)
            }
            IValueImpl::Array(x) => {
                serializer.serialize_newtype_variant("IValueImpl", 8, "Array", x)
            }
            IValueImpl::Set(x) => serializer.serialize_newtype_variant("IValueImpl", 9, "Set", x),
            IValueImpl::Object(x) => {
                serializer.serialize_newtype_variant("IValueImpl", 10, "Object", x)
            }
        }
    }
}

#[cfg(feature = "postcard-schema")]
impl<H> Schema for IValueImpl<H> {
    const SCHEMA: &'static NamedType = &NamedType {
        name: "IValue",
        ty: &DataModelType::Enum(&[
            &NamedVariant {
                name: "Null",
                ty: &DataModelVariant::UnitVariant,
            },
            &NamedVariant {
                name: "Bool",
                ty: &DataModelVariant::NewtypeVariant(bool::SCHEMA),
            },
            &NamedVariant {
                name: "U32",
                ty: &DataModelVariant::NewtypeVariant(u32::SCHEMA),
            },
            &NamedVariant {
                name: "I32",
                ty: &DataModelVariant::NewtypeVariant(i32::SCHEMA),
            },
            &NamedVariant {
                name: "U64",
                ty: &DataModelVariant::NewtypeVariant(Interned::<u64, u64, H, U32>::SCHEMA),
            },
            &NamedVariant {
                name: "I64",
                ty: &DataModelVariant::NewtypeVariant(Interned::<i64, i64, H, U32>::SCHEMA),
            },
            &NamedVariant {
                name: "F64",
                ty: &DataModelVariant::NewtypeVariant(Interned::<Float64, Float64, H, U32>::SCHEMA),
            },
            &NamedVariant {
                name: "String",
                ty: &DataModelVariant::NewtypeVariant(InternedStr::<H, U32>::SCHEMA),
            },
            &NamedVariant {
                name: "Array",
                ty: &DataModelVariant::NewtypeVariant(InternedSlice::<IValue<H>, H, U32>::SCHEMA),
            },
            &NamedVariant {
                name: "Set",
                ty: &DataModelVariant::NewtypeVariant(InternedSlice::<IValue<H>, H, U32>::SCHEMA),
            },
            &NamedVariant {
                name: "Object",
                ty: &DataModelVariant::NewtypeVariant(
                    InternedSlice::<(InternedStrKey<H>, IValue<H>), H, U32>::SCHEMA,
                ),
            },
        ]),
    };
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
                "Null", "Bool", "U32", "I32", "U64", "I64", "F64", "String", "Array", "Set",
                "Object",
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
            U32,
            I32,
            U64,
            I64,
            F64,
            String,
            Array,
            Set,
            Object,
        }

        let (variant, data) = data.variant()?;
        Ok(match variant {
            IValueImplVariant::Null => {
                data.unit_variant()?;
                IValueImpl::Null
            }
            IValueImplVariant::Bool => IValueImpl::Bool(data.newtype_variant()?),
            IValueImplVariant::U32 => {
                let x: u32 = data.newtype_variant()?;
                IValueImpl::U32(x.to_ne_bytes())
            }
            IValueImplVariant::I32 => {
                let x: i32 = data.newtype_variant()?;
                IValueImpl::I32(x.to_ne_bytes())
            }
            IValueImplVariant::U64 => IValueImpl::U64(data.newtype_variant()?),
            IValueImplVariant::I64 => IValueImpl::I64(data.newtype_variant()?),
            IValueImplVariant::F64 => IValueImpl::F64(data.newtype_variant()?),
            IValueImplVariant::String => IValueImpl::String(data.newtype_variant()?),
            IValueImplVariant::Array => IValueImpl::Array(data.newtype_variant()?),
            IValueImplVariant::Set => IValueImpl::Set(data.newtype_variant()?),
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
            IValueImpl::U32(_) => 2,
            IValueImpl::I32(_) => 3,
            IValueImpl::U64(_) => 4,
            IValueImpl::I64(_) => 5,
            IValueImpl::F64(_) => 6,
            IValueImpl::String(_) => 7,
            IValueImpl::Array(_) => 8,
            IValueImpl::Set(_) => 9,
            IValueImpl::Object(_) => 10,
        }
    }
}

impl<H> IValueImpl<H>
where
    H: BuildHasher,
{
    #[cfg(feature = "sync")]
    fn u64(interners: &Jinterners<H>, x: u64) -> Self {
        if let Ok(x32) = u32::try_from(x) {
            IValueImpl::U32(x32.to_ne_bytes())
        } else {
            IValueImpl::U64(interners.uint64.intern(x))
        }
    }

    #[cfg(feature = "sync")]
    fn try_u64(interners: &Jinterners<H>, x: u64) -> Option<Self> {
        Some(if let Ok(x32) = u32::try_from(x) {
            IValueImpl::U32(x32.to_ne_bytes())
        } else {
            IValueImpl::U64(interners.uint64.try_intern(x)?)
        })
    }

    fn u64_mut(interners: &mut Jinterners<H>, x: u64) -> Self {
        if let Ok(x32) = u32::try_from(x) {
            IValueImpl::U32(x32.to_ne_bytes())
        } else {
            IValueImpl::U64(interners.uint64.intern_mut(x))
        }
    }

    fn try_u64_mut(interners: &mut Jinterners<H>, x: u64) -> Option<Self> {
        Some(if let Ok(x32) = u32::try_from(x) {
            IValueImpl::U32(x32.to_ne_bytes())
        } else {
            IValueImpl::U64(interners.uint64.try_intern_mut(x)?)
        })
    }

    #[cfg(feature = "sync")]
    fn i64(interners: &Jinterners<H>, x: i64) -> Self {
        if let Ok(x32) = i32::try_from(x) {
            IValueImpl::I32(x32.to_ne_bytes())
        } else {
            IValueImpl::I64(interners.int64.intern(x))
        }
    }

    #[cfg(feature = "sync")]
    fn try_i64(interners: &Jinterners<H>, x: i64) -> Option<Self> {
        Some(if let Ok(x32) = i32::try_from(x) {
            IValueImpl::I32(x32.to_ne_bytes())
        } else {
            IValueImpl::I64(interners.int64.try_intern(x)?)
        })
    }

    fn i64_mut(interners: &mut Jinterners<H>, x: i64) -> Self {
        if let Ok(x32) = i32::try_from(x) {
            IValueImpl::I32(x32.to_ne_bytes())
        } else {
            IValueImpl::I64(interners.int64.intern_mut(x))
        }
    }

    fn try_i64_mut(interners: &mut Jinterners<H>, x: i64) -> Option<Self> {
        Some(if let Ok(x32) = i32::try_from(x) {
            IValueImpl::I32(x32.to_ne_bytes())
        } else {
            IValueImpl::I64(interners.int64.try_intern_mut(x)?)
        })
    }

    #[cfg(feature = "sync")]
    fn f64(interners: &Jinterners<H>, x: f64) -> Self {
        IValueImpl::F64(interners.float64.intern(Float64(OrderedFloat(x))))
    }

    #[cfg(feature = "sync")]
    fn try_f64(interners: &Jinterners<H>, x: f64) -> Option<Self> {
        interners
            .float64
            .try_intern(Float64(OrderedFloat(x)))
            .map(IValueImpl::F64)
    }

    fn f64_mut(interners: &mut Jinterners<H>, x: f64) -> Self {
        IValueImpl::F64(interners.float64.intern_mut(Float64(OrderedFloat(x))))
    }

    fn try_f64_mut(interners: &mut Jinterners<H>, x: f64) -> Option<Self> {
        interners
            .float64
            .try_intern_mut(Float64(OrderedFloat(x)))
            .map(IValueImpl::F64)
    }

    #[cfg(feature = "sync")]
    fn string(interners: &Jinterners<H>, s: &str) -> Self {
        IValueImpl::String(interners.string.intern(s))
    }

    #[cfg(feature = "sync")]
    fn try_string(interners: &Jinterners<H>, s: &str) -> Option<Self> {
        interners.string.try_intern(s).map(IValueImpl::String)
    }

    fn string_mut(interners: &mut Jinterners<H>, s: &str) -> Self {
        IValueImpl::String(interners.string.intern_mut(s))
    }

    fn try_string_mut(interners: &mut Jinterners<H>, s: &str) -> Option<Self> {
        interners.string.try_intern_mut(s).map(IValueImpl::String)
    }

    #[cfg(feature = "sync")]
    fn array(interners: &Jinterners<H>, a: &mut [IValue<H>], order: ArrayOrder) -> Self {
        match order {
            ArrayOrder::Preserve => IValueImpl::seq(interners, a),
            ArrayOrder::Unordered => IValueImpl::unordered_set(interners, a),
        }
    }

    #[cfg(feature = "sync")]
    fn try_array(
        interners: &Jinterners<H>,
        a: &mut [IValue<H>],
        order: ArrayOrder,
    ) -> Option<Self> {
        match order {
            ArrayOrder::Preserve => IValueImpl::try_seq(interners, a),
            ArrayOrder::Unordered => IValueImpl::try_unordered_set(interners, a),
        }
    }

    fn array_mut(interners: &mut Jinterners<H>, a: &mut [IValue<H>], order: ArrayOrder) -> Self {
        match order {
            ArrayOrder::Preserve => IValueImpl::seq_mut(interners, a),
            ArrayOrder::Unordered => IValueImpl::unordered_set_mut(interners, a),
        }
    }

    fn try_array_mut(
        interners: &mut Jinterners<H>,
        a: &mut [IValue<H>],
        order: ArrayOrder,
    ) -> Option<Self> {
        match order {
            ArrayOrder::Preserve => IValueImpl::try_seq_mut(interners, a),
            ArrayOrder::Unordered => IValueImpl::try_unordered_set_mut(interners, a),
        }
    }

    #[cfg(feature = "sync")]
    fn seq(interners: &Jinterners<H>, a: &[IValue<H>]) -> Self {
        IValueImpl::Array(interners.iarray.intern_copy(a))
    }

    #[cfg(feature = "sync")]
    fn try_seq(interners: &Jinterners<H>, a: &[IValue<H>]) -> Option<Self> {
        interners.iarray.try_intern_copy(a).map(IValueImpl::Array)
    }

    fn seq_mut(interners: &mut Jinterners<H>, a: &[IValue<H>]) -> Self {
        IValueImpl::Array(interners.iarray.intern_copy_mut(a))
    }

    fn try_seq_mut(interners: &mut Jinterners<H>, a: &[IValue<H>]) -> Option<Self> {
        interners
            .iarray
            .try_intern_copy_mut(a)
            .map(IValueImpl::Array)
    }

    #[cfg(feature = "sync")]
    fn unordered_set(interners: &Jinterners<H>, a: &mut [IValue<H>]) -> Self {
        a.sort_unstable();
        IValueImpl::Set(interners.iset.intern_copy(a))
    }

    #[cfg(feature = "sync")]
    fn try_unordered_set(interners: &Jinterners<H>, a: &mut [IValue<H>]) -> Option<Self> {
        a.sort_unstable();
        interners.iset.try_intern_copy(a).map(IValueImpl::Set)
    }

    fn unordered_set_mut(interners: &mut Jinterners<H>, a: &mut [IValue<H>]) -> Self {
        a.sort_unstable();
        IValueImpl::Set(interners.iset.intern_copy_mut(a))
    }

    fn try_unordered_set_mut(interners: &mut Jinterners<H>, a: &mut [IValue<H>]) -> Option<Self> {
        a.sort_unstable();
        interners.iset.try_intern_copy_mut(a).map(IValueImpl::Set)
    }

    #[cfg(feature = "sync")]
    fn object(interners: &Jinterners<H>, o: &mut [(InternedStrKey<H>, IValue<H>)]) -> Self {
        o.sort_unstable_by_key(|(k, _)| *k);
        IValueImpl::Object(interners.iobject.intern_copy(o))
    }

    #[cfg(feature = "sync")]
    fn try_object(
        interners: &Jinterners<H>,
        o: &mut [(InternedStrKey<H>, IValue<H>)],
    ) -> Option<Self> {
        o.sort_unstable_by_key(|(k, _)| *k);
        interners.iobject.try_intern_copy(o).map(IValueImpl::Object)
    }

    fn object_mut(interners: &mut Jinterners<H>, o: &mut [(InternedStrKey<H>, IValue<H>)]) -> Self {
        o.sort_unstable_by_key(|(k, _)| *k);
        IValueImpl::Object(interners.iobject.intern_copy_mut(o))
    }

    fn try_object_mut(
        interners: &mut Jinterners<H>,
        o: &mut [(InternedStrKey<H>, IValue<H>)],
    ) -> Option<Self> {
        o.sort_unstable_by_key(|(k, _)| *k);
        interners
            .iobject
            .try_intern_copy_mut(o)
            .map(IValueImpl::Object)
    }

    /// Interns an object containing a single key-value pair.
    #[cfg(all(feature = "serde", feature = "sync"))]
    fn try_singleton(
        interners: &Jinterners<H>,
        key: InternedStrKey<H>,
        value: IValue<H>,
    ) -> Option<Self> {
        interners
            .iobject
            .try_intern_copy(&[(key, value)])
            .map(IValueImpl::Object)
    }

    #[cfg(feature = "serde")]
    fn try_singleton_mut(
        interners: &mut Jinterners<H>,
        key: InternedStrKey<H>,
        value: IValue<H>,
    ) -> Option<Self> {
        interners
            .iobject
            .try_intern_copy_mut(&[(key, value)])
            .map(IValueImpl::Object)
    }

    #[cfg(feature = "sync")]
    fn from(interners: &Jinterners<H>, source: &Value, array_order: ArrayOrder) -> Self {
        match source {
            Value::Null => IValueImpl::Null,
            Value::Bool(x) => IValueImpl::Bool(*x),
            Value::Number(x) => {
                if x.is_u64() {
                    IValueImpl::u64(interners, x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValueImpl::i64(interners, x.as_i64().unwrap())
                } else {
                    IValueImpl::f64(interners, x.as_f64().unwrap())
                }
            }
            Value::String(s) => IValueImpl::string(interners, s.as_str()),
            Value::Array(a) => IValueImpl::array(
                interners,
                &mut a
                    .iter()
                    .map(|v| interners.intern(v, array_order))
                    .collect::<Box<[_]>>(),
                array_order,
            ),
            Value::Object(o) => IValueImpl::object(
                interners,
                &mut o
                    .iter()
                    .map(|(k, v)| {
                        (
                            InternedStrKey::new(interners, k.as_str()),
                            interners.intern(v, array_order),
                        )
                    })
                    .collect::<Box<[_]>>(),
            ),
        }
    }

    #[cfg(feature = "sync")]
    fn try_from(
        interners: &Jinterners<H>,
        source: &Value,
        array_order: ArrayOrder,
    ) -> Option<Self> {
        match source {
            Value::Null => Some(IValueImpl::Null),
            Value::Bool(x) => Some(IValueImpl::Bool(*x)),
            Value::Number(x) => {
                if x.is_u64() {
                    IValueImpl::try_u64(interners, x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValueImpl::try_i64(interners, x.as_i64().unwrap())
                } else {
                    IValueImpl::try_f64(interners, x.as_f64().unwrap())
                }
            }
            Value::String(s) => IValueImpl::try_string(interners, s.as_str()),
            Value::Array(a) => IValueImpl::try_array(
                interners,
                &mut a
                    .iter()
                    .map(|v| interners.try_intern(v, array_order))
                    .collect::<Option<Box<[_]>>>()?,
                array_order,
            ),
            Value::Object(o) => IValueImpl::try_object(
                interners,
                &mut o
                    .iter()
                    .map(|(k, v)| {
                        Some((
                            InternedStrKey::try_new(interners, k.as_str())?,
                            interners.try_intern(v, array_order)?,
                        ))
                    })
                    .collect::<Option<Box<[_]>>>()?,
            ),
        }
    }

    fn from_mut(interners: &mut Jinterners<H>, source: &Value, array_order: ArrayOrder) -> Self {
        match source {
            Value::Null => IValueImpl::Null,
            Value::Bool(x) => IValueImpl::Bool(*x),
            Value::Number(x) => {
                if x.is_u64() {
                    IValueImpl::u64_mut(interners, x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValueImpl::i64_mut(interners, x.as_i64().unwrap())
                } else {
                    IValueImpl::f64_mut(interners, x.as_f64().unwrap())
                }
            }
            Value::String(s) => IValueImpl::string_mut(interners, s.as_str()),
            Value::Array(a) => {
                let mut a = a
                    .iter()
                    .map(|v| interners.intern_mut(v, array_order))
                    .collect::<Box<[_]>>();
                IValueImpl::array_mut(interners, &mut a, array_order)
            }
            Value::Object(o) => {
                let mut o = o
                    .iter()
                    .map(|(k, v)| {
                        (
                            InternedStrKey::new_mut(interners, k.as_str()),
                            interners.intern_mut(v, array_order),
                        )
                    })
                    .collect::<Box<[_]>>();
                IValueImpl::object_mut(interners, &mut o)
            }
        }
    }

    fn try_from_mut(
        interners: &mut Jinterners<H>,
        source: &Value,
        array_order: ArrayOrder,
    ) -> Option<Self> {
        match source {
            Value::Null => Some(IValueImpl::Null),
            Value::Bool(x) => Some(IValueImpl::Bool(*x)),
            Value::Number(x) => {
                if x.is_u64() {
                    IValueImpl::try_u64_mut(interners, x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValueImpl::try_i64_mut(interners, x.as_i64().unwrap())
                } else {
                    IValueImpl::try_f64_mut(interners, x.as_f64().unwrap())
                }
            }
            Value::String(s) => IValueImpl::try_string_mut(interners, s.as_str()),
            Value::Array(a) => {
                let mut a = a
                    .iter()
                    .map(|v| interners.try_intern_mut(v, array_order))
                    .collect::<Option<Box<[_]>>>()?;
                IValueImpl::try_array_mut(interners, &mut a, array_order)
            }
            Value::Object(o) => {
                let mut o = o
                    .iter()
                    .map(|(k, v)| {
                        Some((
                            InternedStrKey::try_new_mut(interners, k.as_str())?,
                            interners.try_intern_mut(v, array_order)?,
                        ))
                    })
                    .collect::<Option<Box<[_]>>>()?;
                IValueImpl::try_object_mut(interners, &mut o)
            }
        }
    }

    fn lookup(&self, interners: &Jinterners<H>) -> Value {
        match self.try_lookup(interners) {
            Some(x) => x,
            None => panic!("Interned index is out of bounds for this arena"),
        }
    }

    fn try_lookup(&self, interners: &Jinterners<H>) -> Option<Value> {
        Some(match self {
            IValueImpl::Null => Value::Null,
            IValueImpl::Bool(x) => Value::Bool(*x),
            IValueImpl::U32(x) => {
                Value::Number(Number::from_u128(u32::from_ne_bytes(*x) as u128).unwrap())
            }
            IValueImpl::I32(x) => {
                Value::Number(Number::from_i128(i32::from_ne_bytes(*x) as i128).unwrap())
            }
            IValueImpl::U64(x) => {
                let x = interners.uint64.try_lookup(*x)?;
                Value::Number(Number::from_u128(x as u128).unwrap())
            }
            IValueImpl::I64(x) => {
                let x = interners.int64.try_lookup(*x)?;
                Value::Number(Number::from_i128(x as i128).unwrap())
            }
            IValueImpl::F64(x) => {
                let Float64(OrderedFloat(x)) = interners.float64.try_lookup(*x)?;
                Value::Number(Number::from_f64(x).unwrap())
            }
            IValueImpl::String(s) => Value::String(interners.string.try_lookup(*s)?.into()),
            IValueImpl::Array(a) => Value::Array(
                interners
                    .iarray
                    .try_lookup(*a)?
                    .iter()
                    .map(|v| interners.try_lookup(v))
                    .collect::<Option<_>>()?,
            ),
            IValueImpl::Set(a) => Value::Array(
                interners
                    .iset
                    .try_lookup(*a)?
                    .iter()
                    .map(|v| interners.try_lookup(v))
                    .collect::<Option<_>>()?,
            ),
            IValueImpl::Object(o) => Value::Object(
                interners
                    .iobject
                    .try_lookup(*o)?
                    .iter()
                    .map(|(k, v)| {
                        Some((
                            interners.string.try_lookup(k.0)?.into(),
                            interners.try_lookup(v)?,
                        ))
                    })
                    .collect::<Option<_>>()?,
            ),
        })
    }

    fn lookup_ref<'a>(&self, interners: &'a Jinterners<H>) -> ValueRef<'a, H> {
        match self.try_lookup_ref(interners) {
            Some(x) => x,
            None => panic!("Interned index is out of bounds for this arena"),
        }
    }

    fn try_lookup_ref<'a>(&self, interners: &'a Jinterners<H>) -> Option<ValueRef<'a, H>> {
        Some(match self {
            IValueImpl::Null => ValueRef::Null,
            IValueImpl::Bool(x) => ValueRef::Bool(*x),
            IValueImpl::U32(x) => ValueRef::U64(u32::from_ne_bytes(*x) as u64),
            IValueImpl::I32(x) => ValueRef::I64(i32::from_ne_bytes(*x) as i64),
            IValueImpl::U64(x) => ValueRef::U64(interners.uint64.try_lookup(*x)?),
            IValueImpl::I64(x) => ValueRef::I64(interners.int64.try_lookup(*x)?),
            IValueImpl::F64(x) => ValueRef::F64(interners.float64.try_lookup(*x)?.0.0),
            IValueImpl::String(s) => ValueRef::String(interners.string.try_lookup(*s)?),
            IValueImpl::Array(a) => ValueRef::Array(interners.iarray.try_lookup(*a)?),
            IValueImpl::Set(a) => ValueRef::Set(interners.iset.try_lookup(*a)?),
            IValueImpl::Object(o) => ValueRef::Object(MapRef {
                arena_str: &interners.string,
                map: interners.iobject.try_lookup(*o)?,
            }),
        })
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
    /// JSON array, with its sequential order preserved.
    Array(&'a [IValue<H>]),
    /// JSON array, with items in arbitrary order.
    Set(&'a [IValue<H>]),
    /// JSON object.
    Object(MapRef<'a, H>),
}

/// A shallow reference to a JSON map.
pub struct MapRef<'a, H = DefaultBuildHasher> {
    arena_str: &'a ArenaStr<H, U32>,
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
    ///
    /// # Panics
    ///
    /// This function panics if the map references keys that are out of bounds
    /// for this arena, for example because the arena was truncated, or because
    /// of deserializing untrusted data. Use [`try_iter()`](Self::try_iter) for
    /// a fallible API.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&'a str, &'a IValue<H>)> {
        self.map
            .iter()
            .map(|(k, v)| (self.arena_str.lookup(k.0), v))
    }

    /// Iterates over the key-value pairs in this JSON map, in arbitrary order.
    ///
    /// See also [`iter_keys()`](Self::iter_keys) which is more efficient if you
    /// only need to manipulate [`InternedStrKey`]s as it doesn't resolve them
    /// to strings.
    ///
    /// This returns [`None`] for entries that reference a key that is out of
    /// bounds for this arena, for example because the arena was truncated, or
    /// because of deserializing untrusted data.
    pub fn try_iter(&self) -> impl ExactSizeIterator<Item = Option<(&'a str, &'a IValue<H>)>> {
        self.map
            .iter()
            .map(|(k, v)| Some((self.arena_str.try_lookup(k.0)?, v)))
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
    #[cfg(feature = "postcard-schema")]
    use blazinterner::{Arena, ArenaStr};
    use hashbrown::HashMap;
    use serde::de::{Error, SeqAccess, Visitor};
    use serde::ser::SerializeTuple;
    use serde::{Deserializer, Serializer};

    impl<H> Serialize for DeltaEncoding<Jinterners<H>> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut tuple = serializer.serialize_tuple(7)?;

            tuple.serialize_element(&self.inner.uint64)?;
            tuple.serialize_element(&self.inner.int64)?;
            tuple.serialize_element(&self.inner.float64)?;
            tuple.serialize_element(&self.inner.string)?;

            let iarray: RawDeltaEncoding<_, IArrayAccumulator<H>> =
                RawDeltaEncoding::new(&self.inner.iarray);
            tuple.serialize_element(&iarray)?;

            let iset: RawDeltaEncoding<_, IArrayAccumulator<H>> =
                RawDeltaEncoding::new(&self.inner.iset);
            tuple.serialize_element(&iset)?;

            let iobject: RawDeltaEncoding<_, IObjectAccumulator<H>> =
                RawDeltaEncoding::new(&self.inner.iobject);
            tuple.serialize_element(&iobject)?;

            tuple.end()
        }
    }

    #[cfg(feature = "postcard-schema")]
    impl<H> Schema for DeltaEncoding<Jinterners<H>> {
        const SCHEMA: &'static NamedType = &NamedType {
            name: "DeltaEncoding<Jinterners>",
            ty: &DataModelType::Tuple(&[
                Arena::<u64, u64, H, U32>::SCHEMA,
                Arena::<i64, i64, H, U32>::SCHEMA,
                Arena::<Float64, Float64, H, U32>::SCHEMA,
                ArenaStr::<H, U32>::SCHEMA,
                RawDeltaEncoding::<&ArenaSlice<IValue<H>, H, U32>, IArrayAccumulator<H>>::SCHEMA,
                RawDeltaEncoding::<&ArenaSlice<IValue<H>, H, U32>, IArrayAccumulator<H>>::SCHEMA,
                RawDeltaEncoding::<
                    &ArenaSlice<(InternedStrKey<H>, IValue<H>), H, U32>,
                    IObjectAccumulator<H>,
                >::SCHEMA,
            ]),
        };
    }

    impl<'de, H> Deserialize<'de> for DeltaEncoding<Jinterners<H>>
    where
        H: Default + BuildHasher,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_tuple(7, DeltaJinternersVisitor(PhantomData))
        }
    }

    struct DeltaJinternersVisitor<H>(PhantomData<fn() -> DeltaEncoding<Jinterners<H>>>);

    impl<'de, H> Visitor<'de> for DeltaJinternersVisitor<H>
    where
        H: Default + BuildHasher,
    {
        type Value = DeltaEncoding<Jinterners<H>>;

        fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
            formatter.write_str("a tuple with 7 elements")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let uint64 = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(0, &self))?;
            let int64 = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(1, &self))?;
            let float64 = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(2, &self))?;
            let string = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(3, &self))?;

            let iarray: RawDeltaEncoding<ArenaSlice<IValue<H>, H, U32>, IArrayAccumulator<H>> = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(4, &self))?;

            let iset: RawDeltaEncoding<ArenaSlice<IValue<H>, H, U32>, IArrayAccumulator<H>> = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(5, &self))?;

            #[expect(clippy::type_complexity)]
            let iobject: RawDeltaEncoding<
                ArenaSlice<(InternedStrKey<H>, IValue<H>), H, U32>,
                IObjectAccumulator<H>,
            > = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(6, &self))?;

            Ok(DeltaEncoding::new(Jinterners {
                uint64,
                int64,
                float64,
                string,
                iarray: iarray.into_inner(),
                iset: iset.into_inner(),
                iobject: iobject.into_inner(),
            }))
        }
    }

    /// Difference between two JSON values, for better delta encoding
    /// serialization.
    #[derive(Serialize, Deserialize)]
    #[cfg_attr(feature = "postcard-schema", derive(Schema))]
    pub enum IValueDelta {
        Null,
        Bool(bool),
        U32(i32),
        I32(i32),
        U64(i32),
        I64(i32),
        F64(i32),
        String(i32),
        Array(i32),
        Set(i32),
        Object(i32),
    }

    struct IValueAccumulator<H> {
        b: bool,
        u_32: u32,
        i_32: i32,
        u: u32,
        i: u32,
        f: u32,
        s: u32,
        a: u32,
        set: u32,
        o: u32,
        _phantom: PhantomData<fn() -> IValueImpl<H>>,
    }

    impl<H> Default for IValueAccumulator<H> {
        fn default() -> Self {
            Self {
                b: false,
                u_32: 0,
                i_32: 0,
                u: 0,
                i: 0,
                f: 0,
                s: 0,
                a: 0,
                set: 0,
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
                IValueImpl::U32(x) => {
                    let x = u32::from_ne_bytes(*x);
                    let diff = x.wrapping_sub(self.u_32);
                    self.u_32 = x;
                    IValueDelta::U32(diff as i32)
                }
                IValueImpl::I32(x) => {
                    let x = i32::from_ne_bytes(*x);
                    let diff = x.wrapping_sub(self.i_32);
                    self.i_32 = x;
                    IValueDelta::I32(diff)
                }
                IValueImpl::U64(x) => {
                    let id = x.id().to_usize() as u32;
                    let diff = id.wrapping_sub(self.u);
                    self.u = id;
                    IValueDelta::U64(diff as i32)
                }
                IValueImpl::I64(x) => {
                    let id = x.id().to_usize() as u32;
                    let diff = id.wrapping_sub(self.i);
                    self.i = id;
                    IValueDelta::I64(diff as i32)
                }
                IValueImpl::F64(x) => {
                    let id = x.id().to_usize() as u32;
                    let diff = id.wrapping_sub(self.f);
                    self.f = id;
                    IValueDelta::F64(diff as i32)
                }
                IValueImpl::String(x) => {
                    let id = x.id().to_usize() as u32;
                    let diff = id.wrapping_sub(self.s);
                    self.s = id;
                    IValueDelta::String(diff as i32)
                }
                IValueImpl::Array(x) => {
                    let id = x.id().to_usize() as u32;
                    let diff = id.wrapping_sub(self.a);
                    self.a = id;
                    IValueDelta::Array(diff as i32)
                }
                IValueImpl::Set(x) => {
                    let id = x.id().to_usize() as u32;
                    let diff = id.wrapping_sub(self.set);
                    self.set = id;
                    IValueDelta::Set(diff as i32)
                }
                IValueImpl::Object(x) => {
                    let id = x.id().to_usize() as u32;
                    let diff = id.wrapping_sub(self.o);
                    self.o = id;
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
                IValueDelta::U32(x) => {
                    let x = self.u_32.wrapping_add(*x as u32);
                    self.u_32 = x;
                    IValueImpl::U32(x.to_ne_bytes())
                }
                IValueDelta::I32(x) => {
                    let x = self.i_32.wrapping_add(*x);
                    self.i_32 = x;
                    IValueImpl::I32(x.to_ne_bytes())
                }
                IValueDelta::U64(x) => {
                    let x = self.u.wrapping_add(*x as u32);
                    self.u = x;
                    IValueImpl::U64(Interned::from_id(U32::from_usize(x as usize)))
                }
                IValueDelta::I64(x) => {
                    let x = self.i.wrapping_add(*x as u32);
                    self.i = x;
                    IValueImpl::I64(Interned::from_id(U32::from_usize(x as usize)))
                }
                IValueDelta::F64(x) => {
                    let x = self.f.wrapping_add(*x as u32);
                    self.f = x;
                    IValueImpl::F64(Interned::from_id(U32::from_usize(x as usize)))
                }
                IValueDelta::String(x) => {
                    let x = self.s.wrapping_add(*x as u32);
                    self.s = x;
                    IValueImpl::String(InternedStr::from_id(U32::from_usize(x as usize)))
                }
                IValueDelta::Array(x) => {
                    let x = self.a.wrapping_add(*x as u32);
                    self.a = x;
                    IValueImpl::Array(InternedSlice::from_id(U32::from_usize(x as usize)))
                }
                IValueDelta::Set(x) => {
                    let x = self.set.wrapping_add(*x as u32);
                    self.set = x;
                    IValueImpl::Set(InternedSlice::from_id(U32::from_usize(x as usize)))
                }
                IValueDelta::Object(x) => {
                    let x = self.o.wrapping_add(*x as u32);
                    self.o = x;
                    IValueImpl::Object(InternedSlice::from_id(U32::from_usize(x as usize)))
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
                    let k = k.0.id().to_usize() as u32;
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
                    (
                        InternedStrKey(InternedStr::from_id(U32::from_usize(k as usize))),
                        x,
                    )
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn ivalue_size() {
        assert_eq!(core::mem::size_of::<IValue>(), 5);
        assert_eq!(core::mem::align_of::<IValue>(), 1);

        assert_eq!(core::mem::size_of::<InternedStrKey>(), 4);
        assert_eq!(core::mem::align_of::<InternedStrKey>(), 1);

        assert_eq!(core::mem::size_of::<(InternedStrKey, IValue)>(), 9);
        assert_eq!(core::mem::align_of::<(InternedStrKey, IValue)>(), 1);
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
        let ivalue = IValue::from_value(&original, &interners, ArrayOrder::Preserve)
            .expect("Failed to intern value");

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
        let ivalue = IValue::from_value_mut(&original, &mut interners, ArrayOrder::Preserve)
            .expect("Failed to intern value");

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
        let ivalue = interners.intern_mut(&json, ArrayOrder::Preserve);

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
        let ivalue = IValue::from_value(&original, &interners, ArrayOrder::Preserve)
            .expect("Failed to intern value");

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
        let ivalue = IValue::from_value_mut(&original, &mut interners, ArrayOrder::Preserve)
            .expect("Failed to intern value");

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
        let ivalue = IValue::from_value(&original, &interners, ArrayOrder::Preserve)
            .expect("Failed to intern value");

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
        let ivalue = IValue::from_value_mut(&original, &mut interners, ArrayOrder::Preserve)
            .expect("Failed to intern value");

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
        let ivalue = IValue::from_value_mut(&original, &mut interners, ArrayOrder::Preserve)
            .expect("Failed to intern value");

        let json = ivalue.to_json_string(&interners);
        assert_eq!(
            json.unwrap(),
            r#"{"a":true,"b":-305419896,"c":18364758544493064720,"d":3.1415927410125732,"e":2.718281828459045,"f":"Hello world","g":["First",{"Second":[2271560481,-1311768467463790320]},{"Third":{"i":"Hello","j":[1,2,3,4]}}],"h":{"Hello":"First","world":{"Second":[42,-123]}}}"#
        );
    }
}
