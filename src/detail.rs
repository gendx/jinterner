use super::Jinterners;
use blazinterner::{Interned, InternedSlice};
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
use ordered_float::OrderedFloat;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::fmt::Debug;

type InternedStr = Interned<str, Box<str>>;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct InternedStrKey(InternedStr);

impl Default for InternedStrKey {
    fn default() -> Self {
        InternedStrKey(InternedStr::from_id(0))
    }
}

/// An interned JSON value.
#[derive(Default, Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct IValue(IValueImpl);

impl IValue {
    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    pub fn from(interners: &Jinterners, source: Value) -> Self {
        Self(IValueImpl::from(interners, source))
    }

    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    pub fn from_ref(interners: &Jinterners, source: &Value) -> Self {
        Self(IValueImpl::from_ref(interners, source))
    }

    /// Retrieves the corresponding [`serde_json::Value`] inside the given
    /// [`Jinterners`] arena.
    pub fn lookup(&self, interners: &Jinterners) -> Value {
        self.0.lookup(interners)
    }

    /// Performs a shallow lookup of this value inside the given [`Jinterners`]
    /// arena.
    ///
    /// Contrary to [`lookup()`](Self::lookup), this function doesn't create a
    /// deep copy of the value, and is therefore likely more efficient if
    /// you only need to query specific object field(s) or array element(s).
    pub fn lookup_ref<'a>(&self, interners: &'a Jinterners) -> ValueRef<'a> {
        self.0.lookup_ref(interners)
    }
}

#[derive(Default, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
struct Float64(OrderedFloat<f64>);

impl Debug for Float64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.0.fmt(f)
    }
}

#[cfg(feature = "get-size2")]
impl GetSize for Float64 {
    // There is nothing on the heap, so the default implementation works out of the
    // box.
}

#[derive(Default, Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
enum IValueImpl {
    #[default]
    Null,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(Float64),
    String(InternedStr),
    Array(InternedSlice<IValue>),
    Object(InternedSlice<(InternedStrKey, IValue)>),
}

impl IValueImpl {
    fn from(interners: &Jinterners, source: Value) -> Self {
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
            Value::String(s) => IValueImpl::String(Interned::from(&interners.string, s)),
            Value::Array(a) => IValueImpl::Array(InternedSlice::from(
                &interners.iarray,
                &a.into_iter()
                    .map(|v| IValue::from(interners, v))
                    .collect::<Box<[_]>>(),
            )),
            Value::Object(o) => {
                let mut io: Box<[_]> = o
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            InternedStrKey(Interned::from(&interners.string, k)),
                            IValue::from(interners, v),
                        )
                    })
                    .collect();
                io.sort_unstable_by_key(|(k, _)| *k);
                IValueImpl::Object(InternedSlice::from(&interners.iobject, &io))
            }
        }
    }

    fn from_ref(interners: &Jinterners, source: &Value) -> Self {
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
            Value::String(s) => IValueImpl::String(Interned::from(&interners.string, s.as_str())),
            Value::Array(a) => IValueImpl::Array(InternedSlice::from(
                &interners.iarray,
                &a.iter()
                    .map(|v| IValue::from_ref(interners, v))
                    .collect::<Box<[_]>>(),
            )),
            Value::Object(o) => {
                let mut io: Box<[_]> = o
                    .iter()
                    .map(|(k, v)| {
                        (
                            InternedStrKey(Interned::from(&interners.string, k.as_str())),
                            IValue::from_ref(interners, v),
                        )
                    })
                    .collect();
                io.sort_unstable_by_key(|(k, _)| *k);
                IValueImpl::Object(InternedSlice::from(&interners.iobject, &io))
            }
        }
    }

    fn lookup(&self, interners: &Jinterners) -> Value {
        match self {
            IValueImpl::Null => Value::Null,
            IValueImpl::Bool(x) => Value::Bool(*x),
            IValueImpl::U64(x) => Value::Number(Number::from_u128(*x as u128).unwrap()),
            IValueImpl::I64(x) => Value::Number(Number::from_i128(*x as i128).unwrap()),
            IValueImpl::F64(Float64(OrderedFloat(x))) => {
                Value::Number(Number::from_f64(*x).unwrap())
            }
            IValueImpl::String(s) => Value::String(s.lookup_ref(&interners.string).into()),
            IValueImpl::Array(a) => Value::Array(
                a.lookup(&interners.iarray)
                    .iter()
                    .map(|v| v.lookup(interners))
                    .collect(),
            ),
            IValueImpl::Object(o) => Value::Object(
                o.lookup(&interners.iobject)
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.0.lookup_ref(&interners.string).into(),
                            v.lookup(interners),
                        )
                    })
                    .collect(),
            ),
        }
    }

    fn lookup_ref<'a>(&self, interners: &'a Jinterners) -> ValueRef<'a> {
        match self {
            IValueImpl::Null => ValueRef::Null,
            IValueImpl::Bool(x) => ValueRef::Bool(*x),
            IValueImpl::U64(x) => ValueRef::U64(*x),
            IValueImpl::I64(x) => ValueRef::I64(*x),
            IValueImpl::F64(Float64(OrderedFloat(x))) => ValueRef::F64(*x),
            IValueImpl::String(s) => ValueRef::String(s.lookup_ref(&interners.string)),
            IValueImpl::Array(a) => ValueRef::Array(a.lookup(&interners.iarray)),
            IValueImpl::Object(o) => ValueRef::Object(MapRef {
                map: o
                    .lookup(&interners.iobject)
                    .iter()
                    .map(|(k, v)| (k.0.lookup_ref(&interners.string), v))
                    .collect(),
            }),
        }
    }
}

/// A shallow reference to a JSON value.
pub enum ValueRef<'a> {
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
    Array(&'a [IValue]),
    /// JSON object.
    Object(MapRef<'a>),
}

/// A shallow reference to a JSON map.
pub struct MapRef<'a> {
    map: Box<[(&'a str, &'a IValue)]>,
}

impl<'a> MapRef<'a> {
    /// Iterates over the key-value pairs in this JSON map, in arbitrary order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&'a str, &'a IValue)> {
        self.map.iter().copied()
    }
}

#[cfg(all(feature = "delta", feature = "serde"))]
mod delta {
    use super::*;
    use crate::DeltaEncoding;
    use blazinterner::{Accumulator, ArenaSlice, DeltaEncoding as RawDeltaEncoding};
    use serde::de::{Error, SeqAccess, Visitor};
    use serde::ser::SerializeTuple;
    use serde::{Deserializer, Serializer};
    use std::collections::HashMap;

    impl Serialize for DeltaEncoding<Jinterners> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut tuple = serializer.serialize_tuple(3)?;

            tuple.serialize_element(&self.inner.string)?;

            let iarray: RawDeltaEncoding<_, IArrayAccumulator> =
                RawDeltaEncoding::new(&self.inner.iarray);
            tuple.serialize_element(&iarray)?;

            let iobject: RawDeltaEncoding<_, IObjectAccumulator> =
                RawDeltaEncoding::new(&self.inner.iobject);
            tuple.serialize_element(&iobject)?;

            tuple.end()
        }
    }

    impl<'de> Deserialize<'de> for DeltaEncoding<Jinterners> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_tuple(3, DeltaJinternersVisitor)
        }
    }

    struct DeltaJinternersVisitor;

    impl<'de> Visitor<'de> for DeltaJinternersVisitor {
        type Value = DeltaEncoding<Jinterners>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a tuple with 3 elements")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let string = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(0, &self))?;
            let iarray: RawDeltaEncoding<ArenaSlice<IValue>, IArrayAccumulator> = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(1, &self))?;
            let iobject: RawDeltaEncoding<
                ArenaSlice<(InternedStrKey, IValue)>,
                IObjectAccumulator,
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

    struct IValueAccumulator {
        b: bool,
        u: u64,
        i: i64,
        f: f64,
        s: u32,
        a: u32,
        o: u32,
    }

    impl Default for IValueAccumulator {
        fn default() -> Self {
            Self {
                b: false,
                u: 0,
                i: 0,
                f: f64::from_bits(0),
                s: 0,
                a: 0,
                o: 0,
            }
        }
    }

    impl Accumulator for IValueAccumulator {
        type Value = IValueImpl;
        type Storage = IValueImpl;
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
                    IValueImpl::String(Interned::from_id(x))
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

    #[derive(Default)]
    pub struct IArrayAccumulator(IValueAccumulator);

    impl Accumulator for IArrayAccumulator {
        type Value = [IValue];
        type Storage = Box<[IValue]>;
        type Delta = [IValueDelta];
        type DeltaStorage = Box<[IValueDelta]>;

        fn fold(&mut self, v: &Self::Value) -> Self::DeltaStorage {
            v.iter().map(|x| self.0.fold(&x.0)).collect()
        }

        fn unfold(&mut self, d: &Self::Delta) -> Self::Storage {
            d.iter().map(|x| IValue(self.0.unfold(x))).collect()
        }
    }

    #[derive(Default)]
    pub struct IObjectAccumulator {
        map: HashMap<u32, IValueAccumulator>,
    }

    impl Accumulator for IObjectAccumulator {
        type Value = [(InternedStrKey, IValue)];
        type Storage = Box<[(InternedStrKey, IValue)]>;
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
                    (InternedStrKey(Interned::from_id(k)), x)
                })
                .collect()
        }
    }
}

/// Mapping to convert values from one [`Jinterners`] instance to another.
pub struct Mapping {
    pub(crate) string: Box<[u32]>,
    pub(crate) iarray: Box<[u32]>,
    pub(crate) iobject: Box<[u32]>,
}

impl Mapping {
    /// Returns the number of strings that are remapped by this mapping.
    pub fn count_remapped_strings(&self) -> usize {
        self.string
            .iter()
            .enumerate()
            .filter(|&(i, j)| i != *j as usize)
            .count()
    }

    /// Returns the number of arrays that are remapped by this mapping.
    pub fn count_remapped_arrays(&self) -> usize {
        self.iarray
            .iter()
            .enumerate()
            .filter(|&(i, j)| i != *j as usize)
            .count()
    }

    /// Returns the number of objects that are remapped by this mapping.
    pub fn count_remapped_objects(&self) -> usize {
        self.iobject
            .iter()
            .enumerate()
            .filter(|&(i, j)| i != *j as usize)
            .count()
    }

    pub(crate) fn map_str_key(&self, s: InternedStrKey) -> InternedStrKey {
        InternedStrKey(self.map_str(s.0))
    }

    fn map_str(&self, s: InternedStr) -> InternedStr {
        Interned::from_id(self.string[s.id() as usize])
    }

    /// Maps the given value from the source [`Jinterners`] to the destination
    /// [`Jinterners`] of this mapping.
    pub fn map(&self, v: IValue) -> IValue {
        IValue(match v.0 {
            IValueImpl::Null => IValueImpl::Null,
            IValueImpl::Bool(x) => IValueImpl::Bool(x),
            IValueImpl::U64(x) => IValueImpl::U64(x),
            IValueImpl::I64(x) => IValueImpl::I64(x),
            IValueImpl::F64(x) => IValueImpl::F64(x),
            IValueImpl::String(x) => {
                IValueImpl::String(Interned::from_id(self.string[x.id() as usize]))
            }
            IValueImpl::Array(x) => {
                IValueImpl::Array(InternedSlice::from_id(self.iarray[x.id() as usize]))
            }
            IValueImpl::Object(x) => {
                IValueImpl::Object(InternedSlice::from_id(self.iobject[x.id() as usize]))
            }
        })
    }
}
