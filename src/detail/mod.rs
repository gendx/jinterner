#[cfg(feature = "serde")]
mod de;
pub mod mapping;
#[cfg(feature = "serde")]
mod ser;

use super::Jinterners;
use blazinterner::{ArenaStr, InternedSlice, InternedStr};
#[cfg(feature = "serde")]
use de::ValueDeserializer;
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
use ordered_float::OrderedFloat;
#[cfg(feature = "serde")]
use ser::ValueSerializer;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::fmt::Debug;

/// An interned key for JSON objects.
///
/// You can obtain a key with [`Jinterners::find_key()`] and use it to lookup
/// values in JSON objects with [`MapRef::get_by_key()`].
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct InternedStrKey(pub(crate) InternedStr);

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
    pub(crate) fn from(interners: &Jinterners, source: Value) -> Self {
        Self(IValueImpl::from(interners, source))
    }

    /// Interns the given [`serde_json::Value`] into the given [`Jinterners`]
    /// arena.
    pub(crate) fn from_ref(interners: &Jinterners, source: &Value) -> Self {
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

    /// Convert an arbitrary type into an [`IValue`] using that type's
    /// [`Serialize`] implementation.
    #[cfg(feature = "serde")]
    pub fn from_value<T>(value: T, interners: &Jinterners) -> Result<Self, serde_json::error::Error>
    where
        T: Serialize,
    {
        value.serialize(ValueSerializer { interners }).map(IValue)
    }

    /// Convert an [`IValue`] into an arbitrary type using that type's
    /// [`Deserialize`] implementation.
    #[cfg(feature = "serde")]
    pub fn to_value<'de, T>(
        &self,
        interners: &'de Jinterners,
    ) -> Result<T, serde_json::error::Error>
    where
        T: Deserialize<'de>,
    {
        T::deserialize(ValueDeserializer {
            value: &self.0,
            interners,
        })
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

    fn lookup(&self, interners: &Jinterners) -> Value {
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
                    .map(|v| v.lookup(interners))
                    .collect(),
            ),
            IValueImpl::Object(o) => Value::Object(
                interners
                    .iobject
                    .lookup(*o)
                    .iter()
                    .map(|(k, v)| (interners.string.lookup(k.0).into(), v.lookup(interners)))
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
    arena_str: &'a ArenaStr,
    map: &'a [(InternedStrKey, IValue)],
}

impl<'a> MapRef<'a> {
    /// Returns the value associated to the given key, or [`None`] if there is
    /// no such key in this map.
    ///
    /// If you're repeatedly querying the same key, it's more efficient to cache
    /// it once with [`Jinterners::find_key()`] and then use
    /// [`get_by_key()`](Self::get_by_key).
    pub fn get(&self, key: &str) -> Option<&'a IValue> {
        let k = InternedStrKey(self.arena_str.find(key)?);
        self.get_by_key(k)
    }

    /// Returns the value associated to the given key, or [`None`] if there is
    /// no such key in this map.
    pub fn get_by_key(&self, key: InternedStrKey) -> Option<&'a IValue> {
        let i = self.map.binary_search_by_key(&key, |entry| entry.0).ok()?;
        Some(&self.map[i].1)
    }

    /// Iterates over the key-value pairs in this JSON map, in arbitrary order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&'a str, &'a IValue)> {
        self.map
            .iter()
            .map(|(k, v)| (self.arena_str.lookup(k.0), v))
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
                    (InternedStrKey(InternedStr::from_id(k)), x)
                })
                .collect()
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod serde_test {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

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
            d: std::f32::consts::PI,
            e: Some(std::f64::consts::E),
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

    #[test]
    #[allow(clippy::approx_constant)]
    fn round_trip() {
        let interners = Jinterners::default();

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
    #[allow(clippy::approx_constant)]
    fn deserialize_smaller() {
        let interners = Jinterners::default();

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
        let ivalue = interners.intern(json);

        let small_foo: SmallFoo = ivalue
            .to_value(&interners)
            .expect("Failed to convert to value");
        assert_eq!(small_foo, make_small_foo());
    }

    #[test]
    fn round_trip_map_key_enum() {
        let interners = Jinterners::default();

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
    fn round_trip_map_key_newtype() {
        let interners = Jinterners::default();

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
}
