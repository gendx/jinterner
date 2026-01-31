use super::Jinterners;
use blazinterner::{Accumulator, Interned};
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
use ordered_float::OrderedFloat;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::collections::HashMap;
use std::fmt::Debug;

type InternedStr = Interned<str, Box<str>>;

#[derive(Default, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Float64(OrderedFloat<f64>);

impl Debug for Float64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0.0))
    }
}

#[cfg(feature = "get-size2")]
impl GetSize for Float64 {
    // There is nothing on the heap, so the default implementation works out of the
    // box.
}

#[derive(Default, Debug, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub enum IValue {
    #[default]
    Null,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(Float64),
    String(InternedStr),
    Array(Interned<IArray>),
    Object(Interned<IObject>),
}

impl IValue {
    pub fn from(interners: &Jinterners, source: Value) -> Self {
        match source {
            Value::Null => IValue::Null,
            Value::Bool(x) => IValue::Bool(x),
            Value::Number(x) => {
                if x.is_u64() {
                    IValue::U64(x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValue::I64(x.as_i64().unwrap())
                } else {
                    IValue::F64(Float64(OrderedFloat(x.as_f64().unwrap())))
                }
            }
            Value::String(s) => IValue::String(Interned::from(&interners.string, s)),
            Value::Array(a) => IValue::Array(Interned::from(
                &interners.iarray,
                IArray(
                    a.into_iter()
                        .map(|v| IValue::from(interners, v))
                        .collect::<Box<[_]>>(),
                ),
            )),
            Value::Object(o) => {
                let io = IObject::from(interners, o);
                IValue::Object(Interned::from(&interners.iobject, io))
            }
        }
    }

    pub fn from_ref(interners: &Jinterners, source: &Value) -> Self {
        match source {
            Value::Null => IValue::Null,
            Value::Bool(x) => IValue::Bool(*x),
            Value::Number(x) => {
                if x.is_u64() {
                    IValue::U64(x.as_u64().unwrap())
                } else if x.is_i64() {
                    IValue::I64(x.as_i64().unwrap())
                } else {
                    IValue::F64(Float64(OrderedFloat(x.as_f64().unwrap())))
                }
            }
            Value::String(s) => IValue::String(Interned::from(&interners.string, s.as_str())),
            Value::Array(a) => IValue::Array(Interned::from(
                &interners.iarray,
                IArray(
                    a.iter()
                        .map(|v| IValue::from_ref(interners, v))
                        .collect::<Box<[_]>>(),
                ),
            )),
            Value::Object(o) => {
                let io = IObject::from_ref(interners, o);
                IValue::Object(Interned::from(&interners.iobject, io))
            }
        }
    }

    pub fn lookup(&self, interners: &Jinterners) -> Value {
        match self {
            IValue::Null => Value::Null,
            IValue::Bool(x) => Value::Bool(*x),
            IValue::U64(x) => Value::Number(Number::from_u128(*x as u128).unwrap()),
            IValue::I64(x) => Value::Number(Number::from_i128(*x as i128).unwrap()),
            IValue::F64(Float64(OrderedFloat(x))) => Value::Number(Number::from_f64(*x).unwrap()),
            IValue::String(s) => Value::String(s.lookup_ref(&interners.string).into()),
            IValue::Array(a) => Value::Array(
                a.lookup_ref(&interners.iarray)
                    .0
                    .iter()
                    .map(|v| v.lookup(interners))
                    .collect(),
            ),
            IValue::Object(o) => Value::Object(o.lookup_ref(&interners.iobject).lookup(interners)),
        }
    }
}

#[derive(Default, Debug, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct IArray(Box<[IValue]>);

#[derive(Default, Debug, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct IObject {
    map: Box<[(InternedStr, IValue)]>,
}

impl IObject {
    fn from(interners: &Jinterners, source: serde_json::Map<String, Value>) -> Self {
        let mut map: Box<[_]> = source
            .into_iter()
            .map(|(k, v)| {
                (
                    Interned::from(&interners.string, k),
                    IValue::from(interners, v),
                )
            })
            .collect();
        map.sort_unstable_by_key(|(k, _)| *k);
        Self { map }
    }

    fn from_ref(interners: &Jinterners, source: &serde_json::Map<String, Value>) -> Self {
        let mut map: Box<[_]> = source
            .iter()
            .map(|(k, v)| {
                (
                    Interned::from(&interners.string, k.as_str()),
                    IValue::from_ref(interners, v),
                )
            })
            .collect();
        map.sort_unstable_by_key(|(k, _)| *k);
        Self { map }
    }

    fn lookup(&self, interners: &Jinterners) -> serde_json::Map<String, Value> {
        self.map
            .iter()
            .map(|(k, v)| (k.lookup_ref(&interners.string).into(), v.lookup(interners)))
            .collect()
    }
}

/// Difference between two JSON values, for better delta encoding
/// serialization.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
enum IValueDelta {
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
    type Value = IValue;
    type Storage = IValue;
    type Delta = IValueDelta;

    fn fold(&mut self, v: &Self::Value) -> Self::Delta {
        match v {
            IValue::Null => IValueDelta::Null,
            IValue::Bool(x) => {
                let diff = self.b ^ x;
                self.b = *x;
                IValueDelta::Bool(diff)
            }
            IValue::U64(x) => {
                let diff = x.wrapping_sub(self.u);
                self.u = *x;
                IValueDelta::U64(diff as i64)
            }
            IValue::I64(x) => {
                let diff = x.wrapping_sub(self.i);
                self.i = *x;
                IValueDelta::I64(diff)
            }
            IValue::F64(x) => {
                let diff = x.0.to_bits() ^ self.f.to_bits();
                self.f = *x.0;
                IValueDelta::F64(f64::from_bits(diff))
            }
            IValue::String(x) => {
                let diff = x.id().wrapping_sub(self.s);
                self.s = x.id();
                IValueDelta::String(diff as i32)
            }
            IValue::Array(x) => {
                let diff = x.id().wrapping_sub(self.a);
                self.a = x.id();
                IValueDelta::Array(diff as i32)
            }
            IValue::Object(x) => {
                let diff = x.id().wrapping_sub(self.o);
                self.o = x.id();
                IValueDelta::Object(diff as i32)
            }
        }
    }

    fn unfold(&mut self, d: Self::Delta) -> Self::Value {
        match d {
            IValueDelta::Null => IValue::Null,
            IValueDelta::Bool(x) => {
                let x = self.b ^ x;
                self.b = x;
                IValue::Bool(x)
            }
            IValueDelta::U64(x) => {
                let x = self.u.wrapping_add(x as u64);
                self.u = x;
                IValue::U64(x)
            }
            IValueDelta::I64(x) => {
                let x = self.i.wrapping_add(x);
                self.i = x;
                IValue::I64(x)
            }
            IValueDelta::F64(x) => {
                let x = f64::from_bits(self.f.to_bits() ^ x.to_bits());
                self.f = x;
                IValue::F64(Float64(OrderedFloat(x)))
            }
            IValueDelta::String(x) => {
                let x = self.s.wrapping_add(x as u32);
                self.s = x;
                IValue::String(Interned::from_id(x))
            }
            IValueDelta::Array(x) => {
                let x = self.a.wrapping_add(x as u32);
                self.a = x;
                IValue::Array(Interned::from_id(x))
            }
            IValueDelta::Object(x) => {
                let x = self.o.wrapping_add(x as u32);
                self.o = x;
                IValue::Object(Interned::from_id(x))
            }
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct IArrayDelta(Box<[IValueDelta]>);

#[derive(Default)]
pub struct IArrayAccumulator(IValueAccumulator);

impl Accumulator for IArrayAccumulator {
    type Value = IArray;
    type Storage = IArray;
    type Delta = IArrayDelta;

    fn fold(&mut self, v: &Self::Value) -> Self::Delta {
        IArrayDelta(v.0.iter().map(|x| self.0.fold(x)).collect())
    }

    fn unfold(&mut self, d: Self::Delta) -> Self::Value {
        IArray(d.0.into_iter().map(|x| self.0.unfold(x)).collect())
    }
}

#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
pub struct IObjectDelta {
    map: Box<[(i32, IValueDelta)]>,
}

#[derive(Default)]
pub struct IObjectAccumulator {
    map: HashMap<u32, IValueAccumulator>,
}

impl Accumulator for IObjectAccumulator {
    type Value = IObject;
    type Storage = IObject;
    type Delta = IObjectDelta;

    fn fold(&mut self, v: &Self::Value) -> Self::Delta {
        let mut key = 0;
        IObjectDelta {
            map: v
                .map
                .iter()
                .map(|(k, x)| {
                    let k = k.id();
                    let kdiff = k.wrapping_sub(key);
                    key = k;
                    let acc = self.map.entry(k).or_default();
                    let xdiff = acc.fold(x);
                    (kdiff as i32, xdiff)
                })
                .collect(),
        }
    }

    fn unfold(&mut self, d: Self::Delta) -> Self::Value {
        let mut key = 0;
        IObject {
            map: d
                .map
                .into_iter()
                .map(|(kdiff, xdiff)| {
                    let k = (kdiff as u32).wrapping_add(key);
                    key = k;
                    let acc = self.map.entry(k).or_default();
                    let x = acc.unfold(xdiff);
                    (Interned::from_id(k), x)
                })
                .collect(),
        }
    }
}
