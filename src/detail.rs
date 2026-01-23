use super::Jinterners;
use blazinterner::Interned;
#[cfg(feature = "get-size2")]
use get_size2::GetSize;
use ordered_float::OrderedFloat;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::collections::BTreeMap;
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
    map: BTreeMap<InternedStr, IValue>,
}

impl IObject {
    fn from(interners: &Jinterners, source: serde_json::Map<String, Value>) -> Self {
        Self {
            map: source
                .into_iter()
                .map(|(k, v)| {
                    (
                        Interned::from(&interners.string, k),
                        IValue::from(interners, v),
                    )
                })
                .collect(),
        }
    }

    fn from_ref(interners: &Jinterners, source: &serde_json::Map<String, Value>) -> Self {
        Self {
            map: source
                .iter()
                .map(|(k, v)| {
                    (
                        Interned::from(&interners.string, k.as_str()),
                        IValue::from_ref(interners, v),
                    )
                })
                .collect(),
        }
    }

    fn lookup(&self, interners: &Jinterners) -> serde_json::Map<String, Value> {
        self.map
            .iter()
            .map(|(k, v)| (k.lookup_ref(&interners.string).into(), v.lookup(interners)))
            .collect()
    }
}
