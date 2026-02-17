use super::{Float64, IValue, IValueImpl, InternedStrKey};
use crate::Jinterners;
use blazinterner::{InternedSlice, InternedStr};
use ordered_float::OrderedFloat;
use serde::de::{
    DeserializeSeed, EnumAccess, Error, Expected, MapAccess, SeqAccess, Unexpected, VariantAccess,
    Visitor,
};
use serde::{Deserializer, forward_to_deserialize_any};
use serde_json::error::Error as JsonError;

fn deserialize_array<'de, V>(
    visitor: V,
    array: InternedSlice<IValue>,
    interners: &'de Jinterners,
) -> Result<V::Value, JsonError>
where
    V: Visitor<'de>,
{
    let array = interners.iarray.lookup(array);
    let len = array.len();
    let mut array_access = ArrayAccess {
        array,
        index: 0,
        interners,
    };
    let value = visitor.visit_seq(&mut array_access)?;
    if array_access.is_fully_scanned() {
        Ok(value)
    } else {
        Err(Error::invalid_length(len, &"fewer elements in array"))
    }
}

fn deserialize_array_expected_len<'de, V>(
    visitor: V,
    array: InternedSlice<IValue>,
    interners: &'de Jinterners,
    expected_len: usize,
    make_error_msg: impl FnOnce() -> String,
) -> Result<V::Value, JsonError>
where
    V: Visitor<'de>,
{
    let array = interners.iarray.lookup(array);
    let len = array.len();
    if len != expected_len {
        return Err(Error::invalid_length(len, &make_error_msg().as_str()));
    }

    let mut array_access = ArrayAccess {
        array,
        index: 0,
        interners,
    };
    let value = visitor.visit_seq(&mut array_access)?;
    if array_access.is_fully_scanned() {
        Ok(value)
    } else {
        Err(Error::invalid_length(len, &"fewer elements in array"))
    }
}

fn deserialize_object<'de, V>(
    visitor: V,
    object: InternedSlice<(InternedStrKey, IValue)>,
    interners: &'de Jinterners,
) -> Result<V::Value, JsonError>
where
    V: Visitor<'de>,
{
    let object = interners.iobject.lookup(object);
    let len = object.len();
    let mut object_access = ObjectAccess {
        object,
        index: 0,
        interners,
    };
    let value = visitor.visit_map(&mut object_access)?;
    if object_access.is_fully_scanned() {
        Ok(value)
    } else {
        Err(Error::invalid_length(len, &"fewer elements in object"))
    }
}

pub(super) struct ValueDeserializer<'a, 'b> {
    pub value: &'a IValueImpl,
    pub interners: &'b Jinterners,
}

impl<'de> ValueDeserializer<'_, 'de> {
    fn invalid_type<E>(self, exp: &dyn Expected) -> E
    where
        E: Error,
    {
        Error::invalid_type(self.unexpected(), exp)
    }

    fn unexpected(&self) -> Unexpected<'_> {
        match self.value {
            IValueImpl::Null => Unexpected::Unit,
            IValueImpl::Bool(x) => Unexpected::Bool(*x),
            IValueImpl::U64(x) => Unexpected::Unsigned(*x),
            IValueImpl::I64(x) => Unexpected::Signed(*x),
            IValueImpl::F64(Float64(OrderedFloat(x))) => Unexpected::Float(*x),
            IValueImpl::String(s) => Unexpected::Str(self.interners.string.lookup(*s)),
            IValueImpl::Array(_) => Unexpected::Seq,
            IValueImpl::Object(_) => Unexpected::Map,
        }
    }

    fn deserialize_integer<V>(self, visitor: V) -> Result<V::Value, JsonError>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::I64(x) => visitor.visit_i64(*x),
            IValueImpl::U64(x) => visitor.visit_u64(*x),
            _ => Err(self.invalid_type(&visitor)),
        }
    }
}

impl<'de> Deserializer<'de> for ValueDeserializer<'_, 'de> {
    type Error = JsonError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::Null => visitor.visit_unit(),
            IValueImpl::Bool(x) => visitor.visit_bool(*x),
            IValueImpl::U64(x) => visitor.visit_u64(*x),
            IValueImpl::I64(x) => visitor.visit_i64(*x),
            IValueImpl::F64(Float64(OrderedFloat(x))) => visitor.visit_f64(*x),
            IValueImpl::String(s) => visitor.visit_borrowed_str(self.interners.string.lookup(*s)),
            IValueImpl::Array(a) => deserialize_array(visitor, *a, self.interners),
            IValueImpl::Object(o) => deserialize_object(visitor, *o, self.interners),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::Bool(x) => visitor.visit_bool(*x),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_integer(visitor)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::F64(Float64(OrderedFloat(x))) => visitor.visit_f64(*x),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::F64(Float64(OrderedFloat(x))) => visitor.visit_f64(*x),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::String(s) => visitor.visit_borrowed_str(self.interners.string.lookup(*s)),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::Null => visitor.visit_unit(),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::Array(a) => deserialize_array(visitor, *a, self.interners),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::Array(a) => {
                deserialize_array_expected_len(visitor, *a, self.interners, len, || {
                    format!("tuple with {len} elements")
                })
            }
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::Object(o) => deserialize_object(visitor, *o, self.interners),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::Array(a) => deserialize_array(visitor, *a, self.interners),
            IValueImpl::Object(o) => deserialize_object(visitor, *o, self.interners),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            IValueImpl::String(s) => visitor.visit_enum(EnumAccessor {
                variant: *s,
                value: None,
                interners: self.interners,
            }),
            IValueImpl::Object(o) => {
                let object = self.interners.iobject.lookup(*o);
                if object.len() != 1 {
                    Err(Error::invalid_length(
                        object.len(),
                        &"object with a single entry",
                    ))
                } else {
                    let (variant, value) = &object[0];
                    visitor.visit_enum(EnumAccessor {
                        variant: variant.0,
                        value: Some(&value.0),
                        interners: self.interners,
                    })
                }
            }
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    forward_to_deserialize_any! {
        bytes byte_buf
    }
}

struct ArrayAccess<'a, 'b> {
    array: &'a [IValue],
    index: usize,
    interners: &'b Jinterners,
}

impl ArrayAccess<'_, '_> {
    fn is_fully_scanned(&self) -> bool {
        self.index == self.array.len()
    }
}

impl<'de> SeqAccess<'de> for ArrayAccess<'_, 'de> {
    type Error = JsonError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.index < self.array.len() {
            let next = self.array[self.index];
            self.index += 1;
            seed.deserialize(ValueDeserializer {
                value: &next.0,
                interners: self.interners,
            })
            .map(Some)
        } else {
            Ok(None)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.array.len() - self.index)
    }
}

struct ObjectAccess<'a, 'b> {
    object: &'a [(InternedStrKey, IValue)],
    index: usize,
    interners: &'b Jinterners,
}

impl ObjectAccess<'_, '_> {
    fn is_fully_scanned(&self) -> bool {
        self.index == self.object.len()
    }
}

impl<'de> MapAccess<'de> for ObjectAccess<'_, 'de> {
    type Error = JsonError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.index < self.object.len() {
            let next = self.object[self.index];
            self.index += 1;
            seed.deserialize(StringDeserializer {
                istring: next.0.0,
                interners: self.interners,
            })
            .map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<T>(&mut self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(ValueDeserializer {
            value: &self.object[self.index - 1].1.0,
            interners: self.interners,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.object.len() - self.index)
    }
}

struct EnumAccessor<'a, 'b> {
    variant: InternedStr,
    value: Option<&'a IValueImpl>,
    interners: &'b Jinterners,
}

impl<'a, 'de> EnumAccess<'de> for EnumAccessor<'a, 'de> {
    type Error = JsonError;
    type Variant = VariantAccessor<'a, 'de>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(StringDeserializer {
            istring: self.variant,
            interners: self.interners,
        })
        .map(|value| {
            (
                value,
                VariantAccessor {
                    value: self.value,
                    interners: self.interners,
                },
            )
        })
    }
}

struct VariantAccessor<'a, 'b> {
    value: Option<&'a IValueImpl>,
    interners: &'b Jinterners,
}

impl<'de> VariantAccess<'de> for VariantAccessor<'_, 'de> {
    type Error = JsonError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.value {
            None => Ok(()),
            Some(value) => Err(ValueDeserializer {
                value,
                interners: self.interners,
            }
            .invalid_type(&"unit variant")),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.value {
            Some(value) => seed.deserialize(ValueDeserializer {
                value,
                interners: self.interners,
            }),
            None => Err(Error::invalid_type(
                Unexpected::UnitVariant,
                &"newtype variant",
            )),
        }
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Some(IValueImpl::Array(a)) => {
                deserialize_array_expected_len(visitor, *a, self.interners, len, || {
                    format!("tuple with {len} elements")
                })
            }
            Some(value) => Err(ValueDeserializer {
                value,
                interners: self.interners,
            }
            .invalid_type(&"tuple variant")),
            None => Err(Error::invalid_type(
                Unexpected::UnitVariant,
                &"tuple variant",
            )),
        }
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Some(IValueImpl::Array(a)) => {
                let len = fields.len();
                deserialize_array_expected_len(visitor, *a, self.interners, len, || {
                    format!("struct with {len} fields")
                })
            }
            Some(IValueImpl::Object(o)) => deserialize_object(visitor, *o, self.interners),
            Some(value) => Err(ValueDeserializer {
                value,
                interners: self.interners,
            }
            .invalid_type(&"struct variant")),
            None => Err(Error::invalid_type(
                Unexpected::UnitVariant,
                &"struct variant",
            )),
        }
    }
}

struct StringDeserializer<'b> {
    istring: InternedStr,
    interners: &'b Jinterners,
}

impl<'de> StringDeserializer<'de> {
    fn invalid_type<E>(self, exp: &dyn Expected) -> E
    where
        E: Error,
    {
        Error::invalid_type(
            Unexpected::Str(self.interners.string.lookup(self.istring)),
            exp,
        )
    }
}

impl<'de> Deserializer<'de> for StringDeserializer<'de> {
    type Error = JsonError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.interners.string.lookup(self.istring))
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bytes(self.interners.string.lookup_bytes(self.istring))
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.invalid_type(&visitor))
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(EnumAccessor {
            variant: self.istring,
            value: None,
            interners: self.interners,
        })
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }
}
