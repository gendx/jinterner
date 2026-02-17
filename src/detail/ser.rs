use super::{Float64, IValue, IValueImpl, InternedStrKey};
use crate::Jinterners;
use ordered_float::OrderedFloat;
use serde::ser::{
    Error as _, Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Serialize, Serializer};
use serde_json::error::Error;

pub(super) struct ValueSerializer<'a> {
    pub interners: &'a Jinterners,
}

impl<'a> Serializer for ValueSerializer<'a> {
    type Ok = IValueImpl;
    type Error = Error;

    type SerializeSeq = SerializeArray<'a>;
    type SerializeTuple = SerializeArray<'a>;
    type SerializeTupleStruct = SerializeArray<'a>;
    type SerializeTupleVariant = SerializeArrayVariant<'a>;
    type SerializeMap = SerializeObject<'a>;
    type SerializeStruct = SerializeObject<'a>;
    type SerializeStructVariant = SerializeObjectVariant<'a>;

    fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::Bool(value))
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::I64(value.into()))
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::I64(value.into()))
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::I64(value.into()))
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::I64(value))
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::U64(value.into()))
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::U64(value.into()))
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::U64(value.into()))
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::U64(value))
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::F64(Float64(OrderedFloat(value.into()))))
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::F64(Float64(OrderedFloat(value))))
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        let mut b = [0; 4];
        let s = value.encode_utf8(&mut b);
        self.serialize_str(s)
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::String(self.interners.string.intern(value)))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        // TODO: Can we do better?
        let array: Box<[IValue]> = value
            .iter()
            .map(|byte| IValue(IValueImpl::U64(*byte as u64)))
            .collect();
        Ok(IValueImpl::Array(self.interners.iarray.intern_copy(&array)))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::Null)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::Null)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let object = [(
            InternedStrKey(self.interners.string.intern(variant)),
            IValue(value.serialize(ValueSerializer {
                interners: self.interners,
            })?),
        )];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array(object),
        ))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SerializeArray {
            interners: self.interners,
            array: Vec::with_capacity(len.unwrap_or(0)),
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SerializeArrayVariant {
            interners: self.interners,
            variant,
            array: Vec::with_capacity(len),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(SerializeObject {
            interners: self.interners,
            object: Vec::with_capacity(len.unwrap_or(0)),
            key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(SerializeObjectVariant {
            interners: self.interners,
            variant,
            object: Vec::with_capacity(len),
        })
    }
}

pub(super) struct SerializeArray<'a> {
    interners: &'a Jinterners,
    array: Vec<IValue>,
}

impl SerializeSeq for SerializeArray<'_> {
    type Ok = IValueImpl;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.array.push(IValue(value.serialize(ValueSerializer {
            interners: self.interners,
        })?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::Array(
            self.interners.iarray.intern_copy(&self.array),
        ))
    }
}

impl SerializeTuple for SerializeArray<'_> {
    type Ok = IValueImpl;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleStruct for SerializeArray<'_> {
    type Ok = IValueImpl;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeSeq::end(self)
    }
}

pub(super) struct SerializeArrayVariant<'a> {
    interners: &'a Jinterners,
    variant: &'static str,
    array: Vec<IValue>,
}

impl SerializeTupleVariant for SerializeArrayVariant<'_> {
    type Ok = IValueImpl;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.array.push(IValue(value.serialize(ValueSerializer {
            interners: self.interners,
        })?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey(self.interners.string.intern(self.variant));
        let value = IValue(IValueImpl::Array(
            self.interners.iarray.intern_copy(&self.array),
        ));

        let object = [(key, value)];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array(object),
        ))
    }
}

pub(super) struct SerializeObject<'a> {
    interners: &'a Jinterners,
    object: Vec<(InternedStrKey, IValue)>,
    key: Option<InternedStrKey>,
}

impl SerializeMap for SerializeObject<'_> {
    type Ok = IValueImpl;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // Panic because this indicates a bug in the program rather than an expected
        // failure.
        if self.key.is_some() {
            panic!("serialize_key called twice in a row");
        }
        self.key = Some(key.serialize(ObjectKeySerializer {
            interners: self.interners,
        })?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // Panic because this indicates a bug in the program rather than an expected
        // failure.
        let key = self
            .key
            .take()
            .expect("serialize_value called before serialize_key");
        self.object.push((
            key,
            IValue(value.serialize(ValueSerializer {
                interners: self.interners,
            })?),
        ));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        // Panic because this indicates a bug in the program rather than an expected
        // failure.
        if self.key.is_some() {
            panic!("missing serialize_value call after serialize_key");
        }
        self.object.sort_unstable_by_key(|(k, _)| *k);
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_copy(&self.object),
        ))
    }
}

impl SerializeStruct for SerializeObject<'_> {
    type Ok = IValueImpl;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeMap::serialize_entry(self, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeMap::end(self)
    }
}

pub(super) struct SerializeObjectVariant<'a> {
    interners: &'a Jinterners,
    variant: &'static str,
    object: Vec<(InternedStrKey, IValue)>,
}

impl SerializeStructVariant for SerializeObjectVariant<'_> {
    type Ok = IValueImpl;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.object.push((
            InternedStrKey(self.interners.string.intern(key)),
            IValue(value.serialize(ValueSerializer {
                interners: self.interners,
            })?),
        ));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey(self.interners.string.intern(self.variant));

        self.object.sort_unstable_by_key(|(k, _)| *k);
        let value = IValue(IValueImpl::Object(
            self.interners.iobject.intern_copy(&self.object),
        ));

        let object = [(key, value)];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array(object),
        ))
    }
}

struct ObjectKeySerializer<'a> {
    interners: &'a Jinterners,
}

impl ObjectKeySerializer<'_> {
    fn error() -> Error {
        Error::custom(
            "Object key must be a string, unit variant, or a newtype struct or Option::Some of those",
        )
    }
}

impl Serializer for ObjectKeySerializer<'_> {
    type Ok = InternedStrKey;
    type Error = Error;

    type SerializeSeq = Impossible<InternedStrKey, Error>;
    type SerializeTuple = Impossible<InternedStrKey, Error>;
    type SerializeTupleStruct = Impossible<InternedStrKey, Error>;
    type SerializeTupleVariant = Impossible<InternedStrKey, Error>;
    type SerializeMap = Impossible<InternedStrKey, Error>;
    type SerializeStruct = Impossible<InternedStrKey, Error>;
    type SerializeStructVariant = Impossible<InternedStrKey, Error>;

    fn serialize_bool(self, _value: bool) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_i8(self, _value: i8) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_i16(self, _value: i16) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_i32(self, _value: i32) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_i64(self, _value: i64) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_u8(self, _value: u8) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_u16(self, _value: u16) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_u32(self, _value: u32) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_u64(self, _value: u64) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_f32(self, _value: f32) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_f64(self, _value: f64) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        let mut b = [0; 4];
        let s = value.encode_utf8(&mut b);
        self.serialize_str(s)
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        Ok(InternedStrKey(self.interners.string.intern(value)))
    }

    fn serialize_bytes(self, _value: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Self::error())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(InternedStrKey(self.interners.string.intern(variant)))
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Self::error())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(Self::error())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Self::error())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Self::error())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Self::error())
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Self::error())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Self::error())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Self::error())
    }
}
