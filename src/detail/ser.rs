use super::{BoundValue, Float64, IValue, IValueImpl, InternedStrKey};
use crate::{Buffer, BufferPool, Jinterners};
use core::hash::BuildHasher;
use ordered_float::OrderedFloat;
use serde::ser::{
    Error as _, Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Serialize, Serializer};
use serde_json::error::Error;

#[cfg(feature = "sync")]
pub(super) struct ValueSerializer<'a, H> {
    pub interners: &'a Jinterners<H>,
    pub buffers: &'a mut BufferPool<H>,
}

#[cfg(feature = "sync")]
impl<'a, H> Serializer for ValueSerializer<'a, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    type SerializeSeq = SerializeArray<'a, H>;
    type SerializeTuple = SerializeArray<'a, H>;
    type SerializeTupleStruct = SerializeArray<'a, H>;
    type SerializeTupleVariant = SerializeArrayVariant<'a, H>;
    type SerializeMap = SerializeObject<'a, H>;
    type SerializeStruct = SerializeObject<'a, H>;
    type SerializeStructVariant = SerializeObjectVariant<'a, H>;

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
        let iter = value
            .iter()
            .map(|byte| IValue(IValueImpl::U64(*byte as u64)));
        // SAFETY: The iterator length is trusted, as it's a simple mapping on a
        // slice iterator.
        let index = unsafe { self.interners.iarray.intern_iter(iter) };
        Ok(IValueImpl::Array(index))
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
                buffers: self.buffers,
            })?),
        )];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array(object),
        ))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let array = self.buffers.pop_array(len);
        Ok(SerializeArray {
            interners: self.interners,
            buffers: self.buffers,
            array,
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
        let array = self.buffers.pop_array_with_capacity(len);
        Ok(SerializeArrayVariant {
            interners: self.interners,
            buffers: self.buffers,
            variant,
            array,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let object = self.buffers.pop_object(len);
        Ok(SerializeObject {
            interners: self.interners,
            buffers: self.buffers,
            object,
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
        let object = self.buffers.pop_object_with_capacity(len);
        Ok(SerializeObjectVariant {
            interners: self.interners,
            buffers: self.buffers,
            variant,
            object,
        })
    }
}

#[cfg(feature = "sync")]
pub(super) struct SerializeArray<'a, H> {
    interners: &'a Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    array: Buffer<IValue<H>>,
}

#[cfg(feature = "sync")]
impl<H> SerializeSeq for SerializeArray<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.array.push(IValue(value.serialize(ValueSerializer {
            interners: self.interners,
            buffers: self.buffers,
        })?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let iarray = self.interners.iarray.intern_copy(&self.array);
        self.buffers.push_array(self.array);
        Ok(IValueImpl::Array(iarray))
    }
}

#[cfg(feature = "sync")]
impl<H> SerializeTuple for SerializeArray<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
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

#[cfg(feature = "sync")]
impl<H> SerializeTupleStruct for SerializeArray<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
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

#[cfg(feature = "sync")]
pub(super) struct SerializeArrayVariant<'a, H> {
    interners: &'a Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    variant: &'static str,
    array: Buffer<IValue<H>>,
}

#[cfg(feature = "sync")]
impl<H> SerializeTupleVariant for SerializeArrayVariant<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.array.push(IValue(value.serialize(ValueSerializer {
            interners: self.interners,
            buffers: self.buffers,
        })?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey(self.interners.string.intern(self.variant));
        let value = IValue(IValueImpl::Array(
            self.interners.iarray.intern_copy(&self.array),
        ));
        self.buffers.push_array(self.array);

        let object = [(key, value)];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array(object),
        ))
    }
}

#[cfg(feature = "sync")]
pub(super) struct SerializeObject<'a, H> {
    interners: &'a Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    object: Buffer<(InternedStrKey<H>, IValue<H>)>,
    key: Option<InternedStrKey<H>>,
}

#[cfg(feature = "sync")]
impl<H> SerializeMap for SerializeObject<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // Panic because this indicates a bug in the program rather than an
        // expected failure.
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
        // Panic because this indicates a bug in the program rather than an
        // expected failure.
        let key = self
            .key
            .take()
            .expect("serialize_value called before serialize_key");
        self.object.push((
            key,
            IValue(value.serialize(ValueSerializer {
                interners: self.interners,
                buffers: self.buffers,
            })?),
        ));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        // Panic because this indicates a bug in the program rather than an
        // expected failure.
        if self.key.is_some() {
            panic!("missing serialize_value call after serialize_key");
        }
        self.object.sort_unstable_by_key(|(k, _)| *k);
        let iobject = self.interners.iobject.intern_copy(&self.object);
        self.buffers.push_object(self.object);
        Ok(IValueImpl::Object(iobject))
    }
}

#[cfg(feature = "sync")]
impl<H> SerializeStruct for SerializeObject<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
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

#[cfg(feature = "sync")]
pub(super) struct SerializeObjectVariant<'a, H> {
    interners: &'a Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    variant: &'static str,
    object: Buffer<(InternedStrKey<H>, IValue<H>)>,
}

#[cfg(feature = "sync")]
impl<H> SerializeStructVariant for SerializeObjectVariant<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.object.push((
            InternedStrKey(self.interners.string.intern(key)),
            IValue(value.serialize(ValueSerializer {
                interners: self.interners,
                buffers: self.buffers,
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
        self.buffers.push_object(self.object);

        let object = [(key, value)];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array(object),
        ))
    }
}

#[cfg(feature = "sync")]
struct ObjectKeySerializer<'a, H> {
    interners: &'a Jinterners<H>,
}

#[cfg(feature = "sync")]
impl<H> ObjectKeySerializer<'_, H> {
    fn error() -> Error {
        Error::custom(
            "Object key must be a string, unit variant, or a newtype struct or Option::Some of those",
        )
    }
}

#[cfg(feature = "sync")]
impl<H> Serializer for ObjectKeySerializer<'_, H>
where
    H: BuildHasher,
{
    type Ok = InternedStrKey<H>;
    type Error = Error;

    type SerializeSeq = Impossible<InternedStrKey<H>, Error>;
    type SerializeTuple = Impossible<InternedStrKey<H>, Error>;
    type SerializeTupleStruct = Impossible<InternedStrKey<H>, Error>;
    type SerializeTupleVariant = Impossible<InternedStrKey<H>, Error>;
    type SerializeMap = Impossible<InternedStrKey<H>, Error>;
    type SerializeStruct = Impossible<InternedStrKey<H>, Error>;
    type SerializeStructVariant = Impossible<InternedStrKey<H>, Error>;

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

pub(super) struct ValueSerializerMut<'a, H> {
    pub interners: &'a mut Jinterners<H>,
    pub buffers: &'a mut BufferPool<H>,
}

impl<'a, H> Serializer for ValueSerializerMut<'a, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    type SerializeSeq = SerializeArrayMut<'a, H>;
    type SerializeTuple = SerializeArrayMut<'a, H>;
    type SerializeTupleStruct = SerializeArrayMut<'a, H>;
    type SerializeTupleVariant = SerializeArrayVariantMut<'a, H>;
    type SerializeMap = SerializeObjectMut<'a, H>;
    type SerializeStruct = SerializeObjectMut<'a, H>;
    type SerializeStructVariant = SerializeObjectVariantMut<'a, H>;

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
        Ok(IValueImpl::String(self.interners.string.intern_mut(value)))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        // TODO: Can we do better?
        let iter = value
            .iter()
            .map(|byte| IValue(IValueImpl::U64(*byte as u64)));
        // SAFETY: The iterator length is trusted, as it's a simple mapping on a
        // slice iterator.
        let index = unsafe { self.interners.iarray.intern_iter_mut(iter) };
        Ok(IValueImpl::Array(index))
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
            InternedStrKey(self.interners.string.intern_mut(variant)),
            IValue(value.serialize(ValueSerializerMut {
                interners: self.interners,
                buffers: self.buffers,
            })?),
        )];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array_mut(object),
        ))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let array = self.buffers.pop_array(len);
        Ok(SerializeArrayMut {
            interners: self.interners,
            buffers: self.buffers,
            array,
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
        let array = self.buffers.pop_array_with_capacity(len);
        Ok(SerializeArrayVariantMut {
            interners: self.interners,
            buffers: self.buffers,
            variant,
            array,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let object = self.buffers.pop_object(len);
        Ok(SerializeObjectMut {
            interners: self.interners,
            buffers: self.buffers,
            object,
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
        let object = self.buffers.pop_object_with_capacity(len);
        Ok(SerializeObjectVariantMut {
            interners: self.interners,
            buffers: self.buffers,
            variant,
            object,
        })
    }
}

pub(super) struct SerializeArrayMut<'a, H> {
    interners: &'a mut Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    array: Buffer<IValue<H>>,
}

impl<H> SerializeSeq for SerializeArrayMut<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.array.push(IValue(value.serialize(ValueSerializerMut {
            interners: self.interners,
            buffers: self.buffers,
        })?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let iarray = self.interners.iarray.intern_copy_mut(&self.array);
        self.buffers.push_array(self.array);
        Ok(IValueImpl::Array(iarray))
    }
}

impl<H> SerializeTuple for SerializeArrayMut<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
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

impl<H> SerializeTupleStruct for SerializeArrayMut<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
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

pub(super) struct SerializeArrayVariantMut<'a, H> {
    interners: &'a mut Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    variant: &'static str,
    array: Buffer<IValue<H>>,
}

impl<H> SerializeTupleVariant for SerializeArrayVariantMut<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.array.push(IValue(value.serialize(ValueSerializerMut {
            interners: self.interners,
            buffers: self.buffers,
        })?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey(self.interners.string.intern_mut(self.variant));
        let value = IValue(IValueImpl::Array(
            self.interners.iarray.intern_copy_mut(&self.array),
        ));
        self.buffers.push_array(self.array);

        let object = [(key, value)];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array_mut(object),
        ))
    }
}

pub(super) struct SerializeObjectMut<'a, H> {
    interners: &'a mut Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    object: Buffer<(InternedStrKey<H>, IValue<H>)>,
    key: Option<InternedStrKey<H>>,
}

impl<H> SerializeMap for SerializeObjectMut<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // Panic because this indicates a bug in the program rather than an
        // expected failure.
        if self.key.is_some() {
            panic!("serialize_key called twice in a row");
        }
        self.key = Some(key.serialize(ObjectKeySerializerMut {
            interners: self.interners,
        })?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // Panic because this indicates a bug in the program rather than an
        // expected failure.
        let key = self
            .key
            .take()
            .expect("serialize_value called before serialize_key");
        self.object.push((
            key,
            IValue(value.serialize(ValueSerializerMut {
                interners: self.interners,
                buffers: self.buffers,
            })?),
        ));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        // Panic because this indicates a bug in the program rather than an
        // expected failure.
        if self.key.is_some() {
            panic!("missing serialize_value call after serialize_key");
        }
        self.object.sort_unstable_by_key(|(k, _)| *k);
        let iobject = self.interners.iobject.intern_copy_mut(&self.object);
        self.buffers.push_object(self.object);
        Ok(IValueImpl::Object(iobject))
    }
}

impl<H> SerializeStruct for SerializeObjectMut<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
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

pub(super) struct SerializeObjectVariantMut<'a, H> {
    interners: &'a mut Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    variant: &'static str,
    object: Buffer<(InternedStrKey<H>, IValue<H>)>,
}

impl<H> SerializeStructVariant for SerializeObjectVariantMut<'_, H>
where
    H: BuildHasher,
{
    type Ok = IValueImpl<H>;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.object.push((
            InternedStrKey(self.interners.string.intern_mut(key)),
            IValue(value.serialize(ValueSerializerMut {
                interners: self.interners,
                buffers: self.buffers,
            })?),
        ));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey(self.interners.string.intern_mut(self.variant));

        self.object.sort_unstable_by_key(|(k, _)| *k);
        let value = IValue(IValueImpl::Object(
            self.interners.iobject.intern_copy_mut(&self.object),
        ));
        self.buffers.push_object(self.object);

        let object = [(key, value)];
        Ok(IValueImpl::Object(
            self.interners.iobject.intern_array_mut(object),
        ))
    }
}

struct ObjectKeySerializerMut<'a, H> {
    interners: &'a mut Jinterners<H>,
}

impl<H> ObjectKeySerializerMut<'_, H> {
    fn error() -> Error {
        Error::custom(
            "Object key must be a string, unit variant, or a newtype struct or Option::Some of those",
        )
    }
}

impl<H> Serializer for ObjectKeySerializerMut<'_, H>
where
    H: BuildHasher,
{
    type Ok = InternedStrKey<H>;
    type Error = Error;

    type SerializeSeq = Impossible<InternedStrKey<H>, Error>;
    type SerializeTuple = Impossible<InternedStrKey<H>, Error>;
    type SerializeTupleStruct = Impossible<InternedStrKey<H>, Error>;
    type SerializeTupleVariant = Impossible<InternedStrKey<H>, Error>;
    type SerializeMap = Impossible<InternedStrKey<H>, Error>;
    type SerializeStruct = Impossible<InternedStrKey<H>, Error>;
    type SerializeStructVariant = Impossible<InternedStrKey<H>, Error>;

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
        Ok(InternedStrKey(self.interners.string.intern_mut(value)))
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
        Ok(InternedStrKey(self.interners.string.intern_mut(variant)))
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

impl<'a, H> Serialize for BoundValue<'a, H> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.value.0 {
            IValueImpl::Null => serializer.serialize_unit(),
            IValueImpl::Bool(x) => serializer.serialize_bool(*x),
            IValueImpl::U64(x) => serializer.serialize_u64(*x),
            IValueImpl::I64(x) => serializer.serialize_i64(*x),
            IValueImpl::F64(Float64(OrderedFloat(x))) => serializer.serialize_f64(*x),
            IValueImpl::String(s) => serializer.serialize_str(self.interners.string.lookup(*s)),
            IValueImpl::Array(a) => {
                let array = self.interners.iarray.lookup(*a);
                serializer.collect_seq(
                    array
                        .iter()
                        .map(|value| BoundValue::new(value, self.interners)),
                )
            }
            IValueImpl::Object(o) => {
                let object = self.interners.iobject.lookup(*o);
                serializer.collect_map(object.iter().map(|(key, value)| {
                    (
                        self.interners.string.lookup(key.0),
                        BoundValue::new(value, self.interners),
                    )
                }))
            }
        }
    }
}
