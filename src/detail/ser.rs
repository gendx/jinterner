use super::{ArrayOrder, BoundValue, IValue, IValueImpl, InternedStrKey};
use crate::{Buffer, BufferPool, Jinterners};
use core::hash::BuildHasher;
use serde::ser::{
    Error as SerError, Impossible, SerializeMap, SerializeSeq, SerializeStruct,
    SerializeStructVariant, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Serialize, Serializer};
use serde_json::error::Error;

fn make_array_error(array_order: ArrayOrder) -> Error {
    Error::custom(match array_order {
        ArrayOrder::Preserve => "Arena<array> is full, cannot push more values to it",
        ArrayOrder::Unordered => "Arena<set> is full, cannot push more values to it",
    })
}

#[cfg(feature = "sync")]
pub(super) struct ValueSerializer<'a, H> {
    pub interners: &'a Jinterners<H>,
    pub buffers: &'a mut BufferPool<H>,
    pub array_order: ArrayOrder,
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
        self.serialize_i32(value.into())
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(value.into())
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::I32(value.to_ne_bytes()))
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_i64(self.interners, value).ok_or(Error::custom(
            "Arena<i64> is full, cannot push more values to it",
        ))
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(value.into())
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(value.into())
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::U32(value.to_ne_bytes()))
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_u64(self.interners, value).ok_or(Error::custom(
            "Arena<u64> is full, cannot push more values to it",
        ))
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(value.into())
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_f64(self.interners, value).ok_or(Error::custom(
            "Arena<f64> is full, cannot push more values to it",
        ))
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        let mut b = [0; 4];
        let s = value.encode_utf8(&mut b);
        self.serialize_str(s)
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_string(self.interners, value).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_bytes(self.interners, value).ok_or(Error::custom(
            "Arena<bytes> is full, cannot push more values to it",
        ))
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
        let key = InternedStrKey::try_new(self.interners, variant).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))?;
        let value = IValue(value.serialize(ValueSerializer {
            interners: self.interners,
            buffers: self.buffers,
            array_order: self.array_order,
        })?);
        IValueImpl::try_singleton(self.interners, key, value).ok_or(Error::custom(
            "Arena<object> is full, cannot push more values to it",
        ))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let array = self.buffers.pop_array(len);
        Ok(SerializeArray {
            interners: self.interners,
            buffers: self.buffers,
            array_order: self.array_order,
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
            array_order: self.array_order,
            variant,
            array,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let object = self.buffers.pop_object(len);
        Ok(SerializeObject {
            interners: self.interners,
            buffers: self.buffers,
            array_order: self.array_order,
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
            array_order: self.array_order,
            variant,
            object,
        })
    }
}

#[cfg(feature = "sync")]
pub(super) struct SerializeArray<'a, H> {
    interners: &'a Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    array_order: ArrayOrder,
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
            array_order: self.array_order,
        })?));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let ivalue = IValueImpl::try_array(self.interners, &mut self.array, self.array_order)
            .ok_or_else(|| make_array_error(self.array_order))?;
        self.buffers.push_array(self.array);
        Ok(ivalue)
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
    array_order: ArrayOrder,
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
            array_order: self.array_order,
        })?));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey::try_new(self.interners, self.variant).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))?;
        let value = IValue(
            IValueImpl::try_array(self.interners, &mut self.array, self.array_order)
                .ok_or_else(|| make_array_error(self.array_order))?,
        );
        self.buffers.push_array(self.array);

        IValueImpl::try_singleton(self.interners, key, value).ok_or(Error::custom(
            "Arena<object> is full, cannot push more values to it",
        ))
    }
}

#[cfg(feature = "sync")]
pub(super) struct SerializeObject<'a, H> {
    interners: &'a Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    array_order: ArrayOrder,
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
                array_order: self.array_order,
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
        let ivalue = IValueImpl::try_object(self.interners, &mut self.object).ok_or(
            Error::custom("Arena<object> is full, cannot push more values to it"),
        )?;
        self.buffers.push_object(self.object);
        Ok(ivalue)
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
    array_order: ArrayOrder,
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
            InternedStrKey::try_new(self.interners, key).ok_or(Error::custom(
                "Arena<string> is full, cannot push more values to it",
            ))?,
            IValue(value.serialize(ValueSerializer {
                interners: self.interners,
                buffers: self.buffers,
                array_order: self.array_order,
            })?),
        ));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey::try_new(self.interners, self.variant).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))?;
        let value = IValue(
            IValueImpl::try_object(self.interners, &mut self.object).ok_or(Error::custom(
                "Arena<object> is full, cannot push more values to it",
            ))?,
        );
        self.buffers.push_object(self.object);
        IValueImpl::try_singleton(self.interners, key, value).ok_or(Error::custom(
            "Arena<object> is full, cannot push more values to it",
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
        InternedStrKey::try_new(self.interners, value).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))
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
        InternedStrKey::try_new(self.interners, variant).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))
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
    pub array_order: ArrayOrder,
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
        self.serialize_i32(value.into())
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(value.into())
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::I32(value.to_ne_bytes()))
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_i64_mut(self.interners, value).ok_or(Error::custom(
            "Arena<i64> is full, cannot push more values to it",
        ))
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(value.into())
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(value.into())
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        Ok(IValueImpl::U32(value.to_ne_bytes()))
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_u64_mut(self.interners, value).ok_or(Error::custom(
            "Arena<u64> is full, cannot push more values to it",
        ))
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(value.into())
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_f64_mut(self.interners, value).ok_or(Error::custom(
            "Arena<f64> is full, cannot push more values to it",
        ))
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        let mut b = [0; 4];
        let s = value.encode_utf8(&mut b);
        self.serialize_str(s)
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_string_mut(self.interners, value).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        IValueImpl::try_bytes_mut(self.interners, value).ok_or(Error::custom(
            "Arena<bytes> is full, cannot push more values to it",
        ))
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
        let key = InternedStrKey::try_new_mut(self.interners, variant).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))?;
        let value = IValue(value.serialize(ValueSerializerMut {
            interners: self.interners,
            buffers: self.buffers,
            array_order: self.array_order,
        })?);
        IValueImpl::try_singleton_mut(self.interners, key, value).ok_or(Error::custom(
            "Arena<object> is full, cannot push more values to it",
        ))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let array = self.buffers.pop_array(len);
        Ok(SerializeArrayMut {
            interners: self.interners,
            buffers: self.buffers,
            array_order: self.array_order,
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
            array_order: self.array_order,
            variant,
            array,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let object = self.buffers.pop_object(len);
        Ok(SerializeObjectMut {
            interners: self.interners,
            buffers: self.buffers,
            array_order: self.array_order,
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
            array_order: self.array_order,
            variant,
            object,
        })
    }
}

pub(super) struct SerializeArrayMut<'a, H> {
    interners: &'a mut Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    array_order: ArrayOrder,
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
            array_order: self.array_order,
        })?));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let ivalue = IValueImpl::try_array_mut(self.interners, &mut self.array, self.array_order)
            .ok_or_else(|| make_array_error(self.array_order))?;
        self.buffers.push_array(self.array);
        Ok(ivalue)
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
    array_order: ArrayOrder,
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
            array_order: self.array_order,
        })?));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey::try_new_mut(self.interners, self.variant).ok_or(
            Error::custom("Arena<string> is full, cannot push more values to it"),
        )?;
        let value = IValue(
            IValueImpl::try_array_mut(self.interners, &mut self.array, self.array_order)
                .ok_or_else(|| make_array_error(self.array_order))?,
        );
        self.buffers.push_array(self.array);

        IValueImpl::try_singleton_mut(self.interners, key, value).ok_or(Error::custom(
            "Arena<object> is full, cannot push more values to it",
        ))
    }
}

pub(super) struct SerializeObjectMut<'a, H> {
    interners: &'a mut Jinterners<H>,
    buffers: &'a mut BufferPool<H>,
    array_order: ArrayOrder,
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
                array_order: self.array_order,
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
        let ivalue = IValueImpl::try_object_mut(self.interners, &mut self.object).ok_or(
            Error::custom("Arena<object> is full, cannot push more values to it"),
        )?;
        self.buffers.push_object(self.object);
        Ok(ivalue)
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
    array_order: ArrayOrder,
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
            InternedStrKey::try_new_mut(self.interners, key).ok_or(Error::custom(
                "Arena<string> is full, cannot push more values to it",
            ))?,
            IValue(value.serialize(ValueSerializerMut {
                interners: self.interners,
                buffers: self.buffers,
                array_order: self.array_order,
            })?),
        ));
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let key = InternedStrKey::try_new_mut(self.interners, self.variant).ok_or(
            Error::custom("Arena<string> is full, cannot push more values to it"),
        )?;
        let value = IValue(
            IValueImpl::try_object_mut(self.interners, &mut self.object).ok_or(Error::custom(
                "Arena<object> is full, cannot push more values to it",
            ))?,
        );
        self.buffers.push_object(self.object);
        IValueImpl::try_singleton_mut(self.interners, key, value).ok_or(Error::custom(
            "Arena<object> is full, cannot push more values to it",
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
        InternedStrKey::try_new_mut(self.interners, value).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))
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
        InternedStrKey::try_new_mut(self.interners, variant).ok_or(Error::custom(
            "Arena<string> is full, cannot push more values to it",
        ))
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
            IValueImpl::U32(x) => serializer.serialize_u32(u32::from_ne_bytes(*x)),
            IValueImpl::I32(x) => serializer.serialize_i32(i32::from_ne_bytes(*x)),
            IValueImpl::U64(x) => {
                serializer.serialize_u64(self.interners.uint64.try_lookup(*x).ok_or(
                    SerError::custom("Interned<u64> is out of bounds for this arena"),
                )?)
            }
            IValueImpl::I64(x) => {
                serializer.serialize_i64(self.interners.int64.try_lookup(*x).ok_or(
                    SerError::custom("Interned<i64> is out of bounds for this arena"),
                )?)
            }
            IValueImpl::F64(x) => serializer.serialize_f64(
                self.interners
                    .float64
                    .try_lookup(*x)
                    .ok_or(SerError::custom(
                        "Interned<f64> is out of bounds for this arena",
                    ))?
                    .0
                    .0,
            ),
            IValueImpl::String(s) => {
                serializer.serialize_str(self.interners.string.try_lookup(*s).ok_or(
                    SerError::custom("Interned<string> is out of bounds for this arena"),
                )?)
            }
            IValueImpl::Bytes(b) => {
                serializer.serialize_bytes(self.interners.bytes.try_lookup(*b).ok_or(
                    SerError::custom("Interned<bytes> is out of bounds for this arena"),
                )?)
            }
            IValueImpl::Array(a) => {
                let array = self
                    .interners
                    .iarray
                    .try_lookup(*a)
                    .ok_or(SerError::custom(
                        "Interned<array> is out of bounds for this arena",
                    ))?;
                serializer.collect_seq(
                    array
                        .iter()
                        .map(|value| BoundValue::new(value, self.interners)),
                )
            }
            IValueImpl::Set(a) => {
                let set = self.interners.iset.try_lookup(*a).ok_or(SerError::custom(
                    "Interned<set> is out of bounds for this arena",
                ))?;
                serializer.collect_seq(
                    set.iter()
                        .map(|value| BoundValue::new(value, self.interners)),
                )
            }
            IValueImpl::Object(o) => {
                let object = self
                    .interners
                    .iobject
                    .try_lookup(*o)
                    .ok_or(SerError::custom(
                        "Interned<object> is out of bounds for this arena",
                    ))?;

                let mut map = serializer.serialize_map(Some(object.len()))?;
                for (key, value) in object.iter() {
                    map.serialize_entry(
                        self.interners
                            .string
                            .try_lookup(key.0)
                            .ok_or(SerError::custom(
                                "Interned<string> is out of bounds for this arena",
                            ))?,
                        &BoundValue::new(value, self.interners),
                    )?;
                }
                map.end()
            }
        }
    }
}
