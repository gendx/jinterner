use super::{BoundValue, Float64, IValue, IValueImpl, InternedStrKey};
use crate::Jinterners;
use alloc::boxed::Box;
use blazinterner::{Interned, InternedSlice, InternedStr, U32};
use core::hash::BuildHasher;
use std::collections::HashMap;

#[expect(clippy::type_complexity)]
pub struct Comparator<'a, H, I> {
    interners: &'a Jinterners<H>,
    map_uint64: HashMap<Interned<u64, u64, I, U32>, Interned<u64, u64, H, U32>>,
    map_int64: HashMap<Interned<i64, i64, I, U32>, Interned<i64, i64, H, U32>>,
    map_float64: HashMap<Interned<Float64, Float64, I, U32>, Interned<Float64, Float64, H, U32>>,
    map_string: HashMap<InternedStr<I, U32>, InternedStr<H, U32>>,
    map_array: HashMap<InternedSlice<IValue<I>, I, U32>, InternedSlice<IValue<H>, H, U32>>,
    map_set: HashMap<InternedSlice<IValue<I>, I, U32>, InternedSlice<IValue<H>, H, U32>>,
    map_object: HashMap<
        InternedSlice<(InternedStrKey<I>, IValue<I>), I, U32>,
        InternedSlice<(InternedStrKey<H>, IValue<H>), H, U32>,
    >,
}

impl<'a, H, I> Comparator<'a, H, I>
where
    H: BuildHasher,
{
    pub fn eq<'b>(lhs: &BoundValue<'a, H>, rhs: &BoundValue<'b, I>) -> bool {
        Self::new(lhs.interners).mapped(rhs) == Some(*lhs.value)
    }

    fn new(interners: &'a Jinterners<H>) -> Self {
        Self {
            interners,
            map_uint64: HashMap::new(),
            map_int64: HashMap::new(),
            map_float64: HashMap::new(),
            map_string: HashMap::new(),
            map_array: HashMap::new(),
            map_set: HashMap::new(),
            map_object: HashMap::new(),
        }
    }

    fn mapped_key(
        &mut self,
        key: &InternedStrKey<I>,
        interners: &Jinterners<I>,
    ) -> Option<InternedStrKey<H>> {
        let id = match self.map_string.get(&key.0) {
            Some(id) => *id,
            None => {
                let id = self.interners.string.find(interners.string.lookup(key.0))?;
                self.map_string.insert(key.0, id);
                id
            }
        };
        Some(InternedStrKey(id))
    }

    fn mapped<'b>(&mut self, other: &BoundValue<'b, I>) -> Option<IValue<H>> {
        Some(IValue(match other.value.0 {
            IValueImpl::Null => IValueImpl::Null,
            IValueImpl::Bool(x) => IValueImpl::Bool(x),
            IValueImpl::U32(x) => IValueImpl::U32(x),
            IValueImpl::I32(x) => IValueImpl::I32(x),
            IValueImpl::U64(x) => {
                let id = match self.map_uint64.get(&x) {
                    Some(id) => *id,
                    None => {
                        let id = self
                            .interners
                            .uint64
                            .find(&other.interners.uint64.lookup(x))?;
                        self.map_uint64.insert(x, id);
                        id
                    }
                };
                IValueImpl::U64(id)
            }
            IValueImpl::I64(x) => {
                let id = match self.map_int64.get(&x) {
                    Some(id) => *id,
                    None => {
                        let id = self
                            .interners
                            .int64
                            .find(&other.interners.int64.lookup(x))?;
                        self.map_int64.insert(x, id);
                        id
                    }
                };
                IValueImpl::I64(id)
            }
            IValueImpl::F64(x) => {
                let id = match self.map_float64.get(&x) {
                    Some(id) => *id,
                    None => {
                        let id = self
                            .interners
                            .float64
                            .find(&other.interners.float64.lookup(x))?;
                        self.map_float64.insert(x, id);
                        id
                    }
                };
                IValueImpl::F64(id)
            }
            IValueImpl::String(x) => {
                let id = match self.map_string.get(&x) {
                    Some(id) => *id,
                    None => {
                        let id = self
                            .interners
                            .string
                            .find(other.interners.string.lookup(x))?;
                        self.map_string.insert(x, id);
                        id
                    }
                };
                IValueImpl::String(id)
            }
            IValueImpl::Array(x) => {
                let id = match self.map_array.get(&x) {
                    Some(id) => *id,
                    None => {
                        let array = other
                            .interners
                            .iarray
                            .lookup(x)
                            .iter()
                            .map(|v| {
                                self.mapped(&BoundValue {
                                    value: v,
                                    interners: other.interners,
                                })
                            })
                            .collect::<Option<Box<[_]>>>()?;
                        let id = self.interners.iarray.find(&array)?;
                        self.map_array.insert(x, id);
                        id
                    }
                };
                IValueImpl::Array(id)
            }
            IValueImpl::Set(x) => {
                let id = match self.map_set.get(&x) {
                    Some(id) => *id,
                    None => {
                        let mut set = other
                            .interners
                            .iset
                            .lookup(x)
                            .iter()
                            .map(|v| {
                                self.mapped(&BoundValue {
                                    value: v,
                                    interners: other.interners,
                                })
                            })
                            .collect::<Option<Box<[_]>>>()?;
                        set.sort_unstable();
                        let id = self.interners.iset.find(&set)?;
                        self.map_set.insert(x, id);
                        id
                    }
                };
                IValueImpl::Set(id)
            }
            IValueImpl::Object(x) => {
                let id = match self.map_object.get(&x) {
                    Some(id) => *id,
                    None => {
                        let mut object = other
                            .interners
                            .iobject
                            .lookup(x)
                            .iter()
                            .map(|(k, v)| {
                                Some((
                                    self.mapped_key(k, other.interners)?,
                                    self.mapped(&BoundValue {
                                        value: v,
                                        interners: other.interners,
                                    })?,
                                ))
                            })
                            .collect::<Option<Box<[_]>>>()?;
                        object.sort_unstable_by_key(|(k, _)| *k);
                        let id = self.interners.iobject.find(&object)?;
                        self.map_object.insert(x, id);
                        id
                    }
                };
                IValueImpl::Object(id)
            }
        }))
    }
}
