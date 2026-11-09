use super::{BoundValue, Float64, IValue, IValueImpl, InternedStrKey};
use crate::Jinterners;
use alloc::boxed::Box;
use blazinterner::{Index, Interned, InternedSlice, InternedStr, U32};
use core::hash::{BuildHasher, Hash};
use std::collections::HashMap;

#[expect(clippy::type_complexity)]
pub struct Comparator<'a, H, I> {
    interners: &'a Jinterners<H>,
    map_uint64: AdaptiveMap<Interned<u64, u64, I, U32>, Interned<u64, u64, H, U32>>,
    map_int64: AdaptiveMap<Interned<i64, i64, I, U32>, Interned<i64, i64, H, U32>>,
    map_float64:
        AdaptiveMap<Interned<Float64, Float64, I, U32>, Interned<Float64, Float64, H, U32>>,
    map_string: AdaptiveMap<InternedStr<I, U32>, InternedStr<H, U32>>,
    map_bytes: AdaptiveMap<InternedSlice<u8, I, U32>, InternedSlice<u8, H, U32>>,
    map_array: AdaptiveMap<InternedSlice<IValue<I>, I, U32>, InternedSlice<IValue<H>, H, U32>>,
    map_set: AdaptiveMap<InternedSlice<IValue<I>, I, U32>, InternedSlice<IValue<H>, H, U32>>,
    map_object: AdaptiveMap<
        InternedSlice<(InternedStrKey<I>, IValue<I>), I, U32>,
        InternedSlice<(InternedStrKey<H>, IValue<H>), H, U32>,
    >,
}

impl<'a, H, I> Comparator<'a, H, I>
where
    H: BuildHasher,
{
    pub fn eq<'b>(lhs: &BoundValue<'a, H>, rhs: &BoundValue<'b, I>) -> bool {
        Self::new(lhs.interners, rhs.interners).mapped(rhs) == Some(*lhs.value)
    }

    fn new<'b>(interners: &'a Jinterners<H>, other_interners: &'b Jinterners<I>) -> Self {
        Self {
            interners,
            map_uint64: AdaptiveMap::new(other_interners.uint64.len()),
            map_int64: AdaptiveMap::new(other_interners.int64.len()),
            map_float64: AdaptiveMap::new(other_interners.float64.len()),
            map_string: AdaptiveMap::new(other_interners.string.strings()),
            map_bytes: AdaptiveMap::new(other_interners.bytes.slices()),
            map_array: AdaptiveMap::new(other_interners.iarray.slices()),
            map_set: AdaptiveMap::new(other_interners.iset.slices()),
            map_object: AdaptiveMap::new(other_interners.iobject.slices()),
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
            IValueImpl::Bytes(x) => {
                let id = match self.map_bytes.get(&x) {
                    Some(id) => *id,
                    None => {
                        let id = self.interners.bytes.find(other.interners.bytes.lookup(x))?;
                        self.map_bytes.insert(x, id);
                        id
                    }
                };
                IValueImpl::Bytes(id)
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

// A map that switches from a sparse HashMap representation to a dense
// fixed-size Vec once the load factor exceeds a threshold.
enum AdaptiveMap<K, V> {
    HashMap {
        map: HashMap<K, V>,
        /// Threshold for map.len() at which point to switch to the dense
        /// representation.
        threshold: usize,
        /// Size of the Vec for the dense representation.
        range: usize,
    },
    Vec(Vec<Option<V>>),
}

impl<K, V> AdaptiveMap<K, V>
where
    K: Hash + Eq + Indexable,
    V: Copy,
{
    fn new(range: usize) -> Self {
        AdaptiveMap::HashMap {
            map: HashMap::new(),
            threshold: range / 8,
            range,
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        match self {
            AdaptiveMap::HashMap {
                map,
                threshold: _,
                range: _,
            } => map.get(key),
            AdaptiveMap::Vec(v) => match v.get(key.index()) {
                None => None,
                Some(opt) => opt.as_ref(),
            },
        }
    }

    fn insert(&mut self, key: K, value: V) {
        match self {
            AdaptiveMap::HashMap {
                map,
                threshold,
                range,
            } => {
                if map.len() < *threshold {
                    map.insert(key, value);
                } else {
                    let mut vec = vec![None; *range];
                    for (k, v) in map.iter() {
                        vec[k.index()] = Some(*v);
                    }
                    vec[key.index()] = Some(value);
                    *self = AdaptiveMap::Vec(vec);
                }
            }
            AdaptiveMap::Vec(vec) => {
                vec[key.index()] = Some(value);
            }
        }
    }
}

trait Indexable {
    fn index(&self) -> usize;
}

impl<T, U, H, I> Indexable for Interned<T, U, H, I>
where
    I: Index,
{
    fn index(&self) -> usize {
        self.id().to_usize()
    }
}

impl<T, H, I> Indexable for InternedSlice<T, H, I>
where
    I: Index,
{
    fn index(&self) -> usize {
        self.id().to_usize()
    }
}

impl<H, I> Indexable for InternedStr<H, I>
where
    I: Index,
{
    fn index(&self) -> usize {
        self.id().to_usize()
    }
}
