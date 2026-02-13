use super::{IValue, IValueImpl, InternedStr, InternedStrKey};
use blazinterner::{Interned, InternedSlice};

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub struct Mapping {
    pub(crate) string: MappingImpl,
    pub(crate) iarray: MappingImpl,
    pub(crate) iobject: MappingImpl,
}

impl Mapping {
    /// Returns a mapping that applies this mapping followed by the other
    /// mapping.
    pub(crate) fn compose(self, other: MappingNoStrings) -> Self {
        Self {
            string: self.string,
            iarray: self.iarray.compose(other.iarray),
            iobject: self.iobject.compose(other.iobject),
        }
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.string.is_identity() && self.iarray.is_identity() && self.iobject.is_identity()
    }

    /// Returns the number of strings that are remapped by this mapping.
    pub fn count_remapped_strings(&self) -> usize {
        self.string.count_remapped()
    }

    /// Returns the number of arrays that are remapped by this mapping.
    pub fn count_remapped_arrays(&self) -> usize {
        self.iarray.count_remapped()
    }

    /// Returns the number of objects that are remapped by this mapping.
    pub fn count_remapped_objects(&self) -> usize {
        self.iobject.count_remapped()
    }

    pub(crate) fn map_str_key(&self, s: InternedStrKey) -> InternedStrKey {
        InternedStrKey(self.map_str(s.0))
    }

    fn map_str(&self, s: InternedStr) -> InternedStr {
        Interned::from_id(self.string.at(s.id()))
    }

    /// Maps the given value from the source [`Jinterners`](crate::Jinterners)
    /// to the destination [`Jinterners`](crate::Jinterners) of this mapping.
    pub fn map(&self, v: IValue) -> IValue {
        IValue(match v.0 {
            IValueImpl::Null => IValueImpl::Null,
            IValueImpl::Bool(x) => IValueImpl::Bool(x),
            IValueImpl::U64(x) => IValueImpl::U64(x),
            IValueImpl::I64(x) => IValueImpl::I64(x),
            IValueImpl::F64(x) => IValueImpl::F64(x),
            IValueImpl::String(x) => IValueImpl::String(Interned::from_id(self.string.at(x.id()))),
            IValueImpl::Array(x) => {
                IValueImpl::Array(InternedSlice::from_id(self.iarray.at(x.id())))
            }
            IValueImpl::Object(x) => {
                IValueImpl::Object(InternedSlice::from_id(self.iobject.at(x.id())))
            }
        })
    }
}

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub(crate) struct MappingStrings {
    pub(crate) string: MappingImpl,
}

impl MappingStrings {
    pub fn promote(self, num_arrays: u32, num_objects: u32) -> Mapping {
        Mapping {
            string: self.string,
            iarray: MappingImpl::Identity(num_arrays),
            iobject: MappingImpl::Identity(num_objects),
        }
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.string.is_identity()
    }

    pub fn map_str_key(&self, s: InternedStrKey) -> InternedStrKey {
        InternedStrKey(self.map_str(s.0))
    }

    fn map_str(&self, s: InternedStr) -> InternedStr {
        Interned::from_id(self.string.at(s.id()))
    }

    /// Maps the given value from the source [`Jinterners`](crate::Jinterners)
    /// to the destination [`Jinterners`](crate::Jinterners) of this mapping.
    pub fn map(&self, v: IValue) -> IValue {
        IValue(match v.0 {
            IValueImpl::Null => IValueImpl::Null,
            IValueImpl::Bool(x) => IValueImpl::Bool(x),
            IValueImpl::U64(x) => IValueImpl::U64(x),
            IValueImpl::I64(x) => IValueImpl::I64(x),
            IValueImpl::F64(x) => IValueImpl::F64(x),
            IValueImpl::String(x) => IValueImpl::String(Interned::from_id(self.string.at(x.id()))),
            IValueImpl::Array(x) => IValueImpl::Array(x),
            IValueImpl::Object(x) => IValueImpl::Object(x),
        })
    }
}

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub(crate) struct MappingNoStrings {
    pub(crate) iarray: MappingImpl,
    pub(crate) iobject: MappingImpl,
}

impl MappingNoStrings {
    pub fn promote(self, num_strings: u32) -> Mapping {
        Mapping {
            string: MappingImpl::Identity(num_strings),
            iarray: self.iarray,
            iobject: self.iobject,
        }
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.iarray.is_identity() && self.iobject.is_identity()
    }

    /// Maps the given value from the source [`Jinterners`](crate::Jinterners)
    /// to the destination [`Jinterners`](crate::Jinterners) of this mapping.
    pub fn map(&self, v: IValue) -> IValue {
        IValue(match v.0 {
            IValueImpl::Null => IValueImpl::Null,
            IValueImpl::Bool(x) => IValueImpl::Bool(x),
            IValueImpl::U64(x) => IValueImpl::U64(x),
            IValueImpl::I64(x) => IValueImpl::I64(x),
            IValueImpl::F64(x) => IValueImpl::F64(x),
            IValueImpl::String(x) => IValueImpl::String(x),
            IValueImpl::Array(x) => {
                IValueImpl::Array(InternedSlice::from_id(self.iarray.at(x.id())))
            }
            IValueImpl::Object(x) => {
                IValueImpl::Object(InternedSlice::from_id(self.iobject.at(x.id())))
            }
        })
    }
}

pub(crate) struct RevMappingImpl(pub(crate) Box<[u32]>);

impl RevMappingImpl {
    pub fn reverse(&self) -> MappingImpl {
        if self.is_identity() {
            MappingImpl::Identity(self.0.len() as u32)
        } else {
            let mut reverse = vec![0; self.0.len()];
            for i in 0..self.0.len() as u32 {
                reverse[self.0[i as usize] as usize] = i;
            }
            MappingImpl::Map(reverse.into_boxed_slice())
        }
    }

    /// Checks wether this mapping is the identity.
    fn is_identity(&self) -> bool {
        self.0.iter().enumerate().all(|(i, j)| i == *j as usize)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = u32> {
        self.0.iter().copied()
    }
}

pub(crate) enum MappingImpl {
    Identity(u32),
    Map(Box<[u32]>),
}

impl MappingImpl {
    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        match self {
            Self::Identity(_) => true,
            Self::Map(_) => false,
        }
    }

    fn len(&self) -> u32 {
        match self {
            Self::Identity(len) => *len,
            Self::Map(map) => map.len() as u32,
        }
    }

    pub fn at(&self, index: u32) -> u32 {
        match self {
            MappingImpl::Identity(_) => index,
            MappingImpl::Map(map) => map[index as usize],
        }
    }

    pub fn compose(self, other: MappingImpl) -> Self {
        assert_eq!(self.len(), other.len());
        match (self, other) {
            (MappingImpl::Identity(len), MappingImpl::Identity(_)) => MappingImpl::Identity(len),
            (MappingImpl::Map(map), MappingImpl::Identity(_))
            | (MappingImpl::Identity(_), MappingImpl::Map(map)) => MappingImpl::Map(map),
            (MappingImpl::Map(left), MappingImpl::Map(right)) => {
                MappingImpl::Map(left.iter().map(|i| right[*i as usize]).collect())
            }
        }
    }

    /// Returns the number of items that are remapped by this mapping.
    pub fn count_remapped(&self) -> usize {
        match self {
            Self::Identity(_) => 0,
            Self::Map(map) => map
                .iter()
                .enumerate()
                .filter(|&(i, j)| i != *j as usize)
                .count(),
        }
    }
}
