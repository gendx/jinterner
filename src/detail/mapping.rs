use super::{IValue, IValueImpl, InternedStrKey};
use blazinterner::ForwardMapping;

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub struct Mapping {
    pub(crate) string: ForwardMapping,
    pub(crate) iarray: ForwardMapping,
    pub(crate) iobject: ForwardMapping,
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
    #[cfg(feature = "debug")]
    pub fn count_remapped_strings(&self) -> usize {
        self.string.count_remapped()
    }

    /// Returns the number of arrays that are remapped by this mapping.
    #[cfg(feature = "debug")]
    pub fn count_remapped_arrays(&self) -> usize {
        self.iarray.count_remapped()
    }

    /// Returns the number of objects that are remapped by this mapping.
    #[cfg(feature = "debug")]
    pub fn count_remapped_objects(&self) -> usize {
        self.iobject.count_remapped()
    }

    pub(crate) fn map_str_key(&self, s: InternedStrKey) -> InternedStrKey {
        InternedStrKey(self.string.map_str(s.0))
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
            IValueImpl::String(x) => IValueImpl::String(self.string.map_str(x)),
            IValueImpl::Array(x) => IValueImpl::Array(self.iarray.map_slice(x)),
            IValueImpl::Object(x) => IValueImpl::Object(self.iobject.map_slice(x)),
        })
    }
}

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub(crate) struct MappingStrings {
    pub(crate) string: ForwardMapping,
}

impl MappingStrings {
    pub fn promote(self, num_arrays: u32, num_objects: u32) -> Mapping {
        Mapping {
            string: self.string,
            iarray: ForwardMapping::identity(num_arrays),
            iobject: ForwardMapping::identity(num_objects),
        }
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.string.is_identity()
    }

    pub fn map_str_key(&self, s: InternedStrKey) -> InternedStrKey {
        InternedStrKey(self.string.map_str(s.0))
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
            IValueImpl::String(x) => IValueImpl::String(self.string.map_str(x)),
            IValueImpl::Array(x) => IValueImpl::Array(x),
            IValueImpl::Object(x) => IValueImpl::Object(x),
        })
    }
}

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub(crate) struct MappingNoStrings {
    pub(crate) iarray: ForwardMapping,
    pub(crate) iobject: ForwardMapping,
}

impl MappingNoStrings {
    pub fn promote(self, num_strings: u32) -> Mapping {
        Mapping {
            string: ForwardMapping::identity(num_strings),
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
            IValueImpl::Array(x) => IValueImpl::Array(self.iarray.map_slice(x)),
            IValueImpl::Object(x) => IValueImpl::Object(self.iobject.map_slice(x)),
        })
    }
}
