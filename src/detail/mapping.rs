use super::{IValue, IValueImpl, InternedStrKey};
use blazinterner::ForwardMapping;

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub struct Mapping {
    pub(crate) uint64: ForwardMapping,
    pub(crate) int64: ForwardMapping,
    pub(crate) float64: ForwardMapping,
    pub(crate) string: ForwardMapping,
    pub(crate) iarray: ForwardMapping,
    pub(crate) iobject: ForwardMapping,
}

impl Mapping {
    /// Returns a mapping that applies this mapping followed by the other
    /// mapping.
    pub(crate) fn compose(self, other: MappingNoScalars) -> Self {
        Self {
            uint64: self.uint64,
            int64: self.int64,
            float64: self.float64,
            string: self.string,
            iarray: self.iarray.compose(other.iarray),
            iobject: self.iobject.compose(other.iobject),
        }
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.uint64.is_identity()
            && self.int64.is_identity()
            && self.float64.is_identity()
            && self.string.is_identity()
            && self.iarray.is_identity()
            && self.iobject.is_identity()
    }

    /// Returns the number of [`u64`]s that are remapped by this mapping.
    #[cfg(feature = "debug")]
    pub fn count_remapped_u64s(&self) -> usize {
        self.uint64.count_remapped()
    }

    /// Returns the number of [`i64`]s that are remapped by this mapping.
    #[cfg(feature = "debug")]
    pub fn count_remapped_i64s(&self) -> usize {
        self.int64.count_remapped()
    }

    /// Returns the number of [`f64`]s that are remapped by this mapping.
    #[cfg(feature = "debug")]
    pub fn count_remapped_f64s(&self) -> usize {
        self.float64.count_remapped()
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

    pub(crate) fn map_str_key<H>(&self, s: InternedStrKey<H>) -> InternedStrKey<H> {
        InternedStrKey(self.string.map_str(s.0))
    }

    /// Maps the given value from the source [`Jinterners`](crate::Jinterners)
    /// to the destination [`Jinterners`](crate::Jinterners) of this mapping.
    pub fn map<H>(&self, v: IValue<H>) -> IValue<H> {
        IValue(match v.0 {
            IValueImpl::Null => IValueImpl::Null,
            IValueImpl::Bool(x) => IValueImpl::Bool(x),
            IValueImpl::U32(x) => IValueImpl::U32(x),
            IValueImpl::I32(x) => IValueImpl::I32(x),
            IValueImpl::U64(x) => IValueImpl::U64(self.uint64.map(x)),
            IValueImpl::I64(x) => IValueImpl::I64(self.int64.map(x)),
            IValueImpl::F64(x) => IValueImpl::F64(self.float64.map(x)),
            IValueImpl::String(x) => IValueImpl::String(self.string.map_str(x)),
            IValueImpl::Array(x) => IValueImpl::Array(self.iarray.map_slice(x)),
            IValueImpl::Object(x) => IValueImpl::Object(self.iobject.map_slice(x)),
        })
    }
}

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub(crate) struct MappingScalars {
    pub(crate) uint64: ForwardMapping,
    pub(crate) int64: ForwardMapping,
    pub(crate) float64: ForwardMapping,
    pub(crate) string: ForwardMapping,
}

impl MappingScalars {
    pub fn promote(self, num_arrays: u32, num_objects: u32) -> Mapping {
        Mapping {
            uint64: self.uint64,
            int64: self.int64,
            float64: self.float64,
            string: self.string,
            iarray: ForwardMapping::identity(num_arrays),
            iobject: ForwardMapping::identity(num_objects),
        }
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.uint64.is_identity()
            && self.int64.is_identity()
            && self.float64.is_identity()
            && self.string.is_identity()
    }

    pub fn map_str_key<H>(&self, s: InternedStrKey<H>) -> InternedStrKey<H> {
        InternedStrKey(self.string.map_str(s.0))
    }

    /// Maps the given value from the source [`Jinterners`](crate::Jinterners)
    /// to the destination [`Jinterners`](crate::Jinterners) of this mapping.
    pub fn map<H>(&self, v: IValue<H>) -> IValue<H> {
        IValue(match v.0 {
            IValueImpl::Null => IValueImpl::Null,
            IValueImpl::Bool(x) => IValueImpl::Bool(x),
            IValueImpl::U32(x) => IValueImpl::U32(x),
            IValueImpl::I32(x) => IValueImpl::I32(x),
            IValueImpl::U64(x) => IValueImpl::U64(self.uint64.map(x)),
            IValueImpl::I64(x) => IValueImpl::I64(self.int64.map(x)),
            IValueImpl::F64(x) => IValueImpl::F64(self.float64.map(x)),
            IValueImpl::String(x) => IValueImpl::String(self.string.map_str(x)),
            IValueImpl::Array(x) => IValueImpl::Array(x),
            IValueImpl::Object(x) => IValueImpl::Object(x),
        })
    }
}

/// Mapping to convert values from one [`Jinterners`](crate::Jinterners)
/// instance to another.
pub(crate) struct MappingNoScalars {
    pub(crate) iarray: ForwardMapping,
    pub(crate) iobject: ForwardMapping,
}

impl MappingNoScalars {
    pub fn promote(
        self,
        num_uint64s: u32,
        num_int64s: u32,
        num_float64s: u32,
        num_strings: u32,
    ) -> Mapping {
        Mapping {
            uint64: ForwardMapping::identity(num_uint64s),
            int64: ForwardMapping::identity(num_int64s),
            float64: ForwardMapping::identity(num_float64s),
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
    pub fn map<H>(&self, v: IValue<H>) -> IValue<H> {
        IValue(match v.0 {
            IValueImpl::Null => IValueImpl::Null,
            IValueImpl::Bool(x) => IValueImpl::Bool(x),
            IValueImpl::U32(x) => IValueImpl::U32(x),
            IValueImpl::I32(x) => IValueImpl::I32(x),
            IValueImpl::U64(x) => IValueImpl::U64(x),
            IValueImpl::I64(x) => IValueImpl::I64(x),
            IValueImpl::F64(x) => IValueImpl::F64(x),
            IValueImpl::String(x) => IValueImpl::String(x),
            IValueImpl::Array(x) => IValueImpl::Array(self.iarray.map_slice(x)),
            IValueImpl::Object(x) => IValueImpl::Object(self.iobject.map_slice(x)),
        })
    }
}
