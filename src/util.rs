use crate::{IValue, InternedStrKey};
use alloc::vec::Vec;

/// Buffers to reuse allocations when deserializing.
#[derive(Default)]
pub struct BufferPool {
    array: Pool<IValue>,
    object: Pool<(InternedStrKey, IValue)>,
}

impl BufferPool {
    pub(crate) fn pop_array(&mut self, capacity: Option<usize>) -> Vec<IValue> {
        self.array.pop(capacity)
    }

    pub(crate) fn pop_array_with_capacity(&mut self, capacity: usize) -> Vec<IValue> {
        self.array.pop_with_capacity(capacity)
    }

    pub(crate) fn push_array(&mut self, v: Vec<IValue>) {
        self.array.push(v)
    }

    pub(crate) fn pop_object(&mut self, capacity: Option<usize>) -> Vec<(InternedStrKey, IValue)> {
        self.object.pop(capacity)
    }

    pub(crate) fn pop_object_with_capacity(
        &mut self,
        capacity: usize,
    ) -> Vec<(InternedStrKey, IValue)> {
        self.object.pop_with_capacity(capacity)
    }

    pub(crate) fn push_object(&mut self, v: Vec<(InternedStrKey, IValue)>) {
        self.object.push(v)
    }
}

#[derive(Default)]
struct Pool<T> {
    pool: Vec<Vec<T>>,
}

impl<T> Pool<T> {
    fn pop(&mut self, capacity: Option<usize>) -> Vec<T> {
        match (self.pool.pop(), capacity) {
            (Some(v), None) => v,
            (Some(mut v), Some(capacity)) => {
                v.reserve(capacity);
                v
            }
            (None, None) => Vec::new(),
            (None, Some(capacity)) => Vec::with_capacity(capacity),
        }
    }

    fn pop_with_capacity(&mut self, capacity: usize) -> Vec<T> {
        match self.pool.pop() {
            Some(mut v) => {
                v.reserve(capacity);
                v
            }
            None => Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, mut v: Vec<T>) {
        v.clear();
        self.push_empty(v);
    }

    fn push_empty(&mut self, v: Vec<T>) {
        self.pool.push(v);
    }
}
