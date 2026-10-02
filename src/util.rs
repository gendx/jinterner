use crate::{IValue, InternedStrKey};
use alloc::vec::Vec;
use core::ops::{Deref, DerefMut};

/// Buffers to reuse allocations when deserializing.
#[derive(Default)]
pub struct BufferPool {
    array: Pool<IValue>,
    object: Pool<(InternedStrKey, IValue)>,
}

#[cfg(all(feature = "debug", feature = "std"))]
impl BufferPool {
    /// Prints a summary of the storage used by the buffers in this pool.
    pub fn print_summary(&self) {
        self.array.print_summary("array");
        self.object.print_summary("object");
    }
}

impl BufferPool {
    pub(crate) fn pop_array(&mut self, capacity: Option<usize>) -> Buffer<IValue> {
        self.array.pop(capacity)
    }

    pub(crate) fn pop_array_with_capacity(&mut self, capacity: usize) -> Buffer<IValue> {
        self.array.pop_with_capacity(capacity)
    }

    pub(crate) fn push_array(&mut self, b: Buffer<IValue>) {
        self.array.push(b)
    }

    pub(crate) fn pop_object(
        &mut self,
        capacity: Option<usize>,
    ) -> Buffer<(InternedStrKey, IValue)> {
        self.object.pop(capacity)
    }

    pub(crate) fn pop_object_with_capacity(
        &mut self,
        capacity: usize,
    ) -> Buffer<(InternedStrKey, IValue)> {
        self.object.pop_with_capacity(capacity)
    }

    pub(crate) fn push_object(&mut self, b: Buffer<(InternedStrKey, IValue)>) {
        self.object.push(b)
    }
}

#[derive(Default)]
pub(crate) struct Buffer<T> {
    buf: Vec<T>,
    #[cfg(feature = "debug")]
    count_used: usize,
}

impl<T> Buffer<T> {
    pub(crate) fn push(&mut self, t: T) {
        self.buf.push(t);
    }
}

impl<T> Deref for Buffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.buf.deref()
    }
}

impl<T> DerefMut for Buffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.deref_mut()
    }
}

#[derive(Default)]
struct Pool<T> {
    pool: Vec<Buffer<T>>,
}

#[cfg(all(feature = "debug", feature = "std"))]
impl<T> Pool<T> {
    pub fn print_summary(&self, title: &str) {
        println!("{} {title} buffers:", self.pool.len());
        for buf in &self.pool {
            println!(
                "- capacity = {}, bytes = {}, used {} times",
                buf.buf.capacity(),
                buf.buf.capacity() * core::mem::size_of::<T>(),
                buf.count_used
            );
        }
    }
}

impl<T> Pool<T> {
    // mut is only needed in debug mode.
    #[cfg_attr(not(feature = "debug"), expect(unused_mut))]
    fn pop(&mut self, capacity: Option<usize>) -> Buffer<T> {
        match (self.pool.pop(), capacity) {
            (Some(mut b), None) => {
                #[cfg(feature = "debug")]
                {
                    b.count_used += 1;
                }
                b
            }
            (Some(mut b), Some(capacity)) => {
                #[cfg(feature = "debug")]
                {
                    b.count_used += 1;
                }
                b.buf.reserve(capacity);
                b
            }
            (None, None) => Buffer {
                buf: Vec::new(),
                #[cfg(feature = "debug")]
                count_used: 1,
            },
            (None, Some(capacity)) => Buffer {
                buf: Vec::with_capacity(capacity),
                #[cfg(feature = "debug")]
                count_used: 1,
            },
        }
    }

    fn pop_with_capacity(&mut self, capacity: usize) -> Buffer<T> {
        match self.pool.pop() {
            Some(mut b) => {
                #[cfg(feature = "debug")]
                {
                    b.count_used += 1;
                }
                b.buf.reserve(capacity);
                b
            }
            None => Buffer {
                buf: Vec::with_capacity(capacity),
                #[cfg(feature = "debug")]
                count_used: 1,
            },
        }
    }

    fn push(&mut self, mut b: Buffer<T>) {
        b.buf.clear();
        self.push_empty(b);
    }

    fn push_empty(&mut self, b: Buffer<T>) {
        self.pool.push(b);
    }
}
