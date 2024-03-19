/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// A circular buffer which is used to keep the backreferences both in
/// sequential reads and compression.
///
/// For efficency reasons, we re-use the allocated buffers to avoid pressure
/// over the allocator.
///
/// This structure implements [`Index`](std::ops::Index) and
/// [`IndexMut`](std::ops::IndexMut) with both positive and negative indices,
/// which are resolved with modular arithmetic. It is also possible to
/// [take](CircularBuffer::take) and [replace](CircularBuffer::replace) the
/// value at a given index.
#[derive(Debug, Clone)]
pub(crate) struct CircularBuffer<T: Default> {
    data: Vec<T>,
}

impl<T: Default> CircularBuffer<T> {
    /// Creates a new circular buffer which can hold `len` values.
    pub(crate) fn new(len: usize) -> Self {
        Self {
            data: (0..len).map(|_| T::default()).collect::<Vec<_>>(),
        }
    }

    /// Takes an element from the buffer, replacing it with its default value.
    pub(crate) fn take(&mut self, index: usize) -> T {
        let idx = index % self.data.len();
        core::mem::take(&mut self.data[idx])
    }

    /// Replaces an element in the buffer with a new value and
    /// return a reference to the new value in the buffer.
    pub(crate) fn replace(&mut self, index: usize, data: T) -> &T {
        let idx = index % self.data.len();
        self.data[idx] = data;
        &self.data[idx]
    }
}

impl<T: Default> core::ops::Index<usize> for CircularBuffer<T> {
    type Output = T;

    #[inline]
    fn index(&self, node_id: usize) -> &Self::Output {
        let idx = node_id % self.data.len();
        &self.data[idx]
    }
}

impl<T: Default> core::ops::IndexMut<usize> for CircularBuffer<T> {
    #[inline]
    fn index_mut(&mut self, node_id: usize) -> &mut Self::Output {
        let idx = node_id % self.data.len();
        &mut self.data[idx]
    }
}

impl<T: Default> core::ops::Index<isize> for CircularBuffer<T> {
    type Output = T;

    #[inline]
    fn index(&self, node_id: isize) -> &Self::Output {
        let idx = node_id.rem_euclid(self.data.len() as isize) as usize;
        &self.data[idx]
    }
}

impl<T: Default> core::ops::IndexMut<isize> for CircularBuffer<T> {
    #[inline]
    fn index_mut(&mut self, node_id: isize) -> &mut Self::Output {
        let idx = node_id.rem_euclid(self.data.len() as isize) as usize;
        &mut self.data[idx]
    }
}
