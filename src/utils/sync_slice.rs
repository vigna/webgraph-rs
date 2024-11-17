/*
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::cell::Cell;

/// Synchronized slice reference that can be passed to multiple threads,
/// permitting, nonetheless, interior mutability of its elements.
///
/// In some cases, multiple threads access a slice in way that cannot cause data
/// races: for example, if each thread accesses a different element of the
/// slice. In such cases, by [turning a mutable reference to the
/// slice](SyncSlice::from_mut) in a [`SyncSlice`], you can pass a reference to
/// the slice to multiple threads, but still modify its elements using interior
/// mutability. Note that for this to work the elements of the slice must be
/// [`Send`].
///
/// As in the case of [`Cell`]:
///  - For types that implement [`Copy`], the [`get`](Cell::get) method
///    retrieves the current value of an element of the slice by duplicating it.
///  - For types that implement [`Default`], the [`take`](Cell::take) method
///    replaces the current value of an element of the slice with
///    [`Default::default()`] and returns the replaced value.
///  - All types have:
///    - [`replace`](Cell::replace): replaces the current value of an element of
///      the slice and returns the replaced value.
///    - [`set`](Cell::set): replaces the value of an element of the slice,
///      dropping the replaced value.
///
/// All methods have an unsafe variant that does not do bounds checking.
///
/// An [extension trait](SyncSliceExt) provides a [convenient conversion
/// method](SyncSliceExt::as_sync_slice).
///
/// # Undefined Behavior
///
/// Data races will cause undefined behaviour.
pub struct SyncSlice<'a, T>(&'a [Cell<T>]);
unsafe impl<'a, T: Send> Sync for SyncSlice<'a, T> {}

impl<'a, T> SyncSlice<'a, T> {
    /// Creates a new synchronized slice from a mutable reference.
    #[inline(always)]
    pub fn from_mut(slice: &'a mut [T]) -> Self {
        Self(Cell::from_mut(slice).as_slice_of_cells())
    }
}

impl<'a, T: Copy> SyncSlice<'a, T> {
    /// Returns an element of the slice, without doing bounds checking.
    ///
    /// # Safety
    ///
    /// The index must be smaller than the length of the slice.
    #[inline(always)]
    pub unsafe fn get_unchecked(&self, index: usize) -> T {
        self.0.get_unchecked(index).get()
    }

    /// Returns an element of the slice.
    ///
    /// # Panics
    ///
    /// Panics if the index is not within bounds.
    #[inline(always)]
    pub fn get(&self, index: usize) -> T {
        self.0[index].get()
    }
}

impl<'a, T: Default> SyncSlice<'a, T> {
    /// Takes the value of an element of the slice, leaving `Default::default()`
    /// in its place, without doing bounds checking.
    ///
    /// # Safety
    ///
    /// The index must be smaller than the length of the slice.
    #[inline(always)]
    pub unsafe fn take_unchecked(&self, index: usize) -> T {
        self.0.get_unchecked(index).take()
    }

    /// Takes the value of an element of the slice, leaving `Default::default()`
    /// in its place.
    ///
    /// # Panics
    ///
    /// Panics if the index is not within bounds.
    #[inline(always)]
    pub fn take(&self, index: usize) -> T {
        self.0[index].take()
    }
}

impl<'a, T> SyncSlice<'a, T> {
    /// Sets an element of the slice to `value`, without doing bounds
    /// checking.
    ///
    /// # Safety
    ///
    /// The index must be smaller than the length of the slice.
    #[inline(always)]
    pub unsafe fn set_unchecked(&self, index: usize, value: T) {
        self.0.get_unchecked(index).set(value)
    }

    /// Sets an element of the slice to `value`.
    ///
    /// # Panics
    ///
    /// Panics if the index is not within bounds.
    #[inline(always)]
    pub fn set(&self, index: usize, value: T) {
        self.0[index].set(value)
    }

    /// Replaces the contained value with `value`, and returns the old contained
    /// value, without doing bounds checking.
    ///
    /// # Safety
    ///
    /// The index must be smaller than the length of the slice.
    #[inline(always)]
    pub unsafe fn replace_unchecked(&self, index: usize, value: T) -> T {
        self.0.get_unchecked(index).replace(value)
    }

    /// Replaces the contained value with `value`, and returns the old contained
    /// value.
    ///
    /// # Panics
    ///
    /// Panics if the index is not within bounds.
    #[inline(always)]
    pub fn replace(&self, index: usize, value: T) -> T {
        self.0[index].replace(value)
    }

    /// Returns the length of the slice.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns [`true`] if the slice has a length of 0.`
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Extension trait providing a [synchronized view](SyncSlice) of a slice via
/// the [`as_sync_slice`](SyncSliceExt::as_sync_slice) method.
pub trait SyncSliceExt<'a, T: Copy> {
    fn as_sync_slice(&'a mut self) -> SyncSlice<'a, T>;
}

impl<'a, T: Copy> SyncSliceExt<'a, T> for [T] {
    fn as_sync_slice(&'a mut self) -> SyncSlice<'a, T> {
        SyncSlice::from_mut(self)
    }
}
