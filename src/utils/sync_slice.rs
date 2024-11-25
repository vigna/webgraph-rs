/*
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::cell::Cell;

/// A mutable memory location that is [`Sync`].
///
/// # Memory layout
///
/// `SyncCell<T>` has the same memory layout and caveats as [`Cell<T>`], but it
/// is [`Sync`] if `T` is. In particular, if [`Cell<T>`] has the same in-memory
/// representation as its inner type `T`, then `SyncCell<T>` has the same
///  in-memory representation as its inner type `T` (but the code does not rely
/// on this). `SyncCell<T>` is also [`Send`] if [`Cell<T>`] is [`Send`].
///
/// `SyncCell<T>` is useful when you need to share a mutable memory location
/// across threads, and you rely on the fact that the intended behavior will not
/// cause data races. For example, the content will be written once and then
/// read many times, in this order.
///
/// The main usage of `SyncCell<T>` is to be to able to write to different
/// locations of a slice in parallel, leaving the control of data races to the
/// user, without the access cost of an atomic variable. For this purpose,
/// `SyncCell` implements the [`as_slice_of_cells`](SyncCell::as_slice_of_cells)
/// method, which turns a reference to `SyncCell<[T]>` into a reference to
/// `[SyncCell<T>]`, similarly to the [analogous method of
/// `Cell`](Cell::as_slice_of_cells).
///
/// Since this is the most common usage, the extension trait [`SyncSlice`] adds
/// to slices a method [`as_sync_slice`](SyncSlice::as_sync_slice) that turns a
/// mutable reference to a slice of `T` into a reference to a slice of
/// `SyncCell<T>`.
///
/// # Methods
///
/// `SyncCell<T>` painstakingly reimplements the methods of `Cell<T>` as unsafe,
/// since they rely on external synchronization mechanisms to avoid undefined
/// behavior.
///
/// `SyncCell` implements a few traits implemented by [`Cell`] by delegation for
/// convenience, but some, as [`Clone`] or [`PartialOrd`], cannot be implemented
/// because they would use unsafe methods.
///
/// # Safety
///
/// Multiple thread can read from and write to the same `SyncCell` at the same
/// time. It is responsibility of the user to ensure that there are no data
/// races, which would cause undefined behavior.
///
/// # Examples
///
/// In this example, you can see that `SyncCell` enables mutation across
/// threads:
///
/// ```
/// use webgraph::utils::SyncCell;
/// use webgraph::utils::SyncSlice;
///
/// let mut x = 0;
/// let c = SyncCell::new(x);
///
/// let mut v = vec![1, 2, 3, 4];
/// let s = v.as_sync_slice();
///
/// std::thread::scope(|scope| {
///     scope.spawn(|| {
///         // You can use interior mutability in another thread
///         unsafe { c.set(5) };
///     });
///
///     scope.spawn(|| {
///         // You can use interior mutability in another thread
///         unsafe { s[0].set(5) };
///     });
///     scope.spawn(|| {
///         // You can use interior mutability in another thread
///         // on the same slice
///         unsafe { s[1].set(10) };
///     });
/// });
/// ```
///
/// In this example, we invert a permutation in parallel:
///
/// ```
/// use webgraph::utils::SyncCell;
/// use webgraph::utils::SyncSlice;
///
/// let mut perm = vec![0, 2, 3, 1];
/// let mut inv = vec![0; perm.len()];
/// let inv_sync = inv.as_sync_slice();
///
/// std::thread::scope(|scope| {
///     scope.spawn(|| { // Invert first half
///         for i in 0..2 {
///             unsafe { inv_sync[perm[i]].set(i) };
///         }
///     });
///
///     scope.spawn(|| { // Invert second half
///         for i in 2..perm.len() {
///             unsafe { inv_sync[perm[i]].set(i) };
///        }
///     });
/// });
///
/// assert_eq!(inv, vec![0, 3, 1, 2]);

#[repr(transparent)]
pub struct SyncCell<T: ?Sized>(Cell<T>);

// This is where we depart from Cell.
unsafe impl<T: ?Sized> Send for SyncCell<T> where Cell<T>: Send {}
unsafe impl<T: ?Sized + Sync> Sync for SyncCell<T> {}

impl<T: Default> Default for SyncCell<T> {
    /// Creates a `SyncCell<T>`, with the `Default` value for `T`.
    #[inline]
    fn default() -> SyncCell<T> {
        SyncCell::new(Default::default())
    }
}

impl<T> SyncCell<T> {
    /// Creates a new `SyncCell` containing the given value.
    #[inline]
    pub fn new(value: T) -> Self {
        Self(Cell::new(value))
    }

    /// Sets the contained value by delegation to [`Cell::set`]
    ///
    /// # Safety
    ///
    /// Multiple thread can read from and write to the same `SyncCell` at the
    /// same time. It is responsibility of the user to ensure that there are no
    /// data races, which would cause undefined behavior.
    #[inline]
    pub unsafe fn set(&self, val: T) {
        self.0.set(val);
    }

    /// Swaps the values of two `SyncCell`s by delegation to [`Cell::swap`].
    ///
    /// # Safety
    ///
    /// Multiple thread can read from and write to the same `SyncCell` at the
    /// same time. It is responsibility of the user to ensure that there are no
    /// data races, which would cause undefined behavior.
    #[inline]
    pub unsafe fn swap(&self, other: &SyncCell<T>) {
        self.0.swap(&other.0);
    }

    /// Replaces the contained value with `val`, and returns the old contained
    /// value by delegation to [`Cell::replace`].
    ///
    /// # Safety
    ///
    /// Multiple thread can read from and write to the same `SyncCell` at the
    /// same time. It is responsibility of the user to ensure that there are no
    /// data races, which would cause undefined behavior.
    #[inline]
    pub unsafe fn replace(&self, val: T) -> T {
        self.0.replace(val)
    }

    /// Unwraps the value, consuming the cell.
    #[inline]
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }
}

impl<T: Copy> SyncCell<T> {
    /// Returns a copy of the contained value by delegation to [`Cell::get`].
    ///
    /// # Safety
    ///
    /// Multiple thread can read from and write to the same `SyncCell` at the
    /// same time. It is responsibility of the user to ensure that there are no
    /// data races, which would cause undefined behavior.
    #[inline]
    pub unsafe fn get(&self) -> T {
        self.0.get()
    }
}

impl<T: ?Sized> SyncCell<T> {
    /// Returns a raw pointer to the underlying data in this cell
    /// by delegation to [`Cell::as_ptr`].
    ///
    /// # Safety
    ///
    /// Multiple thread can read from and write to the same `SyncCell` at the
    /// same time. It is responsibility of the user to ensure that there are no
    /// data races, which would cause undefined behavior.
    #[inline]
    pub const unsafe fn as_ptr(&self) -> *mut T {
        self.0.as_ptr()
    }

    /// Returns a mutable reference to the underlying data by delegation to
    /// [`Cell::get_mut`].
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    /// Returns a `&SyncCell<T>` from a `&mut T`.
    #[allow(trivial_casts)]
    #[inline]
    pub fn from_mut(value: &mut T) -> &Self {
        // SAFETY: `SyncCell<T>` has the same memory layout as `Cell<T>`.
        unsafe { &*(Cell::from_mut(value) as *const Cell<T> as *const Self) }
    }
}

impl<T: Default> SyncCell<T> {
    /// Takes the value of the cell, leaving [`Default::default`] in its place.
    ///
    /// # Safety
    ///
    /// Multiple thread can read from and write to the same `SyncCell` at the
    /// same time. It is responsibility of the user to ensure that there are no
    /// data races, which would cause undefined behavior.
    #[inline]
    pub unsafe fn take(&self) -> T {
        self.0.replace(Default::default())
    }
}

#[allow(trivial_casts)]
impl<T> SyncCell<[T]> {
    /// Returns a `&[SyncCell<T>]` from a `&SyncCell<[T]>`
    #[inline]
    pub fn as_slice_of_cells(&self) -> &[SyncCell<T>] {
        let slice_of_cells = self.0.as_slice_of_cells();
        // SAFETY: `SyncCell<T>` has the same memory layout as `Cell<T>`
        unsafe { &*(slice_of_cells as *const [Cell<T>] as *const [SyncCell<T>]) }
    }
}

/// Extension trait turning a mutable reference to a slice of `T` into a
/// reference to a slice of `SyncCell<T>`.
///
/// The resulting slice is `Sync` if `T` is `Sync`.
pub trait SyncSlice<T> {
    /// Returns a `&[SyncCell<T>]` from a `&mut [T]`.
    ///
    /// # Safety
    ///
    /// Multiple thread can read from and write to the returned slice at the
    /// same time. It is responsibility of the user to ensure that there are no
    /// data races, which would cause undefined behavior.
    fn as_sync_slice(&mut self) -> &[SyncCell<T>];
}

impl<T> SyncSlice<T> for [T] {
    fn as_sync_slice(&mut self) -> &[SyncCell<T>] {
        SyncCell::from_mut(self).as_slice_of_cells()
    }
}
