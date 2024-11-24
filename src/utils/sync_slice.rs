/*
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::{cell::Cell, cmp::Ordering, ops::Deref};

/// A mutable memory location that is [`Sync`].
///
/// # Memory layout
///
/// `SyncCell<T>` has the same memory layout and caveats as [`Cell<T>`], but it
/// is [`Sync`] if its content is. In particular, if [`Cell<T>`] has the same
/// in-memory representation as its inner type `T`, then `SyncCell<T>` has the
///  same in-memory representation as its inner type `T` (but the code does not
/// rely on this).
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
/// to slices an unsafe method [`as_sync_slice`](SyncSlice::as_sync_slice) that
/// turns a mutable reference to a slice of `T` into a reference to a slice of
/// `SyncCell<T>`.
///
/// # Methods
///
/// Most methods of `SyncCell<T>` come from [`Deref`] to [`Cell<T>`]. Some have
/// been reimplemented: in particular, [`new`](SyncCell::new) and
/// [`from_mut`](SyncCell::from_mut) are unsafe, since they force [`Sync`]. Both
/// [`swap`](SyncCell::swap) and [`into_inner`](SyncCell::into_inner) are
/// reimplemented as the [`Deref`] version would not work.
///
/// `SyncCell` implements almost all traits implemented by [`Cell`] by
/// delegation for convenience, but some, as [`Default`], cannot be implemented
/// because they would use unsafe methods.
///
/// # Examples
///
/// In this example, you can see that `SyncCell<T>` enables mutation across
/// threads.
///
/// ```
/// use webgraph::utils::SyncCell;
/// use webgraph::utils::SyncSlice;
///
/// let mut x = 0;
/// let c = unsafe { SyncCell::new(x) };
///
/// let mut v = vec![1, 2, 3, 4];
/// let s = unsafe { v.as_sync_slice() };
///
/// std::thread::scope(|scope| {
///     scope.spawn(|| {
///         // You can use interior mutability in another thread
///         c.set(5);
///     });
///
///     scope.spawn(|| {
///         // You can use interior mutability in another thread
///         s[0].set(5);
///     });
///     scope.spawn(|| {
///         // You can use interior mutability in another thread
///         // on the same slice
///         s[1].set(10);
///     });
/// });
/// ```

#[repr(transparent)]
pub struct SyncCell<T: ?Sized>(Cell<T>);
unsafe impl<T: Sync> Sync for SyncCell<T> {}

impl<T> SyncCell<T> {
    /// Creates a new `Cell` containing the given value.
    #[inline(always)]
    pub unsafe fn new(value: T) -> Self {
        Self(Cell::new(value))
    }

    /// Unwraps the value, consuming the cell.
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }

    /// Swaps the values of two `SyncCell`s by delegation to [`Cell::swap`].
    pub fn swap(&self, other: &SyncCell<T>) {
        self.0.swap(other);
    }
}

impl<T: ?Sized> SyncCell<T> {
    /// Returns a `&SyncCell<T>` from a `&mut T`
    #[allow(trivial_casts)]
    #[inline(always)]
    pub unsafe fn from_mut(value: &mut T) -> &Self {
        // SAFETY: `SyncCell<T>` has the same memory layout as `Cell<T>`.
        &*(Cell::from_mut(value) as *const Cell<T> as *const Self)
    }
}

#[allow(trivial_casts)]
impl<T> SyncCell<[T]> {
    /// Returns a `&[SyncCell<T>]` from a `&SyncCell<[T]>`
    #[inline(always)]
    pub fn as_slice_of_cells(&self) -> &[SyncCell<T>] {
        let slice_of_cells = Deref::deref(self).as_slice_of_cells();
        // SAFETY: `SyncCell<T>` has the same memory layout as `Cell<T>`
        unsafe { &*(slice_of_cells as *const [Cell<T>] as *const [SyncCell<T>]) }
    }
}

impl<T: ?Sized> std::ops::Deref for SyncCell<T> {
    type Target = Cell<T>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Copy> Clone for SyncCell<T> {
    #[inline]
    fn clone(&self) -> SyncCell<T> {
        unsafe { SyncCell::new(self.get()) }
    }
}

impl<T: PartialEq + Copy> PartialEq for SyncCell<T> {
    #[inline]
    fn eq(&self, other: &SyncCell<T>) -> bool {
        self.get() == other.get()
    }
}

impl<T: Eq + Copy> Eq for SyncCell<T> {}

impl<T: PartialOrd + Copy> PartialOrd for SyncCell<T> {
    #[inline]
    fn partial_cmp(&self, other: &SyncCell<T>) -> Option<Ordering> {
        self.get().partial_cmp(&other.get())
    }

    #[inline]
    fn lt(&self, other: &SyncCell<T>) -> bool {
        self.get() < other.get()
    }

    #[inline]
    fn le(&self, other: &SyncCell<T>) -> bool {
        self.get() <= other.get()
    }

    #[inline]
    fn gt(&self, other: &SyncCell<T>) -> bool {
        self.get() > other.get()
    }

    #[inline]
    fn ge(&self, other: &SyncCell<T>) -> bool {
        self.get() >= other.get()
    }
}

impl<T: Ord + Copy> Ord for SyncCell<T> {
    #[inline]
    fn cmp(&self, other: &SyncCell<T>) -> Ordering {
        self.get().cmp(&other.get())
    }
}

/// Extension trait turning (unsafely) a slice of `T` into a slice of
/// `SyncCell<T>`.
pub trait SyncSlice<T> {
    /// Returns a view of the
    unsafe fn as_sync_slice(&mut self) -> &[SyncCell<T>];
}

impl<'a, T> SyncSlice<T> for [T] {
    unsafe fn as_sync_slice(&mut self) -> &[SyncCell<T>] {
        SyncCell::from_mut(self).as_slice_of_cells()
    }
}
