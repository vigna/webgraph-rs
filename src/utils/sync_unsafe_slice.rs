use std::cell::UnsafeCell;

/// Synchronized, unsafe slice that allows multiple mutable references to its
/// elements in different threads.
///
/// # Safety
///
/// If an element of the slice is [accessed
/// exclusively](SyncUnsafeSlice::get_mut), this can happen exactly once thread.
/// Otherwise, multiple thread can have [shared access](SyncUnsafeSlice::get) to
/// the element.
///
/// # Undefined Behavior
///
/// It is undefined behavior to have more than one reference to the same element
/// if at least one of them is mutable.
pub struct SyncUnsafeSlice<'a, T>(&'a [UnsafeCell<T>]);
unsafe impl<'a, T: Send> Sync for SyncUnsafeSlice<'a, T> {}

impl<'a, T> SyncUnsafeSlice<'a, T> {
    #![allow(clippy::mut_from_ref)]

    pub fn new(slice: &'a mut [T]) -> Self {
        #[allow(trivial_casts)]
        let ptr = slice as *mut [T] as *const [UnsafeCell<T>];
        Self(unsafe { &*ptr })
    }

    /// Returns a mutable reference to an element.
    ///
    /// # Safety
    ///
    /// The index must be smaller than the length of the slice. No other access
    /// method can be called for the same index.
    ///
    /// # Undefined Behavior
    ///
    /// It is undefined behavior if this method is called for a given index and
    /// any other access method is called for the same index.
    #[inline(always)]
    pub unsafe fn get_mut_unchecked(&self, index: usize) -> &mut T {
        &mut *self.0.get_unchecked(index).get()
    }

    /// Returns a mutable reference to an element, checking bounds.
    ///
    /// # Safety
    ///
    /// No other access method can be called for the same index.
    ///
    /// # Panics
    ///
    /// Panics of the index is not within bounds.
    ///
    /// # Undefined Behavior
    ///
    /// It is undefined behavior if this method is called for a given index and
    /// any other access method is called for the same index.
    #[inline(always)]
    pub unsafe fn get_mut(&self, index: usize) -> &mut T {
        &mut *self.0[index].get()
    }

    /// Returns a reference to an element.
    ///
    /// # Safety
    ///
    /// The index must be smaller than the length of the slice. No other method
    /// returning a mutable reference can be called for the same index.
    ///
    /// # Undefined Behavior
    ///
    /// It is undefined behavior if this method is called for a given index and
    /// any other method returning a mutable reference is called for the same
    /// index.
    #[inline(always)]
    pub unsafe fn get_unchecked(&self, index: usize) -> &T {
        &*(self.0.get_unchecked(index).get() as *const T)
    }

    /// Returns a reference to an element, checking bounds.
    ///
    /// # Safety
    ///
    /// The index must be smaller than the length of the slice. No other method
    /// returning a mutable reference can be called for the same index.
    ///
    /// # Panics
    ///
    /// Panics of the index is not within bounds.
    ///
    /// # Undefined Behavior
    ///
    /// It is undefined behavior if this method is called for a given index and
    /// any other method returning a mutable reference is called for the same
    /// index.
    #[inline(always)]
    pub unsafe fn get(&self, index: usize) -> &T {
        &*(self.0[index].get() as *const T)
    }
}
