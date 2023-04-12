//! # Minimal Binary
//!
//! Also called [Truncated binary encoding](https://en.wikipedia.org/wiki/Truncated_binary_encoding)
//! is optimal for uniform distributions. 
//! When the size of the alphabet is a power of two, this is equivalent to
//! the classical binary encoding.

use anyhow::Result;

use super::{BitOrder, BitRead, BitWrite};

/// Returns how long the gamma code for `value` will be
/// 
/// `USE_TABLE` enables or disables the use of pre-computed tables
/// for decoding
#[must_use]
#[inline]
pub fn len_minimal_binary<const USE_TABLE: bool>(_value: u64, _max: u64) -> usize {
    todo!();
}

/// Trait for objects that can read Minimal Binary codes
pub trait MinimalBinaryRead<BO: BitOrder>: BitRead<BO> {
    /// Read a Minimal Binary code from the stream.
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems reading
    /// bits, as when the stream ended unexpectedly
    fn read_minimal_binary<const USE_TABLE: bool>(&mut self, max: u64) -> Result<u64>;
}

/// Trait for objects that can write Gamma codes
pub trait MinimalBinaryWrite<BO: BitOrder>: BitWrite<BO> {
    /// Write a value on the stream
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems writing
    /// bits, as when the stream ended unexpectedly
    fn write_minimal_binary<const USE_TABLE: bool>(&mut self, value: u64, max: u64) -> Result<()>;
}