//! # Minimal Binary
//!
//! Also called [Truncated binary encoding](https://en.wikipedia.org/wiki/Truncated_binary_encoding)
//! is optimal for uniform distributions. 
//! When the size of the alphabet is a power of two, this is equivalent to
//! the classical binary encoding.

use anyhow::{Result, bail};

use super::{BitOrder, BitRead, BitWrite};
use crate::utils::fast_log2_floor;

/// Returns how long the minimal binary code for `value` will be for a given 
/// `max`
#[must_use]
#[inline]
pub fn len_minimal_binary(value: u64, max: u64) -> usize {
    if max == 0 {
        return 0;
    }
    let l = fast_log2_floor(max);
    let limit = (1 << (l + 1)) - max;
    let mut result = l as usize;
    if value >= limit {
        result += 1;
    }
    result
}

/// Trait for objects that can read Minimal Binary codes
pub trait MinimalBinaryRead<BO: BitOrder>: BitRead<BO> {
    /// Read a minimal binary code from the stream.
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems reading
    /// bits, as when the stream ended unexpectedly
    #[inline]
    fn read_minimal_binary(&mut self, max: u64) -> Result<u64> {
        if max == 0 {
            bail!("The max of a minimal binary value can't be zero.");
        }
        let l = fast_log2_floor(max);
        let mut value = self.read_bits(l as _)?;
        let limit = (1 << (l + 1)) - max;

        Ok(if value < limit {
            value
        } else {
            value <<= 1;
            value |= self.read_bits(1)?;
            value - limit
        })
    }
}

/// Trait for objects that can write Minimal Binary codes
pub trait MinimalBinaryWrite<BO: BitOrder>: BitWrite<BO> {
    /// Write a value on the stream
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems writing
    /// bits, as when the stream ended unexpectedly
    #[inline]
    fn write_minimal_binary(
        &mut self, value: u64, max: u64) -> Result<()> {
        if max == 0 {
            bail!("The max of a minimal binary value can't be zero.");
        }
        let l = fast_log2_floor(max);
        let limit = (1 << (l + 1)) - max;

        if value < limit {
            self.write_bits(value, l as _)
        } else {
            let to_write = value + limit;
            self.write_bits(to_write >> 1, l as _)?;
            self.write_bits(to_write & 1, 1)
        }
    }
}

impl<BO: BitOrder, B: BitRead<BO>> MinimalBinaryRead<BO> for B {}
impl<BO: BitOrder, B: BitWrite<BO>> MinimalBinaryWrite<BO> for B {}