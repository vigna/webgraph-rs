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

/// 
pub trait MinimalBinaryRead<BO: BitOrder>: BitRead<BO> {
    /// 
    #[inline]
    fn read_minimal_binary<const USE_TABLE: bool>(&mut self, max: u64) -> Result<u64> {
        let l = fast_log2_floor(max);
        let mut value = self.read_bits(l)?;
        let limit = (1 << l + 1) - max;

        Ok(if value < limit {
            value
        } else {
            value <<= 1;
            value |= self.read_bits(1)?;
            value - limit
        })
    }
}

/// 
pub trait MinimalBinaryWrite<BO: BitOrder>: BitWrite<BO> {
    /// 
    #[inline]
    fn write_minimal_binary<const USE_TABLE: bool>(
        &mut self, value: u64, max: u64) -> Result<()> {
        let l = fast_log2_floor(max);
        let limit = (1 << l + 1) - max;

        if value < limit {
            self.write_bits(value, l)
        } else {
            let to_write = value + limit;
            self.write_bits(to_write >> 1, l)?;
            self.write_bits(to_write & 1, 1);
        }
    }
}