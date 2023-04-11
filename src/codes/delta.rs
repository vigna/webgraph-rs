//! # Elias’ δ
//! universal coding of x ∈ N+ is obtained by representing x in binary
//! preceded by a representation of its length in γ.

use anyhow::Result;

use super::{
    BitRead, BitWrite, 
    GammaRead, GammaWrite, len_gamma, delta_tables,
};
use crate::utils::fast_log2_floor;

#[must_use]
#[inline]
/// Returns how long the Delta code for `value` will be
/// 
/// `USE_TABLE` enables or disables the use of pre-computed tables
/// for decoding
pub fn len_delta<const USE_TABLE: bool>(value: u64) -> usize {
    if USE_TABLE {
        if let Some(idx) = delta_tables::LEN.get(value as usize) {
            return *idx as usize;
        }
    }
    let l = fast_log2_floor(value + 1);
    l as usize + len_gamma::<USE_TABLE>(l)
}

/// Trait for objects that can read Delta codes
pub trait DeltaRead: BitRead + GammaRead {
    /// Read a delta code from the stream.
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems reading
    /// bits, as when the stream ended unexpectedly
    #[inline]
    fn read_delta<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        self._default_read_delta()
    }

    #[inline]
    /// Trick to be able to call the default impl by specialized impls
    /// 
    /// # Errors
    /// Forward `read_unary` and `read_bits` errors.
    fn _default_read_delta(&mut self) -> Result<u64> {
        let n_bits = self.read_gamma::<true>()?;
        debug_assert!(n_bits <= 0xff);
        Ok(self.read_bits(n_bits as u8)? + (1 << n_bits) - 1)
    }
}

/// Trait for objects that can write Delta codes
pub trait DeltaWrite: BitWrite + GammaWrite {
    /// Write a value on the stream
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems writing
    /// bits, as when the stream ended unexpectedly
    #[inline]
    fn write_delta<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()> {
        self._default_write_delta(value)
    }

    /// Trick to be able to call the default impl by specialized impls
    /// 
    /// # Errors
    /// Forward `read_unary` and `read_bits` errors.
    #[inline]
    fn _default_write_delta(&mut self, mut value: u64) -> Result<()> {
        value += 1;
        let number_of_blocks_to_write = fast_log2_floor(value);
        debug_assert!(number_of_blocks_to_write <= u8::MAX as _);
        // remove the most significant 1
        let short_value = value - (1 << number_of_blocks_to_write);
        // Write the code
        self.write_gamma::<true>(number_of_blocks_to_write)?;
        self.write_bits(short_value, number_of_blocks_to_write as u8)?;
        Ok(())
    }

}