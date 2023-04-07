//! # Elias Gamma
//! Optimal for Zipf of exponent 2
//! Elias’ γ universal coding of x ∈ N+ is obtained by representing x in binary
//! preceded by a unary representation of its length (minus one).
//! More precisely, to represent x we write in unary floor(log(x)) and then in
//! binary x - 2^ceil(log(x)) (on floor(log(x)) bits)
//! 

use anyhow::Result;

use super::{BitRead, BitWrite};
use crate::utils::fast_log2_floor;

#[must_use]
/// Returns how long the gamma code for `value` will be
/// 
/// `USE_TABLE` enables or disables the use of pre-computed tables
/// for decoding
pub fn len_gamma<const USE_TABLE: bool>(mut value: u64) -> usize {
    value += 1;
    let number_of_blocks_to_write = fast_log2_floor(value);
    2 * number_of_blocks_to_write as usize
}

/// Trait for objects that can read Gamma codes
pub trait GammaRead: BitRead {
    /// Read a gamma code from the stream.
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    #[must_use]
    fn read_gamma<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        let len = self.read_unary::<true>()?;
        debug_assert!(len <= u8::MAX as _);
        Ok(self.read_bits(len as u8)? + (1 << len) - 1)
    }
}

/// Trait for objects that can write Gamma codes
pub trait GammaWrite: BitWrite {
    /// Write a value on the stream
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    fn write_gamma<const USE_TABLE: bool>(&mut self, mut value: u64) -> Result<()> {
        value += 1;
        let number_of_blocks_to_write = fast_log2_floor(value);
        debug_assert!(number_of_blocks_to_write <= u8::MAX as _);
        // remove the most significant 1
        let short_value = value - (1 << number_of_blocks_to_write);
        // Write the code
        self.write_unary::<true>(number_of_blocks_to_write)?;
        self.write_bits(short_value, number_of_blocks_to_write as u8)?;
        Ok(())
    }
}