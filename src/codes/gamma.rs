//! # Elias Gamma
//! Optimal for Zipf of exponent 2
//! Elias’ γ universal coding of x ∈ N+ is obtained by representing x in binary
//! preceded by a unary representation of its length (minus one).
//! More precisely, to represent x we write in unary floor(log(x)) and then in
//! binary x - 2^ceil(log(x)) (on floor(log(x)) bits)
//! 

use anyhow::Result;

use super::{
    BitOrder, M2L, L2M, 
    BitRead, BitWrite, gamma_tables,
    macros::impl_table_call,
};
use crate::utils::fast_log2_floor;

/// Returns how long the gamma code for `value` will be
/// 
/// `USE_TABLE` enables or disables the use of pre-computed tables
/// for decoding
#[must_use]
#[inline]
pub fn len_gamma<const USE_TABLE: bool>(mut value: u64) -> usize {
    if USE_TABLE {
        if let Some(idx) = gamma_tables::LEN.get(value as usize) {
            return *idx as usize;
        }
    }
    value += 1;
    let number_of_blocks_to_write = fast_log2_floor(value);
    2 * number_of_blocks_to_write as usize + 1
}

/// Trait for objects that can read Gamma codes
pub trait GammaRead<BO: BitOrder>: BitRead<BO> {
    /// Read a gamma code from the stream.
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems reading
    /// bits, as when the stream ended unexpectedly
    fn read_gamma<const USE_TABLE: bool>(&mut self) -> Result<u64>;
}

/// Common part of the M2L and L2M impl
/// 
/// # Errors
/// Forward `read_unary` and `read_bits` errors.
#[inline(always)]
fn default_read_gamma<BO: BitOrder, B: BitRead<BO>>(backend: &mut B) -> Result<u64> {
    let len = backend.read_unary::<false>()?;
    debug_assert!(len <= u8::MAX as _);
    Ok(backend.read_bits(len as u8)? + (1 << len) - 1)
}

impl<B: BitRead<M2L>> GammaRead<M2L> for B {
    #[inline]
    fn read_gamma<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        impl_table_call!(self, USE_TABLE, gamma_tables, M2L);
        default_read_gamma(self)
    }
}
impl<B: BitRead<L2M>> GammaRead<L2M> for B {
    #[inline]
    fn read_gamma<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        impl_table_call!(self, USE_TABLE, gamma_tables, L2M);
        default_read_gamma(self)
    }
}

/// Trait for objects that can write Gamma codes
pub trait GammaWrite<BO: BitOrder>: BitWrite<BO> {
    /// Write a value on the stream
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    /// 
    /// # Errors
    /// This function fails only if the BitWrite backend has problems writing
    /// bits, as when the stream ended unexpectedly
    fn write_gamma<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()>;
}

impl<B: BitWrite<M2L>> GammaWrite<M2L> for B {
    #[inline]
    fn write_gamma<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()> {
        if USE_TABLE {
            if let Some((bits, n_bits)) = gamma_tables::WRITE_M2L.get(value as usize) {
                return self.write_bits(*bits as u64, *n_bits);
            }
        }
        default_write_gamma(self, value)
    }
}
impl<B: BitWrite<L2M>> GammaWrite<L2M> for B {
    #[inline]
    fn write_gamma<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()> {
        if USE_TABLE {
            if let Some((bits, n_bits)) = gamma_tables::WRITE_L2M.get(value as usize) {
                return self.write_bits(*bits as u64, *n_bits);
            }
        }
        default_write_gamma(self, value)
    }
}

/// Common part of the M2L and L2M impl
/// 
/// # Errors
/// Forward `read_unary` and `read_bits` errors.
#[inline(always)]
fn default_write_gamma<BO: BitOrder, B: BitWrite<BO>>(
    backend: &mut B, mut value: u64,
) -> Result<()> {
    value += 1;
    let number_of_bits_to_write = fast_log2_floor(value);
    debug_assert!(number_of_bits_to_write <= u8::MAX as _);
    // remove the most significant 1
    let short_value = value - (1 << number_of_bits_to_write);
    // Write the code
    backend.write_unary::<false>(number_of_bits_to_write)?;
    backend.write_bits(short_value, number_of_bits_to_write as u8)?;
    Ok(())
}
