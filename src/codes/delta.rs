//! # Elias’ δ
//! universal coding of x ∈ N+ is obtained by representing x in binary
//! preceded by a representation of its length in γ.

use anyhow::Result;

use super::{
    BitOrder, M2L, L2M,
    GammaRead, GammaWrite, len_gamma, 
    delta_tables, macros::impl_table_call,
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
pub trait DeltaRead<BO: BitOrder>: GammaRead<BO> {
    /// Read a delta code from the stream.
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems reading
    /// bits, as when the stream ended unexpectedly
    fn read_delta<const USE_TABLE: bool>(&mut self) -> Result<u64>;
}

impl<B: GammaRead<M2L>> DeltaRead<M2L> for B {
    fn read_delta<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        impl_table_call!(self, USE_TABLE, delta_tables, M2L);
        default_read_delta(self)
    }
}
impl<B: GammaRead<L2M>> DeltaRead<L2M> for B {
    fn read_delta<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        impl_table_call!(self, USE_TABLE, delta_tables, L2M);
        default_read_delta(self)
    }
}

#[inline(always)]
/// Default impl, so specialized impls can call it
/// 
/// # Errors
/// Forward `read_unary` and `read_bits` errors.
fn default_read_delta<BO: BitOrder, B: GammaRead<BO>>(
    backend: &mut B
) -> Result<u64> {
    let n_bits = backend.read_gamma::<true>()?;
    debug_assert!(n_bits <= 0xff);
    Ok(backend.read_bits(n_bits as u8)? + (1 << n_bits) - 1)
}

/// Trait for objects that can write Delta codes
pub trait DeltaWrite<BO: BitOrder>: GammaWrite<BO> {
    /// Write a value on the stream
    /// 
    /// `USE_TABLE` enables or disables the use of pre-computed tables
    /// for decoding
    /// 
    /// # Errors
    /// This function fails only if the BitRead backend has problems writing
    /// bits, as when the stream ended unexpectedly
    fn write_delta<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()>;
}

impl<B: GammaWrite<M2L>> DeltaWrite<M2L> for B {
    #[inline]
    fn write_delta<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()> {
        if let Some((bits, n_bits)) = delta_tables::WRITE_M2L.get(value as usize) {
            return self.write_bits(*bits as u64, *n_bits);
        }
        default_write_delta(self, value)
    }
}
impl<B: GammaWrite<L2M>> DeltaWrite<L2M> for B {
    #[inline]
    fn write_delta<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()> {
        if let Some((bits, n_bits)) = delta_tables::WRITE_L2M.get(value as usize) {
            return self.write_bits(*bits as u64, *n_bits);
        }
        default_write_delta(self, value)
    }
}

/// Default impl, so specialized impls can call it
/// 
/// # Errors
/// Forward `write_unary` and `write_bits` errors.
#[inline(always)]
fn default_write_delta<BO: BitOrder, B: GammaWrite<BO>>(
    backend: &mut B, mut value: u64,
) -> Result<()> {
    value += 1;
    let number_of_blocks_to_write = fast_log2_floor(value);
    debug_assert!(number_of_blocks_to_write <= u8::MAX as _);
    // remove the most significant 1
    let short_value = value - (1 << number_of_blocks_to_write);
    // Write the code
    backend.write_gamma::<true>(number_of_blocks_to_write)?;
    backend.write_bits(short_value, number_of_blocks_to_write as u8)?;
    Ok(())
}