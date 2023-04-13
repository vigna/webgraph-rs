//! # Elias zeta
//! Optimal for Zipf of exponent 2
//! Elias’ γ universal coding of x ∈ N+ is obtained by representing x in binary
//! preceded by a unary representation of its length (minus one).
//! More precisely, to represent x we write in unary floor(log(x)) and then in
//! binary x - 2^ceil(log(x)) (on floor(log(x)) bits)
//! 

use anyhow::Result;

use super::{
    BitOrder, M2L, L2M, 
    BitRead, BitWrite, zeta_tables,
    macros::impl_table_call,
};
use crate::utils::fast_log2_floor;

/// Returns how long the zeta code for `value` will be
/// 
/// `USE_TABLE` enables or disables the use of pre-computed tables
/// for decoding
#[must_use]
#[inline]
pub fn len_zeta<const USE_TABLE: bool>(mut value: u64) -> usize {
    if USE_TABLE {
        if let Some(idx) = zeta_tables::LEN.get(value as usize) {
            return *idx as usize;
        }
    }
    value += 1;
    let number_of_blocks_to_write = fast_log2_floor(value);
    2 * number_of_blocks_to_write as usize + 1
}

/// 
pub trait ZetaRead<BO: BitOrder>: BitRead<BO> {
    /// 
    fn read_zeta<const USE_TABLE: bool>(&mut self, k: u64) -> Result<u64> {
        // check if the value is in one of the tables
        if USE_TABLE {
            if K == 3 {
                if let Ok(idx) = self.peek_bits(zeta_tables::READ_BITS) {
                    let (value, len) = zeta_tables::READ_
                }
            }
        }
    }
}

#[inline(always)]
fn default_read_zeta<BO: BitOrder, B: BitRead<BO>>(backend: &mut B) -> Result<u64> {
    let len = backend.read_unary::<false>()?;
    debug_assert!(len <= u8::MAX as _);
    Ok(backend.read_bits(len as u8)? + (1 << len) - 1)
}

/// 
pub trait ZetaWrite<BO: BitOrder>: BitWrite<BO> {
    /// 
    fn write_zeta<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()>;
}