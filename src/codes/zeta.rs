//! # Zeta
//! 

use anyhow::Result;

use super::{
    BitOrder, M2L, L2M, 
    BitRead, BitWrite, zeta_tables,
    MinimalBinaryRead, MinimalBinaryWrite,
    len_unary, len_minimal_binary,
};
use crate::utils::{fast_log2_floor, fast_pow_2};

/// Returns how long the zeta code for `value` will be
/// 
/// `USE_TABLE` enables or disables the use of pre-computed tables
/// for decoding
#[must_use]
#[inline]
pub fn len_zeta<const USE_TABLE: bool>(mut value: u64, k: u64) -> usize {
    if USE_TABLE && k == zeta_tables::K {
        if let Some(idx) = zeta_tables::LEN.get(value as usize) {
            return *idx as usize;
        }
    }
    value += 1;
    let h = fast_log2_floor(value) / k;
    let u = fast_pow_2((h + 1) * k);
    let l = fast_pow_2(h * k);
    len_unary::<false>(h) + len_minimal_binary(value - l, u - l)
}

/// 
pub trait ZetaRead<BO: BitOrder>: MinimalBinaryRead<BO> {
    /// 
    fn read_zeta<const USE_TABLE: bool>(&mut self, k: u64) -> Result<u64>;
}

impl<B: BitRead<M2L>> ZetaRead<M2L> for B {
    #[inline]
    fn read_zeta<const USE_TABLE: bool>(&mut self, k: u64) -> Result<u64> {
        if USE_TABLE && k == zeta_tables::K {
            if let Ok(idx) = self.peek_bits(zeta_tables::READ_BITS) {
                let (value, len) = zeta_tables::READ_M2L[idx as usize];
                if len != zeta_tables::MISSING_VALUE_LEN {
                    self.skip_bits(len as u8)?;
                    return Ok(value as u64);
                }
            }
        }
        default_read_zeta(self, k)
    }
}
impl<B: BitRead<L2M>> ZetaRead<L2M> for B {
    #[inline]
    fn read_zeta<const USE_TABLE: bool>(&mut self, k: u64) -> Result<u64> {
        if USE_TABLE && k == zeta_tables::K {
            if let Ok(idx) = self.peek_bits(zeta_tables::READ_BITS) {
                let (value, len) = zeta_tables::READ_L2M[idx as usize];
                if len != zeta_tables::MISSING_VALUE_LEN {
                    self.skip_bits(len as u8)?;
                    return Ok(value as u64);
                }
            }
        }
        default_read_zeta(self, k)
    }
}

#[inline(always)]
fn default_read_zeta<BO: BitOrder, B: BitRead<BO>>(backend: &mut B, k: u64) -> Result<u64> {
    // implementation taken from github.com/vigna/dsiutils @ InputBitStram.java
    let h = backend.read_unary::<false>()?;
    let u = fast_pow_2((h + 1) * k);
    let l = fast_pow_2(h * k);
    let res = backend.read_minimal_binary(u - l)?;
    Ok(l + res - 1)
}

/// 
pub trait ZetaWrite<BO: BitOrder>: MinimalBinaryWrite<BO> {
    /// 
    fn write_zeta<const USE_TABLE: bool>(&mut self, value: u64, k: u64) -> Result<()>;
}

impl<B: BitWrite<M2L>> ZetaWrite<M2L> for B {
    #[inline]
    fn write_zeta<const USE_TABLE: bool>(&mut self, value: u64, k: u64) -> Result<()> {
        if USE_TABLE && k == zeta_tables::K {
            if let Some((bits, n_bits)) = zeta_tables::WRITE_M2L.get(value as usize) {
                return self.write_bits(*bits as u64, *n_bits);
            }
        }
        default_write_zeta(self, value, k)
    }
}
impl<B: BitWrite<L2M>> ZetaWrite<L2M> for B {
    #[inline]
    fn write_zeta<const USE_TABLE: bool>(&mut self, value: u64, k: u64) -> Result<()> {
        if USE_TABLE && k == zeta_tables::K {
            if let Some((bits, n_bits)) = zeta_tables::WRITE_L2M.get(value as usize) {
                return self.write_bits(*bits as u64, *n_bits);
            }
        }
        default_write_zeta(self, value, k)
    }
}

/// Common part of the M2L and L2M impl
/// 
/// # Errors
/// Forward `read_unary` and `read_bits` errors.
#[inline(always)]
fn default_write_zeta<BO: BitOrder, B: BitWrite<BO>>(
    backend: &mut B, mut value: u64, k: u64,
) -> Result<()> {
    value += 1;
    let h = fast_log2_floor(value) / k;
    let u = fast_pow_2((h + 1) * k);
    let l = fast_pow_2(h * k);

    debug_assert!(l <= value, "{} <= {}", l, value);
    debug_assert!(value < u, "{} < {}", value, u);

    // Write the code
    backend.write_unary::<true>(h)?;
    backend.write_minimal_binary(value - l, u - l)
}
