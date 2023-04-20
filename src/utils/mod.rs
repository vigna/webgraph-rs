//! Collection of common functions we use throughout the codebase
use crate::traits::*;

/// Return the lowest `n_bits` of `value`.
/// Calling with `n_bits == 0` or `n_bits > 64` will result in undefined 
/// behaviour.
/// 
/// ### Example
/// ```
/// use webgraph::utils::get_lowest_bits;
/// 
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101_u64, 1), 0b1);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101_u64, 2), 0b01);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101_u64, 3), 0b101);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101_u64, 4), 0b1101);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101_u64, 5), 0b0_1101);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101_u64, 6), 0b10_1101);
/// 
/// assert_eq!(get_lowest_bits(u64::MAX, 64), u64::MAX);
/// ```
#[inline(always)] 
#[must_use]
pub fn get_lowest_bits<W: Word>(value: W, n_bits: usize) -> W {
    debug_assert!(n_bits <= W::BITS);
    value & (W::MAX >> (W::BITS - n_bits))
}

/// Compute the `floor(log2(value))` exploiting BMI instructions 
/// based on <https://bugzilla.mozilla.org/show_bug.cgi?id=327129>
///
/// On `x86_64` this should compile to:
/// ```asm
/// or      rdi, 1
/// lzcnt   rax, rdi
/// xor     rax, 63
/// ```
/// or
/// ```asm
/// or      rdi, 1
/// bsr     rax, rdi
/// ```
#[inline(always)]
#[must_use]
pub fn fast_log2_floor<W: Word + UpcastableFrom<u8>>(value: W) -> W {
    debug_assert!(value > W::ZERO);
    let a: W = ((W::BITS - 1) as u8).upcast();
    let b: W = ((value | W::ONE).leading_zeros() as u8).upcast();
    a - b
}

/// power of two
#[inline(always)]
#[must_use]
pub fn fast_pow_2<W: Word>(value: W) -> W {
    W::ONE << value
}