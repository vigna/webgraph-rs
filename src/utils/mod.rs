//! Collection of common functions we use throughout the codebase

/// Return the lowest `n_bits` of `value`.
/// Calling with `n_bits == 0` or `n_bits > 64` will result in undefined 
/// behaviour.
/// 
/// ### Example
/// ```
/// use webgraph::utils::get_lowest_bits;
/// 
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101, 1), 0b1);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101, 2), 0b01);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101, 3), 0b101);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101, 4), 0b1101);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101, 5), 0b0_1101);
/// assert_eq!(get_lowest_bits(0b1011_0110_1010_1101, 6), 0b10_1101);
/// 
/// assert_eq!(get_lowest_bits(u64::MAX, 64), u64::MAX);
/// ```
#[inline(always)] 
#[must_use]
pub fn get_lowest_bits(value: u64, n_bits: u8) -> u64 {
    debug_assert_ne!(n_bits, 0);
    debug_assert!(n_bits <= 64);
    value & (u64::MAX >> (64 - n_bits))
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
pub const fn fast_log2_floor(value: u64) -> u64 {
    debug_assert!(value > 0);
    63 - (value | 1).leading_zeros() as u64
}