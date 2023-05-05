use core::fmt::{Binary, Debug, Display, LowerHex};
use core::ops::*;

/// Trait with all the common operations that we need for a generic word
/// of memory
pub trait Word:
    Sized
    + Send
    + Sync
    + Debug
    + Display
    + LowerHex
    + Binary
    + Default
    + Clone
    + Copy
    + PartialOrd
    + Ord
    + PartialEq
    + Eq
    + Add<Output = Self>
    + AddAssign<Self>
    + BitAnd<Output = Self>
    + BitAndAssign<Self>
    + BitOr<Output = Self>
    + BitOrAssign<Self>
    + BitXor<Output = Self>
    + BitXorAssign<Self>
    + Div<Output = Self>
    + DivAssign<Self>
    + Mul<Output = Self>
    + MulAssign<Self>
    + Not<Output = Self>
    + Rem<Output = Self>
    + RemAssign<Self>
    + Shl<Output = Self>
    + ShlAssign<Self>
    + Shl<usize, Output = Self>
    + ShlAssign<usize>
    + Shr<Output = Self>
    + ShrAssign<Self>
    + Shr<usize, Output = Self>
    + ShrAssign<usize>
    + Sub<Output = Self>
    + SubAssign<Self>
{
    /// Number of bits in the word
    const BITS: usize;
    /// Number of bytes in the word
    const BYTES: usize;
    /// The byte array form of the value = `[u8; Self::BYTES]`
    type BytesForm;
    /// Zero represented by `Self`
    const ZERO: Self;
    /// One represented by `Self`
    const ONE: Self;
    /// Minimum value represented by `Self`
    const MIN: Self;
    /// Maximum value represented by `Self`
    const MAX: Self;

    /// Converts self to big endian from the target’s endianness.
    /// On big endian this is a no-op. On little endian the bytes are swapped.
    fn to_be(self) -> Self;

    /// Converts self to little endian from the target’s endianness.
    /// On little endian this is a no-op. On big endian the bytes are swapped.
    fn to_le(self) -> Self;

    /// Returns the number of leading ones in the binary representation of self.
    fn leading_ones(self) -> usize;

    /// Returns the number of trailing zeros in the binary representation of self.
    fn leading_zeros(self) -> usize;

    /// Returns the number of trailing ones in the binary representation of self.
    fn trailing_ones(self) -> usize;

    /// Returns the number of trailing zeros in the binary representation of self.
    fn trailing_zeros(self) -> usize;

    /// Panic-free bitwise shift-left; yields self << mask(rhs), where mask
    /// removes any high-order bits of rhs that would cause the shift to exceed
    /// the bitwidth of the type.
    /// Note that this is not the same as a rotate-left; the RHS of a wrapping
    /// shift-left is restricted to the range of the type, rather than the bits
    /// shifted out of the LHS being returned to the other end. The primitive
    /// integer types all implement a rotate_left function, which may be what
    /// you want instead.
    fn wrapping_shl(self, rhs: usize) -> Self;
}

macro_rules! impl_word {
    ($($ty:ty),*) => {$(

impl Word for $ty {
    const BITS: usize = <$ty>::BITS as _;
    const BYTES: usize = core::mem::size_of::<$ty>() as _;
    type BytesForm = [u8; core::mem::size_of::<$ty>()];
    const MIN: Self = <$ty>::MIN as _;
    const MAX: Self = <$ty>::MAX as _;
    const ZERO: Self = 0;
    const ONE: Self = 1;

    #[inline(always)]
    fn to_be(self) -> Self{self.to_be()}
    #[inline(always)]
    fn to_le(self) -> Self{self.to_le()}
    #[inline(always)]
    fn leading_ones(self) -> usize {self.leading_ones() as usize}
    #[inline(always)]
    fn leading_zeros(self) -> usize {self.leading_zeros() as usize}
    #[inline(always)]
    fn trailing_ones(self) -> usize {self.trailing_ones() as usize}
    #[inline(always)]
    fn trailing_zeros(self) -> usize{self.trailing_zeros() as usize}
    #[inline(always)]
    fn wrapping_shl(self, exp: usize) -> Self { self.wrapping_shl(exp as _)}
}

    )*};
}

impl_word!(u8, u16, u32, u64, u128, usize);
