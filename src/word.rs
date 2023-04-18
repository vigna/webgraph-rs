
use core::fmt::{Debug, Display, LowerHex, Binary};
use core::ops::*;

/// Trait with all the common operations that we need for a generic word
/// of memory
pub trait Word : Sized + Send + Sync +
    Debug + Display + LowerHex + Binary +
    Default + Clone + Copy +
    PartialOrd + Ord + 
    PartialEq + Eq + 
    Add<Output=Self> + AddAssign<Self> +
    BitAnd<Output=Self> + BitAndAssign<Self> +
    BitOr<Output=Self> + BitOrAssign<Self> +
    BitXor<Output=Self> + BitXorAssign<Self> +
    Div<Output=Self> + DivAssign<Self> +
    Mul<Output=Self> + MulAssign<Self> + 
    Not<Output=Self> + 
    Rem<Output=Self> + RemAssign<Self> +
    Shl<Output=Self> + ShlAssign<Self> +
    Shr<Output=Self> + ShrAssign<Self> +
    Sub<Output=Self> + SubAssign<Self> + 
    CastableInto<usize> + CastableInto<u8> + CastableInto<u16> + CastableInto<u32> + 
    CastableInto<u64> + CastableInto<u128> + 
    CastableFrom<usize> + CastableFrom<u8> + CastableFrom<u16> + CastableFrom<u32> + 
    CastableFrom<u64> + CastableFrom<u128> + 
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
}

    )*};
}

impl_word!(u8, u16, u32, u64, u128, usize);

/// Cast between primitive types for Words of memory
pub trait CastableInto<W>: Sized {
    /// Cast between types, the inteded behaviour when downcasting is truncating
    /// and when upcasting is zero extending for unsigned types and sign 
    /// extending for signed types.
    fn cast(self) -> W;
}

///
pub trait CastableFrom<W>: Sized {
    ///
    fn cast_from(value: W) -> Self;
}

impl<T, U> CastableInto<U> for T 
where 
    U: CastableFrom<T>
{
    fn cast(self) -> U {
        U::cast_from(self)
    }
}

macro_rules! impl_cast {
    ($($ty:ty),*) => {$(
        impl_cast_inner!($ty => u8, u16, u32, u64, u128, usize);
    )*};
}
macro_rules! impl_cast_inner {
    ($base:ty => $($ty:ty),*) => {$(
impl CastableFrom<$ty> for $base {
    fn cast_from(value: $ty) -> Self {
        value as $base
    }
}
    )*};
}

impl_cast!(u8, u16, u32, u64, u128, usize);
