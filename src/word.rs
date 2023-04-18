
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
    Shl<usize, Output=Self> + ShlAssign<usize> +
    Shr<Output=Self> + ShrAssign<Self> +
    Shr<usize, Output=Self> + ShrAssign<usize> +
    Sub<Output=Self> + SubAssign<Self> + 
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

///
pub trait DowncastableInto<W>: Sized {
    ///
    fn downcast(self) -> W;
}

///
pub trait UpcastableInto<W>: Sized {
    ///
    fn upcast(self) -> W;
}

///
pub trait DowncastableFrom<W>: Sized {
    ///
    fn downcast_from(value: W) -> Self;
}

///
pub trait UpcastableFrom<W>: Sized {
    ///
    fn upcast_from(value: W) -> Self;
}

/// UpcastableFrom implies UpcastableInto
impl<T, U> UpcastableInto<U> for T 
where 
    U: UpcastableFrom<T>
{
    #[inline(always)]
    fn upcast(self) -> U {
        U::upcast_from(self)
    }
}

/// DowncastableFrom implies DowncastableInto
impl<T, U> DowncastableInto<U> for T 
where 
    U: DowncastableFrom<T>
{
    #[inline(always)]
    fn downcast(self) -> U {
        U::downcast_from(self)
    }
}

/// Riflexivity
impl<T> DowncastableFrom<T> for T {
    #[inline(always)]
    fn downcast_from(value: T) -> Self {
        value
    }
}

/// Riflexivity
impl<T> UpcastableFrom<T> for T {
    #[inline(always)]
    fn upcast_from(value: T) -> Self {
        value
    }
}

macro_rules! impl_upcasts {
    ($base_type:ty, $($ty:ty,)*) => {$(
impl UpcastableFrom<$base_type> for $ty {
    fn upcast_from(value: $base_type) -> Self {
        value as $ty
    }
}
    )*
    impl_upcasts!($($ty,)*);
};
    () => {};
}

impl_upcasts!(u8, u16, u32, u64, u128,);

impl UpcastableFrom<u8> for usize {
    fn upcast_from(value: u8) -> Self {
        value as usize
    }
}
impl UpcastableFrom<u16> for usize {
    fn upcast_from(value: u16) -> Self {
        value as usize
    }
}
impl UpcastableFrom<u32> for usize {
    fn upcast_from(value: u32) -> Self {
        value as usize
    }
}
impl UpcastableFrom<usize> for u64 {
    fn upcast_from(value: usize) -> Self {
        value as u64
    }
}
impl UpcastableFrom<usize> for u128 {
    fn upcast_from(value: usize) -> Self {
        value as u128
    }
}

macro_rules! impl_downcasts {
    ($base_type:ty, $($ty:ty,)*) => {$(
impl DowncastableFrom<$base_type> for $ty {
    fn downcast_from(value: $base_type) -> Self {
        value as $ty
    }
}
    )*
    impl_downcasts!($($ty,)*);
};
    () => {};
}

impl_downcasts!(u128, u64, u32, u16, u8,);

impl DowncastableFrom<usize> for u8 {
    fn downcast_from(value: usize) -> Self {
        value as u8
    }
}
impl DowncastableFrom<usize> for u16 {
    fn downcast_from(value: usize) -> Self {
        value as u16
    }
}
impl DowncastableFrom<usize> for u32 {
    fn downcast_from(value: usize) -> Self {
        value as u32
    }
}
impl DowncastableFrom<u64> for usize {
    fn downcast_from(value: u64) -> Self {
        value as usize
    }
}
impl DowncastableFrom<u128> for usize {
    fn downcast_from(value: u128) -> Self {
        value as usize
    }
}

///
pub trait CastableInto<W>: Sized {
    ///
    fn cast(self) -> W;
}

///
pub trait CastableFrom<W>: Sized {
    ///
    fn cast_from(value: W) -> Self;
}

/// Riflexivity
impl<T> CastableFrom<T> for T {
    #[inline(always)]
    fn cast_from(value: T) -> Self {
        value
    }
}

/// UpcastableFrom implies UpcastableInto
impl<T, U> CastableInto<U> for T 
where 
    U: CastableFrom<T>
{
    #[inline(always)]
    fn cast(self) -> U {
        U::cast_from(self)
    }
}

macro_rules! impl_casts {
    ($base_type:ty, $($ty:ty,)*) => {$(
impl CastableFrom<$base_type> for $ty {
    fn cast_from(value: $base_type) -> Self {
        value as $ty
    }
}
    )*
    impl_casts!($($ty,)*);
};
    () => {};
}

impl_casts!(u8, u16, u32, u64, u128, usize,);