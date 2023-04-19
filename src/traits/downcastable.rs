
/// `DowncastableInto : DowncastableFrom = Into : From`, It's easyer to use to
/// specify bounds on generic variables
pub trait DowncastableInto<W>: Sized {
    /// Call `W::downcast_from(self)`
    fn downcast(self) -> W;
}

/// Trait for primitive integers, the expected behaviour is to **truncate**
/// the bits in the word to the possibly smaller word size.
pub trait DowncastableFrom<W>: Sized {
    /// Truncate the current word to a possibly smaller size
    fn downcast_from(value: W) -> Self;
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
