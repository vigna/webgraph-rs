
/// `UpcastableInto : UpcastableFrom = Into : From`, It's easyer to use to
/// specify bounds on generic variables
pub trait UpcastableInto<W>: Sized {
    /// Call `W::upcast_from(self)`
    fn upcast(self) -> W;
}

/// Trait for primitive integers, the expected behaviour for unsigned integers
/// is to zero extend the value, while for signed integers it will sign-extend
/// it to the possibly bigger word size.
pub trait UpcastableFrom<W>: Sized {
    /// Extend the current word to a possibly bigger size.
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