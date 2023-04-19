

/// `CastableInto : CastableFrom = Into : From`, It's easyer to use to
/// specify bounds on generic variables
pub trait CastableInto<W>: Sized {
    /// Call `W::cast_from(self)`
    fn cast(self) -> W;
}

/// Trait for primitive integers, this is the combination of 
/// [`DowncastableFrom`] and [`UpcastableFrom`]. Prefer using the other two 
/// traits, as casting without knowing which value will be bigger might result
/// in hard to find bugs.
/// 
/// This is equivalent to calling `as` between two types
pub trait CastableFrom<W>: Sized {
    /// Call `Self as W`
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
impl CastableFrom<$ty> for $base_type {
    fn cast_from(value: $ty) -> $base_type {
        value as $base_type
    }
}
    )*
    impl_casts!($($ty,)*);
};
    () => {};
}

impl_casts!(u8, u16, u32, u64, u128, usize,);