//! Marker types and trait used to conditionally implement MSB to LSB or LSB to 
//! MSB bit orders in readers and writers.
//! 
//! Note that we use an inner private trait `BitOrderCore` so that an user can
//! use [`BitOrder`] for its generics, but cannot implement it, so all the 
//! types that will ever implement [`BitOrder`] are defined in this file.
//! 
//! Apparently this pattern is a [SealedTrait](https://predr.ag/blog/definitive-guide-to-sealed-traits-in-rust/).

/// Inner private trait used to remove the possibility that anyone could 
/// implement [`BitOrder`] on other structs
#[allow(private_in_public)]
trait BitOrderCore {}
impl<T: BitOrderCore> BitOrder for T {}

/// Marker trait to require that something is either [`L2M`] or 
/// [`M2L`]
pub trait BitOrder: BitOrderCore {}

/// Marker type that represents LSB to MSB bit order
pub struct L2M;
/// Marker type that represents MSB to LSB bit order
pub struct M2L;

impl BitOrderCore for L2M {}
impl BitOrderCore for M2L {}