/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Traits for bit serialization and deserialization of graph labels.

use dsi_bitstream::prelude::*;
use num_primitive::PrimitiveInteger;
use std::marker::PhantomData;

/// A trait for types implementing logic for serializing another type to a
/// bitstream with code-writing capabilities.
pub trait BitSerializer<E: Endianness, BW: BitWrite<E>> {
    /// The type that implementations of this trait can serialize.
    type SerType;
    /// Serializes the given value to a [`BitWrite`].
    fn serialize(&self, value: &Self::SerType, bitstream: &mut BW) -> Result<usize, BW::Error>;
    /// Returns a stable, human-readable name for this codec.
    ///
    /// The name is written to the `.properties` file during compression
    /// and checked against the deserializer's name at load time to prevent
    /// mismatches.
    ///
    /// The format mirrors Rust constructor syntax: e.g.,
    /// `"FixedWidth<u32>"` for the default width, or
    /// `"FixedWidth<u32>(20)"` for a custom width.
    fn name(&self) -> String;
}

/// A trait for types implementing logic for deserializing another type from a
/// bitstream with code-reading capabilities.
pub trait BitDeserializer<E: Endianness, BR: BitRead<E>> {
    /// The type that implementations of this trait can deserialize.
    type DeserType;
    /// Deserializes the given value from a [`BitRead`].
    fn deserialize(&self, bitstream: &mut BR) -> Result<Self::DeserType, BR::Error>;
    /// Returns a stable, human-readable name for this codec.
    ///
    /// See [`BitSerializer::name`] for the naming convention.
    fn name(&self) -> String;
}

/// Combines a [`BitSerializer`] and a [`BitDeserializer`] into a single type
/// implementing both traits.
///
/// This is useful when an API requires a single type parameter bounded by both
/// [`BitSerializer`] and [`BitDeserializer`], but you have separate
/// implementations for each.
#[derive(Clone, Copy, Debug, Default)]
pub struct BitSerDeser<S, D>(pub S, pub D);

impl<E: Endianness, BW: BitWrite<E>, S: BitSerializer<E, BW>, D> BitSerializer<E, BW>
    for BitSerDeser<S, D>
{
    type SerType = S::SerType;
    #[inline(always)]
    fn serialize(&self, value: &Self::SerType, bitstream: &mut BW) -> Result<usize, BW::Error> {
        self.0.serialize(value, bitstream)
    }
    fn name(&self) -> String {
        self.0.name()
    }
}

impl<E: Endianness, BR: BitRead<E>, S, D: BitDeserializer<E, BR>> BitDeserializer<E, BR>
    for BitSerDeser<S, D>
{
    type DeserType = D::DeserType;
    #[inline(always)]
    fn deserialize(&self, bitstream: &mut BR) -> Result<Self::DeserType, BR::Error> {
        self.1.deserialize(bitstream)
    }
    fn name(&self) -> String {
        self.1.name()
    }
}

/// No-op implementation of [`BitSerializer`] for `()`.
impl<E: Endianness, BW: BitWrite<E>> BitSerializer<E, BW> for () {
    type SerType = ();
    #[inline(always)]
    fn serialize(&self, _value: &Self::SerType, _bitstream: &mut BW) -> Result<usize, BW::Error> {
        Ok(0)
    }
    fn name(&self) -> String {
        "()".to_string()
    }
}

/// No-op implementation of [`BitDeserializer`] for `()`.
impl<E: Endianness, BR: BitRead<E>> BitDeserializer<E, BR> for () {
    type DeserType = ();
    #[inline(always)]
    fn deserialize(&self, _bitstream: &mut BR) -> Result<Self::DeserType, BR::Error> {
        Ok(())
    }
    fn name(&self) -> String {
        "()".to_string()
    }
}

/// Serializes and deserializes a [`PrimitiveInteger`] type `T` using a
/// fixed number of bits.
///
/// By default ([`new`]), the full width of the type is used
/// ([`T::BITS`](PrimitiveInteger::BITS) bits). With [`with_bits`], you can
/// specify a smaller number of bits to save space when values are known to
/// fit in a narrower range.
///
/// Signed types are handled correctly: the low `bits` bits of the two's
/// complement representation are stored, and sign extension is applied on
/// deserialization. Only types with at most 64 bits are supported.
///
/// # Examples
///
/// ```rust
/// # use webgraph::traits::FixedWidth;
/// // Full width (32 bits)
/// let sd = <FixedWidth<u32>>::new();
///
/// // Only 10 bits; values must be in [0 . . 1024)
/// let sd = <FixedWidth<u32>>::with_bits(10);
///
/// // Signed with 5 bits; values must be in [−16 . . 16)
/// let sd = <FixedWidth<i8>>::with_bits(5);
/// ```
///
/// [`new`]: FixedWidth::new
/// [`with_bits`]: FixedWidth::with_bits
#[derive(Clone, Copy, Debug)]
pub struct FixedWidth<T: PrimitiveInteger> {
    bits: usize,
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveInteger> FixedWidth<T> {
    /// Creates a new [`FixedWidth`] serializer/deserializer using
    /// [`T::BITS`](PrimitiveInteger::BITS) bits.
    ///
    /// # Panics
    ///
    /// Panics if `T` has more than 64 bits.
    pub fn new() -> Self {
        Self::with_bits(T::BITS as usize)
    }

    /// Creates a new [`FixedWidth`] serializer/deserializer using the
    /// specified number of bits.
    ///
    /// For unsigned types, values must be in [0 . . 2^`bits`). For signed
    /// types, values must be in [−2^(`bits` − 1) . . 2^(`bits` − 1)).
    ///
    /// # Panics
    ///
    /// Panics if `bits` is greater than `T::BITS` or greater than 64.
    pub fn with_bits(bits: usize) -> Self {
        assert!(
            bits <= T::BITS as usize,
            "FixedWidth: bits ({}) exceeds T::BITS ({})",
            bits,
            T::BITS,
        );
        assert!(
            bits <= 64,
            "FixedWidth only supports types with at most 64 bits, got {}",
            bits,
        );
        FixedWidth {
            bits,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveInteger> Default for FixedWidth<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: PrimitiveInteger> FixedWidth<T> {
    fn codec_name(&self) -> String {
        if self.bits == T::BITS as usize {
            format!("FixedWidth<{}>", std::any::type_name::<T>())
        } else {
            format!("FixedWidth<{}>({})", std::any::type_name::<T>(), self.bits)
        }
    }
}

impl<E: Endianness, BW: BitWrite<E>, T: PrimitiveInteger> BitSerializer<E, BW> for FixedWidth<T> {
    type SerType = T;
    #[inline(always)]
    fn serialize(&self, value: &T, bitstream: &mut BW) -> Result<usize, BW::Error> {
        bitstream.write_bits(value.as_to::<u64>(), self.bits)
    }
    fn name(&self) -> String {
        self.codec_name()
    }
}

impl<E: Endianness, BR: BitRead<E>, T: PrimitiveInteger> BitDeserializer<E, BR> for FixedWidth<T> {
    type DeserType = T;
    #[inline(always)]
    fn deserialize(&self, bitstream: &mut BR) -> Result<T, BR::Error> {
        let raw = bitstream.read_bits(self.bits)?;
        if T::MIN < T::default() && self.bits > 0 && self.bits < 64 {
            // Sign-extend from self.bits to 64 bits
            let shift = 64 - self.bits;
            Ok(T::as_from(((raw as i64) << shift >> shift) as u64))
        } else {
            Ok(T::as_from(raw))
        }
    }
    fn name(&self) -> String {
        self.codec_name()
    }
}

/// Serializes and deserializes a `u64` using [Elias γ codes](dsi_bitstream::codes::gamma).
///
/// This is functionally equivalent to [`PrefixFree`] (whose default code is
/// gamma), but provided as a convenience so you can write `Gamma` directly
/// in value position without a turbofish.
/// # Examples
///
/// ```rust
/// # use webgraph::traits::Gamma;
/// let sd = Gamma;
/// ```
#[derive(Clone, Copy, Debug)]
pub struct Gamma;

impl<E: Endianness, BW: GammaWrite<E>> BitSerializer<E, BW> for Gamma {
    type SerType = u64;
    #[inline(always)]
    fn serialize(&self, value: &u64, bitstream: &mut BW) -> Result<usize, BW::Error> {
        bitstream.write_gamma(*value)
    }
}

impl<E: Endianness, BR: GammaRead<E>> BitDeserializer<E, BR> for Gamma {
    type DeserType = u64;
    #[inline(always)]
    fn deserialize(&self, bitstream: &mut BR) -> Result<u64, BR::Error> {
        bitstream.read_gamma()
    }
}

/// Serializes and deserializes a tuple `(A, B)` by delegating each element
/// to a separate [`BitSerializer`]/[`BitDeserializer`].
///
/// The first field handles the first element and the second field handles
/// the second element. Elements are written and read in order.
///
/// # Examples
///
/// ```rust
/// # use webgraph::traits::{Pair, FixedWidth, Gamma};
/// // (u32 fixed-width, u64 gamma)
/// let sd = Pair(FixedWidth::<u32>::new(), Gamma);
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct Pair<F, S>(pub F, pub S);

impl<E: Endianness, BW: BitWrite<E>, F: BitSerializer<E, BW>, S: BitSerializer<E, BW>>
    BitSerializer<E, BW> for Pair<F, S>
{
    type SerType = (F::SerType, S::SerType);
    #[inline(always)]
    fn serialize(&self, value: &Self::SerType, bitstream: &mut BW) -> Result<usize, BW::Error> {
        let a = self.0.serialize(&value.0, bitstream)?;
        let b = self.1.serialize(&value.1, bitstream)?;
        Ok(a + b)
    }
}

impl<E: Endianness, BR: BitRead<E>, F: BitDeserializer<E, BR>, S: BitDeserializer<E, BR>>
    BitDeserializer<E, BR> for Pair<F, S>
{
    type DeserType = (F::DeserType, S::DeserType);
    #[inline(always)]
    fn deserialize(&self, bitstream: &mut BR) -> Result<Self::DeserType, BR::Error> {
        let a = self.0.deserialize(bitstream)?;
        let b = self.1.deserialize(bitstream)?;
        Ok((a, b))
    }
}

/// Serializes and deserializes a `u64` using a compile-time–selected
/// prefix-free code from [`dsi_bitstream::dispatch::ConstCode`].
///
/// The `CODE` const parameter is one of the constants from
/// [`dsi_bitstream::dispatch::code_consts`] (e.g., `GAMMA`, `DELTA`,
/// `ZETA3`). Because the code is chosen at compile time, the dispatch is
/// fully inlined with no runtime overhead.
///
/// # Examples
///
/// ```rust
/// # use webgraph::traits::PrefixFree;
/// # use dsi_bitstream::prelude::code_consts;
/// let sd = PrefixFree::<{ code_consts::DELTA }>;
/// ```
#[derive(Clone, Copy, Debug)]
pub struct PrefixFree<const CODE: usize = { code_consts::GAMMA }>;

impl<E: Endianness, BW: CodesWrite<E>, const CODE: usize> BitSerializer<E, BW>
    for PrefixFree<CODE>
{
    type SerType = u64;
    #[inline(always)]
    fn serialize(&self, value: &u64, bitstream: &mut BW) -> Result<usize, BW::Error> {
        ConstCode::<CODE>.write(bitstream, *value)
    }
}

impl<E: Endianness, BR: CodesRead<E>, const CODE: usize> BitDeserializer<E, BR>
    for PrefixFree<CODE>
{
    type DeserType = u64;
    #[inline(always)]
    fn deserialize(&self, bitstream: &mut BR) -> Result<u64, BR::Error> {
        ConstCode::<CODE>.read(bitstream)
    }
}

/// Maps integers to natural numbers via the [`ToNat`]/[`ToInt`] bijection
/// and delegates to an inner [`BitSerializer`]/[`BitDeserializer`] that
/// operates on `u64`.
///
/// The mapping sends `0 → 0, -1 → 1, 1 → 2, -2 → 3, 2 → 4, …`,
/// so small absolute values remain small — ideal for pairing with
/// variable-length codes like [`Gamma`].
///
/// # Examples
///
/// ```rust
/// # use webgraph::traits::{ZigZag, Gamma};
/// // Encode signed values with gamma codes
/// let sd = ZigZag(Gamma);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct ZigZag<C>(pub C);

impl<E: Endianness, BW: BitWrite<E>, C: BitSerializer<E, BW, SerType = u64>> BitSerializer<E, BW>
    for ZigZag<C>
{
    type SerType = i64;
    #[inline(always)]
    fn serialize(&self, value: &i64, bitstream: &mut BW) -> Result<usize, BW::Error> {
        let encoded = value.to_nat();
        self.0.serialize(&encoded, bitstream)
    }
}

impl<E: Endianness, BR: BitRead<E>, C: BitDeserializer<E, BR, DeserType = u64>>
    BitDeserializer<E, BR> for ZigZag<C>
{
    type DeserType = i64;
    #[inline(always)]
    fn deserialize(&self, bitstream: &mut BR) -> Result<i64, BR::Error> {
        let n = self.0.deserialize(bitstream)?;
        Ok(n.to_int())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Writer = BufBitWriter<BE, MemWordWriterVec<u64, Vec<u64>>>;
    type Reader = BufBitReader<BE, MemWordReader<u64, Vec<u64>>>;

    /// Round-trip a value through serialize then deserialize.
    fn round_trip<S>(serde: &S, value: S::SerType) -> S::DeserType
    where
        S: BitSerializer<BE, Writer> + BitDeserializer<BE, Reader>,
        S::SerType: std::fmt::Debug,
    {
        let mut writer = BufBitWriter::<BE, _>::new(MemWordWriterVec::new(Vec::new()));
        serde.serialize(&value, &mut writer).unwrap();
        let buf = writer.into_inner().unwrap().into_inner();
        let mut reader = BufBitReader::<BE, _>::new(MemWordReader::new(buf));
        serde.deserialize(&mut reader).unwrap()
    }

    // ─── FixedWidth unsigned ───────────────────────────────────────────

    #[test]
    fn fixed_width_u8_full() {
        let sd = FixedWidth::<u8>::new();
        for v in [0u8, 1, 127, 128, 255] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn fixed_width_u16_full() {
        let sd = FixedWidth::<u16>::new();
        for v in [0u16, 1, 255, 256, 32767, 65535] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn fixed_width_u32_full() {
        let sd = FixedWidth::<u32>::new();
        for v in [0u32, 1, u32::MAX / 2, u32::MAX] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn fixed_width_u64_full() {
        let sd = FixedWidth::<u64>::new();
        for v in [0u64, 1, u64::MAX / 2, u64::MAX] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn fixed_width_u32_narrow() {
        let sd = FixedWidth::<u32>::with_bits(10);
        for v in [0u32, 1, 512, 1023] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    // ─── FixedWidth signed ───────────────────────────────────────────

    #[test]
    fn fixed_width_i8_full() {
        let sd = FixedWidth::<i8>::new();
        for v in [0i8, 1, -1, 127, -128] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn fixed_width_i16_full() {
        let sd = FixedWidth::<i16>::new();
        for v in [0i16, 1, -1, i16::MAX, i16::MIN] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn fixed_width_i32_full() {
        let sd = FixedWidth::<i32>::new();
        for v in [0i32, 1, -1, i32::MAX, i32::MIN] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn fixed_width_i64_full() {
        let sd = FixedWidth::<i64>::new();
        for v in [0i64, 1, -1, i64::MAX, i64::MIN] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn fixed_width_i8_narrow() {
        let sd = FixedWidth::<i8>::with_bits(5);
        // 5 bits signed: [-16, 15]
        for v in [0i8, 1, -1, 15, -16, -5, 7] {
            assert_eq!(round_trip(&sd, v), v, "failed for {v}");
        }
    }

    #[test]
    fn fixed_width_i16_narrow() {
        let sd = FixedWidth::<i16>::with_bits(9);
        // 9 bits signed: [-256, 255]
        for v in [0i16, 1, -1, 255, -256, -100, 100] {
            assert_eq!(round_trip(&sd, v), v, "failed for {v}");
        }
    }

    #[test]
    fn fixed_width_i8_one_bit() {
        let sd = FixedWidth::<i8>::with_bits(1);
        // 1 bit signed: [-1, 0]
        assert_eq!(round_trip(&sd, 0i8), 0);
        assert_eq!(round_trip(&sd, -1i8), -1);
    }

    // ─── Gamma ───────────────────────────────────────────────────────

    #[test]
    fn gamma_round_trip() {
        for v in [0u64, 1, 2, 7, 100, 1000, u64::MAX / 2] {
            assert_eq!(round_trip(&Gamma, v), v);
        }
    }

    // ─── PrefixFree ──────────────────────────────────────────────────

    #[test]
    fn prefix_free_gamma() {
        let sd = PrefixFree::<{ code_consts::GAMMA }>;
        for v in [0u64, 1, 2, 100, 1000] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn prefix_free_delta() {
        let sd = PrefixFree::<{ code_consts::DELTA }>;
        for v in [0u64, 1, 2, 100, 1000, 1_000_000] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    #[test]
    fn prefix_free_zeta3() {
        let sd = PrefixFree::<{ code_consts::ZETA3 }>;
        for v in [0u64, 1, 2, 100, 1000, 1_000_000] {
            assert_eq!(round_trip(&sd, v), v);
        }
    }

    // ─── Pair ────────────────────────────────────────────────────────

    #[test]
    fn pair_fixed_gamma() {
        let sd = Pair(FixedWidth::<u32>::new(), Gamma);
        for (a, b) in [(0u32, 0u64), (42, 100), (u32::MAX, 0), (0, 999)] {
            assert_eq!(round_trip(&sd, (a, b)), (a, b));
        }
    }

    #[test]
    fn pair_signed_fixed() {
        let sd = Pair(FixedWidth::<i8>::with_bits(5), FixedWidth::<i16>::new());
        for (a, b) in [(0i8, 0i16), (-1, -1), (15, i16::MAX), (-16, i16::MIN)] {
            assert_eq!(round_trip(&sd, (a, b)), (a, b), "failed for ({a}, {b})");
        }
    }

    #[test]
    fn pair_nested() {
        let sd = Pair(Gamma, Pair(FixedWidth::<u8>::new(), Gamma));
        let value = (42u64, (255u8, 7u64));
        assert_eq!(round_trip(&sd, value), value);
    }

    // ─── ZigZag ──────────────────────────────────────────────────────

    #[test]
    fn zigzag_gamma_round_trip() {
        let sd = ZigZag(Gamma);
        for v in [0i64, 1, -1, 2, -2, 100, -100, 1_000_000, -1_000_000] {
            assert_eq!(round_trip(&sd, v), v, "failed for {v}");
        }
    }

    #[test]
    fn zigzag_delta_round_trip() {
        let sd = ZigZag(PrefixFree::<{ code_consts::DELTA }>);
        for v in [0i64, 1, -1, i64::MAX, i64::MIN + 1] {
            assert_eq!(round_trip(&sd, v), v, "failed for {v}");
        }
    }

    #[test]
    fn pair_zigzag_unsigned() {
        let sd = Pair(ZigZag(Gamma), FixedWidth::<u32>::new());
        for (a, b) in [(0i64, 0u32), (-42, 100), (1_000_000, u32::MAX)] {
            assert_eq!(round_trip(&sd, (a, b)), (a, b));
        }
    }
}
