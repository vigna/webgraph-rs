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
}

/// A trait for types implementing logic for deserializing another type from a
/// bitstream with code-reading capabilities.
pub trait BitDeserializer<E: Endianness, BR: BitRead<E>> {
    /// The type that implementations of this trait can deserialize.
    type DeserType;
    /// Deserializes the given value from a [`BitRead`].
    fn deserialize(&self, bitstream: &mut BR) -> Result<Self::DeserType, BR::Error>;
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
}

impl<E: Endianness, BR: BitRead<E>, S, D: BitDeserializer<E, BR>> BitDeserializer<E, BR>
    for BitSerDeser<S, D>
{
    type DeserType = D::DeserType;
    #[inline(always)]
    fn deserialize(&self, bitstream: &mut BR) -> Result<Self::DeserType, BR::Error> {
        self.1.deserialize(bitstream)
    }
}

/// No-op implementation of [`BitSerializer`] for `()`.
impl<E: Endianness, BW: BitWrite<E>> BitSerializer<E, BW> for () {
    type SerType = ();
    #[inline(always)]
    fn serialize(&self, _value: &Self::SerType, _bitstream: &mut BW) -> Result<usize, BW::Error> {
        Ok(0)
    }
}

/// No-op implementation of [`BitDeserializer`] for `()`.
impl<E: Endianness, BR: BitRead<E>> BitDeserializer<E, BR> for () {
    type DeserType = ();
    #[inline(always)]
    fn deserialize(&self, _bitstream: &mut BR) -> Result<Self::DeserType, BR::Error> {
        Ok(())
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

impl<E: Endianness, BW: BitWrite<E>, T: PrimitiveInteger> BitSerializer<E, BW> for FixedWidth<T> {
    type SerType = T;
    #[inline(always)]
    fn serialize(&self, value: &T, bitstream: &mut BW) -> Result<usize, BW::Error> {
        bitstream.write_bits(value.as_to::<u64>(), self.bits)
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
}
