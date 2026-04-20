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

/// Serializes and deserializes a [`PrimitiveInteger`] type `T` using
/// exactly [`T::BITS`](PrimitiveInteger::BITS) bits.
///
/// This is useful for labelled graphs whose labels are primitive integer
/// types. Only types with at most 64 bits are supported.
///
/// # Examples
///
/// ```rust
/// # use webgraph::traits::FixedWidth;
/// let sd = <FixedWidth<u32>>::new();
/// ```
#[derive(Clone, Copy, Debug)]
pub struct FixedWidth<T: PrimitiveInteger> {
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveInteger> FixedWidth<T> {
    /// Creates a new [`FixedWidth`] serializer/deserializer.
    ///
    /// # Panics
    ///
    /// Panics if `T` has more than 64 bits.
    pub fn new() -> Self {
        assert!(
            T::BITS <= 64,
            "FixedWith only supports types with at most 64 bits, got {}",
            T::BITS
        );
        FixedWidth {
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
        bitstream.write_bits(value.as_to::<u64>(), T::BITS as usize)
    }
}

impl<E: Endianness, BR: BitRead<E>, T: PrimitiveInteger> BitDeserializer<E, BR> for FixedWidth<T> {
    type DeserType = T;
    #[inline(always)]
    fn deserialize(&self, bitstream: &mut BR) -> Result<T, BR::Error> {
        Ok(T::as_from(bitstream.read_bits(T::BITS as usize)?))
    }
}
