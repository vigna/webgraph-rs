/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_bitstream::prelude::*;

/// A trait for types implementing logic for serializing another type to a
/// bitstream with code-writing capabilities.
pub trait BitSerializer<E: Endianness, BW: BitWrite<E>> {
    /// The type that implementations of this trait can serialize.
    type SerType;
    /// Serializes the given value to a [`CodeWrite`].
    fn serialize(&self, value: &Self::SerType, bitstream: &mut BW) -> Result<usize, BW::Error>;
}

/// A trait for types implementing logic for deserializing another type from a
/// bitstream with code-reading capabilities.
pub trait BitDeserializer<E: Endianness, BR: BitRead<E>> {
    /// The type that implementations of this trait can deserialized.
    type DeserType;
    /// Deserializes the given value from a [`CodeRead`].
    fn deserialize(&self, bitstream: &mut BR) -> Result<Self::DeserType, BR::Error>;
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
