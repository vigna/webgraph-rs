/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use dsi_bitstream::prelude::*;

use crate::prelude::{CodeRead, CodeWrite};

/// A trait for types implementing logic for serializing another type to a
/// bitstream with code-writing capabilities.
pub trait BitSerializer {
    /// The type that implementations of this trait can serialize.
    type SerType: Send;
    /// Serialize the given value to a [`CodeRead`].
    fn serialize<E: Endianness, B: CodeWrite<E>>(
        &self,
        value: &Self::SerType,
        bitstream: &mut B,
    ) -> Result<usize, <B as BitWrite<E>>::Error>;
}

/// A trait for types implementing logic for deserializing another type from a
/// bitstream with code-reading capabilities.
pub trait BitDeserializer {
    /// The type that implementations of this trait can deserialized.
    type DeserType;
    /// Deserialize the given value from a [`CodeWrite`].
    fn deserialize<E: Endianness, B: CodeRead<E>>(
        &self,
        bitstream: &mut B,
    ) -> Result<Self::DeserType, <B as BitRead<E>>::Error>;
}

/// No-op implementation of [`BitSerializer`] for `()`.
impl BitSerializer for () {
    type SerType = ();
    #[inline(always)]
    fn serialize<E: Endianness, B: CodeWrite<E>>(
        &self,
        _value: &Self::SerType,
        _bitstream: &mut B,
    ) -> Result<usize, <B as BitWrite<E>>::Error> {
        Ok(0)
    }
}

/// No-op implementation of [`BitDeserializer`] for `()`.
impl BitDeserializer for () {
    type DeserType = ();
    #[inline(always)]
    fn deserialize<E: Endianness, B: CodeRead<E>>(
        &self,
        _bitstream: &mut B,
    ) -> Result<Self::DeserType, <B as BitRead<E>>::Error> {
        Ok(())
    }
}
