/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

This modules contains the traits that are used throughout the crate.

*/

use anyhow::Result;
use dsi_bitstream::prelude::*;

pub trait BitSerializer {
    type SerType: Send;
    /// Write the given value to a bitstream of given endianness and providing
    /// support to write codes.
    fn serialize<E: Endianness, B: CodeWrite<E>>(
        &self,
        value: &Self::SerType,
        bitstream: &mut B,
    ) -> Result<usize>;
}

///
/// This trait requires Clone because we need to be able to clone `BatchIterators`
/// to be able to do the parallel compression of BVGraphs. Thus, it's suggested
/// that if you have big structures, you wrap them in an [`Arc`](`std::sync::Arc`) or use references.
pub trait BitDeserializer: Clone {
    type DeserType;
    /// Reads a value from a bitstream of given endianness and providing
    /// support to read codes.
    fn deserialize<E: Endianness, B: CodeRead<E>>(
        &self,
        bitstream: &mut B,
    ) -> Result<Self::DeserType>;
}

impl BitSerializer for () {
    type SerType = ();
    #[inline(always)]
    fn serialize<E: Endianness, B: CodeWrite<E>>(
        &self,
        _value: &Self::SerType,
        _bitstream: &mut B,
    ) -> Result<usize> {
        Ok(0)
    }
}

impl BitDeserializer for () {
    type DeserType = ();
    #[inline(always)]
    fn deserialize<E: Endianness, B: CodeRead<E>>(
        &self,
        _bitstream: &mut B,
    ) -> Result<Self::DeserType> {
        Ok(())
    }
}
