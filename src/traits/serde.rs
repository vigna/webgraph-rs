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
    type SerType;
    /// Write the given value to a bitstream of given endianness and providing
    /// support to write codes.
    fn serialize<E: Endianness, B: WriteCodes<E>>(
        &self,
        value: &Self::SerType,
        bitstream: &mut B,
    ) -> Result<usize>;
}

pub trait BitDeserializer {
    type DeserType;
    /// Reads a value from a bitstream of given endianness and providing
    /// support to read codes.
    fn deserialize<E: Endianness, B: ReadCodes<E>>(bitstream: &mut B) -> Result<Self::DeserType>;
}
