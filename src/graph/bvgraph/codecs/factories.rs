/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::Decoder;

/// A trait providing decoders with random access.
pub trait RandomAccessDecoderFactory {
    /// The type of the reader that we are building
    type Decoder<'a>: Decoder + 'a
    where
        Self: 'a;

    /// Create a new reader starting at the given node.
    fn new_decoder(&self, node: usize) -> anyhow::Result<Self::Decoder<'_>>;
}

/// A trait providing decoders on the whole graph.
pub trait SequentialDecoderFactory {
    /// The type xof the reader that we are building
    type Decoder<'a>: Decoder + 'a
    where
        Self: 'a;

    /// Create a new reader starting at the given node.
    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>>;
}
