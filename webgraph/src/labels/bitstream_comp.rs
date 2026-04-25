/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! A [`LabelComp`] that serializes labels to a bitstream using a
//! [`BitSerializer`], recording per-node offsets for random access.

use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

/// Compresses arc labels into a bitstream file with a companion
/// delta-encoded offsets file.
///
/// The label file contains only serialized label values — no node IDs,
/// no degrees. The number of labels per node equals the graph's
/// outdegree, which is already encoded in the graph bitstream.
///
/// The offsets file stores γ-coded deltas of bit positions, one per
/// node, using the same [`OffsetsWriter`] as graph offsets. Together
/// with the initial zero written by [`init`], this gives _n_ + 1
/// cumulative offsets for _n_ nodes — exactly the format that
/// [`BitStreamLabeling`] expects.
///
/// [`init`]: LabelComp::init
pub struct BitStreamLabelComp<E: Endianness, S> {
    serializer: S,
    bitstream: BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>,
    offsets_writer: OffsetsWriter<File>,
    bits_for_current_node: u64,
    started: bool,
}

impl<E: Endianness, S> BitStreamLabelComp<E, S> {
    /// Creates a new label compressor writing to the given paths.
    ///
    /// The `labels_path` receives the serialized label bitstream,
    /// and `offsets_path` receives the γ-coded delta offsets.
    pub fn new(
        serializer: S,
        labels_path: impl AsRef<Path>,
        offsets_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let labels_path = labels_path.as_ref();
        let offsets_path = offsets_path.as_ref();
        let bitstream = buf_bit_writer::from_path::<E, usize>(labels_path)
            .with_context(|| format!("Could not create {}", labels_path.display()))?;
        let offsets_writer = OffsetsWriter::from_path(offsets_path, false)?;
        Ok(Self {
            serializer,
            bitstream,
            offsets_writer,
            bits_for_current_node: 0,
            started: false,
        })
    }
}

impl<E: Endianness, S> LabelComp for BitStreamLabelComp<E, S>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: BitWrite<E>,
    S: BitSerializer<E, BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>>,
{
    type Label = S::SerType;

    fn init(&mut self) -> Result<()> {
        self.offsets_writer.push(0)?;
        Ok(())
    }

    fn push_node(&mut self) -> Result<()> {
        if self.started {
            self.offsets_writer.push(self.bits_for_current_node)?;
        }
        self.started = true;
        self.bits_for_current_node = 0;
        Ok(())
    }

    fn push_label(&mut self, label: &Self::Label) -> Result<()> {
        let bits = self
            .serializer
            .serialize(label, &mut self.bitstream)
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        self.bits_for_current_node += bits as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.started {
            self.offsets_writer.push(self.bits_for_current_node)?;
        }
        BitWrite::flush(&mut self.bitstream)?;
        self.offsets_writer.flush()?;
        Ok(())
    }
}
