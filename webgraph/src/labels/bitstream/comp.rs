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
/// The label file contains only serialized label values. The number of labels
/// per node equals the graph's outdegree.
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
    bits_for_curr_node: u64,
    total_label_bits: u64,
    total_offsets_bits: u64,
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
            .with_context(|| format!("Could not create label file {}", labels_path.display()))?;
        // We write here the initial offset zero
        let offsets_writer = OffsetsWriter::from_path(offsets_path, true)?;
        Ok(Self {
            serializer,
            bitstream,
            offsets_writer,
            bits_for_curr_node: 0,
            total_label_bits: 0,
            total_offsets_bits: 0,
            started: false,
        })
    }
}

impl<E: Endianness, S> StoreLabels for BitStreamLabelComp<E, S>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: BitWrite<E>,
    S: BitSerializer<E, BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>>,
{
    type Label = S::SerType;

    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn push_node(&mut self) -> Result<()> {
        if self.started {
            self.total_offsets_bits += self.offsets_writer.push(self.bits_for_curr_node)? as u64;
        }
        self.started = true;
        self.bits_for_curr_node = 0;
        Ok(())
    }

    fn push_label(&mut self, label: &Self::Label) -> Result<()> {
        let bits = self.serializer.serialize(label, &mut self.bitstream)?;
        self.bits_for_curr_node += bits as u64;
        self.total_label_bits += bits as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.started {
            self.total_offsets_bits += self.offsets_writer.push(self.bits_for_curr_node)? as u64;
        }
        self.bitstream.flush()?;
        self.offsets_writer.flush()?;
        Ok(())
    }

    #[inline(always)]
    fn label_written_bits(&self) -> u64 {
        self.total_label_bits
    }

    #[inline(always)]
    fn offsets_written_bits(&self) -> u64 {
        self.total_offsets_bits
    }
}
