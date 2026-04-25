/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! [`StoreLabelsConfig`] implementations for bitstream-based label
//! compression.
//!
//! Compression of labeled graphs or pairs via [`par_comp_labeled`] requires
//! providing a [`StoreLabelsConfig`] that can create [`StoreLabels`] instances.
//! This module provides a concrete implementation
//! [`BitStreamStoreLabelsConfig`] of [`StoreLabelsConfig`] that stores
//! labels in a bitstream format, with support for both uncompressed and
//! [`Zstd`]-compressed per-thread part files. The format can be loaded using
//! [`BitStreamLabelingSeq`]/[`BitStreamLabeling`].
//!
//! [`par_comp_labeled`]: crate::graphs::bvgraph::BvCompConfig::par_comp_labeled
//! [`Zstd`]: zstd

use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use sealed::sealed;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::marker::PhantomData;
use std::path::Path;

/// Compression mode for per-thread label part files.
#[doc(hidden)]
#[sealed]
pub trait PartComp: 'static {}

/// Label part files are written uncompressed.
pub struct Uncompressed;
#[sealed]
impl PartComp for Uncompressed {}

/// Label part files are zstd-compressed.
pub struct Zstd;
#[sealed]
impl PartComp for Zstd {}

/// Configures and spawns [`BitStreamStoreLabels`] instances.
///
/// # Examples
///
/// ```rust
/// # use webgraph::labels::bitstream::store::BitStreamStoreLabelsConfig;
/// # use webgraph::traits::bit_serde::FixedWidth;
/// # use dsi_bitstream::prelude::BE;
/// // Uncompressed (default)
/// let config = BitStreamStoreLabelsConfig::<BE, _>::new(FixedWidth::<u32>::new());
///
/// // Zstd-compressed temp files
/// let config = BitStreamStoreLabelsConfig::<BE, _>::new(FixedWidth::<u32>::new())
///     .with_zstd();
/// ```
pub struct BitStreamStoreLabelsConfig<E: Endianness, S, C: PartComp = Uncompressed> {
    serializer: S,
    labels_writer: Option<BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>>,
    offsets_writer: Option<BufBitWriter<BigEndian, WordAdapter<usize, BufWriter<File>>>>,
    _marker: PhantomData<C>,
}

impl<E: Endianness, S: Clone> BitStreamStoreLabelsConfig<E, S, Uncompressed> {
    /// Creates a new configuration with the given serializer.
    ///
    /// If your labels are highly compressible, consider using [`with_zstd`].
    ///
    /// [`with_zstd`]: Self::with_zstd
    pub fn new(serializer: S) -> Self {
        Self {
            serializer,
            labels_writer: None,
            offsets_writer: None,
            _marker: PhantomData,
        }
    }

    /// Enables [`Zstd`] compression for per-thread label part files.
    ///
    /// [`Zstd`]: zstd
    pub fn with_zstd(self) -> BitStreamStoreLabelsConfig<E, S, Zstd> {
        BitStreamStoreLabelsConfig {
            serializer: self.serializer,
            labels_writer: None,
            offsets_writer: None,
            _marker: PhantomData,
        }
    }
}

// --- Shared helpers ---

impl<E, S, C: PartComp> BitStreamStoreLabelsConfig<E, S, C>
where
    E: Endianness,
    S: Clone,
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: BitWrite<E>,
{
    fn init_concat_inner(&mut self, labels_path: &Path, offsets_path: &Path) -> Result<()> {
        let labels_writer = buf_bit_writer::from_path::<E, usize>(labels_path)
            .with_context(|| format!("Could not create {}", labels_path.display()))?;
        let mut offsets_writer = buf_bit_writer::from_path::<BE, usize>(offsets_path)
            .with_context(|| format!("Could not create {}", offsets_path.display()))?;
        offsets_writer.write_gamma(0)?;
        self.labels_writer = Some(labels_writer);
        self.offsets_writer = Some(offsets_writer);
        Ok(())
    }

    fn concat_offsets_part(
        &mut self,
        part_offsets_path: &Path,
        offsets_written_bits: u64,
    ) -> Result<()> {
        let offsets_writer = self.offsets_writer.as_mut().unwrap();
        let mut reader =
            <BufBitReader<BigEndian, _>>::new(<WordAdapter<u32, _>>::new(BufReader::new(
                File::open(part_offsets_path)
                    .with_context(|| format!("Could not open {}", part_offsets_path.display()))?,
            )));
        offsets_writer.copy_from(&mut reader, offsets_written_bits)?;
        std::fs::remove_file(part_offsets_path)?;
        Ok(())
    }

    fn flush_concat_inner(&mut self) -> Result<()> {
        if let Some(ref mut w) = self.labels_writer {
            BitWrite::<E>::flush(w)?;
        }
        if let Some(ref mut w) = self.offsets_writer {
            w.flush()?;
        }
        self.labels_writer = None;
        self.offsets_writer = None;
        Ok(())
    }
}

// --- Uncompressed StoreLabelsConfig impl ---

impl<E, S> StoreLabelsConfig for BitStreamStoreLabelsConfig<E, S, Uncompressed>
where
    E: Endianness,
    S: Clone,
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: BitWrite<E>,
    S: BitSerializer<E, BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>>,
    BufBitReader<E, WordAdapter<u32, BufReader<File>>>: BitRead<E>,
{
    type StoreLabels = BitStreamStoreLabels<E, S, File>;

    fn new_storage(&self, labels_path: &Path, offsets_path: &Path) -> Result<Self::StoreLabels> {
        BitStreamStoreLabels::new(self.serializer.clone(), labels_path, offsets_path)
    }

    fn init_concat(&mut self, labels_path: &Path, offsets_path: &Path) -> Result<()> {
        self.init_concat_inner(labels_path, offsets_path)
    }

    fn concat_part(
        &mut self,
        part_labels_path: &Path,
        labels_written_bits: u64,
        part_offsets_path: &Path,
        offsets_written_bits: u64,
    ) -> Result<()> {
        let labels_writer = self.labels_writer.as_mut().unwrap();
        let mut reader = buf_bit_reader::from_path::<E, u32>(part_labels_path)?;
        labels_writer.copy_from(&mut reader, labels_written_bits)?;
        std::fs::remove_file(part_labels_path)?;
        self.concat_offsets_part(part_offsets_path, offsets_written_bits)
    }

    fn flush_concat(&mut self) -> Result<()> {
        self.flush_concat_inner()
    }

    fn label_serializer_name(&self) -> String {
        self.serializer.name()
    }
}

// --- Zstd StoreLabelsConfig impl ---

impl<E, S> StoreLabelsConfig for BitStreamStoreLabelsConfig<E, S, Zstd>
where
    E: Endianness,
    S: Clone,
    BufBitWriter<E, WordAdapter<usize, BufWriter<zstd::Encoder<'static, BufWriter<File>>>>>:
        BitWrite<E>,
    S: BitSerializer<
            E,
            BufBitWriter<E, WordAdapter<usize, BufWriter<zstd::Encoder<'static, BufWriter<File>>>>>,
        >,
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: BitWrite<E>,
    BufBitReader<E, WordAdapter<u32, zstd::Decoder<'static, BufReader<File>>>>: BitRead<E>,
{
    type StoreLabels = BitStreamStoreLabels<E, S, zstd::Encoder<'static, BufWriter<File>>>;

    fn new_storage(&self, labels_path: &Path, offsets_path: &Path) -> Result<Self::StoreLabels> {
        let file = File::create(labels_path)
            .with_context(|| format!("Could not create {}", labels_path.display()))?;
        let encoder = zstd::Encoder::new(BufWriter::new(file), 0)?;
        let offsets_writer = OffsetsWriter::from_path(offsets_path, false)?;
        Ok(BitStreamStoreLabels::from_writer(
            self.serializer.clone(),
            encoder,
            offsets_writer,
        ))
    }

    fn init_concat(&mut self, labels_path: &Path, offsets_path: &Path) -> Result<()> {
        self.init_concat_inner(labels_path, offsets_path)
    }

    fn concat_part(
        &mut self,
        part_labels_path: &Path,
        labels_written_bits: u64,
        part_offsets_path: &Path,
        offsets_written_bits: u64,
    ) -> Result<()> {
        let labels_writer = self.labels_writer.as_mut().unwrap();
        let file = File::open(part_labels_path)
            .with_context(|| format!("Could not open {}", part_labels_path.display()))?;
        let decoder = zstd::Decoder::new(file)?;
        let mut reader = BufBitReader::<E, _>::new(WordAdapter::<u32, _>::new(decoder));
        labels_writer.copy_from(&mut reader, labels_written_bits)?;
        std::fs::remove_file(part_labels_path)?;
        self.concat_offsets_part(part_offsets_path, offsets_written_bits)
    }

    fn flush_concat(&mut self) -> Result<()> {
        self.flush_concat_inner()
    }

    fn label_serializer_name(&self) -> String {
        self.serializer.name()
    }
}

/// Compresses arc labels into a bitstream with a companion
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
/// The type parameter `W` controls the underlying writer for the label
/// bitstream (e.g., [`File`] for uncompressed, or
/// [`zstd::Encoder`] for compressed temp files). The offsets
/// writer is always file-backed.
///
/// [`init`]: StoreLabels::init
pub struct BitStreamStoreLabels<E: Endianness, S, W: std::io::Write> {
    serializer: S,
    bitstream: BufBitWriter<E, WordAdapter<usize, BufWriter<W>>>,
    offsets_writer: OffsetsWriter<File>,
    bits_for_curr_node: u64,
    total_label_bits: u64,
    total_offsets_bits: u64,
    started: bool,
}

impl<E: Endianness, S> BitStreamStoreLabels<E, S, File> {
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
        let offsets_writer = OffsetsWriter::from_path(offsets_path, false)?;
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

impl<E: Endianness, S, W: std::io::Write> BitStreamStoreLabels<E, S, W> {
    /// Creates a new label compressor from an existing writer and
    /// offsets writer.
    pub fn from_writer(serializer: S, writer: W, offsets_writer: OffsetsWriter<File>) -> Self {
        let bitstream = BufBitWriter::new(WordAdapter::new(BufWriter::new(writer)));
        Self {
            serializer,
            bitstream,
            offsets_writer,
            bits_for_curr_node: 0,
            total_label_bits: 0,
            total_offsets_bits: 0,
            started: false,
        }
    }
}

impl<E: Endianness, S, W: std::io::Write> StoreLabels for BitStreamStoreLabels<E, S, W>
where
    BufBitWriter<E, WordAdapter<usize, BufWriter<W>>>: BitWrite<E>,
    S: BitSerializer<E, BufBitWriter<E, WordAdapter<usize, BufWriter<W>>>>,
{
    type Label = S::SerType;

    fn init(&mut self) -> Result<()> {
        self.total_offsets_bits += self.offsets_writer.push(0)? as u64;
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

    fn label_written_bits(&self) -> u64 {
        self.total_label_bits
    }

    fn offsets_written_bits(&self) -> u64 {
        self.total_offsets_bits
    }
}
