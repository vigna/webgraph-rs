/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Concrete [`StoreLabelsConfig`] implementations for bitstream-based
//! label compression.

use crate::prelude::*;
use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::marker::PhantomData;
use std::path::Path;

use super::BitStreamStoreLabels;

/// Typestate marker: label part files are written uncompressed.
pub struct Uncompressed;

/// Typestate marker: label part files are zstd-compressed.
pub struct Zstd;

/// Configures and spawns [`BitStreamStoreLabels`] instances.
///
/// The typestate parameter `C` controls whether per-thread part files
/// are written with zstd compression ([`Zstd`]) or uncompressed
/// ([`Uncompressed`]). The final concatenated output is always
/// uncompressed.
///
/// # Examples
///
/// ```ignore
/// // Uncompressed (default)
/// let config = BitStreamStoreLabelsConfig::<BE, _>::new(FixedWidth::<u32>::new());
///
/// // Zstd-compressed temp files
/// let config = BitStreamStoreLabelsConfig::<BE, _>::new(FixedWidth::<u32>::new())
///     .with_compressed();
/// ```
pub struct BitStreamStoreLabelsConfig<E: Endianness, S, C = Uncompressed> {
    serializer: S,
    labels_writer: Option<BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>>,
    offsets_writer: Option<BufBitWriter<BigEndian, WordAdapter<usize, BufWriter<File>>>>,
    _marker: PhantomData<C>,
}

impl<E: Endianness, S: Clone> BitStreamStoreLabelsConfig<E, S, Uncompressed> {
    /// Creates a new configuration with the given serializer.
    pub fn new(serializer: S) -> Self {
        Self {
            serializer,
            labels_writer: None,
            offsets_writer: None,
            _marker: PhantomData,
        }
    }

    /// Transitions to the [`Zstd`] typestate, enabling zstd compression
    /// for per-thread part files.
    pub fn with_compressed(self) -> BitStreamStoreLabelsConfig<E, S, Zstd> {
        BitStreamStoreLabelsConfig {
            serializer: self.serializer,
            labels_writer: None,
            offsets_writer: None,
            _marker: PhantomData,
        }
    }
}

impl<E: Endianness, S: Clone> BitStreamStoreLabelsConfig<E, S, Zstd> {
    /// Creates a new zstd-compressed configuration with the given serializer.
    pub fn new(serializer: S) -> Self {
        BitStreamStoreLabelsConfig::<E, S, Uncompressed>::new(serializer).with_compressed()
    }
}

// --- Shared helpers ---

impl<E, S, C> BitStreamStoreLabelsConfig<E, S, C>
where
    E: Endianness,
    S: Clone,
    BufBitWriter<E, WordAdapter<usize, BufWriter<File>>>: BitWrite<E>,
{
    fn init_concat_inner(
        &mut self,
        labels_path: &Path,
        offsets_path: &Path,
    ) -> Result<()> {
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
        let mut reader = <BufBitReader<BigEndian, _>>::new(<WordAdapter<u32, _>>::new(
            BufReader::new(File::open(part_offsets_path).with_context(|| {
                format!("Could not open {}", part_offsets_path.display())
            })?),
        ));
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

    fn new_storage(
        &self,
        labels_path: &Path,
        offsets_path: &Path,
    ) -> Result<Self::StoreLabels> {
        BitStreamStoreLabels::new(self.serializer.clone(), labels_path, offsets_path)
    }

    fn init_concat(
        &mut self,
        labels_path: &Path,
        offsets_path: &Path,
    ) -> Result<()> {
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
    BufBitReader<E, WordAdapter<u32, BufReader<zstd::Decoder<'static, BufReader<File>>>>>: BitRead<E>,
{
    type StoreLabels = BitStreamStoreLabels<E, S, zstd::Encoder<'static, BufWriter<File>>>;

    fn new_storage(
        &self,
        labels_path: &Path,
        offsets_path: &Path,
    ) -> Result<Self::StoreLabels> {
        let file = File::create(labels_path)
            .with_context(|| format!("Could not create {}", labels_path.display()))?;
        let encoder = zstd::Encoder::new(BufWriter::new(file), 0)?;
        BitStreamStoreLabels::from_writer(self.serializer.clone(), encoder, offsets_path)
    }

    fn init_concat(
        &mut self,
        labels_path: &Path,
        offsets_path: &Path,
    ) -> Result<()> {
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
        let decoder = zstd::Decoder::with_buffer(BufReader::new(file))?;
        let mut reader =
            BufBitReader::<E, _>::new(WordAdapter::<u32, _>::new(BufReader::new(decoder)));
        labels_writer.copy_from(&mut reader, labels_written_bits)?;
        std::fs::remove_file(part_labels_path)?;
        self.concat_offsets_part(part_offsets_path, offsets_written_bits)
    }

    fn flush_concat(&mut self) -> Result<()> {
        self.flush_concat_inner()
    }
}
