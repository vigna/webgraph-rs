/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Basic skeleton for a simple bitstream-based implementation of a labeling.
//!
//! Labels are stored as a bitstream, and are deserialized using a [custom deserializer](BitDeserializer).
//! An [`IndexedSeq`] provides pointers into the bitstream. Both sequential
//! and random access are provided.

use anyhow::{Context, Result};
use dsi_bitstream::{
    impls::{BufBitReader, MemWordReader},
    traits::{BitRead, BitSeek, Endianness, BE},
};
use epserde::prelude::*;
use lender::{Lend, Lender, Lending};
use mmap_rs::MmapFlags;
use std::{ops::Deref, path::Path};
use sux::traits::{IndexedSeq, Types};

use crate::prelude::{MmapHelper, NodeLabelsLender, RandomAccessLabeling, SequentialLabeling};
use crate::{graphs::bvgraph::EF, prelude::BitDeserializer};

pub trait Supply {
    type Item<'a>
    where
        Self: 'a;
    fn request(&self) -> Self::Item<'_>;
}

pub struct MmapReaderSupplier<E: Endianness> {
    backend: MmapHelper<u32>,
    _marker: std::marker::PhantomData<E>,
}

impl Supply for MmapReaderSupplier<BE> {
    type Item<'a> = BufBitReader<BE, MemWordReader<u32, &'a [u32]>>
    where Self: 'a;

    fn request(&self) -> Self::Item<'_> {
        BufBitReader::<BE, _>::new(MemWordReader::new(self.backend.as_ref()))
    }
}

pub struct BitStream<E: Endianness, S: Supply, D, O>
where
    for<'a> S::Item<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::Item<'a>>,
{
    reader_supplier: S,
    bit_deser: D,
    offsets: O,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, S: Supply, D, O> BitStream<E, S, D, O>
where
    for<'a> S::Item<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::Item<'a>>,
{
    /// Creates a new labeling using the given suppliers and offsets.
    ///
    /// # Arguments
    ///
    /// * `reader_supplier`: A supplier of readers on the bitsteam containing
    ///   the labels.
    ///
    /// * `bit_deser_supplier`: A supplier of deserializers for the labels.
    ///
    /// * `offsets`: An indexed sequence of offsets into the bitstream.
    pub fn new(reader_supplier: S, bit_deser: D, offsets: O) -> Self {
        Self {
            reader_supplier,
            bit_deser,
            offsets,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<D> BitStream<BE, MmapReaderSupplier<BE>, D, MemCase<DeserType<'static, EF>>>
where
    for<'a> D: BitDeserializer<BE, <MmapReaderSupplier<BE> as Supply>::Item<'a>>,
{
    pub fn mmap(path: impl AsRef<Path>, bit_deser: D) -> Result<Self> {
        let path = path.as_ref();
        let backend_path = path.with_extension("labels");
        let offsets_path = path.with_extension("ef");
        Ok(BitStream::new(
            MmapReaderSupplier {
                backend: MmapHelper::<u32>::mmap(&backend_path, MmapFlags::empty())
                    .with_context(|| format!("Could not mmap {}", backend_path.display()))?,
                _marker: std::marker::PhantomData,
            },
            bit_deser,
            EF::mmap(&offsets_path, Flags::empty())
                .with_context(|| format!("Could not parse {}", offsets_path.display()))?,
        ))
    }
}

pub struct Iter<'a, 'b, BR, D, O> {
    reader: BR,
    bit_deser: &'a D,
    offsets: &'b O,
    next_node: usize,
    num_nodes: usize,
}

impl<
        'a,
        'b,
        'succ,
        BR: BitRead<BE> + BitSeek,
        D: BitDeserializer<BE, BR>,
        O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>,
    > NodeLabelsLender<'succ> for Iter<'a, 'b, BR, D, O>
{
    type Label = D::DeserType;
    type IntoIterator = SeqLabels<'succ, BR, D>;
}

impl<
        'a,
        'b,
        'succ,
        BR: BitRead<BE> + BitSeek,
        D: BitDeserializer<BE, BR>,
        O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>,
    > Lending<'succ> for Iter<'a, 'b, BR, D, O>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<
        'a,
        'b,
        BR: BitRead<BE> + BitSeek,
        D: BitDeserializer<BE, BR>,
        O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>,
    > Lender for Iter<'a, 'b, BR, D, O>
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        if self.next_node >= self.num_nodes {
            return None;
        }
        self.reader
            .set_bit_pos(self.offsets.get(self.next_node) as u64)
            .unwrap();
        let res = (
            self.next_node,
            SeqLabels {
                reader: &mut self.reader,
                bit_deser: self.bit_deser,
                end_pos: self.offsets.get(self.next_node + 1) as u64,
            },
        );
        self.next_node += 1;
        Some(res)
    }
}

pub struct SeqLabels<'a, BR: BitRead<BE> + BitSeek, D: BitDeserializer<BE, BR>> {
    reader: &'a mut BR,
    bit_deser: &'a D,
    end_pos: u64,
}

impl<'a, BR: BitRead<BE> + BitSeek, D: BitDeserializer<BE, BR>> Iterator for SeqLabels<'a, BR, D> {
    type Item = D::DeserType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.bit_pos().unwrap() >= self.end_pos {
            None
        } else {
            Some(self.bit_deser.deserialize(self.reader).unwrap())
        }
    }
}

impl<L, S: Supply, D, O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>>
    SequentialLabeling for BitStream<BE, S, D, O>
where
    for<'a> S::Item<'a>: BitRead<BE> + BitSeek,
    for<'a> D: BitDeserializer<BE, S::Item<'a>, DeserType = L>,
{
    type Label = L;
    type Lender<'node> = Iter<'node, 'node, S::Item<'node>, D, O>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.offsets.len() - 1
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        Iter {
            offsets: &self.offsets,
            reader: self.reader_supplier.request(),
            bit_deser: &self.bit_deser,
            next_node: from,
            num_nodes: self.num_nodes(),
        }
    }
}

// TODO: avoid duplicate implementation for labels

pub struct RanLabels<'a, BR: BitRead<BE> + BitSeek, D: BitDeserializer<BE, BR>> {
    reader: BR,
    deserializer: &'a D,
    end_pos: u64,
}

impl<'a, BR: BitRead<BE> + BitSeek, D: BitDeserializer<BE, BR>> Iterator for RanLabels<'a, BR, D> {
    type Item = <D as BitDeserializer<dsi_bitstream::traits::BigEndian, BR>>::DeserType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.bit_pos().unwrap() >= self.end_pos {
            None
        } else {
            self.deserializer.deserialize(&mut self.reader).ok()
        }
    }
}

impl<L, S: Supply, D, O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>>
    RandomAccessLabeling for BitStream<BE, S, D, O>
where
    for<'a> S::Item<'a>: BitRead<BE> + BitSeek,
    for<'a> D: BitDeserializer<BE, S::Item<'a>, DeserType = L>,
{
    type Labels<'succ> = RanLabels<'succ, S::Item<'succ>, D> where Self: 'succ;

    fn num_arcs(&self) -> u64 {
        todo!();
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        let mut reader = self.reader_supplier.request();
        reader
            .set_bit_pos(self.offsets.get(node_id) as u64)
            .unwrap();
        RanLabels {
            reader,
            deserializer: &self.bit_deser,
            end_pos: self.offsets.get(node_id + 1) as u64,
        }
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.labels(node_id).count()
    }
}
