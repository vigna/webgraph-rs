/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Label format of the SWH graph.

*/

use anyhow::{Context, Result};
use dsi_bitstream::{
    codes::GammaRead,
    impls::{BufBitReader, MemWordReader},
    traits::{BitRead, BitSeek, Endianness, BE},
};
use epserde::prelude::*;
use lender::{Lend, Lender, Lending};
use mmap_rs::MmapFlags;
use std::path::Path;
use sux::traits::IndexedSeq;

use crate::prelude::{MmapHelper, NodeLabelsLender, RandomAccessLabeling, SequentialLabeling};
use crate::{graphs::bvgraph::EF, prelude::BitDeserializer};

pub trait ReaderBuilder<E: Endianness> {
    /// The type of the reader that we are building.
    type Reader<'a>: BitRead<E> + BitSeek + 'a
    where
        Self: 'a;

    /// Creates a new reader.
    fn get_reader(&self) -> Self::Reader<'_>;
}

pub struct MmapReaderBuilder<E: Endianness> {
    backend: MmapHelper<u32>,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness> ReaderBuilder<E> for MmapReaderBuilder<E>
where
    for<'a> Self::Reader<'a>: BitRead<E> + BitSeek,
{
    type Reader<'a> = BufBitReader<E, MemWordReader<u32, &'a [u32]>>
    where Self: 'a;

    fn get_reader(&self) -> Self::Reader<'_> {
        BufBitReader::<E, _>::new(MemWordReader::new(self.backend.as_ref()))
    }
}

pub struct BitStream<E: Endianness, RB: ReaderBuilder<E>, D, O: IndexedSeq>
where
    for<'a> RB::Reader<'a>: BitRead<E> + BitSeek,
    D: for<'a> BitDeserializer<E, RB::Reader<'a>>,
{
    width: usize,
    reader_builder: RB,
    offsets: MemCase<O>,
    _marker: std::marker::PhantomData<(E, D)>,
}

impl<E: Endianness, D> BitStream<E, MmapReaderBuilder<E>, D, DeserType<'static, EF>>
where
    D: for<'a> BitDeserializer<E, <MmapReaderBuilder<E> as ReaderBuilder<E>>::Reader<'a>>,
    for<'a> <MmapReaderBuilder<E> as ReaderBuilder<E>>::Reader<'a>: BitRead<E> + BitSeek,
{
    pub fn load_from_file(width: usize, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let backend_path = path.with_extension("labels");
        let offsets_path = path.with_extension("ef");
        Ok(BitStream {
            width,
            reader_builder: MmapReaderBuilder {
                backend: MmapHelper::<u32>::mmap(&backend_path, MmapFlags::empty())
                    .with_context(|| format!("Could not mmap {}", backend_path.display()))?,
            },

            offsets: EF::mmap(&offsets_path, Flags::empty())
                .with_context(|| format!("Could not parse {}", offsets_path.display()))?,
            _marker: std::marker::PhantomData,
        })
    }
}

pub struct Iter<'a, BR, O> {
    width: usize,
    reader: BR,
    offsets: &'a MemCase<O>,
    next_node: usize,
    num_nodes: usize,
}

impl<
        'a,
        'succ,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        O: IndexedSeq<Input = usize, Output = usize>,
    > NodeLabelsLender<'succ> for Iter<'a, BR, O>
{
    type Label = Vec<u64>;
    type IntoIterator = SeqLabels<'succ, BR>;
}

impl<
        'a,
        'succ,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        O: IndexedSeq<Input = usize, Output = usize>,
    > Lending<'succ> for Iter<'a, BR, O>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<
        'a,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        O: IndexedSeq<Input = usize, Output = usize>,
    > Lender for Iter<'a, BR, O>
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
                width: self.width,
                reader: &mut self.reader,
                end_pos: self.offsets.get(self.next_node + 1) as u64,
            },
        );
        self.next_node += 1;
        Some(res)
    }
}

pub struct SeqLabels<'a, BR: BitRead<BE> + BitSeek + GammaRead<BE>> {
    width: usize,
    reader: &'a mut BR,
    end_pos: u64,
}

impl<'a, BR: BitRead<BE> + BitSeek + GammaRead<BE>> Iterator for SeqLabels<'a, BR> {
    type Item = Vec<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.bit_pos().unwrap() >= self.end_pos {
            return None;
        }
        let num_labels = self.reader.read_gamma().unwrap() as usize;
        Some(Vec::from_iter(
            (0..num_labels).map(|_| self.reader.read_bits(self.width).unwrap()),
        ))
    }
}

impl<E: Endianness, D> SequentialLabeling
    for BitStream<E, MmapReaderBuilder<E>, D, DeserType<'static, EF>>
where
    D: for<'a> BitDeserializer<E, <MmapReaderBuilder<E> as ReaderBuilder<E>>::Reader<'a>>,
    for<'a> <MmapReaderBuilder<E> as ReaderBuilder<E>>::Reader<'a>: BitRead<E> + BitSeek,
{
    type Label = Vec<u64>;

    type Lender<'node> = Iter<'node, <MmapReaderBuilder as ReaderBuilder>::Reader<'node>, <EF as DeserializeInner>::DeserType<'node>>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.offsets.len() - 1
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        Iter {
            width: self.width,
            offsets: &self.offsets,
            reader: self.reader_builder.get_reader(),
            next_node: from,
            num_nodes: self.num_nodes(),
        }
    }
}

// TODO: avoid duplicate implementation for labels

struct SwhDeserializer<'a, BR: BitRead<BE> + BitSeek + GammaRead<BE> + 'a> {}

impl BitDeserializer<BE, BR> for SwhDeserializer<'_, BR> {
    type DeserType = Vec<u64>;

    fn deserialize(
        &self,
        bitstream: &mut BR,
    ) -> std::result::Result<Self::DeserType, <BR as BitRead>::Error> {
        let num_labels = self.reader.read_gamma().unwrap() as usize;
        let labels = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            labels.push(self.reader.read_bits(self.width)?);
        }
        Ok(labels)
    }
}

pub struct RanLabels<D: BitDeserializer> {
    width: usize,
    deserializer: D,
    end_pos: u64,
}

impl<BR: BitRead<BE> + BitSeek + GammaRead<BE>> Iterator for RanLabels<BR> {
    type Item = Vec<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        return if self.reader.bit_pos().unwrap() >= self.end_pos {
            None
        } else {
            self.deserializer.deserialize(self.reader).ok()
        };
    }
}

impl<E: Endianness, D> RandomAccessLabeling
    for BitStream<E, MmapReaderBuilder<E>, D, DeserType<'static, EF>>
where
    D: for<'a> BitDeserializer<E, <MmapReaderBuilder<E> as ReaderBuilder<E>>::Reader<'a>>,
    for<'a> <MmapReaderBuilder<E> as ReaderBuilder<E>>::Reader<'a>: BitRead<E> + BitSeek,
{
    type Labels<'succ> = RanLabels<<MmapReaderBuilder as ReaderBuilder>::Reader<'succ>> where Self: 'succ;

    fn num_arcs(&self) -> u64 {
        todo!();
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        let mut reader = self.reader_builder.get_reader();
        reader
            .set_bit_pos(self.offsets.get(node_id) as u64)
            .unwrap();
        RanLabels {
            width: self.width,
            reader,
            end_pos: self.offsets.get(node_id + 1) as u64,
        }
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.labels(node_id).count()
    }
}
