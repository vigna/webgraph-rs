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
    traits::{BitRead, BitSeek, BE},
};
use epserde::prelude::*;
use lender::{Lend, Lender, Lending};
use mmap_rs::MmapFlags;
use std::path::Path;
use sux::traits::IndexedSeq;

use crate::graphs::bvgraph::EF;
use crate::prelude::{MmapHelper, NodeLabelsLender, RandomAccessLabeling, SequentialLabeling};

pub trait ReaderBuilder {
    /// The type of the reader that we are building
    type Reader<'a>: BitRead<BE> + BitSeek + 'a
    where
        Self: 'a;

    /// Creates a new reader at bit-offset `offset`
    fn get_reader(&self) -> Self::Reader<'_>;
}

pub struct MmapReaderBuilder {
    backend: MmapHelper<u32>,
}

impl ReaderBuilder for MmapReaderBuilder {
    type Reader<'a> = BufBitReader<BE, MemWordReader<u32, &'a [u32]>>
    where Self: 'a;

    fn get_reader(&self) -> Self::Reader<'_> {
        BufBitReader::<BE, _>::new(MemWordReader::new(self.backend.as_ref()))
    }
}

pub struct SwhLabels<RB: ReaderBuilder, O: IndexedSeq> {
    width: usize,
    reader_builder: RB,
    offsets: MemCase<O>,
}

impl SwhLabels<MmapReaderBuilder, DeserType<'static, EF>> {
    pub fn load_from_file(width: usize, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let backend_path = path.with_extension("labels");
        let offsets_path = path.with_extension("ef");
        Ok(SwhLabels {
            width,
            reader_builder: MmapReaderBuilder {
                backend: MmapHelper::<u32>::mmap(&backend_path, MmapFlags::empty())
                    .with_context(|| format!("Could not mmap {}", backend_path.display()))?,
            },

            offsets: EF::mmap(&offsets_path, Flags::empty())
                .with_context(|| format!("Could not parse {}", offsets_path.display()))?,
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

impl SequentialLabeling for SwhLabels<MmapReaderBuilder, DeserType<'static, EF>> {
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

pub struct RanLabels<BR: BitRead<BE> + BitSeek + GammaRead<BE>> {
    width: usize,
    reader: BR,
    end_pos: u64,
}

impl<BR: BitRead<BE> + BitSeek + GammaRead<BE>> Iterator for RanLabels<BR> {
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

impl RandomAccessLabeling for SwhLabels<MmapReaderBuilder, DeserType<'static, EF>> {
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
