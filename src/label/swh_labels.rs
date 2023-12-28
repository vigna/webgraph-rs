/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Label format of the SWH graph.

*/

use anyhow::Result;
use dsi_bitstream::{
    codes::GammaRead,
    impls::{BufBitReader, MemWordReader},
    traits::{BitRead, BitSeek, BE},
};
use epserde::deser::{Deserialize, Flags, MemCase};
use lender::{Lend, Lender, Lending};
use mmap_rs::MmapFlags;
use std::path::Path;
use sux::traits::IndexedDict;

use crate::{
    prelude::{MmapBackend, NodeLabelsLending, RandomAccessLabelling, SequentialLabelling},
    EF,
};

pub trait ReaderBuilder {
    /// The type of the reader that we are building
    type Reader<'a>: BitRead<BE> + BitSeek + 'a
    where
        Self: 'a;

    /// Create a new reader at bit-offset `offset`
    fn get_reader(&self) -> Self::Reader<'_>;
}

pub struct MmapReaderBuilder {
    backend: MmapBackend<u32>,
}

impl ReaderBuilder for MmapReaderBuilder {
    type Reader<'a> = BufBitReader<BE, MemWordReader<u32, &'a [u32]>>
    where Self: 'a;

    fn get_reader(&self) -> Self::Reader<'_> {
        BufBitReader::<BE, _>::new(MemWordReader::new(self.backend.as_ref()))
    }
}

pub struct SwhLabels<RB: ReaderBuilder, O: IndexedDict> {
    width: usize,
    reader_builder: RB,
    offsets: MemCase<O>,
}

impl SwhLabels<MmapReaderBuilder, EF<&[usize], &[u64]>> {
    pub fn load_from_file(width: usize, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        Ok(SwhLabels {
            width,
            reader_builder: MmapReaderBuilder {
                backend: MmapBackend::<u32>::load(
                    path.with_extension("labels"),
                    MmapFlags::empty(),
                )?,
            },

            offsets: EF::<Vec<usize>, Vec<u64>>::mmap(path.with_extension("ef"), Flags::empty())?,
        })
    }
}

pub struct Iterator<'a, BR, O> {
    width: usize,
    reader: BR,
    offsets: &'a MemCase<O>,
    next_node: usize,
}

impl<
        'a,
        'succ,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        O: IndexedDict<Input = usize, Output = usize>,
    > NodeLabelsLending<'succ> for Iterator<'a, BR, O>
{
    type Item = Vec<u64>;
    type IntoIterator = SeqLabels<'succ, BR>;
}

impl<
        'a,
        'succ,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        O: IndexedDict<Input = usize, Output = usize>,
    > Lending<'succ> for Iterator<'a, BR, O>
{
    type Lend = (usize, <Self as NodeLabelsLending<'succ>>::IntoIterator);
}

impl<
        'a,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        O: IndexedDict<Input = usize, Output = usize>,
    > Lender for Iterator<'a, BR, O>
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.reader
            .set_bit_pos(self.offsets.get(self.next_node))
            .unwrap();
        let res = (
            self.next_node,
            SeqLabels {
                width: self.width,
                reader: &mut self.reader,
                end_pos: self.offsets.get(self.next_node + 1),
            },
        );
        self.next_node += 1;
        Some(res)
    }
}

pub struct SeqLabels<'a, BR: BitRead<BE> + BitSeek + GammaRead<BE>> {
    width: usize,
    reader: &'a mut BR,
    end_pos: usize,
}

impl<'a, BR: BitRead<BE> + BitSeek + GammaRead<BE>> std::iter::Iterator for SeqLabels<'a, BR> {
    type Item = Vec<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.get_bit_pos() >= self.end_pos {
            return None;
        }
        let num_labels = self.reader.read_gamma().unwrap() as usize;
        Some(Vec::from_iter(
            (0..num_labels).map(|_| self.reader.read_bits(self.width).unwrap()),
        ))
    }
}

impl SequentialLabelling for SwhLabels<MmapReaderBuilder, EF<&[usize], &[u64]>> {
    type Label = Vec<u64>;

    type Iterator<'node> = Iterator<'node, <MmapReaderBuilder as ReaderBuilder>::Reader<'node>, EF<&'node [usize], &'node[u64]>>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.offsets.len() - 1
    }

    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        Iterator {
            width: self.width,
            offsets: &self.offsets,
            reader: self.reader_builder.get_reader(),
            next_node: from,
        }
    }
}

// TODO: avoid duplicate implementation for labels

pub struct RanLabels<BR: BitRead<BE> + BitSeek + GammaRead<BE>> {
    width: usize,
    reader: BR,
    end_pos: usize,
}

impl<BR: BitRead<BE> + BitSeek + GammaRead<BE>> std::iter::Iterator for RanLabels<BR> {
    type Item = Vec<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.get_bit_pos() >= self.end_pos {
            return None;
        }
        let num_labels = self.reader.read_gamma().unwrap() as usize;
        Some(Vec::from_iter(
            (0..num_labels).map(|_| self.reader.read_bits(self.width).unwrap()),
        ))
    }
}

impl RandomAccessLabelling for SwhLabels<MmapReaderBuilder, EF<&[usize], &[u64]>> {
    type Successors<'succ> = RanLabels<<MmapReaderBuilder as ReaderBuilder>::Reader<'succ>> where Self: 'succ;

    fn num_arcs(&self) -> usize {
        todo!();
    }

    fn successors(&self, node_id: usize) -> <Self as RandomAccessLabelling>::Successors<'_> {
        let mut reader = self.reader_builder.get_reader();
        reader.set_bit_pos(self.offsets.get(node_id)).unwrap();
        RanLabels {
            width: self.width,
            reader,
            end_pos: self.offsets.get(node_id + 1),
        }
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.successors(node_id).count()
    }
}
