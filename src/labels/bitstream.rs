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

pub struct BitStream<E: Endianness, RS: Supply, DS: Supply, O: IndexedSeq>
where
    for<'a> RS::Item<'a>: BitRead<E> + BitSeek,
    for<'a> DS::Item<'a>: BitDeserializer<E, RS::Item<'a>>,
{
    reader_supplier: RS,
    bit_deser_supplier: DS,
    offsets: MemCase<O>,
    _marker: std::marker::PhantomData<E>,
}

impl<DS: Supply> BitStream<BE, MmapReaderSupplier<BE>, DS, DeserType<'static, EF>>
where
    for<'a> DS::Item<'a>: BitDeserializer<BE, <MmapReaderSupplier<BE> as Supply>::Item<'a>>,
{
    pub fn load_from_file(path: impl AsRef<Path>, bit_deser_supplier: DS) -> Result<Self> {
        let path = path.as_ref();
        let backend_path = path.with_extension("labels");
        let offsets_path = path.with_extension("ef");
        Ok(BitStream {
            reader_supplier: MmapReaderSupplier {
                backend: MmapHelper::<u32>::mmap(&backend_path, MmapFlags::empty())
                    .with_context(|| format!("Could not mmap {}", backend_path.display()))?,
                _marker: std::marker::PhantomData,
            },
            bit_deser_supplier,
            offsets: EF::mmap(&offsets_path, Flags::empty())
                .with_context(|| format!("Could not parse {}", offsets_path.display()))?,
            _marker: std::marker::PhantomData,
        })
    }
}

pub struct Iter<'a, BR, D, O> {
    reader: BR,
    bit_deser: D,
    offsets: &'a MemCase<O>,
    next_node: usize,
    num_nodes: usize,
}

impl<
        'a,
        'succ,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        D: BitDeserializer<BE, BR>,
        O: IndexedSeq<Input = usize, Output = usize>,
    > NodeLabelsLender<'succ> for Iter<'a, BR, D, O>
{
    type Label = D::DeserType;
    type IntoIterator = SeqLabels<'succ, BR, D>;
}

impl<
        'a,
        'succ,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        D: BitDeserializer<BE, BR>,
        O: IndexedSeq<Input = usize, Output = usize>,
    > Lending<'succ> for Iter<'a, BR, D, O>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<
        'a,
        BR: BitRead<BE> + BitSeek + GammaRead<BE>,
        D: BitDeserializer<BE, BR>,
        O: IndexedSeq<Input = usize, Output = usize>,
    > Lender for Iter<'a, BR, D, O>
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
                bit_deser: &mut self.bit_deser,
                end_pos: self.offsets.get(self.next_node + 1) as u64,
            },
        );
        self.next_node += 1;
        Some(res)
    }
}

pub struct SeqLabels<'a, BR: BitRead<BE> + BitSeek + GammaRead<BE>, D: BitDeserializer<BE, BR>> {
    reader: &'a mut BR,
    bit_deser: &'a mut D,
    end_pos: u64,
}

impl<'a, BR: BitRead<BE> + BitSeek + GammaRead<BE>, D: BitDeserializer<BE, BR>> Iterator
    for SeqLabels<'a, BR, D>
{
    type Item = D::DeserType;

    fn next(&mut self) -> Option<Self::Item> {
        return if self.reader.bit_pos().unwrap() >= self.end_pos {
            None
        } else {
            Some(self.bit_deser.deserialize(self.reader).unwrap())
        };
    }
}

impl<DS: Supply + 'static> SequentialLabeling
    for BitStream<BE, MmapReaderSupplier<BE>, DS, DeserType<'static, EF>>
where
    for<'a> DS::Item<'a>: BitDeserializer<BE, <MmapReaderSupplier<BE> as Supply>::Item<'a>>,
{
    type Label = <<DS as Supply>::Item<'static> as BitDeserializer<
        BE,
        <MmapReaderSupplier<BE> as Supply>::Item<'static>,
    >>::DeserType;
    type Lender<'node> = Iter<'node, <MmapReaderSupplier<BE> as Supply>::Item<'node>, <DS as Supply>::Item<'node>, <EF as DeserializeInner>::DeserType<'node>>
    where
        Self: 'node;

    fn num_nodes(&self) -> usize {
        self.offsets.len() - 1
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        Iter {
            offsets: &self.offsets,
            reader: self.reader_supplier.request(),
            bit_deser: self.bit_deser_supplier.request(),
            next_node: from,
            num_nodes: self.num_nodes(),
        }
    }
}

// TODO: avoid duplicate implementation for labels

struct SwhDeserializer<BR: BitRead<BE> + BitSeek + GammaRead<BE>> {
    width: usize,
    _marker: std::marker::PhantomData<BR>,
}

impl<BR: BitRead<BE> + BitSeek + GammaRead<BE>> BitDeserializer<BE, BR> for SwhDeserializer<BR> {
    type DeserType = Vec<u64>;

    fn deserialize(
        &self,
        bitstream: &mut BR,
    ) -> std::result::Result<Self::DeserType, <BR as BitRead<BE>>::Error> {
        let num_labels = bitstream.read_gamma().unwrap() as usize;
        let mut labels = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            labels.push(bitstream.read_bits(self.width)?);
        }
        Ok(labels)
    }
}

pub struct RanLabels<R: BitRead<BE> + BitSeek, D: BitDeserializer<BE, R>> {
    reader: R,
    deserializer: D,
    end_pos: u64,
}

impl<R: BitRead<BE> + BitSeek, D: BitDeserializer<BE, R>> Iterator for RanLabels<R, D> {
    type Item = <D as BitDeserializer<dsi_bitstream::traits::BigEndian, R>>::DeserType;

    fn next(&mut self) -> Option<Self::Item> {
        return if self.reader.bit_pos().unwrap() >= self.end_pos {
            None
        } else {
            self.deserializer.deserialize(&mut self.reader).ok()
        };
    }
}

impl<DS: Supply + 'static> RandomAccessLabeling
    for BitStream<BE, MmapReaderSupplier<BE>, DS, DeserType<'static, EF>>
where
    for<'a> DS::Item<'a>: BitDeserializer<BE, <MmapReaderSupplier<BE> as Supply>::Item<'a>>,
{
    type Labels<'succ> = RanLabels<<MmapReaderSupplier<BE> as Supply>::Item<'succ>, <DS as Supply>::Item<'succ>> where Self: 'succ;

    fn num_arcs(&self) -> u64 {
        todo!();
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        let mut reader = self.reader_supplier.request();
        reader
            .set_bit_pos(self.offsets.get(node_id) as u64)
            .unwrap();
        let bit_deser = self.bit_deser_supplier.request();
        RanLabels {
            reader,
            deserializer: bit_deser,
            end_pos: self.offsets.get(node_id + 1) as u64,
        }
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.labels(node_id).count()
    }
}
