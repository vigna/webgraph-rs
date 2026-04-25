/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Basic skeleton for a simple bitstream-based implementation of a labeling.
//!
//! Labels are stored as a bitstream, and are deserialized using a [custom
//! deserializer](BitDeserializer). An [`IndexedSeq`] provides pointers into the
//! bitstream. Both sequential and random access are provided.
//!
//! See the examples for a complete implementation based on memory mapping.

use std::iter::FusedIterator;
use std::path::Path;

use crate::graphs::bvgraph::{EF, MemBufReader, parse_label_properties};
use crate::prelude::{BitDeserializer, Offsets, SortedIterator, SortedLender};
use crate::prelude::{NodeLabelsLender, RandomAccessLabeling, SequentialLabeling};
use crate::utils::MmapHelper;
use anyhow::Context;
use dsi_bitstream::prelude::{CodesRead, CodesReaderFactory};
use dsi_bitstream::traits::{BitRead, BitSeek, Endianness};
use epserde::deser::{Deserialize, Flags, MemCase};
use lender::*;
use mmap_rs::MmapFlags;
use sux::traits::IndexedSeq;

/// A labeling based on a bitstream of labels and an indexed sequence of offsets.
///
/// Use [`load`](BitStreamLabeling::load) to memory-map a labeling from a label
/// basename, or [`new`](BitStreamLabeling::new) for custom setups.
pub struct BitStreamLabeling<E: Endianness, S: CodesReaderFactory<E>, D, O: Offsets>
where
    for<'a> S::CodesReader<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::CodesReader<'a>>,
{
    factory: S,
    bit_deser: D,
    offsets: MemCase<O>,
    num_arcs: u64,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, S: CodesReaderFactory<E>, D, O: Offsets> BitStreamLabeling<E, S, D, O>
where
    for<'a> S::CodesReader<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::CodesReader<'a>>,
{
    /// Creates a new labeling from a reader factory, a deserializer,
    /// offsets, and the number of arcs.
    pub fn new(factory: S, bit_deser: D, offsets: MemCase<O>, num_arcs: u64) -> Self {
        Self {
            factory,
            bit_deser,
            offsets,
            num_arcs,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, D> BitStreamLabeling<E, MmapHelper<u32>, D, EF>
where
    for<'a> MemBufReader<'a, E>: CodesRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, MemBufReader<'a, E>>,
{
    /// Loads a labeling from the given label basename by memory mapping
    /// the `.labels` bitstream and the `.ef` Elias–Fano pointer list.
    ///
    /// The `.properties` file is parsed to obtain the number of arcs and
    /// to check that the endianness matches `E`.
    pub fn load(label_basename: impl AsRef<Path>, bit_deser: D) -> anyhow::Result<Self> {
        let label_basename = label_basename.as_ref();
        let label_props = parse_label_properties::<E>(label_basename)?;
        let labels_path = label_basename.with_extension("labels");
        let ef_path = label_basename.with_extension("ef");
        Ok(Self::new(
            MmapHelper::<u32>::mmap(&labels_path, MmapFlags::empty())
                .with_context(|| format!("Could not mmap {}", labels_path.display()))?,
            bit_deser,
            // SAFETY: the file was written by a compatible version of ε-serde.
            unsafe {
                EF::mmap(&ef_path, Flags::empty())
                    .with_context(|| format!("Could not mmap {}", ef_path.display()))
            }?,
            label_props.num_arcs,
        ))
    }
}

pub struct NodeLabels<'a, 'b, E, BR, D, O: Offsets> {
    reader: BR,
    bit_deser: &'a D,
    offsets: &'b MemCase<O>,
    next_node: usize,
    num_nodes: usize,
    _marker: std::marker::PhantomData<E>,
}

impl<'succ, E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>, O: Offsets>
    NodeLabelsLender<'succ> for NodeLabels<'_, '_, E, BR, D, O>
{
    type Label = D::DeserType;
    type IntoIterator = SeqLabels<'succ, E, BR, D>;
}

impl<'succ, E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>, O: Offsets>
    Lending<'succ> for NodeLabels<'_, '_, E, BR, D, O>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>, O: Offsets> Lender
    for NodeLabels<'_, '_, E, BR, D, O>
{
    check_covariance!();

    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        if self.next_node >= self.num_nodes {
            return None;
        }
        self.reader
            .set_bit_pos(self.offsets.uncase().get(self.next_node))
            .unwrap();
        let res = (
            self.next_node,
            SeqLabels {
                reader: &mut self.reader,
                bit_deser: self.bit_deser,
                end_pos: self.offsets.uncase().get(self.next_node + 1),
                _marker: std::marker::PhantomData,
            },
        );
        self.next_node += 1;
        Some(res)
    }
}

pub struct SeqLabels<'a, E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> {
    pub(crate) reader: &'a mut BR,
    pub(crate) bit_deser: &'a D,
    pub(crate) end_pos: u64,
    pub(crate) _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> Iterator
    for SeqLabels<'_, E, BR, D>
{
    type Item = D::DeserType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.bit_pos().unwrap() >= self.end_pos {
            None
        } else {
            Some(self.bit_deser.deserialize(self.reader).unwrap())
        }
    }
}

impl<E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> FusedIterator
    for SeqLabels<'_, E, BR, D>
{
}

// SAFETY: nodes are visited in order 0, 1, 2, …
unsafe impl<E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>, O: Offsets>
    SortedLender for NodeLabels<'_, '_, E, BR, D, O>
{
}

// SAFETY: labels within a node are read sequentially from the bitstream.
unsafe impl<E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> SortedIterator
    for SeqLabels<'_, E, BR, D>
{
}

impl<L, E: Endianness, S: CodesReaderFactory<E>, D, O: Offsets> SequentialLabeling
    for BitStreamLabeling<E, S, D, O>
where
    for<'a> S::CodesReader<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::CodesReader<'a>, DeserType = L>,
{
    type Label = L;
    type Lender<'node>
        = NodeLabels<'node, 'node, E, S::CodesReader<'node>, D, O>
    where
        Self: 'node;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.offsets.uncase().len() - 1
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(self.num_arcs)
    }

    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        NodeLabels {
            offsets: &self.offsets,
            reader: self.factory.new_reader(),
            bit_deser: &self.bit_deser,
            next_node: from,
            num_nodes: self.num_nodes(),
            _marker: std::marker::PhantomData,
        }
    }
}

// TODO: avoid duplicate implementation for labels

pub struct Labels<'a, E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> {
    reader: BR,
    deserializer: &'a D,
    end_pos: u64,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> Iterator
    for Labels<'_, E, BR, D>
{
    type Item = <D as BitDeserializer<E, BR>>::DeserType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.bit_pos().unwrap() >= self.end_pos {
            None
        } else {
            Some(self.deserializer.deserialize(&mut self.reader).unwrap())
        }
    }
}

impl<E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> FusedIterator
    for Labels<'_, E, BR, D>
{
}

impl<L, E: Endianness, S: CodesReaderFactory<E>, D, O: Offsets> RandomAccessLabeling
    for BitStreamLabeling<E, S, D, O>
where
    for<'a> S::CodesReader<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::CodesReader<'a>, DeserType = L>,
{
    type Labels<'succ>
        = Labels<'succ, E, S::CodesReader<'succ>, D>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        self.num_arcs
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        let mut reader = self.factory.new_reader();
        reader
            .set_bit_pos(self.offsets.uncase().get(node_id))
            .unwrap();
        Labels {
            reader,
            deserializer: &self.bit_deser,
            end_pos: self.offsets.uncase().get(node_id + 1),
            _marker: std::marker::PhantomData,
        }
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.labels(node_id).count()
    }
}
