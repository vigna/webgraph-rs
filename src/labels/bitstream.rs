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

use crate::prelude::BitDeserializer;
use crate::prelude::{NodeLabelsLender, RandomAccessLabeling, SequentialLabeling};
use dsi_bitstream::traits::{BitRead, BitSeek, Endianness};
use lender::*;
use std::ops::Deref;
use sux::traits::{IndexedSeq, Types};

/// A basic supplier trait.
///
/// This trait is used to supply readers on the bitstream containing the labels.
/// It will probably be replaced by a more general supplier trait in the future.
pub trait Supply {
    type Item<'a>
    where
        Self: 'a;
    fn request(&self) -> Self::Item<'_>;
}

/// A labeling based on a bitstream of labels and an indexed sequence of offsets.
pub struct BitStreamLabeling<E: Endianness, S: Supply, D, O>
where
    for<'a> S::Item<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::Item<'a>>,
{
    reader_supplier: S,
    bit_deser: D,
    offsets: O,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, S: Supply, D, O> BitStreamLabeling<E, S, D, O>
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

pub struct Iter<'a, 'b, E, BR, D, O> {
    reader: BR,
    bit_deser: &'a D,
    offsets: &'b O,
    next_node: usize,
    num_nodes: usize,
    _marker: std::marker::PhantomData<E>,
}

impl<
        'succ,
        E: Endianness,
        BR: BitRead<E> + BitSeek,
        D: BitDeserializer<E, BR>,
        O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>,
    > NodeLabelsLender<'succ> for Iter<'_, '_, E, BR, D, O>
{
    type Label = D::DeserType;
    type IntoIterator = SeqLabels<'succ, E, BR, D>;
}

impl<
        'succ,
        E: Endianness,
        BR: BitRead<E> + BitSeek,
        D: BitDeserializer<E, BR>,
        O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>,
    > Lending<'succ> for Iter<'_, '_, E, BR, D, O>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<
        E: Endianness,
        BR: BitRead<E> + BitSeek,
        D: BitDeserializer<E, BR>,
        O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>,
    > Lender for Iter<'_, '_, E, BR, D, O>
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
                _marker: std::marker::PhantomData,
            },
        );
        self.next_node += 1;
        Some(res)
    }
}

pub struct SeqLabels<'a, E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> {
    reader: &'a mut BR,
    bit_deser: &'a D,
    end_pos: u64,
    _marker: std::marker::PhantomData<E>,
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

impl<
        L,
        E: Endianness,
        S: Supply,
        D,
        O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>,
    > SequentialLabeling for BitStreamLabeling<E, S, D, O>
where
    for<'a> S::Item<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::Item<'a>, DeserType = L>,
{
    type Label = L;
    type Lender<'node>
        = Iter<'node, 'node, E, S::Item<'node>, D, O>
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
            _marker: std::marker::PhantomData,
        }
    }
}

// TODO: avoid duplicate implementation for labels

pub struct RanLabels<'a, E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> {
    reader: BR,
    deserializer: &'a D,
    end_pos: u64,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, BR: BitRead<E> + BitSeek, D: BitDeserializer<E, BR>> Iterator
    for RanLabels<'_, E, BR, D>
{
    type Item = <D as BitDeserializer<E, BR>>::DeserType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.bit_pos().unwrap() >= self.end_pos {
            None
        } else {
            self.deserializer.deserialize(&mut self.reader).ok()
        }
    }
}

impl<
        L,
        E: Endianness,
        S: Supply,
        D,
        O: Deref<Target: IndexedSeq + Types<Input = usize, Output = usize>>,
    > RandomAccessLabeling for BitStreamLabeling<E, S, D, O>
where
    for<'a> S::Item<'a>: BitRead<E> + BitSeek,
    for<'a> D: BitDeserializer<E, S::Item<'a>, DeserType = L>,
{
    type Labels<'succ>
        = RanLabels<'succ, E, S::Item<'succ>, D>
    where
        Self: 'succ;

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
            _marker: std::marker::PhantomData,
        }
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.labels(node_id).count()
    }
}
