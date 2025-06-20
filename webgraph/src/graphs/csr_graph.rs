/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::bvgraph::EF;
use crate::traits::*;
use common_traits::UnsignedInt;
use epserde::Epserde;
use lender::{for_, IntoLender, Lend, Lender, Lending};
use sux::{
    bits::BitFieldVec, dict::EliasFanoBuilder, prelude::SelectAdaptConst, traits::BitFieldSliceCore,
};
use value_traits::{
    iter::{IterFrom, IterateByValueFrom},
    slices::{SliceByValue, SliceByValueGet},
};

pub type CompressedCsrGraph = CsrGraph<EF, BitFieldVec>;

#[derive(Debug, Clone, Epserde)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// A compressed sparse row graph.
/// It is a graph representation that stores the degree-cumulative function (DCF)
/// and the successors in a compressed format.
/// The DCF is a sequence of offsets that indicates the start of the neighbors
/// for each node in the graph.
///
/// Depending on the performance and memory requirements, both the DCF and
/// successors can be stored in different formats.
///
/// The fastest format is `CsrGraph<BoxedList, BoxedList>`, while a compressed
/// representation can be achieved with `CsrGraph<webgraph::bvgraph::DCF, BitFieldVec>`.
pub struct CsrGraph<DCF = Vec<usize>, S = Vec<usize>> {
    dcf: DCF,
    successors: S,
}

impl<DCF, S> CsrGraph<DCF, S> {
    /// Creates a new CSR graph from the given degree-cumulative function and
    /// successors.
    ///
    /// # Safety
    /// The degree-cumulative function must be monotone and coherent with the
    /// successors.
    pub unsafe fn from_parts(dcf: DCF, successors: S) -> Self {
        Self { dcf, successors }
    }

    pub fn dcf(&self) -> &DCF {
        &self.dcf
    }

    pub fn successors(&self) -> &S {
        &self.successors
    }

    pub fn into_inner(self) -> (DCF, S) {
        (self.dcf, self.successors)
    }
}

impl core::default::Default for CsrGraph {
    fn default() -> Self {
        Self {
            dcf: vec![0],
            successors: vec![],
        }
    }
}

impl CsrGraph {
    /// Creates an empty CSR graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new graph from a [`SequentialGraph`].
    pub fn from_seq_graph<G: SequentialGraph>(g: &G) -> Self {
        let mut dcf = Vec::with_capacity(g.num_nodes() + 1);
        let mut successors = Vec::with_capacity(g.num_arcs_hint().unwrap_or(0) as usize);
        dcf.push(0);
        let mut last_src = 0;
        for_!((src, succ) in g.iter() {
            while last_src < src {
                dcf.push(successors.len());
                last_src += 1;
            }
            successors.extend(succ);
        });
        for _ in last_src..g.num_nodes() {
            dcf.push(successors.len());
        }
        unsafe { Self::from_parts(dcf, successors) }
    }

    /// Creates a new graph from a sorted [`IntoLender`] yielding a
    /// [`NodeLabelsLender`].
    ///
    /// This method is faster than [`from_lender`](Self::from_lender) as
    /// it does not need to sort the output of the lender.
    pub fn from_sorted_lender<I: IntoLender>(iter_nodes: I) -> Self
    where
        I::Lender: for<'next> NodeLabelsLender<'next, Label = usize>,
        I::Lender: SortedLender,
        for<'succ> LenderIntoIter<'succ, I::Lender>: SortedIterator,
    {
        let mut max_node = 0;
        let mut dcf = vec![0];
        let mut successors = vec![];
        let mut last_src = 0;
        for_!( (src, succs) in iter_nodes {
            while last_src < src {
                dcf.push(successors.len());
                last_src += 1;
            }
            max_node = max_node.max(src);
            for succ in succs {
                successors.push(succ);
                max_node = max_node.max(succ);
            }
        });
        for _ in last_src..=max_node {
            dcf.push(successors.len());
        }
        dcf.shrink_to_fit();
        successors.shrink_to_fit();
        unsafe { Self::from_parts(dcf, successors) }
    }
}

impl CompressedCsrGraph {
    /// Creates a new compressed CSR graph from a random access graph.
    pub fn from_graph<G: RandomAccessGraph>(g: &G) -> Self {
        let n = g.num_nodes();
        let u = g.num_arcs();
        let mut efb = EliasFanoBuilder::new(n + 1, u as usize + 1);
        efb.push(0);
        let mut successors = BitFieldVec::with_capacity(n.ilog2_ceil() as usize, u as usize);
        let mut last_src = 0;
        for_!((src, succ) in g.iter() {
            while last_src < src {
                efb.push(successors.len());
                last_src += 1;
            }
            successors.extend(succ);
        });
        for _ in last_src..g.num_nodes() {
            efb.push(successors.len());
        }
        let ef = efb.build();
        let ef: EF = unsafe { ef.map_high_bits(SelectAdaptConst::<_, _, 12, 4>::new) };
        unsafe { Self::from_parts(ef, successors) }
    }
}

impl<'a, DCF, S> IntoLender for &'a CsrGraph<DCF, S>
where
    DCF: SliceByValue + IterateByValueFrom<Item = usize>,
    S: SliceByValue + IterateByValueFrom<Item = usize>,
{
    type Lender = LenderImpl<IterFrom<'a, DCF>, IterFrom<'a, S>>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<DCF, S> SequentialLabeling for CsrGraph<DCF, S>
where
    DCF: SliceByValue + IterateByValueFrom<Item = usize>,
    S: SliceByValue + IterateByValueFrom<Item = usize>,
{
    type Label = usize;
    type Lender<'a>
        = LenderImpl<IterFrom<'a, DCF>, IterFrom<'a, S>>
    where
        Self: 'a;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.dcf.len().saturating_sub(1)
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(self.successors.len() as u64)
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        let mut offsets_iter = self.dcf.iter_value_from(from);
        // skip the first offset, we don't start from `from + 1`
        // because it might not exist
        let offset = offsets_iter.next().unwrap_or(0);

        LenderImpl {
            node: from,
            last_offset: offset,
            current_offset: offset,
            offsets_iter,
            successors_iter: self.successors.iter_value_from(offset),
        }
    }
}

impl<DCF, S> SequentialGraph for CsrGraph<DCF, S>
where
    DCF: SliceByValue + IterateByValueFrom<Item = usize>,
    S: SliceByValue + IterateByValueFrom<Item = usize>,
{
}

impl<DCF, S> RandomAccessLabeling for CsrGraph<DCF, S>
where
    DCF: SliceByValueGet<Value = usize> + IterateByValueFrom<Item = usize>,
    S: SliceByValueGet<Value = usize> + IterateByValueFrom<Item = usize>,
{
    type Labels<'succ>
        = core::iter::Take<IterFrom<'succ, S>>
    where
        Self: 'succ;

    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.successors.len() as u64
    }

    #[inline(always)]
    fn outdegree(&self, node: usize) -> usize {
        self.dcf.index_value(node + 1) - self.dcf.index_value(node)
    }

    #[inline(always)]
    fn labels(&self, node: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        let start = self.dcf.index_value(node);
        let end = self.dcf.index_value(node + 1);
        self.successors.iter_value_from(start).take(end - start)
    }
}

impl<DCF, S> RandomAccessGraph for CsrGraph<DCF, S>
where
    DCF: SliceByValueGet<Value = usize> + IterateByValueFrom<Item = usize>,
    S: SliceByValueGet<Value = usize> + IterateByValueFrom<Item = usize>,
{
}

/// Sequential Lender for the CSR graph.
pub struct LenderImpl<I: Iterator<Item = usize>, D: Iterator<Item = usize>> {
    /// The next node to lend labels for.
    node: usize,
    /// This is the offset of the last successor of the previous node.
    last_offset: usize,
    /// This is the offset of the next successor to lend. This is modified
    /// by the iterator we return.
    current_offset: usize,
    /// The offsets iterator.
    offsets_iter: I,
    /// The successors iterator.
    successors_iter: D,
}

unsafe impl<I: Iterator<Item = usize>, D: Iterator<Item = usize>> SortedLender
    for LenderImpl<I, D>
{
}

impl<'succ, I, D> NodeLabelsLender<'succ> for LenderImpl<I, D>
where
    I: Iterator<Item = usize>,
    D: Iterator<Item = usize>,
{
    type Label = usize;
    type IntoIterator = IteratorImpl<'succ, D>;
}

impl<'succ, I, D> Lending<'succ> for LenderImpl<I, D>
where
    I: Iterator<Item = usize>,
    D: Iterator<Item = usize>,
{
    type Lend = (usize, IteratorImpl<'succ, D>);
}

impl<I, D> Lender for LenderImpl<I, D>
where
    I: Iterator<Item = usize>,
    D: Iterator<Item = usize>,
{
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        // if the user of the iterator wasn't fully consumed,
        // we need to skip the remaining successors
        while self.current_offset < self.last_offset {
            self.current_offset += 1;
            self.successors_iter.next()?;
        }

        // implicitly exit if the offsets iterator is empty
        let offset = self.offsets_iter.next()?;
        self.last_offset = offset;

        let node = self.node;
        self.node += 1;

        Some((
            node,
            IteratorImpl {
                succ_iter: &mut self.successors_iter,
                current_offset: &mut self.current_offset,
                last_offset: &self.last_offset,
            },
        ))
    }
}

/// The iterator returned by the lender.
/// This is different from the Random access iterator because
/// for better efficiency we have a single successors iterators
/// that is forwarded by the lender.
///
/// If the dcf and the successors are compressed representations,
/// this might be much faster than the random access iterator,
/// like we do in BVGraph. When using vectors it might be
/// slower, but it is still a good idea to use this
/// iterator to avoid the overhead of creating a new iterator
/// for each node.
pub struct IteratorImpl<'a, D> {
    succ_iter: &'a mut D,
    current_offset: &'a mut usize,
    last_offset: &'a usize,
}

impl<D: Iterator<Item = usize>> Iterator for IteratorImpl<'_, D> {
    type Item = usize;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if *self.current_offset >= *self.last_offset {
            return None;
        }
        *self.current_offset += 1;
        self.succ_iter.next()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.last_offset - *self.current_offset;
        (len, Some(len))
    }
}

impl<D: Iterator<Item = usize>> ExactSizeIterator for IteratorImpl<'_, D> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.last_offset - *self.current_offset
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::VecGraph;

    use super::*;
    #[test]
    fn test_csr_graph() {
        let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
        let g = VecGraph::from_arcs(arcs.iter().copied());

        let csr = <CsrGraph>::from_seq_graph(&g);
        labels::check_impl(&csr);
        // We should be able to use eq_sorted
        graph::eq(&g, &csr);

        let csr = CompressedCsrGraph::from_graph(&g);
        /*graph::eq(&g, &csr);
        labels::check_impl(&csr);*/
    }
}
