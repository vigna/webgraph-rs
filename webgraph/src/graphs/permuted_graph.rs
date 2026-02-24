/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use lender::*;
use value_traits::slices::SliceByValue;

/// A wrapper applying a permutation to the iterators of an
/// underlying graph.
///
/// Note that nodes are simply remapped: thus, neither the iterator
/// on the graph nor the successors are sorted.
#[derive(Debug, Clone)]
pub struct PermutedGraph<'a, G: SequentialGraph, P: SliceByValue<Value = usize> + ?Sized> {
    /// The underlying graph.
    pub graph: &'a G,
    /// The permutation to apply: node *i* of the permuted graph
    /// corresponds to node `perm[`*i*`]` of the underlying graph.
    pub perm: &'a P,
}

impl<G: SequentialGraph, P: SliceByValue<Value = usize>> SequentialLabeling
    for PermutedGraph<'_, G, P>
{
    type Label = usize;
    type Lender<'b>
        = NodeLabels<'b, G::Lender<'b>, P>
    where
        Self: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        self.graph.num_arcs_hint()
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        NodeLabels {
            iter: self.graph.iter_from(from),
            perm: self.perm,
        }
    }
}

impl<'b, G: SequentialGraph + SplitLabeling, P: SliceByValue<Value = usize> + Send + Sync + Clone>
    SplitLabeling for PermutedGraph<'b, G, P>
where
    for<'a> <G as SequentialLabeling>::Lender<'a>: Clone + ExactSizeLender + Send + Sync,
{
    type SplitLender<'a>
        = split::seq::Lender<'a, PermutedGraph<'b, G, P>>
    where
        Self: 'a;
    type IntoIterator<'a>
        = split::seq::IntoIterator<'a, PermutedGraph<'b, G, P>>
    where
        Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        split::seq::Iter::new(self.iter(), self.num_nodes(), how_many)
    }
}

impl<G: SequentialGraph, P: SliceByValue<Value = usize>> SequentialGraph
    for PermutedGraph<'_, G, P>
{
}

impl<'a, 'b, G: SequentialGraph, P: SliceByValue<Value = usize>> IntoLender
    for &'b PermutedGraph<'a, G, P>
{
    type Lender = <PermutedGraph<'a, G, P> as SequentialLabeling>::Lender<'b>;

    #[inline(always)]
    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

/// An iterator over the nodes of a graph that applies on the fly a permutation of the nodes.
#[derive(Debug, Clone)]
pub struct NodeLabels<'node, I, P> {
    iter: I,
    perm: &'node P,
}

impl<
    'succ,
    I: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
    P: SliceByValue<Value = usize>,
> NodeLabelsLender<'succ> for NodeLabels<'_, I, P>
{
    type Label = usize;
    type IntoIterator = Succ<'succ, LenderIntoIter<'succ, I>, P>;
}

impl<
    'succ,
    I: Lender + for<'next> NodeLabelsLender<'next, Label = usize>,
    P: SliceByValue<Value = usize>,
> Lending<'succ> for NodeLabels<'_, I, P>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<L: Lender + for<'next> NodeLabelsLender<'next, Label = usize>, P: SliceByValue<Value = usize>>
    Lender for NodeLabels<'_, L, P>
{
    // SAFETY: the lend is covariant as it contains only a usize and an iterator
    // over usize values derived from the underlying lender L.
    unsafe_assume_covariance!();

    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.iter.next().map(|x| {
            let (node, succ) = x.into_pair();
            (
                self.perm.index_value(node),
                Succ {
                    iter: succ.into_iter(),
                    perm: self.perm,
                },
            )
        })
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<
    L: ExactSizeLender + for<'next> NodeLabelsLender<'next, Label = usize>,
    P: SliceByValue<Value = usize>,
> ExactSizeLender for NodeLabels<'_, L, P>
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.iter.len()
    }
}

#[derive(Debug, Clone)]
pub struct Succ<'a, I: Iterator<Item = usize>, P> {
    iter: I,
    perm: &'a P,
}

impl<I: Iterator<Item = usize>, P: SliceByValue<Value = usize>> Iterator for Succ<'_, I, P> {
    type Item = usize;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|succ| self.perm.index_value(succ))
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.iter.count()
    }
}

impl<I: ExactSizeIterator<Item = usize>, P: SliceByValue<Value = usize>> ExactSizeIterator
    for Succ<'_, I, P>
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<I: Iterator<Item = usize> + std::iter::FusedIterator, P: SliceByValue<Value = usize>>
    std::iter::FusedIterator for Succ<'_, I, P>
{
}

#[cfg(test)]
#[test]
fn test_permuted_graph() -> anyhow::Result<()> {
    use crate::graphs::vec_graph::VecGraph;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (2, 1)]);
    let p = PermutedGraph {
        graph: &g,
        perm: &[2, 0, 1],
    };
    assert_eq!(p.num_nodes(), 3);
    assert_eq!(p.num_arcs_hint(), Some(4));
    let v = VecGraph::from_lender(p.iter());

    assert_eq!(v.num_nodes(), 3);
    assert_eq!(v.outdegree(0), 1);
    assert_eq!(v.outdegree(1), 2);
    assert_eq!(v.outdegree(2), 1);
    assert_eq!(v.successors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(v.successors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(v.successors(2).collect::<Vec<_>>(), vec![0]);

    Ok(())
}
