/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::graphs::{impl_into_lender, impl_split};
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
    graph: &'a G,
    /// The permutation to apply: node *x* of the underlying graph
    /// becomes node `perm[`*x*`]` of the permuted graph.
    perm: &'a P,
}

impl<'a, G: SequentialGraph, P: SliceByValue<Value = usize>> PermutedGraph<'a, G, P> {
    /// Creates a new [`PermutedGraph`] with the given underlying graph and
    /// permutation.
    ///
    /// Note that the permutation is not checked for validity: it is the
    /// caller's responsibility to ensure that it is a valid permutation of the
    /// nodes of the graph.
    ///
    /// # Panics
    ///
    /// Panics if the permutation does not have the same length as the number of
    /// nodes in the graph.
    pub fn new(graph: &'a G, perm: &'a P) -> Self {
        assert_eq!(
            graph.num_nodes(),
            perm.len(),
            "the permutation must have the same length as the number of nodes in the graph"
        );
        Self { graph, perm }
    }

    /// Returns the underlying graph and the permutation.
    pub fn into_parts(self) -> (&'a G, &'a P) {
        (self.graph, self.perm)
    }
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
    fn get_num_arcs(&self) -> Option<u64> {
        self.graph.get_num_arcs()
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        NodeLabels {
            iter: self.graph.iter_from(from),
            perm: self.perm,
        }
    }
}

impl_split!(
    seq
    ['b, G: SequentialGraph + SplitLabeling, P: SliceByValue<Value = usize> + Send + Sync + Clone]
    PermutedGraph<'b, G, P>
    [for<'a> <G as SequentialLabeling>::Lender<'a>: Clone + ExactSizeLender + Send + Sync]
    [for<'a> <G as SequentialLabeling>::Lender<'a>: Clone + ExactSizeLender + lender::FusedLender + Send + Sync]
);

impl<G: SequentialGraph, P: SliceByValue<Value = usize>> SequentialGraph
    for PermutedGraph<'_, G, P>
{
}

impl_into_lender!(['a, G: SequentialGraph, P: SliceByValue<Value = usize>] PermutedGraph<'a, G, P>);

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

impl<
    L: lender::FusedLender + for<'next> NodeLabelsLender<'next, Label = usize>,
    P: SliceByValue<Value = usize>,
> lender::FusedLender for NodeLabels<'_, L, P>
{
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
    assert_eq!(p.get_num_arcs(), Some(4));
    crate::traits::graph::eq(
        &VecGraph::from_lender(p.iter()),
        &VecGraph::from_arcs([(0, 1), (1, 0), (1, 2), (2, 0)]),
    )?;
    Ok(())
}
