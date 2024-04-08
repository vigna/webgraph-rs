/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::*;
use core::mem::MaybeUninit;
use lender::*;

/// An adapter exhibiting a list of labeled
/// arcs sorted by source as a [labeled sequential graph](LabeledSequentialGraph).
///
/// If for every source the arcs are sorted by destination, the
/// successors of the graph will be sorted.
#[derive(Clone)]
pub struct ArcListGraph<I: Clone> {
    num_nodes: usize,
    into_iter: I,
}

impl<L: Clone + Copy + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> ArcListGraph<I> {
    /// Create a new arc list graph from the given [`IntoIterator`].
    #[inline(always)]
    pub fn new_labeled(num_nodes: usize, iter: I) -> Self {
        Self {
            num_nodes,
            into_iter: iter,
        }
    }
}

impl<I: Iterator<Item = (usize, usize)> + Clone>
    ArcListGraph<std::iter::Map<I, fn((usize, usize)) -> (usize, usize, ())>>
{
    /// Create a new arc list graph from the given [`IntoIterator`].
    ///
    /// Note that the resulting graph will be labeled by the unit type `()`.
    /// To obtain an unlabeled graph, use a [left projection](crate::prelude::proj::Left).
    #[inline(always)]
    pub fn new(num_nodes: usize, iter: impl IntoIterator<IntoIter = I>) -> Self {
        Self {
            num_nodes,
            into_iter: iter.into_iter().map(|(src, dst)| (src, dst, ())),
        }
    }
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> SplitLabeling
    for ArcListGraph<I>
where
    <I as std::iter::IntoIterator>::IntoIter: Clone + Send + Sync,
    L: Send + Sync,
{
    type SplitLender<'a> = split::seq::Lender<'a, ArcListGraph<I>> where Self: 'a;
    type IntoIterator<'a> = split::seq::IntoIterator<'a, ArcListGraph<I>> where Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        split::seq::Iter::new(self.iter(), how_many)
    }
}

#[derive(Clone)]
pub struct Iter<L, I: IntoIterator<Item = (usize, usize, L)>> {
    num_nodes: usize,
    curr_node: usize,
    next_pair: (usize, usize, L),
    iter: I::IntoIter,
}

unsafe impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> SortedLender
    for Iter<L, I>
{
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)>> Iter<L, I> {
    pub fn new(num_nodes: usize, mut iter: I::IntoIter) -> Self {
        Iter {
            num_nodes,
            curr_node: 0_usize.wrapping_sub(1), // No node seen yet
            next_pair: iter.next().unwrap_or((usize::MAX, usize::MAX, unsafe {
                #[allow(clippy::uninit_assumed_init)]
                // SAFETY: L is Copy
                MaybeUninit::uninit().assume_init()
            })),
            iter,
        }
    }
}

impl<'succ, L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone>
    NodeLabelsLender<'succ> for Iter<L, I>
{
    type Label = (usize, L);
    type IntoIterator = Succ<'succ, L, I>;
}

impl<'succ, L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> Lending<'succ>
    for Iter<L, I>
{
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> Lender for Iter<L, I> {
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.curr_node = self.curr_node.wrapping_add(1);
        if self.curr_node == self.num_nodes {
            return None;
        }

        // This happens if the user doesn't use the successors iter
        while self.next_pair.0 < self.curr_node {
            self.next_pair = self.iter.next().unwrap_or((usize::MAX, usize::MAX, unsafe {
                #[allow(clippy::uninit_assumed_init)]
                // SAFETY: L is Copy
                MaybeUninit::uninit().assume_init()
            }));
        }

        Some((self.curr_node, Succ { node_iter: self }))
    }
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> ExactSizeLender
    for Iter<L, I>
{
    fn len(&self) -> usize {
        self.num_nodes - self.curr_node.wrapping_add(1)
    }
}

impl<'lend, L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> Lending<'lend>
    for &ArcListGraph<I>
{
    type Lend = (usize, Succ<'lend, L, I>);
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> IntoLender
    for &ArcListGraph<I>
{
    type Lender = Iter<L, I>;

    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> SequentialLabeling
    for ArcListGraph<I>
{
    type Label = (usize, L);
    type Lender<'node> = Iter<L, I> where Self: 'node;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        None
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        let mut iter = Iter::new(self.num_nodes, self.into_iter.clone().into_iter());
        for _ in 0..from {
            iter.next();
        }

        iter
    }
}

/// Iter until we found a triple with src different than curr_node
pub struct Succ<'succ, L, I: IntoIterator<Item = (usize, usize, L)>> {
    node_iter: &'succ mut Iter<L, I>,
}

impl<'a, L, I: IntoIterator<Item = (usize, usize, L)>> Iterator for Succ<'a, L, I> {
    type Item = (usize, L);
    fn next(&mut self) -> Option<Self::Item> {
        // If the source of the next pair is not the current node,
        // we return None.
        if self.node_iter.next_pair.0 != self.node_iter.curr_node {
            None
        } else {
            // get the next triple
            let pair = self
                .node_iter
                .iter
                .next()
                .unwrap_or((usize::MAX, usize::MAX, unsafe {
                    #[allow(clippy::uninit_assumed_init)]
                    MaybeUninit::uninit().assume_init()
                }));
            // store the triple and return the previous successor
            // storing the label since it should be one step behind the successor
            let (_src, dst, label) = core::mem::replace(&mut self.node_iter.next_pair, pair);
            Some((dst, label))
        }
    }
}

#[cfg(test)]
#[cfg_attr(test, test)]
fn test() -> anyhow::Result<()> {
    use crate::graphs::vec_graph::VecGraph;
    let arcs = [
        (0, 1, Some(1.0)),
        (0, 2, None),
        (1, 2, Some(2.0)),
        // the labels should never be read :)
        (1, 3, Some(f64::NAN)),
        (2, 4, Some(f64::INFINITY)),
        (3, 4, Some(f64::NEG_INFINITY)),
    ];
    let g = VecGraph::from_labeled_arc_list(arcs.iter().copied());
    let coo = ArcListGraph::new_labeled(g.num_nodes(), arcs.iter().copied());
    let g2 = VecGraph::from_labeled_lender(&coo);
    assert_eq!(g, g2);
    Ok(())
}
