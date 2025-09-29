/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{labels::Left, traits::*};
use lender::*;

/// An adapter exhibiting a list of labeled arcs sorted by source as a [labeled
/// sequential graph](LabeledSequentialGraph).
///
/// If for every source the arcs are sorted by destination, the successors of
/// the graph will be sorted.
///
/// The structure [`Iter`] implementing the [`Lender`] returned by the
/// [`iter`](SequentialLabeling::iter) method of this graph can be [built
/// independently](Iter::new). This is useful in circumstances in which one has
/// a list of arcs sorted by source that represent only part of a graph, but
/// need to exhibit them has a [`NodeLabelsLender`], for example, for feeding
/// such lenders to
/// [`parallel_iter`](crate::graphs::bvgraph::BvComp::parallel_iter).
#[derive(Clone)]
pub struct ArcListGraph<I: Clone> {
    num_nodes: usize,
    into_iter: I,
}

impl<L: Clone + Copy + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> ArcListGraph<I> {
    /// Creates a new arc-list graph from the given [`IntoIterator`].
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
    /// Creates a new arc-list graph from the given [`IntoIterator`].
    ///
    /// # Implementation Notes
    ///
    /// Note that the resulting graph will be an arc-list graph labeled by the
    /// unit type `()` wrapped into a  [left
    /// projection](crate::prelude::proj::Left).
    #[inline(always)]
    pub fn new(num_nodes: usize, iter: impl IntoIterator<IntoIter = I>) -> Left<Self> {
        Left(Self {
            num_nodes,
            into_iter: iter.into_iter().map(|(src, dst)| (src, dst, ())),
        })
    }
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> SplitLabeling
    for ArcListGraph<I>
where
    <I as IntoIterator>::IntoIter: Clone + Send + Sync,
    L: Send + Sync,
{
    type SplitLender<'a>
        = split::seq::Lender<'a, ArcListGraph<I>>
    where
        Self: 'a;
    type IntoIterator<'a>
        = split::seq::IntoIterator<'a, ArcListGraph<I>>
    where
        Self: 'a;

    fn split_iter(&self, how_many: usize) -> Self::IntoIterator<'_> {
        split::seq::Iter::new(self.iter(), self.num_nodes(), how_many)
    }
}

#[derive(Clone)]
pub struct Iter<L, I: IntoIterator<Item = (usize, usize, L)>> {
    num_nodes: usize,
    /// The next node that will be returned by the lender.
    next_node: usize,
    iter: core::iter::Peekable<I::IntoIter>,
}

unsafe impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> SortedLender
    for Iter<L, I>
{
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)>> Iter<L, I> {
    pub fn new(num_nodes: usize, iter: I::IntoIter) -> Self {
        Iter {
            num_nodes,
            next_node: 0,
            iter: iter.peekable(),
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
        if self.next_node == self.num_nodes {
            return None;
        }

        // Discard residual arcs from the previous iteration
        while let Some(&(node, _, _)) = self.iter.peek() {
            if node >= self.next_node {
                break;
            }
            assert!(node == self.next_node - 1);
            self.iter.next();
        }

        let src = self.next_node;
        self.next_node += 1;
        Some((src, Succ { node_iter: self }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone> ExactSizeLender
    for Iter<L, I>
{
    fn len(&self) -> usize {
        self.num_nodes - self.next_node
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
    type Lender<'node>
        = Iter<L, I>
    where
        Self: 'node;

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

pub struct Succ<'succ, L, I: IntoIterator<Item = (usize, usize, L)>> {
    node_iter: &'succ mut Iter<L, I>,
}

unsafe impl<L, I: IntoIterator<Item = (usize, usize, L)>> SortedIterator for Succ<'_, L, I> where
    I::IntoIter: SortedIterator
{
}

impl<L, I: IntoIterator<Item = (usize, usize, L)>> Iterator for Succ<'_, L, I> {
    type Item = (usize, L);
    fn next(&mut self) -> Option<Self::Item> {
        // If the next pair is not there, or it has a different source, we are done
        if self.node_iter.iter.peek()?.0 >= self.node_iter.next_node {
            return None;
        }
        // get the next triple
        let triple = self.node_iter.iter.next();
        // Peek already checks this and the compiler doesn't seem to optimize it out
        // so we use unwrap_unchecked here.
        let (curr, succ, label) = unsafe { triple.unwrap_unchecked() };
        // Here `next_node` is one beyond the node whose successors we are returning
        assert_eq!(curr, self.node_iter.next_node - 1);
        Some((succ, label))
    }
}
