/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::*;
use core::mem::MaybeUninit;
use lender::*;

/// An adapter exhibiting a list of labeled
/// arcs sorted by source as a [labeled sequential graph](LabelledSequentialGraph).
///
/// If for every source the arcs are sorted by destination, the
/// successors of the graph will be sorted.
#[derive(Debug, Clone)]
pub struct LabeledArcListGraph<I: Clone> {
    num_nodes: usize,
    into_iter: I,
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone + 'static>
    LabeledArcListGraph<I>
{
    #[inline(always)]
    pub fn new(num_nodes: usize, iter: I) -> Self {
        Self {
            num_nodes,
            into_iter: iter,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeIterator<L, I: IntoIterator<Item = (usize, usize, L)>> {
    num_nodes: usize,
    curr_node: usize,
    next_pair: (usize, usize, L),
    iter: I::IntoIter,
}

unsafe impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone + 'static>
    SortedIterator for NodeIterator<L, I>
{
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)>> NodeIterator<L, I> {
    pub fn new(num_nodes: usize, mut iter: I::IntoIter) -> Self {
        NodeIterator {
            num_nodes,
            curr_node: 0_usize.wrapping_sub(1), // No node seen yet
            next_pair: iter.next().unwrap_or((usize::MAX, usize::MAX, unsafe {
                #[allow(clippy::uninit_assumed_init)]
                // TODO: why only here?
                MaybeUninit::uninit().assume_init()
            })),
            iter,
        }
    }
}

impl<'succ, L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone + 'static>
    NodeLabelsLending<'succ> for NodeIterator<L, I>
{
    type Item = (usize, L);
    type IntoIterator = Successors<'succ, L, I>;
}

impl<'succ, L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone + 'static>
    Lending<'succ> for NodeIterator<L, I>
{
    type Lend = (usize, <Self as NodeLabelsLending<'succ>>::IntoIterator);
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone + 'static> Lender
    for NodeIterator<L, I>
{
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        self.curr_node = self.curr_node.wrapping_add(1);
        if self.curr_node == self.num_nodes {
            return None;
        }

        // This happens if the user doesn't use the successors iter
        while self.next_pair.0 < self.curr_node {
            self.next_pair = self.iter.next().unwrap_or((usize::MAX, usize::MAX, unsafe {
                #[allow(clippy::uninit_assumed_init)]
                MaybeUninit::uninit().assume_init()
            }));
        }

        Some((
            self.curr_node,
            Successors {
                node_iter: { self },
            },
        ))
    }
}

impl<'lend, L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone + 'static>
    Lending<'lend> for &LabeledArcListGraph<I>
{
    type Lend = (usize, Successors<'lend, L, I>);
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone + 'static> IntoLender
    for &LabeledArcListGraph<I>
{
    type Lender = NodeIterator<L, I>;

    fn into_lender(self) -> Self::Lender {
        self.iter()
    }
}

impl<L: Clone + 'static, I: IntoIterator<Item = (usize, usize, L)> + Clone + 'static>
    SequentialLabelling for LabeledArcListGraph<I>
{
    type Label = (usize, L);
    type Iterator<'node> = NodeIterator<L, I> where Self: 'node;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    #[inline(always)]
    fn iter_from(&self, from: usize) -> Self::Iterator<'_> {
        let mut iter = NodeIterator::new(self.num_nodes, self.into_iter.clone().into_iter());
        for _ in 0..from {
            iter.next();
        }

        iter
    }
}

/// Iter until we found a triple with src different than curr_node
pub struct Successors<'succ, L, I: IntoIterator<Item = (usize, usize, L)>> {
    node_iter: &'succ mut NodeIterator<L, I>,
}

impl<'a, L, I: IntoIterator<Item = (usize, usize, L)>> Iterator for Successors<'a, L, I> {
    type Item = (usize, L);
    fn next(&mut self) -> Option<Self::Item> {
        // if we reached a new node, the successors of curr_node are finished
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
/*
#[cfg(test)]
#[cfg_attr(test, test)]
fn test_coo_labeled_iter() -> anyhow::Result<()> {
    use crate::graph::vec_graph::VecGraph;
    let arcs = vec![
        (0, 1, Some(1.0)),
        (0, 2, None),
        (1, 2, Some(2.0)),
        // the labels should never be read :)
        (1, 3, Some(f64::NAN)),
        (2, 4, Some(f64::INFINITY)),
        (3, 4, Some(f64::NEG_INFINITY)),
    ];
    let g = VecGraph::from_arc_and_label_list(&arcs);
    let coo = LabeledArcListGraph::new(g.num_nodes(), arcs);
    let g2 = VecGraph::from_labeled_node_iter::<&LabeledArcListGraph<_>>(&coo);
    assert_eq!(g, g2);
    Ok(())
}
*/
