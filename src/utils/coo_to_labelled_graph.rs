use crate::traits::*;
use core::marker::PhantomData;
use core::mem::MaybeUninit;

/// A Sequential graph built on an iterator of pairs of nodes and their labels
#[derive(Debug, Clone)]
pub struct COOIterToLabelledGraph<I: Clone> {
    num_nodes: usize,
    iter: I,
}

impl<L: Clone + 'static, I: Iterator<Item = (usize, usize, L)> + Clone> COOIterToLabelledGraph<I> {
    /// Create a new graph from an iterator of pairs of nodes
    #[inline(always)]
    pub fn new(num_nodes: usize, iter: I) -> Self {
        Self { num_nodes, iter }
    }
}

impl<L: Clone + 'static, I: Iterator<Item = (usize, usize, L)> + Clone> SequentialGraph
    for COOIterToLabelledGraph<I>
{
    type NodesIter<'b> = SortedLabelledNodePermutedIterator<'b, L, I> where Self: 'b;
    type SequentialSuccessorIter<'b> = SortedLabelledSequentialPermutedIterator<'b, L, I> where Self: 'b;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    #[inline(always)]
    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        let mut iter = self.iter.clone();
        SortedLabelledNodePermutedIterator {
            num_nodes: self.num_nodes,
            curr_node: 0_usize.wrapping_sub(1), // No node seen yet
            next_pair: iter.next().unwrap_or((usize::MAX, usize::MAX, unsafe {
                #[allow(clippy::uninit_assumed_init)]
                MaybeUninit::uninit().assume_init()
            })),
            label: unsafe {
                #[allow(clippy::uninit_assumed_init)]
                MaybeUninit::uninit().assume_init()
            },
            iter,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SortedLabelledNodePermutedIterator<'a, L, I: Iterator<Item = (usize, usize, L)>> {
    num_nodes: usize,
    curr_node: usize,
    next_pair: (usize, usize, L),
    label: L,
    iter: I,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, L, I: Iterator<Item = (usize, usize, L)>> Iterator
    for SortedLabelledNodePermutedIterator<'a, L, I>
{
    type Item = (usize, SortedLabelledSequentialPermutedIterator<'a, L, I>);
    fn next(&mut self) -> Option<Self::Item> {
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
            SortedLabelledSequentialPermutedIterator {
                node_iter_ptr: {
                    let self_ptr: *mut Self = self;
                    self_ptr
                },
            },
        ))
    }
}

#[derive(Debug, Clone)]
/// Iter until we found a triple with src different than curr_node
pub struct SortedLabelledSequentialPermutedIterator<'a, L, I: Iterator<Item = (usize, usize, L)>> {
    node_iter_ptr: *mut SortedLabelledNodePermutedIterator<'a, L, I>,
}

impl<'a, L, I: Iterator<Item = (usize, usize, L)>> Iterator
    for SortedLabelledSequentialPermutedIterator<'a, L, I>
{
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        let node_iter = unsafe { &mut *self.node_iter_ptr };
        // if we reached a new node, the successors of curr_node are finished
        if node_iter.next_pair.0 != node_iter.curr_node {
            None
        } else {
            // get the next triple
            let pair = node_iter
                .iter
                .next()
                .unwrap_or((usize::MAX, usize::MAX, unsafe {
                    #[allow(clippy::uninit_assumed_init)]
                    MaybeUninit::uninit().assume_init()
                }));
            // store the triple and return the previous successor
            // storing the label since it should be one step behind the successor
            let (_src, dst, label) = core::mem::replace(&mut node_iter.next_pair, pair);
            node_iter.label = label;
            Some(dst)
        }
    }
}

impl<'a, L, I: Iterator<Item = (usize, usize, L)>> Labelled
    for SortedLabelledSequentialPermutedIterator<'a, L, I>
{
    type Label = L;
}

impl<'a, L: Clone, I: Iterator<Item = (usize, usize, L)>> LabelledIterator
    for SortedLabelledSequentialPermutedIterator<'a, L, I>
{
    #[inline(always)]
    fn label(&self) -> Self::Label {
        let node_iter = unsafe { &*self.node_iter_ptr };
        if node_iter.curr_node == usize::MAX {
            panic!("You cannot call label() on an iterator that has not been advanced yet!");
        }
        node_iter.label.clone()
    }
}

#[cfg(test)]
#[cfg_attr(test, test)]
fn test_coo_labelled_iter() -> anyhow::Result<()> {
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
    let coo = COOIterToLabelledGraph::new(g.num_nodes(), arcs.clone().into_iter());
    let g2 = VecGraph::from_labelled_node_iter(coo.iter_nodes());
    assert_eq!(g, g2);
    Ok(())
}
