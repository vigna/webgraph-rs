/// A struct used to implement the [`SequentialGraph`] trait for a struct that
/// implements [`RandomAccessGraph`].
pub struct SequentialGraphImplIter<'a, G: RandomAccessGraph> {
    pub graph: &'a G,
    pub nodes: core::ops::Range<usize>,
}

impl<'a, G> Iterator for SequentialGraphImplIter<'a, G>
where
    G: RandomAccessGraph
        + SequentialGraph<SequentialSuccessorIter<'a> = G::RandomSuccessorIter<'a>>,
{
    type Item = (usize, G::RandomSuccessorIter<'a>);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.graph.successors(node_id)))
    }
}

impl<'a, G> ExactSizeIterator for SequentialGraphImplIter<'a, G>
where
    G: RandomAccessGraph
        + SequentialGraph<SequentialSuccessorIter<'a> = G::RandomSuccessorIter<'a>>,
{
    fn len(&self) -> usize {
        self.graph.num_nodes()
    }
}

/// We iter on the node ids in a range so it is sorted
unsafe impl<'a, G: RandomAccessGraph> SortedIterator for SequentialGraphImplIter<'a, G> {}

/// A graph that can be accessed sequentially
pub trait SequentialGraph {
    /// Iterator over the nodes of the graph
    type NodesIter<'a>: ExactSizeIterator<Item = (usize, Self::SequentialSuccessorIter<'a>)> + 'a
    where
        Self: 'a;
    /// Iterator over the successors of a node
    type SequentialSuccessorIter<'a>: Iterator<Item = usize> + 'a
    where
        Self: 'a;

    /// Get the number of nodes in the graph
    fn num_nodes(&self) -> usize;

    /// Get the number of arcs in the graph if available
    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    /// Get an iterator over the nodes of the graph
    fn iter_nodes(&self) -> Self::NodesIter<'_>;

    /// Get an iterator over the nodes of the graph starting at `start_node`
    /// (included)
    fn iter_nodes_from(&self, start_node: usize) -> Self::NodesIter<'_> {
        let mut iter = self.iter_nodes();
        for _ in 0..start_node {
            iter.next();
        }
        iter
    }
}

/// A graph that can be accessed randomly
pub trait RandomAccessGraph: SequentialGraph {
    /// Iterator over the successors of a node
    type RandomSuccessorIter<'a>: ExactSizeIterator<Item = usize> + 'a
    where
        Self: 'a;

    /// Get the number of arcs in the graph
    fn num_arcs(&self) -> usize;

    /// Get a sorted iterator over the neighbours node_id
    fn successors(&self, node_id: usize) -> Self::RandomSuccessorIter<'_>;

    /// Get the number of outgoing edges of a node
    fn outdegree(&self, node_id: usize) -> usize {
        self.successors(node_id).count()
    }

    /// Return if the given edge `src_node_id -> dst_node_id` exists or not
    fn has_arc(&self, src_node_id: usize, dst_node_id: usize) -> bool {
        for neighbour_id in self.successors(src_node_id) {
            // found
            if neighbour_id == dst_node_id {
                return true;
            }
            // early stop
            if neighbour_id > dst_node_id {
                return false;
            }
        }
        false
    }
}

/// A graph where each arc has a label
pub trait Labelled {
    /// The type of the label on the arcs
    type Label;
}

/// A trait to allow to ask for the label of the current node on a successors
/// iterator
pub trait LabelledIterator: Labelled + Iterator<Item = usize> {
    /// Get the label of the current node, this panics if called before the first
    fn label(&self) -> Self::Label;

    /// Wrap the `Self` into a [`LabelledIteratorWrapper`] to be able to iter
    /// on `(successor, label)` easily
    #[inline(always)]
    fn labelled(self) -> LabelledIteratorWrapper<Self>
    where
        Self: Sized,
    {
        LabelledIteratorWrapper(self)
    }
}

/// A trait to constraint the successors iterator to implement [`LabelledIterator`]
pub trait LabelledSequentialGraph: SequentialGraph + Labelled
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: SequentialGraph + Labelled> LabelledSequentialGraph for G where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<Label = Self::Label>
{
}

/// A trait to constraint the successors iterator to implement [`LabelledIterator`]
pub trait LabelledRandomAccessGraph: RandomAccessGraph + Labelled
where
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: RandomAccessGraph + Labelled> LabelledRandomAccessGraph for G where
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<Label = Self::Label>
{
}

/// Marker trait iterators that return sorted values
///
/// # Safety
/// The values returned by the iterator must be sorted, putting this iterator on
/// a not sorted iterator will result in undefined behavior
pub unsafe trait SortedIterator {}

/// A graph that can be accessed both sequentially and randomly,
/// and which enumerates nodes and successors in increasing order.
pub trait Graph: SequentialGraph + RandomAccessGraph
where
    for<'a> Self::SequentialSuccessorIter<'a>: SortedIterator,
    for<'a> Self::RandomSuccessorIter<'a>: SortedIterator,
    for<'a> Self::NodesIter<'a>: SortedIterator,
{
}
/// Blanket implementation
impl<G: SequentialGraph + RandomAccessGraph> Graph for G
where
    for<'a> Self::SequentialSuccessorIter<'a>: SortedIterator,
    for<'a> Self::RandomSuccessorIter<'a>: SortedIterator,
    for<'a> Self::NodesIter<'a>: SortedIterator,
{
}

/// The same as [`Graph`], but with a label on each node.
pub trait LabelledGraph: LabelledSequentialGraph + LabelledRandomAccessGraph
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<Label = Self::Label>,
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<Label = Self::Label>,
{
}
/// Blanket implementation
impl<G: LabelledSequentialGraph + LabelledRandomAccessGraph> LabelledGraph for G
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<Label = Self::Label>,
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<Label = Self::Label>,
{
}

#[repr(transparent)]
/// A wrapper around a [`LabelledIterator`] to make it implement [`Iterator`]
/// with a tuple of `(successor, label)`
pub struct LabelledIteratorWrapper<I: LabelledIterator + Iterator<Item = usize>>(I);

impl<I: LabelledIterator + Iterator<Item = usize>> Iterator for LabelledIteratorWrapper<I> {
    type Item = (usize, I::Label);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|successor| (successor, self.0.label()))
    }
    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<I: LabelledIterator + Iterator<Item = usize> + ExactSizeIterator> ExactSizeIterator
    for LabelledIteratorWrapper<I>
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// We are transparent regarding the sortedness of the underlying iterator
unsafe impl<I: LabelledIterator + Iterator<Item = usize> + SortedIterator> SortedIterator
    for LabelledIteratorWrapper<I>
{
}
