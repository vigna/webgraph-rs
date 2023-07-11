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

    fn next(&mut self) -> Option<Self::Item> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.graph.successors(node_id)))
    }
}

/// A graph that can be accessed sequentially
pub trait SequentialGraph {
    /// Iterator over the nodes of the graph
    type NodesIter<'a>: Iterator<Item = (usize, Self::SequentialSuccessorIter<'a>)> + 'a
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

/// A graph where each node has a label
pub trait Labelled {
    /// The type of the label
    type LabelType;
}

/// A trait to allow to ask for the label of the current node on a successors
/// iterator
pub trait LabelledIterator: Labelled {
    /// Get the label of the current node, if it has one
    fn label() -> Option<Self::LabelType>;
}

/// A trait to constraint the successors iterator to implement [`LabelledIterator`]
pub trait LabelledSequentialGraph: SequentialGraph + Labelled
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
}
/// Blanket implementation
impl<G: SequentialGraph + Labelled + SortedNodes + SortedSuccessors> LabelledSequentialGraph for G where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>
{
}

/// A trait to constraint the successors iterator to implement [`LabelledIterator`]
pub trait LabelledRandomAccessGraph: RandomAccessGraph + Labelled
where
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
}
/// Blanket implementation
impl<G: RandomAccessGraph + Labelled + SortedNodes + SortedSuccessors> LabelledRandomAccessGraph
    for G
where
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
}

/// Marker trait for sequential graphs that enumerate nodes in increasing order
pub trait SortedNodes {}

/// Marker trait for graphs that enumerate nodes in increasing order
pub trait SortedSuccessors {}

/// A graph that can be accessed both sequentially and randomly,
/// and which enumerates nodes and successors in increasing order.
pub trait Graph: SequentialGraph + RandomAccessGraph + SortedNodes + SortedSuccessors {}
/// Blanket implementation
impl<G: SequentialGraph + RandomAccessGraph + SortedNodes + SortedSuccessors> Graph for G {}

/// The same as [`Graph`], but with a label on each node.
pub trait LabelledGraph:
    LabelledSequentialGraph + LabelledRandomAccessGraph + SortedNodes + SortedSuccessors
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
}
/// Blanket implementation
impl<G: LabelledSequentialGraph + LabelledRandomAccessGraph + SortedNodes + SortedSuccessors>
    LabelledGraph for G
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
}
