use anyhow::Result;

pub struct SequentialGraphImplIter<'a, G: RandomAccessGraph> {
    graph: &'a G,
    nodes: core::ops::Range<usize>,
}

impl<'a, G: RandomAccessGraph> Iterator for SequentialGraphImplIter<'a, G> {
    type Item = (usize, G::RandomSuccessorIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.nodes
            .next()
            .map(|node_id| (node_id, self.graph.successors(node_id).unwrap()))
    }
}

/// Marker trait to inherit the default implementation of [`SequentialGraph`]
/// if your struct implements [`RandomAccessGraph`]. This can be avoided when
/// the specialization feature becomes stable.
pub trait SequentialGraphImpl: RandomAccessGraph {}

impl<T: SequentialGraphImpl> SequentialGraph for T {
    type NodesIter<'a> =  SequentialGraphImplIter<'a, Self>
        where
            Self: 'a;

    type SequentialSuccessorIter<'a> = <Self as RandomAccessGraph>::RandomSuccessorIter<'a>
        where
            Self: 'a;

    fn num_arcs_hint(&self) -> Option<usize> {
        Some(self.num_arcs())
    }

    fn iter_nodes(&self) -> Self::NodesIter<'_> {
        SequentialGraphImplIter {
            graph: self,
            nodes: (0..self.num_nodes()),
        }
    }
}

/// Made to avoid ambiguity when calling num_nodes on a struct that implements
/// both [`SequentialGraph`] and [`RandomAccessGraph`].
pub trait NumNodes {
    fn num_nodes(&self) -> usize;
}

// A graph that can be accessed sequentially
pub trait SequentialGraph: NumNodes {
    type NodesIter<'a>: Iterator<Item = (usize, Self::SequentialSuccessorIter<'a>)> + 'a
    where
        Self: 'a;
    type SequentialSuccessorIter<'a>: ExactSizeIterator<Item = usize> + 'a
    where
        Self: 'a;

    fn num_arcs_hint(&self) -> Option<usize> {
        None
    }

    fn iter_nodes(&self) -> Self::NodesIter<'_>;
}

pub trait Labelled {
    type LabelType;
}

pub trait LabelledIterator: Labelled {
    fn label() -> Option<Self::LabelType>;
}

pub trait LabelledSequentialGraph: SequentialGraph + Labelled
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
}
// A graph that can be accessed randomly
pub trait RandomAccessGraph: NumNodes {
    type RandomSuccessorIter<'a>: ExactSizeIterator<Item = usize> + 'a
    where
        Self: 'a;

    fn num_arcs(&self) -> usize;

    /// Get a sorted iterator over the neighbours node_id
    fn successors(&self, node_id: usize) -> Result<Self::RandomSuccessorIter<'_>>;

    /// Get the number of outgoing edges of a node
    fn outdegree(&self, node_id: usize) -> Result<usize> {
        Ok(self.successors(node_id)?.count())
    }

    /// Return if the given edge `src_node_id -> dst_node_id` exists or not
    fn has_arc(&self, src_node_id: usize, dst_node_id: usize) -> Result<bool> {
        for neighbour_id in self.successors(src_node_id)? {
            // found
            if neighbour_id == dst_node_id {
                return Ok(true);
            }
            // early stop
            if neighbour_id > dst_node_id {
                return Ok(false);
            }
        }
        Ok(false)
    }
}

pub trait LabelledRandomAccessGraph: RandomAccessGraph + Labelled
where
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
    type LabelledRandomSuccessorIter<'a>: Iterator<Item = (usize, Self::LabelType)> + 'a
    where
        Self: 'a;
}

// Marker trait for sequential graphs that enumerate nodes in increasing order
pub trait SortedNodes {}

// Marker trait for graphs that enumerate nodes in increasing order
pub trait SortedSuccessors {}

// A graph that can be accessed both sequentially and randomly,
// and which enumerates nodes and successors in increasing order.
pub trait Graph: SequentialGraph + RandomAccessGraph + SortedNodes + SortedSuccessors {}
impl<G: SequentialGraph + RandomAccessGraph + SortedNodes + SortedSuccessors> Graph for G {}

pub trait LabelledGraph:
    LabelledSequentialGraph + LabelledRandomAccessGraph + SortedNodes + SortedSuccessors
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
}

impl<G: LabelledSequentialGraph + LabelledRandomAccessGraph + SortedNodes + SortedSuccessors>
    LabelledGraph for G
where
    for<'a> Self::SequentialSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
    for<'a> Self::RandomSuccessorIter<'a>: LabelledIterator<LabelType = Self::LabelType>,
{
}
