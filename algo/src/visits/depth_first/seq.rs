/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::visits::{
    Sequential,
    depth_first::{EventNoPred, EventPred, FilterArgsNoPred, FilterArgsPred},
};
use sealed::sealed;
use std::ops::ControlFlow::{self, Continue};
use sux::bits::BitVec;
use webgraph::traits::{RandomAccessGraph, RandomAccessLabeling};

/// A depth-first visit which does not keep track of predecessors, or nodes on the stack.
pub type SeqNoPred<'a, G> = SeqIter<'a, TwoStates, G, (), false>;

/// A depth-first visit which keeps track of predecessors, but not nodes on the stack.
pub type SeqPred<'a, G> = SeqIter<'a, TwoStates, G, usize, true>;

/// A depth-first visit which keeps track of predecessors and nodes on the stack.
pub type SeqPath<'a, G> = SeqIter<'a, ThreeStates, G, usize, true>;

/// Sequential depth-first visits.
///
/// This is an iterative implementation that does not need a large stack size.
///
/// There are three version of the visit, which are type aliases to the same
/// common implementation: [`SeqNoPred`], [`SeqPred`] and [`SeqPath`] (the
/// generic implementation should not be instantiated by the user).
///
/// * [`SeqNoPred`] does not keep track of predecessors, nor of nodes on the
///   stack; it can be used, for example, to compute reachability information.
/// * [`SeqPred`] keeps track of predecessors, but not of nodes on the stack; it
///   can be used, for example, to compute a [topological
///   sort](crate::algo::top_sort()).
/// * [`SeqPath`] keeps track of predecessors and nodes on the stack; it can be
///   used, for example, to establish [acyclicity](crate::algo::acyclicity()).
///
/// Each type of visit uses incrementally more space:
/// * [`SeqNoPred`] uses one bit per node to remember known nodes and a stack of
///   iterators, one for each node on the visit path.
/// * [`SeqPred`] uses one bit per node to remember known nodes and a stack of
///   pairs made of an iterator and a predecessor, one for each node on the
///   visit path.
/// * [`SeqPath`] uses two bits per node to remember known nodes and whether the
///   node is on the visit path, and a stack of pairs made of an iterator and a
///   predecessor, one for each node on the visit path.
///
/// The visits differ also in the type of events they generate:
/// * [`SeqNoPred`] generates events of type [`EventNoPred`].
/// * [`SeqPred`] generates events of type [`EventPred`], with the proviso that
///   the Boolean associated with events of type
///   [`Revisit`](`EventPred::Revisit`) is always false.
/// * [`SeqPath`] generates events of type [`EventPred`].
///
/// With respect to [`EventNoPred`], [`EventPred`] provides the predecessor of the
/// current node and a [postvisit event](EventPred::Postvisit).
///
/// If the visit was interrupted, the nodes still on the visit path can be
/// retrieved using the [`stack`](SeqPred::stack) method (only for [`SeqPred`] and
/// [`SeqPath`]).
///
/// # Examples
///
/// Let's test acyclicity:
///
/// ```
/// use webgraph_algo::visits::*;
/// use webgraph_algo::visits::depth_first::*;
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph::traits::SequentialLabeling;
/// use webgraph::labels::proj::Left;
/// use std::ops::ControlFlow::*;
///
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
/// let mut visit = depth_first::SeqPath::new(&graph);
///
/// assert!(visit.visit(
///     0..graph.num_nodes(),
///     |event| {
///         match event {
///             // Stop the visit as soon as a back edge is found
///             EventPred::Revisit { on_stack: true, .. } => Break(StoppedWhenDone),
///             _ => Continue(()),
///         }
///     },
/// ).is_break()); // As the graph is not acyclic
/// ```
///
/// Or, assuming the input is acyclic, let us compute the reverse of a
/// topological sort:
///
/// ```
/// use webgraph_algo::visits::*;
/// use webgraph_algo::visits::depth_first::*;
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph::labels::proj::Left;
/// use webgraph::traits::labels::SequentialLabeling;
/// use std::ops::ControlFlow::Continue;
/// use no_break::NoBreak;
///
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (1, 3), (0, 3)]);
/// let mut visit = depth_first::SeqPred::new(&graph);
/// let mut top_sort = Vec::with_capacity(graph.num_nodes());
///
/// visit.visit(
///     0..graph.num_nodes(),
///     |event| {
///         if let EventPred::Postvisit { node, .. } = event {
///             top_sort.push(node);
///         }
///         Continue(())
///     }
/// ).continue_value_no_break();
/// ```
pub struct SeqIter<'a, S, G: RandomAccessGraph, P, const PRED: bool> {
    graph: &'a G,
    /// Entries on this stack represent the iterator on the successors of a node
    /// and the parent of the node. This approach makes it possible to avoid
    /// storing both the current and the parent node in the stack.
    stack: Vec<(
        <<G as RandomAccessLabeling>::Labels<'a> as IntoIterator>::IntoIter,
        P,
    )>,
    state: S,
    // General depth-first visit implementation. The user shouldn't see this.
    // Allowed combinations for `PRED`, `S` and `P` are:
    // * `false`, `TwoStates` and `()` (no predecessors, no stack tracking)
    // * `true`, `TwoStates` and `usize` (predecessors, no stack tracking)
    // * `true`, `ThreeStates` and `usize` (predecessors, stack tracking)
}

/// The iterator returned by [`stack`](SeqPred::stack).
pub struct StackIterator<'a, 'b, S, G: RandomAccessGraph> {
    visit: &'b mut SeqIter<'a, S, G, usize, true>,
}

impl<S, G: RandomAccessGraph> Iterator for StackIterator<'_, '_, S, G> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        // Since we put predecessors on the stack, the
        // first two stack entries are equal to the root,
        // so we avoid to return the first one
        if self.visit.stack.len() <= 1 {
            return None;
        }
        self.visit.stack.pop().map(|(_, parent)| parent)
    }
}

impl<'a, S: NodeStates, G: RandomAccessGraph, P, const PRED: bool> SeqIter<'a, S, G, P, PRED> {
    /// Creates a new sequential visit.
    ///
    /// # Arguments
    /// * `graph`: an immutable reference to the graph to visit.
    pub fn new(graph: &'a G) -> SeqIter<'a, S, G, P, PRED> {
        let num_nodes = graph.num_nodes();
        Self {
            graph,
            stack: Vec::with_capacity(16),
            state: S::new(num_nodes),
        }
    }
}

impl<'a, S, G: RandomAccessGraph> SeqIter<'a, S, G, usize, true> {
    /// Returns an iterator over the nodes still on the visit path,
    /// except for the last one.
    ///
    /// Node will be returned in reverse order of visit.
    ///
    /// This method is useful only in the case of interrupted visits,
    /// as in a completed visit the stack will be empty. The last node
    /// on the visit path at the moment of the interruption must be
    /// treated separately.
    pub fn stack(&mut self) -> StackIterator<'a, '_, S, G> {
        StackIterator { visit: self }
    }
}

#[doc(hidden)]
#[sealed]
pub trait NodeStates {
    fn new(n: usize) -> Self;
    fn set_on_stack(&mut self, node: usize);
    fn set_off_stack(&mut self, node: usize);
    fn on_stack(&self, node: usize) -> bool;
    fn set_known(&mut self, node: usize);
    fn known(&self, node: usize) -> bool;
    fn reset(&mut self);
}

#[doc(hidden)]
/// A two-state selector type for [sequential depth-first visits](Seq).
///
/// This implementation does not keep track of nodes on the stack, so events of
/// type [`Revisit`](`EventPred::Revisit`) will always have the associated
/// Boolean equal to false.
pub struct TwoStates(BitVec);

#[sealed]
impl NodeStates for TwoStates {
    fn new(n: usize) -> TwoStates {
        TwoStates(BitVec::new(n))
    }
    #[inline(always)]
    fn set_on_stack(&mut self, _node: usize) {}
    #[inline(always)]
    fn set_off_stack(&mut self, _node: usize) {}
    #[inline(always)]
    fn on_stack(&self, _node: usize) -> bool {
        false
    }
    #[inline(always)]
    fn set_known(&mut self, node: usize) {
        self.0.set(node, true);
    }
    #[inline(always)]
    fn known(&self, node: usize) -> bool {
        self.0.get(node)
    }
    #[inline(always)]
    fn reset(&mut self) {
        self.0.reset();
    }
}

#[doc(hidden)]
/// A three-state selector type for [sequential depth-first visits](Seq).
///
/// This implementation does keep track of nodes on the stack, so events of type
/// [`Revisit`](`EventPred::Revisit`) will provide information about whether the
/// node associated with event is currently on the visit path.
pub struct ThreeStates(BitVec);

#[sealed]
impl NodeStates for ThreeStates {
    fn new(n: usize) -> ThreeStates {
        ThreeStates(BitVec::new(2 * n))
    }
    #[inline(always)]
    fn set_on_stack(&mut self, node: usize) {
        self.0.set(node * 2 + 1, true);
    }
    #[inline(always)]
    fn set_off_stack(&mut self, node: usize) {
        self.0.set(node * 2 + 1, false);
    }
    #[inline(always)]
    fn on_stack(&self, node: usize) -> bool {
        self.0.get(node * 2 + 1)
    }
    #[inline(always)]
    fn set_known(&mut self, node: usize) {
        self.0.set(node * 2, true);
    }
    #[inline(always)]
    fn known(&self, node: usize) -> bool {
        self.0.get(node * 2)
    }
    #[inline(always)]
    fn reset(&mut self) {
        self.0.reset();
    }
}

impl<S: NodeStates, G: RandomAccessGraph> Sequential<EventPred> for SeqIter<'_, S, G, usize, true> {
    fn visit_filtered_with<
        R: IntoIterator<Item = usize>,
        T,
        E,
        C: FnMut(&mut T, EventPred) -> ControlFlow<E, ()>,
        F: FnMut(&mut T, FilterArgsPred) -> bool,
    >(
        &mut self,
        roots: R,
        mut init: T,
        mut callback: C,
        mut filter: F,
    ) -> ControlFlow<E, ()> {
        let state = &mut self.state;

        for root in roots {
            if state.known(root)
                || !filter(
                    &mut init,
                    FilterArgsPred {
                        node: root,
                        pred: root,
                        root,
                        depth: 0,
                    },
                )
            {
                // We ignore the node: it might be visited later
                continue;
            }

            callback(&mut init, EventPred::Init { root })?;

            state.set_known(root);
            callback(
                &mut init,
                EventPred::Previsit {
                    node: root,
                    pred: root,
                    root,
                    depth: 0,
                },
            )?;

            self.stack
                .push((self.graph.successors(root).into_iter(), root));

            state.set_on_stack(root);

            // This variable keeps track of the current node being visited; the
            // parent node is derived at each iteration of the 'recurse loop.
            let mut curr = root;

            'recurse: loop {
                let depth = self.stack.len();
                let Some((iter, parent)) = self.stack.last_mut() else {
                    callback(&mut init, EventPred::Done { root })?;
                    break;
                };

                for succ in iter {
                    // Check if node should be visited
                    if state.known(succ) {
                        // Node has already been discovered
                        callback(
                            &mut init,
                            EventPred::Revisit {
                                node: succ,
                                pred: curr,
                                root,
                                depth,
                                on_stack: state.on_stack(succ),
                            },
                        )?;
                    } else {
                        // First time seeing node
                        if filter(
                            &mut init,
                            FilterArgsPred {
                                node: succ,
                                pred: curr,
                                root,
                                depth,
                            },
                        ) {
                            state.set_known(succ);
                            callback(
                                &mut init,
                                EventPred::Previsit {
                                    node: succ,
                                    pred: curr,
                                    root,
                                    depth,
                                },
                            )?;
                            // current_node is the parent of succ
                            self.stack
                                .push((self.graph.successors(succ).into_iter(), curr));

                            state.set_on_stack(succ);

                            // At the next iteration, succ will be the current node
                            curr = succ;

                            continue 'recurse;
                        } // Else we ignore the node: it might be visited later
                    }
                }

                callback(
                    &mut init,
                    EventPred::Postvisit {
                        node: curr,
                        pred: *parent,
                        root,
                        depth: depth - 1,
                    },
                )?;

                state.set_off_stack(curr);

                // We're going up one stack level, so the next current_node
                // is the current parent.
                curr = *parent;
                self.stack.pop();
            }
        }

        Continue(())
    }

    fn reset(&mut self) {
        self.stack.clear();
        self.state.reset();
    }
}

impl<G: RandomAccessGraph> Sequential<EventNoPred> for SeqIter<'_, TwoStates, G, (), false> {
    fn visit_filtered_with<
        R: IntoIterator<Item = usize>,
        T,
        E,
        C: FnMut(&mut T, EventNoPred) -> ControlFlow<E, ()>,
        F: FnMut(&mut T, FilterArgsNoPred) -> bool,
    >(
        &mut self,
        roots: R,
        mut init: T,
        mut callback: C,
        mut filter: F,
    ) -> ControlFlow<E, ()> {
        let state = &mut self.state;

        for root in roots {
            if state.known(root)
                || !filter(
                    &mut init,
                    FilterArgsNoPred {
                        node: root,
                        root,
                        depth: 0,
                    },
                )
            {
                // We ignore the node: it might be visited later
                continue;
            }

            callback(&mut init, EventNoPred::Init { root })?;

            state.set_known(root);

            callback(
                &mut init,
                EventNoPred::Previsit {
                    node: root,
                    root,
                    depth: 0,
                },
            )?;

            self.stack
                .push((self.graph.successors(root).into_iter(), ()));

            'recurse: loop {
                let depth = self.stack.len();
                let Some((iter, _)) = self.stack.last_mut() else {
                    callback(&mut init, EventNoPred::Done { root })?;
                    break;
                };

                for succ in iter {
                    // Check if node should be visited
                    if state.known(succ) {
                        // Node has already been discovered
                        callback(
                            &mut init,
                            EventNoPred::Revisit {
                                node: succ,
                                root,
                                depth,
                            },
                        )?;
                    } else {
                        // First time seeing node
                        if filter(
                            &mut init,
                            FilterArgsNoPred {
                                node: succ,
                                root,
                                depth,
                            },
                        ) {
                            state.set_known(succ);
                            callback(
                                &mut init,
                                EventNoPred::Previsit {
                                    node: succ,
                                    root,
                                    depth,
                                },
                            )?;
                            // current_node is the parent of succ
                            self.stack
                                .push((self.graph.successors(succ).into_iter(), ()));

                            continue 'recurse;
                        } // Else we ignore the node: it might be visited later
                    }
                }

                // We're going up one stack level, so the next current_node
                // is the current parent.
                self.stack.pop();
            }
        }

        Continue(())
    }

    fn reset(&mut self) {
        self.stack.clear();
        self.state.reset();
    }
}
