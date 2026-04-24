/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Computes the radius and/or the diameter and/or all eccentricities of a
//! graph, using the ExactSumSweep algorithm.
//!
//! The algorithm has been described by Michele Borassi, Pierluigi Crescenzi,
//! Michel Habib, Walter A. Kosters, Andrea Marino, and Frank W. Takes in "[Fast
//! diameter and radius BFS-based computation in (weakly connected) real-world
//! graphs—With an application to the six degrees of separation
//! games][ExactSumSweep paper]", _Theoretical Computer Science_,
//! 586:59–80, 2015.
//!
//! # Definitions
//!
//! We define the _positive_, or _forward_ (resp., _negative_, or _backward_)
//! _eccentricity_ of a node _v_ in a graph _G_ = (_V_, _E_) as
//! ecc⁺(_v_) = max{_d_(_v_, _w_) : _w_ reachable from _v_} (resp.,
//! ecc⁻(_v_) = max{_d_(_w_, _v_) : _w_ reaches _v_}), where _d_(_v_, _w_) is
//! the number of arcs in a shortest path from _v_ to _w_. The _diameter_ is
//! max{ecc⁺(_v_) : _v_ ∈ _V_}, which is also equal to
//! max{ecc⁻(_v_) : _v_ ∈ _V_}, while the _radius_ is
//! min{ecc⁺(_v_) : _v_ ∈ _V_'}, where _V_' is a set of vertices specified by
//! the user. These definitions are slightly different from the standard ones due
//! to the restriction to reachable nodes. In particular, if we simply define the
//! radius as the minimum eccentricity, the radius of a graph containing a
//! vertex with out-degree 0 would be 0, and this does not make much sense. For
//! this reason, we restrict our attention only to a subset _V_' of the set of
//! all vertices: by choosing a suitable _V_', we can specialize this definition
//! to all definitions proposed in the literature. If _V_' is not specified, we
//! include in _V_' all vertices from which it is possible to reach the largest
//! strongly connected component, as suggested in the aforementioned paper.
//!
//! # Algorithm
//!
//! The algorithm performs some BFSs from "clever" vertices, and uses these BFSs
//! to bound the eccentricity of all vertices. More specifically, for each vertex
//! _v_, the algorithm keeps a lower and an upper bound on the forward and
//! backward eccentricity of _v_, named _lF_\[_v_\], _lB_\[_v_\],
//! _uF_\[_v_\], and _uB_\[_v_\]. Furthermore, it keeps a lower bound _dL_ on
//! the diameter and an upper bound _rU_ on the radius. At each step, the
//! algorithm performs a BFS and updates all these bounds: the radius is found as
//! soon as _rU_ is smaller than the minimum value of _lF_, and the diameter is
//! found as soon as _dL_ is bigger than _uF_\[_v_\] for each _v_, or _dL_ is
//! bigger than _uB_\[_v_\] for each _v_.
//!
//! More specifically, the upper bound on the radius (resp., lower bound on the
//! diameter) is defined as the minimum forward (resp., maximum forward or
//! backward) eccentricity of a vertex from which we performed a BFS. Moreover,
//! if we perform a forward (resp., backward) BFS from a vertex _s_, we update
//! _lB_\[_v_\] = max(_lB_\[_v_\], _d_(_s_, _v_)) (resp.,
//! _lF_\[_v_\] = max(_lF_\[_v_\], _d_(_v_, _s_))). Finally, for the upper
//! bounds, a more complicated procedure handles different strongly connected
//! components separately.
//!
//! # Performance
//!
//! Although the running time is _O_(_mn_) in the worst case, the algorithm is
//! usually much more efficient on real-world networks when only radius and
//! diameter are needed. It has been used, for example, on the [whole Facebook
//! graph][Facebook].
//!
//! If all eccentricities are needed, the algorithm could be faster than
//! _O_(_mn_), but in many networks it achieves performance similar to the
//! textbook algorithm that performs a breadth-first search from each node.
//!
//! # Memory requirements
//!
//! All large allocations are in `usize`. For the symmetric case the algorithm
//! permanently allocates three arrays of size _n_ (two for forward-eccentricity
//! bounds and one for the SCC component assignment), plus one array of size _n_
//! if `USE_TOT` is true. At the time of pivot computation, one more array of
//! size _n_ and two arrays of size equal to the number of components are
//! allocated. Thus, normal usage is three `usize` per node (plus one if
//! `USE_TOT` is true), while at peak there is one additional `usize` per
//! node and two per component.
//!
//! For the directed case, there are two additional arrays of size _n_ for the
//! backward-eccentricity bounds, plus one array of size _n_ if `USE_TOT` is
//! true, bringing the permanent allocation to five `usize` per node (plus two
//! if `USE_TOT` is true). Additionally, the DAG of strongly connected
//! components is stored permanently, using one `usize` per component for
//! offsets and three `usize` per DAG edge for the connection data. At peak, the
//! pivot computation allocates two arrays of size _n_ and three of size equal
//! to the number of components.
//!
//! # Usage
//!
//! Depending on what you intend to compute, you have to choose the right
//! [_level_] between [`All`], [`AllForward`], [`RadiusDiameter`],
//! [`Diameter`], and [`Radius`]. Then you have to invoke [`run`] or
//! [`run_symm`]. In the first case, you have to provide a graph and its
//! transpose; in the second case, you have to provide a symmetric graph. The
//! methods return a suitable structure containing the result of the
//! computation.
//!
//! [_level_]: Level
//! [`run`]: Level::run
//! [`run_symm`]: Level::run_symm
//!
//! [ExactSumSweep paper]: <https://doi.org/10.1016/j.tcs.2015.02.033>
//! [Facebook]: <https://doi.org/10.1145/2380718.2380723>
//!
//! # Examples
//!
//! ```
//! use webgraph_algo::distances::exact_sum_sweep::{self, *};
//! use dsi_progress_logger::no_logging;
//! use webgraph::graphs::vec_graph::VecGraph;
//! use webgraph::labels::proj::Left;
//!
//! let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
//! let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);
//!
//! // Let's compute all eccentricities
//! let result = <exact_sum_sweep::All as Level>::run(
//!     &graph,
//!     &transpose,
//!     None,
//!     no_logging![]
//! );
//!
//! assert_eq!(result.diameter, 4);
//! assert_eq!(result.radius, 3);
//! assert_eq!(result.forward_eccentricities.as_ref(), &vec![3, 3, 3, 4, 0]);
//! assert_eq!(result.backward_eccentricities.as_ref(), &vec![3, 3, 3, 3, 4]);
//!
//! // Let's just compute the radius and diameter
//! let result = <exact_sum_sweep::RadiusDiameter as Level>::run(
//!     &graph,
//!     &transpose,
//!     None,
//!     no_logging![]
//! );
//!
//! assert_eq!(result.diameter, 4);
//! assert_eq!(result.radius, 3);
//! ```
//!
//! Note how certain information is not available if not computed.
//! ```compile_fail
//! use webgraph_algo::distances::exact_sum_sweep::{self, *};
//! use dsi_progress_logger::no_logging;
//! use webgraph::graphs::vec_graph::VecGraph;
//!
//! let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0), (2, 4)]);
//! let transpose = VecGraph::from_arcs([(1, 0), (2, 1), (3, 2), (0, 3), (4, 2)]);
//!
//! let result = <exact_sum_sweep::RadiusDiameter as Level>::run(
//!     &graph,
//!     &transpose,
//!     None,
//!     no_logging![]
//! );
//!
//! assert_eq!(result.diameter, 4);
//! assert_eq!(result.radius, 3);
//! // Without these it would compile
//! assert_eq!(result.forward_eccentricities.as_ref(), &vec![3, 3, 3, 4, 0]);
//! assert_eq!(result.backward_eccentricities.as_ref(), &vec![3, 3, 3, 3, 4]);
//! ```
//!
//! If the graph is symmetric (i.e., undirected), you may use [`run_symm`].
//!
//! [`run_symm`]: Level::run_symm
//! ```
//! use webgraph_algo::distances::exact_sum_sweep::{self, *};
//! use dsi_progress_logger::no_logging;
//! use webgraph::graphs::vec_graph::VecGraph;
//!
//! let graph = VecGraph::from_arcs(
//!     [(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2), (3, 4), (4, 3)]
//! );
//!
//! let result = <exact_sum_sweep::RadiusDiameter as Level>::run_symm(
//!     &graph,
//!     no_logging![]
//! );
//!
//! assert_eq!(result.diameter, 1);
//! assert_eq!(result.radius, 1);
//! ```

mod level;
pub use level::*;

pub mod output;
pub mod output_symm;

mod scc_graph;

use crate::{
    distances::exact_sum_sweep::scc_graph::SccGraph,
    sccs::{self, Sccs},
    utils::math,
};
use crossbeam_utils::CachePadded;
use dsi_progress_logger::*;
use no_break::NoBreak;
use nonmax::NonMaxUsize;
use rayon::prelude::*;
use std::{
    iter::repeat_with,
    ops::ControlFlow::Continue,
    sync::{
        RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use sux::{bits::AtomicBitVec, traits::AtomicBitVecOps, utils::transmute_boxed_slice_from_atomic};
use sync_cell_slice::SyncSlice;
use webgraph::traits::RandomAccessGraph;
use webgraph::visits::{
    FilterArgs, Parallel,
    breadth_first::{EventNoPred, ParFairNoPred},
};

struct ExactSumSweep<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    V1: Parallel<EventNoPred> + Sync,
    V2: Parallel<EventNoPred> + Sync,
    OL: Level<USE_TOT>,
    const USE_TOT: bool,
> {
    pub graph: &'a G1,
    pub transpose: &'a G2,
    pub num_nodes: usize,
    pub radial_vertices: AtomicBitVec,
    /// The lower bound of the diameter.
    pub diameter_low: usize,
    /// The upper bound of the diameter.
    pub diameter_high: usize,
    /// The lower bound of the radius.
    pub radius_low: usize,
    /// The upper bound of the radius.
    pub radius_high: usize,
    /// A vertex whose eccentricity equals the diameter.
    pub diameter_vertex: usize,
    /// A vertex whose eccentricity equals the radius.
    pub radius_vertex: usize,
    /// Number of iterations performed until now.
    pub iterations: usize,
    /// The lower bound of the forward eccentricities.
    pub forward_low: Box<[usize]>,
    /// The upper bound of the forward eccentricities.
    pub forward_high: Box<[usize]>,
    /// The lower bound of the backward eccentricities.
    pub backward_low: Box<[usize]>,
    /// The upper bound of the backward eccentricities.
    pub backward_high: Box<[usize]>,
    /// Number of iterations before the radius was found.
    pub radius_iterations: Option<usize>,
    /// Number of iterations before the diameter was found.
    pub diameter_iterations: Option<usize>,
    /// Number of iterations before all forward eccentricities were found.
    pub forward_iter: Option<usize>,
    /// Number of iterations before all eccentricities were found.
    pub all_iter: Option<usize>,
    /// The strongly connected components.
    pub scc: Sccs,
    /// The strongly connected components diagram.
    pub scc_graph: SccGraph<G1, G2>,
    /// Total forward distance from already processed vertices (used as tie-break for the choice
    /// of the next vertex to process).
    pub forward_tot: Box<[usize]>,
    /// Total backward distance from already processed vertices (used as tie-break for the choice
    /// of the next vertex to process).
    pub backward_tot: Box<[usize]>,
    pub compute_radial_vertices: bool,
    pub symmetric: bool,
    pub visit: V1,
    pub transposed_visit: V2,
    _marker: std::marker::PhantomData<OL>,
}

impl<'a, G: RandomAccessGraph + Sync, OL: Level<USE_TOT>, const USE_TOT: bool>
    ExactSumSweep<'a, G, G, ParFairNoPred<&'a G>, ParFairNoPred<&'a G>, OL, USE_TOT>
{
    /// Builds a new instance to compute the *ExactSumSweep* algorithm on
    /// symmetric (i.e., undirected) graphs.
    pub(super) fn new_symm(graph: &'a G, pl: &mut impl ProgressLog) -> Self {
        // TODO debug_assert!(check_symmetric(graph), "graph should be symmetric");
        let scc = sccs::symm_par(graph, &mut pl.concurrent());
        pl.info(format_args!(
            "Number of connected components: {}",
            scc.num_components(),
        ));
        let visit = ParFairNoPred::new(graph);
        let transposed_visit = ParFairNoPred::new(graph);

        Self::_new(
            graph,
            graph,
            true,
            None,
            scc,
            SccGraph::default(),
            visit,
            transposed_visit,
            pl,
        )
    }
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    OL: Level<USE_TOT>,
    const USE_TOT: bool,
> ExactSumSweep<'a, G1, G2, ParFairNoPred<&'a G1>, ParFairNoPred<&'a G2>, OL, USE_TOT>
{
    /// Builds a new instance to compute the *ExactSumSweep* algorithm on
    /// directed graphs.
    ///
    /// # Arguments
    /// * `graph` - the directed graph.
    /// * `transpose` - the transpose of `graph`.
    /// * `output` - the desired output of the algorithm.
    /// * `radial_vertices` - an [`AtomicBitVec`] where `v[i]` is true if node
    ///   `i` is to be considered radial vertex. If [`None`] the algorithm will
    ///   use the largest connected component.
    /// * `pl` - a progress logger.
    pub(super) fn new(
        graph: &'a G1,
        transpose: &'a G2,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ProgressLog,
    ) -> Self {
        assert_eq!(
            graph.num_nodes(),
            transpose.num_nodes(),
            "the graph has {} nodes, but the transpose has {} nodes",
            graph.num_nodes(),
            transpose.num_nodes()
        );
        assert_eq!(
            graph.num_arcs(),
            transpose.num_arcs(),
            "the graph has {} arcs, but the transpose has {} arcs",
            graph.num_arcs(),
            transpose.num_arcs()
        );

        let scc = sccs::tarjan(graph, pl);
        pl.info(format_args!(
            "Number of strongly connected components: {}",
            scc.num_components(),
        ));
        let scc_graph = SccGraph::new(graph, transpose, &scc, pl);
        let visit = ParFairNoPred::new(graph);
        let transposed_visit = ParFairNoPred::new(transpose);

        Self::_new(
            graph,
            transpose,
            false,
            radial_vertices,
            scc,
            scc_graph,
            visit,
            transposed_visit,
            pl,
        )
    }
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    V1: Parallel<EventNoPred> + Sync,
    V2: Parallel<EventNoPred> + Sync,
    OL: Level<USE_TOT>,
    const USE_TOT: bool,
> ExactSumSweep<'a, G1, G2, V1, V2, OL, USE_TOT>
{
    #[allow(clippy::too_many_arguments)]
    fn _new(
        graph: &'a G1,
        transpose: &'a G2,
        symmetric: bool,
        radial_vertices: Option<AtomicBitVec>,
        scc: Sccs,
        scc_graph: SccGraph<G1, G2>,
        visit: V1,
        transposed_visit: V2,
        pl: &mut impl ProgressLog,
    ) -> Self {
        let num_nodes = graph.num_nodes();

        let compute_radial_vertices = radial_vertices.is_none();

        pl.info(format_args!(
            "Allocating memory ({}B)...",
            humanize(
                ((num_nodes
                    * std::mem::size_of::<usize>()
                    * (2 + (USE_TOT as usize))
                    * (1 + (!symmetric as usize)))
                    + (num_nodes / 8) * compute_radial_vertices as usize) as f64
            )
        ));

        let acc_radial = if let Some(r) = radial_vertices {
            assert_eq!(
                r.len(),
                num_nodes,
                "the graph has {} nodes but the bit vector of radial vertices has length {}",
                num_nodes,
                r.len()
            );
            r
        } else {
            AtomicBitVec::new(num_nodes)
        };

        ExactSumSweep {
            graph,
            transpose,
            num_nodes,
            forward_tot: if USE_TOT {
                vec![0; num_nodes].into()
            } else {
                Box::default()
            },
            backward_tot: if USE_TOT && !symmetric {
                vec![0; num_nodes].into()
            } else {
                Box::default()
            },
            forward_low: vec![0; num_nodes].into(),
            forward_high: vec![num_nodes; num_nodes].into(),
            backward_low: if symmetric {
                Box::default()
            } else {
                vec![0; num_nodes].into()
            },
            backward_high: if symmetric {
                Box::default()
            } else {
                vec![num_nodes; num_nodes].into()
            },
            scc_graph,
            scc,
            diameter_low: 0,
            diameter_high: num_nodes - 1,
            radius_low: 0,
            radius_high: if symmetric {
                num_nodes / 2
            } else {
                num_nodes - 1
            },
            radius_iterations: None,
            diameter_iterations: None,
            all_iter: None,
            forward_iter: None,
            iterations: 0,
            radial_vertices: acc_radial,
            radius_vertex: 0,
            diameter_vertex: 0,
            compute_radial_vertices,
            symmetric,
            visit,
            transposed_visit,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    V1: Parallel<EventNoPred> + Sync,
    V2: Parallel<EventNoPred> + Sync,
    OL: Level<USE_TOT>,
    const USE_TOT: bool,
> ExactSumSweep<'_, G1, G2, V1, V2, OL, USE_TOT>
{
    #[inline(always)]
    fn incomplete_forward(&self, index: usize) -> bool {
        self.forward_low[index] != self.forward_high[index]
    }

    #[inline(always)]
    fn incomplete_backward(&self, index: usize) -> bool {
        if self.symmetric {
            self.incomplete_forward(index)
        } else {
            self.backward_low[index] != self.backward_high[index]
        }
    }

    /// Returns a reference to the backward lower-bound array, redirecting to
    /// the forward array when `symmetric`.
    #[inline(always)]
    fn bw_low(&self) -> &[usize] {
        if self.symmetric {
            &self.forward_low
        } else {
            &self.backward_low
        }
    }

    /// Returns a reference to the backward upper-bound array, redirecting to
    /// the forward array when `symmetric`.
    #[inline(always)]
    fn bw_high(&self) -> &[usize] {
        if self.symmetric {
            &self.forward_high
        } else {
            &self.backward_high
        }
    }

    /// Returns a reference to the backward total-distance array, redirecting
    /// to the forward array when `symmetric`.
    #[inline(always)]
    fn bw_tot(&self) -> &[usize] {
        if self.symmetric {
            &self.forward_tot
        } else {
            &self.backward_tot
        }
    }

    /// Performs `iterations` steps of the SumSweep heuristic, starting from vertex `start`.
    ///
    /// For more information see Section 3 of the paper.
    ///
    /// # Arguments
    /// * `start` - The starting vertex.
    /// * `iterations` - The number of iterations.
    /// * `pl` - A concurrent progress logger.
    fn sum_sweep_heuristic(
        &mut self,
        start: usize,
        iterations: usize,
        pl: &mut impl ConcurrentProgressLog,
    ) {
        self.step_sum_sweep(Some(start), true, pl, |node| {
            format!(
                "Performing initial forward SumSweep heuristic visit from {}...",
                node
            )
        });

        for i in 2..iterations {
            if i % 2 == 0 {
                let v = if USE_TOT {
                    math::argmax_filtered(self.bw_tot(), self.bw_low(), |i, _| {
                        self.incomplete_backward(i)
                    })
                } else {
                    math::argmax_filtered(self.bw_low(), std::iter::repeat(0usize), |i, _| {
                        self.incomplete_backward(i)
                    })
                };
                self.step_sum_sweep(v, false, pl, |node| {
                    format!(
                        "Performing initial backward SumSweep heuristic visit from {}...",
                        node
                    )
                });
            } else {
                let v = if USE_TOT {
                    math::argmax_filtered(&self.forward_tot, &self.forward_low, |i, _| {
                        self.incomplete_forward(i)
                    })
                } else {
                    math::argmax_filtered(&self.forward_low, std::iter::repeat(0usize), |i, _| {
                        self.incomplete_forward(i)
                    })
                };
                self.step_sum_sweep(v, true, pl, |node| {
                    format!(
                        "Performing initial forward SumSweep heuristic visit from {}...",
                        node
                    )
                });
            }
        }
    }

    /// Computes diameter, radius, and/or all eccentricities.
    ///
    /// # Arguments
    /// * `pl` - A progress logger.
    pub(super) fn compute(&mut self, pl: &mut impl ProgressLog) {
        if self.num_nodes == 0 {
            return;
        }

        pl.start("Running ExactSumSweep...");
        let mut cpl = pl.concurrent();

        if self.compute_radial_vertices {
            self.compute_radial_vertices(&mut cpl);
        }

        let max_outdegree_vertex = (0..self.num_nodes)
            .into_par_iter()
            .map(|v| (self.graph.outdegree(v), v))
            .max_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)))
            .unwrap()
            .1; // The iterator is not empty

        self.sum_sweep_heuristic(max_outdegree_vertex, 6, &mut cpl);

        let mut points = [self.graph.num_nodes() as f64; 6];
        let mut missing_nodes = self.find_missing_nodes(&mut cpl);
        let mut old_missing_nodes;

        self.log_status(pl, missing_nodes);

        while missing_nodes > 0 {
            let step_to_perform = math::argmax(points).expect("Could not find step to perform");

            match step_to_perform {
                0 => self.all_cc_upper_bound(&mut cpl),
                1 => {
                    let v = if USE_TOT {
                        math::argmax_filtered(&self.forward_high, &self.forward_tot, |i, _| {
                            self.incomplete_forward(i)
                        })
                    } else {
                        math::argmax_filtered(
                            &self.forward_high,
                            std::iter::repeat(0usize),
                            |i, _| self.incomplete_forward(i),
                        )
                    };
                    self.step_sum_sweep(v, true, &mut cpl, |node| {
                        format!(
                            "Performing a forward BFV from a node maximizing the upper bound ({})...",
                            node
                        )
                    })
                }
                2 => {
                    let v = if USE_TOT {
                        math::argmin_filtered(&self.forward_low, &self.forward_tot, |i, _| {
                            self.radial_vertices[i]
                        })
                    } else {
                        math::argmin_filtered(
                            &self.forward_low,
                            std::iter::repeat(0usize),
                            |i, _| self.radial_vertices[i],
                        )
                    };
                    self.step_sum_sweep(v, true, &mut cpl, |node| {
                        format!(
                            "Performing a forward BFV from a node minimizing the lower bound ({})...",
                            node
                        )
                    })
                }
                3 => {
                    let v = if USE_TOT {
                        math::argmax_filtered(self.bw_high(), self.bw_tot(), |i, _| {
                            self.incomplete_backward(i)
                        })
                    } else {
                        math::argmax_filtered(self.bw_high(), std::iter::repeat(0usize), |i, _| {
                            self.incomplete_backward(i)
                        })
                    };
                    self.step_sum_sweep(v, false, &mut cpl, |node| {
                        format!(
                            "Performing a backward BFV from a node maximizing the upper bound ({})...",
                            node
                        )
                    })
                }
                4 => {
                    let v = if USE_TOT {
                        math::argmax_filtered(self.bw_tot(), self.bw_high(), |i, _| {
                            self.incomplete_backward(i)
                        })
                    } else {
                        math::argmax_filtered(self.bw_high(), std::iter::repeat(0usize), |i, _| {
                            self.incomplete_backward(i)
                        })
                    };
                    self.step_sum_sweep(v, false, &mut cpl, |node| {
                        format!(
                            "Performing a backward BFV from a node maximizing the distance sum ({})",
                            node
                        )
                    })
                }
                5 => {
                    let v = if USE_TOT {
                        math::argmax_filtered(&self.forward_tot, &self.forward_high, |i, _| {
                            self.incomplete_forward(i)
                        })
                    } else {
                        math::argmax_filtered(
                            &self.forward_high,
                            std::iter::repeat(0usize),
                            |i, _| self.incomplete_forward(i),
                        )
                    };
                    self.step_sum_sweep(v, true, &mut cpl, |node| {
                        format!(
                            "Performing a forward BFV from a node maximizing the distance sum ({})...",
                            node
                        )
                    })
                }
                6.. => panic!(),
            }

            // Update each step utility.
            // For more information see Section 4.6 of the paper.

            old_missing_nodes = missing_nodes;
            let had_radius = self.radius_iterations.is_some();
            let had_diameter = self.diameter_iterations.is_some();

            missing_nodes = self.find_missing_nodes(&mut cpl);
            points[step_to_perform] = (old_missing_nodes - missing_nodes) as f64;

            // This is to make rust-analyzer happy as it cannot recognize mut reference
            #[allow(clippy::needless_range_loop)]
            for i in 0..points.len() {
                if i != step_to_perform && points[i] >= 0.0 {
                    points[i] += 2.0 / self.iterations as f64;
                }
            }

            if !had_radius && self.radius_iterations.is_some() {
                pl.info(format_args!(
                    "Radius determined: {} (vertex {})",
                    self.radius_high, self.radius_vertex,
                ));
            }
            if !had_diameter && self.diameter_iterations.is_some() {
                pl.info(format_args!(
                    "Diameter determined: {} (vertex {})",
                    self.diameter_low, self.diameter_vertex,
                ));
            }

            self.log_status(pl, missing_nodes);
        }

        pl.done();
    }

    fn log_status(&self, pl: &mut impl ProgressLog, missing_nodes: usize) {
        if self.radius_high == usize::MAX {
            pl.info(format_args!(
                "Missing bounds: {} out of 2 · {} = {} ({:3}%); {} ≤ diameter ≤ {} (no radial vertices)",
                missing_nodes,
                self.num_nodes,
                self.num_nodes * 2,
                (100.0 * missing_nodes as f64) / (self.num_nodes as f64 * 2.0),
                self.diameter_low,
                self.diameter_high,
            ));
        } else {
            pl.info(format_args!(
                "Missing bounds: {} out of 2 · {} = {} ({:3}%); {} ≤ diameter ≤ {}, {} ≤ radius ≤ {}",
                missing_nodes,
                self.num_nodes,
                self.num_nodes * 2,
                (100.0 * missing_nodes as f64) / (self.num_nodes as f64 * 2.0),
                self.diameter_low,
                self.diameter_high,
                self.radius_low,
                self.radius_high,
            ));
        }
    }

    /// Uses a heuristic to decide which is the best pivot to choose in each strongly connected
    /// component, in order to call the `all_cc_upper_bound` method.
    ///
    /// # Arguments
    /// * `pl` - A progress logger.
    fn find_best_pivot(&self, pl: &mut impl ProgressLog) -> Box<[usize]> {
        debug_assert!(self.num_nodes < usize::MAX);

        let mut pivot: Box<[Option<NonMaxUsize>]> = vec![None; self.scc.num_components()].into();
        let components = self.scc.components();
        pl.expected_updates(components.len());
        pl.item_name("node");
        pl.start("Computing best pivots...");

        let bw_low = self.bw_low();
        let bw_tot = self.bw_tot();
        for (v, &component) in components.iter().enumerate().rev() {
            if let Some(p) = pivot[component] {
                let p = p.into();
                let current = bw_low[v]
                    + self.forward_low[v]
                    + if self.incomplete_forward(v) {
                        0
                    } else {
                        self.num_nodes
                    }
                    + if self.incomplete_backward(v) {
                        0
                    } else {
                        self.num_nodes
                    };

                let best = bw_low[p]
                    + self.forward_low[p]
                    + if self.incomplete_forward(p) {
                        0
                    } else {
                        self.num_nodes
                    }
                    + if self.incomplete_backward(p) {
                        0
                    } else {
                        self.num_nodes
                    };

                if current < best
                    || (USE_TOT
                        && current == best
                        && self.forward_tot[v] + bw_tot[v] <= self.forward_tot[p] + bw_tot[p])
                {
                    pivot[component] = NonMaxUsize::new(v);
                }
            } else {
                pivot[component] = NonMaxUsize::new(v);
            }
            pl.light_update();
        }

        pl.done();

        pivot.into_iter().map(|x| x.unwrap().into()).collect()
    }

    /// Computes and stores in variable [`Self::radial_vertices`] the set of vertices that are
    /// either in the largest strongly connected component or that are able to reach vertices in
    /// the largest strongly connected component.
    ///
    /// # Arguments
    /// * `pl` - A progress logger.
    fn compute_radial_vertices(&mut self, pl: &mut impl ConcurrentProgressLog) {
        if self.num_nodes == 0 {
            return;
        }

        let component = self.scc.components();
        let scc_sizes = self.scc.compute_sizes();
        let max_size_scc = math::argmax(&scc_sizes).expect("Could not find max size scc.");
        pl.info(format_args!(
            "The largest component contains {max_size_scc} nodes ({:.3}%)",
            100.0 * max_size_scc as f64 / self.num_nodes as f64
        ));
        let mut v = self.num_nodes;

        while v > 0 {
            v -= 1;
            if component[v] == max_size_scc {
                break;
            }
        }

        pl.item_name("node");
        pl.start("Computing radial vertices...");

        let radial_vertices = &self.radial_vertices;
        self.transposed_visit
            .par_visit_with([v], pl.clone(), |pl, event| {
                if let EventNoPred::Visit { node, .. } = event {
                    pl.light_update();
                    radial_vertices.set(node, true, Ordering::Relaxed)
                }
                Continue(())
            })
            .continue_value_no_break();
        self.transposed_visit.reset();

        pl.done();
    }

    /// Performs a (forward or backward) BFS, updating lower bounds on the
    /// eccentricities of all visited vertices.
    ///
    /// For more information see Section 4.1 of the paper.
    ///
    /// # Arguments
    ///
    /// * `start` - The starting vertex of the BFS. If [`None`], no visit happens.
    ///
    /// * `forward` - Whether the BFS is performed following the direction of edges or
    ///   in the opposite direction.
    ///
    /// * `pl` - A progress logger.
    ///
    /// * `message` - The message to print to the log.
    fn step_sum_sweep(
        &mut self,
        start: Option<usize>,
        forward: bool,
        pl: &mut impl ConcurrentProgressLog,
        message: impl FnOnce(usize) -> String,
    ) {
        if let Some(start) = start {
            if forward {
                self.forward_step_sum_sweep(start, pl, message);
            } else {
                self.backwards_step_sum_sweep(start, pl, message);
            }
            self.iterations += 1;
        }
    }

    fn backwards_step_sum_sweep(
        &mut self,
        start: usize,
        pl: &mut impl ConcurrentProgressLog,
        message: impl FnOnce(usize) -> String,
    ) {
        pl.item_name("node");
        pl.start(message(start));

        let max_dist = CachePadded::new(AtomicUsize::new(0));
        let radius = RwLock::new((self.radius_high, self.radius_vertex));

        let forward_low = self.forward_low.as_sync_slice();
        let forward_tot = self.forward_tot.as_sync_slice();

        self.transposed_visit
            .par_visit_with([start], pl.clone(), |pl, event| {
                if let EventNoPred::Visit { node, distance, .. } = event {
                    pl.light_update();
                    max_dist.fetch_max(distance, Ordering::Relaxed);

                    // SAFETY: each node gets accessed exactly once, so no data races can happen.
                    let node_forward_low = unsafe { forward_low[node].get() };
                    let node_forward_high = self.forward_high[node];

                    if USE_TOT {
                        unsafe { forward_tot[node].set(forward_tot[node].get() + distance) };
                    }

                    if node_forward_low != node_forward_high && node_forward_low < distance {
                        unsafe { forward_low[node].set(distance) };

                        if distance == node_forward_high && self.radial_vertices[node] {
                            let mut update_radius = false;
                            {
                                let radius_lock = radius.read().unwrap();
                                if distance < radius_lock.0 {
                                    update_radius = true;
                                }
                            }

                            if update_radius {
                                let mut radius_lock = radius.write().unwrap();
                                if distance < radius_lock.0 {
                                    radius_lock.0 = distance;
                                    radius_lock.1 = node;
                                }
                            }
                        }
                    }
                };
                Continue(())
            })
            .continue_value_no_break();

        self.transposed_visit.reset();

        let ecc_start = max_dist.load(Ordering::Relaxed);

        if self.symmetric {
            self.forward_low[start] = ecc_start;
            self.forward_high[start] = ecc_start;
        } else {
            self.backward_low[start] = ecc_start;
            self.backward_high[start] = ecc_start;
        }

        (self.radius_high, self.radius_vertex) = radius.into_inner().unwrap();

        if self.diameter_low < ecc_start {
            self.diameter_low = ecc_start;
            self.diameter_vertex = start;
        }

        pl.done();
    }

    fn forward_step_sum_sweep(
        &mut self,
        start: usize,
        pl: &mut impl ConcurrentProgressLog,
        message: impl FnOnce(usize) -> String,
    ) {
        pl.item_name("node");
        pl.start(message(start));

        let max_dist = CachePadded::new(AtomicUsize::new(0));

        let bw_high = if self.symmetric {
            &*self.forward_high
        } else {
            &*self.backward_high
        };
        let backward_low = if self.symmetric {
            self.forward_low.as_sync_slice()
        } else {
            self.backward_low.as_sync_slice()
        };
        let backward_tot = if self.symmetric {
            self.forward_tot.as_sync_slice()
        } else {
            self.backward_tot.as_sync_slice()
        };

        self.visit.reset();
        self.visit
            .par_visit_with([start], pl.clone(), |pl, event| {
                if let EventNoPred::Visit { node, distance, .. } = event {
                    // SAFETY: each node gets accessed exactly once, so no data races can happen
                    pl.light_update();
                    max_dist.fetch_max(distance, Ordering::Relaxed);

                    let node_backward_high = bw_high[node];
                    let node_backward_low = unsafe { backward_low[node].get() };

                    if USE_TOT {
                        unsafe { backward_tot[node].set(backward_tot[node].get() + distance) };
                    }

                    if node_backward_low != node_backward_high && node_backward_low < distance {
                        unsafe { backward_low[node].set(distance) };
                    }
                }
                Continue(())
            })
            .continue_value_no_break();

        let ecc_start = max_dist.load(Ordering::Relaxed);

        self.forward_low[start] = ecc_start;
        self.forward_high[start] = ecc_start;

        if self.diameter_low < ecc_start {
            self.diameter_low = ecc_start;
            self.diameter_vertex = start;
        }
        if self.radial_vertices[start] && self.radius_high > ecc_start {
            self.radius_high = ecc_start;
            self.radius_vertex = start;
        }

        pl.done();
    }

    /// Performs a (forward or backward) BFS inside each strongly connected component, starting
    /// from the pivot.
    ///
    /// For more information see Section 4.2 on the paper.
    ///
    /// # Arguments
    /// * `pivot` - An array containing in position `i` the pivot of the `i`-th strongly connected
    ///   component.
    /// * `forward` - Whether the BFS is performed following the direction of edges or
    ///   in the opposite direction.
    /// * `pl` - A progress logger.
    ///
    /// # Return
    /// Two arrays.
    ///
    /// The first one contains the distance of each vertex from the pivot of its strongly connected
    /// component, while the second one contains in position `i` the eccentricity of the pivot of the
    /// `i`-th strongly connected component.
    fn compute_dist_pivot(
        &self,
        pivot: &[usize],
        forward: bool,
        pl: &mut impl ProgressLog,
    ) -> (Box<[usize]>, Box<[usize]>) {
        let (dist_pivot, usize_ecc_pivot) = if forward {
            pl.start("Computing forward dist pivots...");
            self.compute_dist_pivot_from_graph(pivot, self.graph)
        } else {
            pl.start("Computing backwards dist pivots...");
            self.compute_dist_pivot_from_graph(pivot, self.transpose)
        };

        pl.done();

        (dist_pivot, usize_ecc_pivot)
    }

    fn compute_dist_pivot_from_graph(
        &self,
        pivot: &[usize],
        graph: &(impl RandomAccessGraph + Sync),
    ) -> (Box<[usize]>, Box<[usize]>) {
        let components = self.scc.components();

        let ecc_pivot: Box<[AtomicUsize]> = repeat_with(|| AtomicUsize::new(0))
            .take(self.scc.num_components())
            .collect();
        let mut dist_pivot = vec![0; self.num_nodes].into_boxed_slice();
        let dist_pivot_mut = dist_pivot.as_sync_slice();

        let current_index = CachePadded::new(AtomicUsize::new(0));

        rayon::broadcast(|_| {
            let mut bfs = ParFairNoPred::new(graph);
            let mut current_pivot_index = current_index.fetch_add(1, Ordering::Relaxed);

            while let Some(&p) = pivot.get(current_pivot_index) {
                let pivot_component = components[p];
                let component_ecc_pivot = &ecc_pivot[pivot_component];

                bfs.par_visit_filtered(
                    [p],
                    |event| {
                        if let EventNoPred::Visit { node, distance, .. } = event {
                            // SAFETY: each node is accessed exactly once.
                            unsafe { dist_pivot_mut[node].set(distance) };
                            component_ecc_pivot.store(distance, Ordering::Relaxed);
                        };
                        Continue(())
                    },
                    |FilterArgs::<EventNoPred> { node, .. }| components[node] == pivot_component,
                )
                .continue_value_no_break();

                current_pivot_index = current_index.fetch_add(1, Ordering::Relaxed);
            }
        });

        (dist_pivot, transmute_boxed_slice_from_atomic(ecc_pivot))
    }

    /// Performs a step of the ExactSumSweep algorithm.
    ///
    /// For more information see Section 4.2 of the paper.
    ///
    /// # Arguments
    /// * `pl` - A progress logger.
    fn all_cc_upper_bound(&mut self, pl: &mut impl ProgressLog) {
        pl.item_name("element");
        pl.expected_updates(2 * self.scc.num_components() + self.num_nodes);

        let pivot = self.find_best_pivot(pl);

        let (dist_pivot_f, mut ecc_pivot_f) = self.compute_dist_pivot(&pivot, true, pl);
        let components = self.scc.components();

        if self.symmetric {
            // In the symmetric case each SCC is a connected component, so
            // the component DAG has no edges and no propagation is needed.
            // A single BFS per pivot suffices since graph == transpose.
            let radius = RwLock::new((self.radius_high, self.radius_vertex));
            let forward_high = self.forward_high.as_sync_slice();

            pl.info(format_args!("Refining upper bounds of nodes..."));

            (0..self.num_nodes).into_par_iter().for_each(|node| {
                let pivot_value = dist_pivot_f[node] + ecc_pivot_f[components[node]];
                // SAFETY: each node is accessed exactly once.
                let node_forward_high = unsafe { forward_high[node].get() };

                if pivot_value < node_forward_high {
                    // SAFETY: each node is accessed exactly once.
                    unsafe { forward_high[node].set(pivot_value) };

                    if pivot_value == self.forward_low[node] && self.radial_vertices[node] {
                        let mut update_radius = false;
                        {
                            let radius_lock = radius.read().unwrap();
                            if pivot_value < radius_lock.0 {
                                update_radius = true;
                            }
                        }
                        if update_radius {
                            let mut radius_lock = radius.write().unwrap();
                            if pivot_value < radius_lock.0 {
                                radius_lock.0 = pivot_value;
                                radius_lock.1 = node;
                            }
                        }
                    }
                } else if node_forward_high == self.forward_low[node] && self.radial_vertices[node]
                {
                    let mut update_radius = false;
                    {
                        let radius_lock = radius.read().unwrap();
                        if node_forward_high < radius_lock.0 {
                            update_radius = true;
                        }
                    }
                    if update_radius {
                        let mut radius_lock = radius.write().unwrap();
                        if node_forward_high < radius_lock.0 {
                            radius_lock.0 = node_forward_high;
                            radius_lock.1 = node;
                        }
                    }
                }
            });

            pl.update_with_count(self.num_nodes);
            (self.radius_high, self.radius_vertex) = radius.into_inner().unwrap();
        } else {
            let (dist_pivot_b, mut ecc_pivot_b) = self.compute_dist_pivot(&pivot, false, pl);

            // Tarjan's algorithm emits components in reverse topological order.
            // In order to bound forward eccentricities in reverse topological order the components
            // are traversed as is.
            pl.info(format_args!("Bounding forward eccentricities of pivots..."));
            for (c, &p) in pivot.iter().enumerate() {
                for connection in self.scc_graph.successors(c) {
                    let next_c = connection.target;
                    let start = connection.start;
                    let end = connection.end;

                    ecc_pivot_f[c] = std::cmp::max(
                        ecc_pivot_f[c],
                        dist_pivot_f[start] + 1 + dist_pivot_b[end] + ecc_pivot_f[next_c],
                    );

                    if ecc_pivot_f[c] >= self.forward_high[p] {
                        ecc_pivot_f[c] = self.forward_high[p];
                        break;
                    }
                }
                pl.light_update();
            }

            // Tarjan's algorithm emits components in reverse topological order.
            // In order to bound backward eccentricities in topological order the components order
            // must be reversed.
            pl.info(format_args!(
                "Bounding backward eccentricities of pivots..."
            ));
            for c in (0..self.scc.num_components()).rev() {
                for component in self.scc_graph.successors(c) {
                    let next_c = component.target;
                    let start = component.start;
                    let end = component.end;

                    ecc_pivot_b[next_c] = std::cmp::max(
                        ecc_pivot_b[next_c],
                        dist_pivot_f[start] + 1 + dist_pivot_b[end] + ecc_pivot_b[c],
                    );

                    if ecc_pivot_b[next_c] >= self.bw_high()[pivot[next_c]] {
                        ecc_pivot_b[next_c] = self.bw_high()[pivot[next_c]];
                    }
                }
                pl.light_update();
            }

            let radius = RwLock::new((self.radius_high, self.radius_vertex));

            let forward_high = self.forward_high.as_sync_slice();
            let backward_high = self.backward_high.as_sync_slice();

            pl.info(format_args!("Refining upper bounds of nodes..."));

            (0..self.num_nodes).into_par_iter().for_each(|node| {
                // SAFETY: each node gets accessed exactly once, so no data
                // races can happen.
                let mut node_forward_high = unsafe { forward_high[node].get() };
                let pivot_value = dist_pivot_b[node] + ecc_pivot_f[components[node]];

                if pivot_value < node_forward_high {
                    // SAFETY: each node is accessed exactly once, so there are no data races.
                    unsafe { forward_high[node].set(pivot_value) };
                    node_forward_high = pivot_value;
                }

                if node_forward_high == self.forward_low[node] {
                    let new_ecc = node_forward_high;

                    if self.radial_vertices[node] {
                        let mut update_radius = false;
                        {
                            let radius_lock = radius.read().unwrap();
                            if new_ecc < radius_lock.0 {
                                update_radius = true;
                            }
                        }

                        if update_radius {
                            let mut radius_lock = radius.write().unwrap();
                            if new_ecc < radius_lock.0 {
                                radius_lock.0 = new_ecc;
                                radius_lock.1 = node;
                            }
                        }
                    }
                }

                // SAFETY: each node is accessed exactly once, so there are no data races.
                unsafe {
                    backward_high[node].set(std::cmp::min(
                        backward_high[node].get(),
                        dist_pivot_f[node] + ecc_pivot_b[components[node]],
                    ))
                };
            });

            pl.update_with_count(self.num_nodes);
            (self.radius_high, self.radius_vertex) = radius.into_inner().unwrap();
        }

        self.iterations += 3;

        pl.done();
    }

    /// Computes how many nodes are still to be processed, before outputting the result.
    ///
    /// # Arguments
    /// * `pl` - A progress logger.
    fn find_missing_nodes(&mut self, pl: &mut impl ProgressLog) -> usize {
        pl.item_name("node");
        pl.expected_updates(self.num_nodes);
        pl.start("Computing missing bounds...");

        let missing = (0..self.num_nodes)
            .into_par_iter() // TODO: with_min_len (also elsewhere)
            .fold(Default::default, |mut acc: Missing, node| {
                acc.diameter_high_forward = acc.diameter_high_forward.max(self.forward_high[node]);
                acc.diameter_high_backward = acc.diameter_high_backward.max(self.bw_high()[node]);
                if self.radial_vertices[node] {
                    acc.radius_low = acc.radius_low.min(self.forward_low[node]);
                }
                if self.incomplete_forward(node) {
                    acc.all_forward += 1;
                    if self.forward_high[node] > self.diameter_low {
                        acc.diameter_forward += 1;
                    }
                    if self.radial_vertices[node] && self.forward_low[node] < self.radius_high {
                        acc.radius += 1;
                    }
                }
                if self.incomplete_backward(node) {
                    acc.all_backward += 1;
                    if self.bw_high()[node] > self.diameter_low {
                        acc.diameter_backward += 1;
                    }
                }
                acc
            })
            .reduce(Default::default, |acc, elem| acc + elem);

        pl.update_with_count(self.num_nodes);

        self.diameter_high = missing
            .diameter_high_forward
            .min(missing.diameter_high_backward);
        self.radius_low = missing.radius_low;

        if missing.radius_low == usize::MAX {
            self.radius_high = usize::MAX;
            self.radius_low = usize::MAX;
        }

        if missing.radius == 0 && self.radius_iterations.is_none() {
            self.radius_iterations = Some(self.iterations);
        }
        if (missing.diameter_forward == 0 || missing.diameter_backward == 0)
            && self.diameter_iterations.is_none()
        {
            self.diameter_iterations = Some(self.iterations);
        }
        if missing.all_forward == 0 && self.forward_iter.is_none() {
            self.forward_iter = Some(self.iterations);
        }
        if missing.all_forward == 0 && missing.all_backward == 0 {
            self.all_iter = Some(self.iterations);
        }

        pl.done();

        OL::missing_nodes(&missing)
    }
}
