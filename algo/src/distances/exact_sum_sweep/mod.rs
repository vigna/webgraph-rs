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
//! # Usage
//!
//! Depending on what you intend to compute, you have to choose the right
//! [_level_](Level) between [`All`], [`AllForward`], [`RadiusDiameter`],
//! [`Diameter`], and [`Radius`]. Then you have to invoke [`run`](Level::run) or
//! [`run_symm`](Level::run_symm). In the first case, you have to provide a
//! graph and its transpose; in the second case, you have to provide a symmetric
//! graph. The methods return a suitable structure containing the result of the
//! computation.
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
//! let result = exact_sum_sweep::All::run(
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
//! let result = exact_sum_sweep::RadiusDiameter::run(
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
//! let result = exact_sum_sweep::RadiusDiameter::run(
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
//! If the graph is symmetric (i.e., undirected), you may use
//! [run_symm](Level::run_symm).
//! ```
//! use webgraph_algo::distances::exact_sum_sweep::{self, *};
//! use dsi_progress_logger::no_logging;
//! use webgraph::graphs::vec_graph::VecGraph;
//!
//! let graph = VecGraph::from_arcs(
//!     [(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2), (3, 4), (4, 3)]
//! );
//!
//! let result = exact_sum_sweep::RadiusDiameter::run_symm(
//!     &graph,
//!     no_logging![]
//! );
//!
//! assert_eq!(result.diameter, 1);
//! assert_eq!(result.radius, 1);
//! ```

mod computer;
mod level;
pub mod output;
pub mod output_symm;
mod scc_graph;

pub use level::*;
