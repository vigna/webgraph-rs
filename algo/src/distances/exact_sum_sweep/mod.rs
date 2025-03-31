/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! An implementation of the ExactSumSweep algorithm.
//!
//! The algorithm has been described by Michele Borassi, Pierluigi Crescenzi,
//! Michel Habib, Walter A. Kosters, Andrea Marino, and Frank W. Takes in “[Fast
//! diameter and radius BFS-based computation in (weakly connected) real-world
//! graphs–With an application to the six degrees of separation
//! games](https://doi.org/10.1016/j.tcs.2015.02.033)”.
//!
//! The algorithm can compute the diameter, the radius, and even the
//! eccentricities (forward and backward) of a graph. These tasks are quadratic
//! in nature, but ExactSumSweep uses a number of heuristic to reduce the
//! computation to a relatively small number of visits.
//!
//! Depending on what you intend to compute, you have to choose the right
//! *output level* between [`All`], [`AllForward`], [`RadiusDiameter`],
//! [`Diameter`], and [`Radius`]. Then you have to invoke
//! [`compute`](OutputLevel::run) or [`compute_symm`](OutputLevel::run_symm).
//! In the first case, you have to provide a graph and its transpose;
//! in the second case, you have to provide a symmetric graph.
//!
//! # Examples
//!
//! ```
//! use webgraph_algo::distances::exact_sum_sweep::{self, *};
//! use webgraph_algo::thread_pool;
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
//!     &thread_pool![],
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
//!     &thread_pool![],
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
//! use webgraph_algo::thread_pool;
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
//!     &threads![],
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
//! [compute_symm](OutputLevel::run_symm).
//! ```
//! use webgraph_algo::distances::exact_sum_sweep::{self, *};
//! use webgraph_algo::thread_pool;
//! use dsi_progress_logger::no_logging;
//! use webgraph::graphs::vec_graph::VecGraph;
//!
//! let graph = VecGraph::from_arcs(
//!     [(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2), (3, 4), (4, 3)]
//! );
//!
//! let result = exact_sum_sweep::RadiusDiameter::run_symm(
//!     &graph,
//!     &thread_pool![],
//!     no_logging![]
//! );
//!
//! assert_eq!(result.diameter, 1);
//! assert_eq!(result.radius, 1);
//! ```

mod computer;
mod level;
mod outputs;
mod outputs_symm;
mod scc_graph;

pub use level::*;
