/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{computer::DirExactSumSweepComputer, output, output_symm};
use dsi_progress_logger::ConcurrentProgressLog;
use sux::bits::AtomicBitVec;
use webgraph::traits::RandomAccessGraph;

#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Missing {
    pub radius: usize,
    pub diameter_forward: usize,
    pub diameter_backward: usize,
    pub all_forward: usize,
    pub all_backward: usize,
}

impl core::ops::Add for Missing {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {
            radius: self.radius + rhs.radius,
            diameter_forward: self.diameter_forward + rhs.diameter_forward,
            diameter_backward: self.diameter_backward + rhs.diameter_backward,
            all_forward: self.all_forward + rhs.all_forward,
            all_backward: self.all_backward + rhs.all_backward,
        }
    }
}

/// Trait used to run the ExactSumSweep algorithm.
///
/// This trait can be used to run the algorithm either [providing a graph and
/// its transpose](Self::run) or [using a symmetric graph](Self::run_symm).
///
/// It is implemented by the following structs: [`All`], [`AllForward`],
/// [`RadiusDiameter`], [`Diameter`], and [`Radius`], which correspond to
/// different level of computation, with decreasing cost in term of memory and
/// execution time.
///
/// # Examples
///
/// See the [module documentation](crate::distances::exact_sum_sweep).
pub trait Level: Sync {
    /// The type of the result of [`run`](Self::run).
    type Output: Send;
    /// The type of the result of [`run_symm`](Self::run_symm).
    type OutputSymm: Send;

    /// Runs the ExactSumSweep algorithm on the specified graph.
    ///
    /// # Arguments
    ///
    /// * `graph`: a graph.
    ///
    /// * `transpose`: the transpose of `graph`. Note that you are responsible
    ///   for providing a correct transpose. The result of the computation is
    ///   undefined otherwise.
    ///
    /// * `radial_vertices`: an [`AtomicBitVec`] where `v[i]` is true if node
    ///   `i` is to be considered radial vertex. If [`None`] the algorithm will
    ///   use the biggest connected component.
    ///
    /// * `thread_pool`: The thread pool to use for parallel computations.
    ///
    /// * `pl`: a progress logger.
    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output;

    /// Runs the ExactSumSweep algorithm on the specified symmetric graph.
    ///
    /// # Arguments
    ///
    /// * `graph`: a symmetric graph. Note that you are responsible for the
    ///   graph being symmetric. The result of the computation is undefined
    ///   otherwise.
    ///
    /// * `thread_pool`: The thread pool to use for parallel computations.
    ///
    /// * `pl`: a progress logger.
    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm;

    #[doc(hidden)]
    fn missing_nodes(missing_nodes: &Missing) -> usize;
}

/// Computes all eccentricities of a graph, its diameter, and its radius.
///
/// This variant is equivalent to [`AllForward`] in the symmetric case.
pub struct All;

impl Level for All {
    type Output = output::All;
    type OutputSymm = output_symm::All;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.all_iter.is_some(),);
        assert!(computer.forward_iter.is_some(),);
        assert!(computer.diameter_iterations.is_some(),);
        assert!(computer.radius_iterations.is_some(),);

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();
        let forward_iterations = computer.forward_iter.unwrap();
        let all_iterations = computer.all_iter.unwrap();
        let forward_eccentricities = computer.forward_low;
        let backward_eccentricities = computer.backward_high;

        Self::Output {
            forward_eccentricities,
            backward_eccentricities,
            diameter,
            radius,
            diametral_vertex,
            radial_vertex,
            radius_iterations,
            diameter_iterations,
            forward_iterations,
            all_iterations,
        }
    }

    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new_symm(&graph, pl);
        computer.compute(pl);

        assert!(computer.forward_iter.is_some(),);
        assert!(computer.diameter_iterations.is_some(),);
        assert!(computer.radius_iterations.is_some(),);

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();
        let iterations = computer.forward_iter.unwrap();
        let eccentricities = computer.forward_low;

        Self::OutputSymm {
            eccentricities,
            diameter,
            radius,
            diametral_vertex,
            radial_vertex,
            radius_iterations,
            diameter_iterations,
            iterations,
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.all_forward + missing.all_backward
    }
}

/// Computes all forward eccentricities of a graph, its diameter, and its radius.
pub struct AllForward;

impl Level for AllForward {
    type Output = output::AllForward;
    type OutputSymm = output_symm::All;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.forward_iter.is_some(),);
        assert!(computer.diameter_iterations.is_some());
        assert!(computer.radius_iterations.is_some(),);

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();
        let forward_iterations = computer.forward_iter.unwrap();
        let forward_eccentricities = computer.forward_low;

        Self::Output {
            forward_eccentricities,
            diameter,
            radius,
            diametral_vertex,
            radial_vertex,
            radius_iterations,
            diameter_iterations,
            forward_iterations,
        }
    }

    #[inline(always)]
    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        All::run_symm(graph, pl)
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.all_forward
    }
}

/// Computes the diameter and the radius of a graph.
pub struct RadiusDiameter;

impl Level for RadiusDiameter {
    type Output = output::RadiusDiameter;
    type OutputSymm = output_symm::RadiusDiameter;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.diameter_iterations.is_some(),);
        assert!(computer.radius_iterations.is_some(),);

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();

        Self::Output {
            diameter,
            radius,
            diametral_vertex,
            radial_vertex,
            radius_iterations,
            diameter_iterations,
        }
    }

    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new_symm(&graph, pl);
        computer.compute(pl);

        assert!(computer.diameter_iterations.is_some(),);
        assert!(computer.radius_iterations.is_some(),);

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();

        Self::OutputSymm {
            diameter,
            radius,
            diametral_vertex,
            radial_vertex,
            radius_iterations,
            diameter_iterations,
        }
    }

    #[doc(hidden)]
    fn missing_nodes(missing: &Missing) -> usize {
        missing.radius + std::cmp::min(missing.diameter_forward, missing.diameter_backward)
    }
}

/// Computes the diameter of a graph.
pub struct Diameter;

impl Level for Diameter {
    type Output = output::Diameter;
    type OutputSymm = output_symm::Diameter;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.diameter_iterations.is_some(),);

        let diameter = computer.diameter_low;
        let diametral_vertex = computer.diameter_vertex;
        let diameter_iterations = computer.diameter_iterations.unwrap();

        Self::Output {
            diameter,
            diametral_vertex,
            diameter_iterations,
        }
    }

    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new_symm(&graph, pl);
        computer.compute(pl);

        assert!(computer.diameter_iterations.is_some(),);

        let diameter = computer.diameter_low;
        let diametral_vertex = computer.diameter_vertex;
        let diameter_iterations = computer.diameter_iterations.unwrap();

        Self::OutputSymm {
            diameter,
            diametral_vertex,
            diameter_iterations,
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        std::cmp::min(missing.diameter_forward, missing.diameter_backward)
    }
}

/// Computes the radius of a graph.
pub struct Radius;

impl Level for Radius {
    type Output = output::Radius;
    type OutputSymm = output_symm::Radius;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.radius_iterations.is_some(),);

        let radius = computer.radius_high;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();

        Self::Output {
            radius,
            radial_vertex,
            radius_iterations,
        }
    }

    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new_symm(&graph, pl);
        computer.compute(pl);

        assert!(computer.radius_iterations.is_some(),);

        let radius = computer.radius_high;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();

        Self::OutputSymm {
            radius,
            radial_vertex,
            radius_iterations,
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.radius
    }
}
