/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{ExactSumSweep, output, output_symm};
use dsi_progress_logger::ConcurrentProgressLog;
use sux::bits::AtomicBitVec;
use webgraph::traits::RandomAccessGraph;

#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Missing {
    pub radius: usize,
    pub diameter_forward: usize,
    pub diameter_backward: usize,
    pub all_forward: usize,
    pub all_backward: usize,
    pub diameter_high_forward: usize,
    pub diameter_high_backward: usize,
    pub radius_low: usize,
}

impl Default for Missing {
    fn default() -> Self {
        Self {
            radius: 0,
            diameter_forward: 0,
            diameter_backward: 0,
            all_forward: 0,
            all_backward: 0,
            diameter_high_forward: 0,
            diameter_high_backward: 0,
            radius_low: usize::MAX,
        }
    }
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
            diameter_high_forward: self.diameter_high_forward.max(rhs.diameter_high_forward),
            diameter_high_backward: self.diameter_high_backward.max(rhs.diameter_high_backward),
            radius_low: self.radius_low.min(rhs.radius_low),
        }
    }
}

/// Trait used to run the ExactSumSweep algorithm.
///
/// This trait can be used to run the algorithm either [providing a graph and
/// its transpose] or [using a symmetric graph].
///
/// [providing a graph and its transpose]: Self::run
/// [using a symmetric graph]: Self::run_symm
///
/// It is implemented by the following structs: [`All`], [`AllForward`],
/// [`RadiusDiameter`], [`Diameter`], and [`Radius`], which correspond to
/// different level of computation, with decreasing cost in term of memory and
/// execution time.
///
/// The const generic parameter `USE_TOT` (default `true`) controls whether the
/// algorithm keeps total-distance accumulators for tie-breaking when choosing
/// the next pivot. Setting it to `false` saves 16 bytes per node but may cause
/// the algorithm to perform more iterations.
///
/// # Examples
///
/// See the [module documentation].
///
/// [module documentation]: crate::distances::exact_sum_sweep
pub trait Level<const USE_TOT: bool = true>: Sync {
    /// The type of the result of [`run`].
    ///
    /// [`run`]: Self::run
    type Output: Send;
    /// The type of the result of [`run_symm`].
    ///
    /// [`run_symm`]: Self::run_symm
    type OutputSymm: Send;

    /// Runs the ExactSumSweep algorithm on the specified graph.
    ///
    /// # Arguments
    ///
    /// * `graph` - a graph.
    ///
    /// * `transpose` - the transpose of `graph`. Note that you are responsible
    ///   for providing a correct transpose. The result of the computation is
    ///   undefined otherwise.
    ///
    /// * `radial_vertices` - an [`AtomicBitVec`] where `v[i]` is true if node
    ///   `i` is to be considered radial vertex. If [`None`] the algorithm will
    ///   use the biggest connected component.
    ///
    /// * `pl` - a progress logger.
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
    /// * `graph` - a symmetric graph. Note that you are responsible for the
    ///   graph being symmetric. The result of the computation is undefined
    ///   otherwise.
    ///
    /// * `pl` - a progress logger.
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
#[derive(Debug, Clone, Copy)]
pub struct All;

impl<const USE_TOT: bool> Level<USE_TOT> for All {
    type Output = output::All;
    type OutputSymm = output_symm::All;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, false>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.all_iter.is_some());
        assert!(computer.forward_iter.is_some());
        assert!(computer.diameter_iterations.is_some());
        assert!(computer.radius_iterations.is_some());

        output::All {
            forward_eccentricities: computer.forward_low,
            backward_eccentricities: computer.backward_high,
            diameter: computer.diameter_low,
            radius: computer.radius_high,
            diametral_vertex: computer.diameter_vertex,
            radial_vertex: computer.radius_vertex,
            radius_iterations: computer.radius_iterations.unwrap(),
            diameter_iterations: computer.diameter_iterations.unwrap(),
            forward_iterations: computer.forward_iter.unwrap(),
            all_iterations: computer.all_iter.unwrap(),
        }
    }

    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, true>::new_symm(&graph, pl);
        computer.compute(pl);

        assert!(computer.forward_iter.is_some());
        assert!(computer.diameter_iterations.is_some());
        assert!(computer.radius_iterations.is_some());

        output_symm::All {
            eccentricities: computer.forward_low,
            diameter: computer.diameter_low,
            radius: computer.radius_high,
            diametral_vertex: computer.diameter_vertex,
            radial_vertex: computer.radius_vertex,
            radius_iterations: computer.radius_iterations.unwrap(),
            diameter_iterations: computer.diameter_iterations.unwrap(),
            iterations: computer.forward_iter.unwrap(),
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.all_forward + missing.all_backward
    }
}

/// Computes all forward eccentricities of a graph, its diameter, and its radius.
#[derive(Debug, Clone, Copy)]
pub struct AllForward;

impl<const USE_TOT: bool> Level<USE_TOT> for AllForward {
    type Output = output::AllForward;
    type OutputSymm = output_symm::All;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, false>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.forward_iter.is_some());
        assert!(computer.diameter_iterations.is_some());
        assert!(computer.radius_iterations.is_some());

        output::AllForward {
            forward_eccentricities: computer.forward_low,
            diameter: computer.diameter_low,
            radius: computer.radius_high,
            diametral_vertex: computer.diameter_vertex,
            radial_vertex: computer.radius_vertex,
            radius_iterations: computer.radius_iterations.unwrap(),
            diameter_iterations: computer.diameter_iterations.unwrap(),
            forward_iterations: computer.forward_iter.unwrap(),
        }
    }

    #[inline(always)]
    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        <All as Level<USE_TOT>>::run_symm(graph, pl)
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.all_forward
    }
}

/// Computes the diameter and the radius of a graph.
#[derive(Debug, Clone, Copy)]
pub struct RadiusDiameter;

impl<const USE_TOT: bool> Level<USE_TOT> for RadiusDiameter {
    type Output = output::RadiusDiameter;
    type OutputSymm = output_symm::RadiusDiameter;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, false>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.diameter_iterations.is_some());
        assert!(computer.radius_iterations.is_some());

        output::RadiusDiameter {
            diameter: computer.diameter_low,
            radius: computer.radius_high,
            diametral_vertex: computer.diameter_vertex,
            radial_vertex: computer.radius_vertex,
            radius_iterations: computer.radius_iterations.unwrap(),
            diameter_iterations: computer.diameter_iterations.unwrap(),
        }
    }

    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, true>::new_symm(&graph, pl);
        computer.compute(pl);

        assert!(computer.diameter_iterations.is_some());
        assert!(computer.radius_iterations.is_some());

        output_symm::RadiusDiameter {
            diameter: computer.diameter_low,
            radius: computer.radius_high,
            diametral_vertex: computer.diameter_vertex,
            radial_vertex: computer.radius_vertex,
            radius_iterations: computer.radius_iterations.unwrap(),
            diameter_iterations: computer.diameter_iterations.unwrap(),
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.radius + std::cmp::min(missing.diameter_forward, missing.diameter_backward)
    }
}

/// Computes the diameter of a graph.
#[derive(Debug, Clone, Copy)]
pub struct Diameter;

impl<const USE_TOT: bool> Level<USE_TOT> for Diameter {
    type Output = output::Diameter;
    type OutputSymm = output_symm::Diameter;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, false>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.diameter_iterations.is_some());

        output::Diameter {
            diameter: computer.diameter_low,
            diametral_vertex: computer.diameter_vertex,
            diameter_iterations: computer.diameter_iterations.unwrap(),
        }
    }

    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, true>::new_symm(&graph, pl);
        computer.compute(pl);

        assert!(computer.diameter_iterations.is_some());

        output_symm::Diameter {
            diameter: computer.diameter_low,
            diametral_vertex: computer.diameter_vertex,
            diameter_iterations: computer.diameter_iterations.unwrap(),
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        std::cmp::min(missing.diameter_forward, missing.diameter_backward)
    }
}

/// Computes the radius of a graph.
#[derive(Debug, Clone, Copy)]
pub struct Radius;

impl<const USE_TOT: bool> Level<USE_TOT> for Radius {
    type Output = output::Radius;
    type OutputSymm = output_symm::Radius;

    fn run(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::Output {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, false>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(pl);

        assert!(computer.radius_iterations.is_some());

        output::Radius {
            radius: computer.radius_high,
            radial_vertex: computer.radius_vertex,
            radius_iterations: computer.radius_iterations.unwrap(),
        }
    }

    fn run_symm(
        graph: impl RandomAccessGraph + Sync,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::OutputSymm {
        let mut computer = ExactSumSweep::<_, _, _, _, Self, USE_TOT, true>::new_symm(&graph, pl);
        computer.compute(pl);

        assert!(computer.radius_iterations.is_some());

        output_symm::Radius {
            radius: computer.radius_high,
            radial_vertex: computer.radius_vertex,
            radius_iterations: computer.radius_iterations.unwrap(),
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.radius
    }
}
