/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{computer::DirExactSumSweepComputer, outputs, outputs_symm};
use dsi_progress_logger::ConcurrentProgressLog;
use rayon::ThreadPool;
use sux::bits::AtomicBitVec;
use webgraph::traits::RandomAccessGraph;

#[derive(Debug, Clone, Copy, Default)]
pub struct Missing {
    pub r: usize,
    pub df: usize,
    pub db: usize,
    pub all_forward: usize,
    pub all_backward: usize,
}

impl core::ops::Add for Missing {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {
            r: self.r + rhs.r,
            df: self.df + rhs.df,
            db: self.db + rhs.db,
            all_forward: self.all_forward + rhs.all_forward,
            all_backward: self.all_backward + rhs.all_backward,
        }
    }
}
/// Trait used to compute the results of the ExactSumSweep algorithm.
pub trait OutputLevel: Sync {
    /// The result of the algorithm when called on directed graphs.
    type DirectedOutput;
    /// The result of the algorithm when called on undirected graphs.
    type UndirectedOutput;

    /// Build a new instance to compute the *ExactSumSweep* algorithm on
    /// the specified directed graph and returns the results.
    ///
    /// # Arguments
    /// * `graph`: the direct graph.
    /// * `transpose`: the transpose of `graph`.
    /// * `radial_vertices`: an [`AtomicBitVec`] where `v[i]` is true if node `i` is to be considered
    ///    radial vertex. If [`None`] the algorithm will use the biggest connected component.
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: a progress logger.
    fn compute(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::DirectedOutput;

    /// Build a new instance to compute the *ExactSumSweep* algorithm on the specified
    /// undirected graph and returns the results.
    ///
    /// # Arguments
    /// * `graph`: the graph.
    /// * `output`: the desired output of the algorithm.
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: a progress logger.
    fn compute_symm(
        graph: impl RandomAccessGraph + Sync,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::UndirectedOutput;

    fn missing_nodes(missing_nodes: &Missing) -> usize;
}

/// Computes all the eccentricities of the graph.
///
/// This variant is equivalent to [`AllForward`] in the undirected case.
pub struct All;

impl OutputLevel for All {
    type DirectedOutput = outputs::All;
    type UndirectedOutput = outputs_symm::All;

    fn compute(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::DirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(thread_pool, pl);

        assert!(
            computer.all_iter.is_some(),
            "Trying to build All without all eccentricities computed"
        );
        assert!(
            computer.forward_iter.is_some(),
            "Trying to build All without all forward eccentricities computed"
        );
        assert!(
            computer.diameter_iterations.is_some(),
            "Trying to build All without the diameter computed"
        );
        assert!(
            computer.radius_iterations.is_some(),
            "Trying to build All without the radius computed"
        );

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

        Self::DirectedOutput {
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

    fn compute_symm(
        graph: impl RandomAccessGraph + Sync,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::UndirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new_symm(&graph, pl);
        computer.compute(thread_pool, pl);

        assert!(
            computer.forward_iter.is_some(),
            "Trying to build All without all forward eccentricities computed"
        );
        assert!(
            computer.diameter_iterations.is_some(),
            "Trying to build All without the diameter computed"
        );
        assert!(
            computer.radius_iterations.is_some(),
            "Trying to build All without the radius computed"
        );

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();
        let iterations = computer.forward_iter.unwrap();
        let eccentricities = computer.forward_low;

        Self::UndirectedOutput {
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

/// Computes all the forward eccentricities of the graph.
pub struct AllForward;

impl OutputLevel for AllForward {
    type DirectedOutput = outputs::AllForward;
    type UndirectedOutput = outputs_symm::All;

    fn compute(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::DirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(thread_pool, pl);

        assert!(
            computer.forward_iter.is_some(),
            "Trying to build AllForward without all forward eccentricities computed"
        );
        assert!(
            computer.diameter_iterations.is_some(),
            "Trying to build AllForward without the diameter computed"
        );
        assert!(
            computer.radius_iterations.is_some(),
            "Trying to build AllForward without the radius computed"
        );

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();
        let forward_iterations = computer.forward_iter.unwrap();
        let forward_eccentricities = computer.forward_low;

        Self::DirectedOutput {
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
    fn compute_symm(
        graph: impl RandomAccessGraph + Sync,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::UndirectedOutput {
        All::compute_symm(graph, thread_pool, pl)
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.all_forward
    }
}

/// Computes both the diameter and the radius of the graph.
pub struct RadiusDiameter;

impl OutputLevel for RadiusDiameter {
    type DirectedOutput = outputs::RadiusDiameter;
    type UndirectedOutput = outputs_symm::RadiusDiameter;

    fn compute(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::DirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(thread_pool, pl);

        assert!(
            computer.diameter_iterations.is_some(),
            "Trying to build RadiusDiameter without the diameter computed"
        );
        assert!(
            computer.radius_iterations.is_some(),
            "Trying to build RadiusDiameter without the radius computed"
        );

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();

        Self::DirectedOutput {
            diameter,
            radius,
            diametral_vertex,
            radial_vertex,
            radius_iterations,
            diameter_iterations,
        }
    }

    fn compute_symm(
        graph: impl RandomAccessGraph + Sync,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::UndirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new_symm(&graph, pl);
        computer.compute(thread_pool, pl);

        assert!(
            computer.diameter_iterations.is_some(),
            "Trying to build RadiusDiameter without the diameter computed"
        );
        assert!(
            computer.radius_iterations.is_some(),
            "Trying to build RadiusDiameter without the radius computed"
        );

        let diameter = computer.diameter_low;
        let radius = computer.radius_high;
        let diametral_vertex = computer.diameter_vertex;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();
        let diameter_iterations = computer.diameter_iterations.unwrap();

        Self::UndirectedOutput {
            diameter,
            radius,
            diametral_vertex,
            radial_vertex,
            radius_iterations,
            diameter_iterations,
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.r + std::cmp::min(missing.df, missing.db)
    }
}

/// Computes the diameter of the graph.
pub struct Diameter;

impl OutputLevel for Diameter {
    type DirectedOutput = outputs::Diameter;
    type UndirectedOutput = outputs_symm::Diameter;

    fn compute(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::DirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(thread_pool, pl);

        assert!(
            computer.diameter_iterations.is_some(),
            "Trying to build Diameter without the diameter computed"
        );

        let diameter = computer.diameter_low;
        let diametral_vertex = computer.diameter_vertex;
        let diameter_iterations = computer.diameter_iterations.unwrap();

        Self::DirectedOutput {
            diameter,
            diametral_vertex,
            diameter_iterations,
        }
    }

    fn compute_symm(
        graph: impl RandomAccessGraph + Sync,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::UndirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new_symm(&graph, pl);
        computer.compute(thread_pool, pl);

        assert!(
            computer.diameter_iterations.is_some(),
            "Trying to build Diameter without the diameter computed"
        );

        let diameter = computer.diameter_low;
        let diametral_vertex = computer.diameter_vertex;
        let diameter_iterations = computer.diameter_iterations.unwrap();

        Self::UndirectedOutput {
            diameter,
            diametral_vertex,
            diameter_iterations,
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        std::cmp::min(missing.df, missing.db)
    }
}

/// Computes the radius of the graph.
pub struct Radius;

impl OutputLevel for Radius {
    type DirectedOutput = outputs::Radius;
    type UndirectedOutput = outputs_symm::Radius;

    fn compute(
        graph: impl RandomAccessGraph + Sync,
        transpose: impl RandomAccessGraph + Sync,
        radial_vertices: Option<AtomicBitVec>,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::DirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new(
            &graph,
            &transpose,
            radial_vertices,
            pl,
        );
        computer.compute(thread_pool, pl);

        assert!(
            computer.radius_iterations.is_some(),
            "Trying to build Radius without the radius computed"
        );

        let radius = computer.radius_high;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();

        Self::DirectedOutput {
            radius,
            radial_vertex,
            radius_iterations,
        }
    }

    fn compute_symm(
        graph: impl RandomAccessGraph + Sync,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Self::UndirectedOutput {
        let mut computer = DirExactSumSweepComputer::<_, _, _, _, Self>::new_symm(&graph, pl);
        computer.compute(thread_pool, pl);

        assert!(
            computer.radius_iterations.is_some(),
            "Trying to build Radius without the radius computed"
        );

        let radius = computer.radius_high;
        let radial_vertex = computer.radius_vertex;
        let radius_iterations = computer.radius_iterations.unwrap();

        Self::UndirectedOutput {
            radius,
            radial_vertex,
            radius_iterations,
        }
    }

    fn missing_nodes(missing: &Missing) -> usize {
        missing.r
    }
}
