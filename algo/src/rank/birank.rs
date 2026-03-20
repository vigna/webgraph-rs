/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! BiRank for bipartite graphs.
//!
//! This implementation computes [BiRank] scores for vertices of a bipartite
//! graph using parallel power iteration. BiRank simultaneously ranks both
//! vertex sets of a bipartite graph through a mutually reinforcing
//! relationship: a vertex from one side should be ranked high if it is
//! connected to higher-ranked vertices from the other side.
//!
//! # Graph representation
//!
//! The bipartite graph *G* = (*U* ∪ *P*, *E*) is represented as a
//! [`RandomAccessGraph`] in which vertices [0 . . `num_sources`) form
//! the source set *U* and vertices [`num_sources` . . *n*) form the target set
//! *P*, with all arcs directed from *U* to *P*. Both the graph and its
//! [transpose](BiRank::new) are required.
//!
//! # The formula
//!
//! Let *W* be the |*U*| × |*P*| biadjacency matrix, where *wᵢⱼ*
//! is the weight of the edge between *uᵢ* ∈ *U* and *pⱼ* ∈ *P* (in this
//! implementation, *wᵢⱼ* ∈ {0, 1} since the graph is unweighted). Let *dᵢ*
//! and *dⱼ* be the weighted degrees of *uᵢ* and *pⱼ*, respectively.
//!
//! The ranking scores are defined by an additive update rule with
//! normalization:
//!
//! > *pⱼ* = ∑ᵢ *wᵢⱼ* · *uᵢ* ;  *uᵢ* = ∑ⱼ *wᵢⱼ* · *pⱼ* .
//!
//! To ensure convergence and stability, BiRank adopts the _symmetric
//! normalization_ scheme from Zhou _et al._, smoothing each edge weight by
//! the degrees of _both_ its endpoints:
//!
//! > *pⱼ* = ∑ᵢ (*wᵢⱼ* / √*dᵢ* √*dⱼ*) · *uᵢ* ;  *uᵢ* = ∑ⱼ (*wᵢⱼ* / √*dᵢ* √*dⱼ*) · *pⱼ* .
//!
//! This can be expressed in matrix form as **p** = *S*ᵀ **u** and
//! **u** = *S* **p**, where the symmetrically normalized matrix is
//!
//! > *S* = *Dᵤ*⁻½ *W* *Dₚ*⁻½,
//!
//! *Dᵤ* and *Dₚ* being diagonal matrices with (*Dᵤ*)ᵢᵢ = *dᵢ* and
//! (*Dₚ*)ⱼⱼ = *dⱼ*.
//!
//! To incorporate prior information, BiRank factors a _query vector_ (also
//! called _preference vector_) directly into the update. The full iterative
//! BiRank update is:
//!
//! > *pⱼ* ← α ∑ᵢ (*wᵢⱼ* / √*dᵢ* √*dⱼ*) · *uᵢ*  +  (1 − α) *pⱼ*⁰ ;
//! >
//! > *uᵢ* ← β ∑ⱼ (*wᵢⱼ* / √*dᵢ* √*dⱼ*) · *pⱼ*  +  (1 − β) *uᵢ*⁰ ;
//!
//! or equivalently in matrix form:
//!
//! > **p** ← α *S*ᵀ **u**  +  (1 − α) **p**⁰ ;
//! >
//! > **u** ← β *S* **p**  +  (1 − β) **u**⁰ ;
//!
//! where α, β ∈ [0 . . 1] are damping factors controlling the balance between
//! graph structure and the query vectors **p**⁰, **u**⁰. When both are 1 the
//! ranking is purely structural; when both are 0 the ranking is given entirely
//! by the query vectors.
//!
//! ## Connection with other algorithms
//!
//! Several bipartite graph ranking algorithms share the same update form
//! **p** = α *S*ᵀ **u** + (1 − α) **p**⁰, **u** = β *S* **p** + (1 − β) **u**⁰,
//! but differ in how they construct *S* and *S*ᵀ:
//!
//! | Method    | *S*                             | *S*ᵀ                           |
//! |-----------|---------------------------------|---------------------------------|
//! | HITS      | *W*                             | *Wᵀ*                           |
//! | Co‑HITS   | *W* *Dₚ*⁻¹                     | *Wᵀ* *Dᵤ*⁻¹                   |
//! | BGER      | *Dᵤ*⁻¹ *W*                     | *Dₚ*⁻¹ *Wᵀ*                   |
//! | BGRM      | *Dᵤ*⁻¹ *W* *Dₚ*⁻¹              | *Dₚ*⁻¹ *Wᵀ* *Dᵤ*⁻¹            |
//! | **BiRank** | *Dᵤ*⁻½ *W* *Dₚ*⁻½              | *Dₚ*⁻½ *Wᵀ* *Dᵤ*⁻½            |
//!
//! BiRank's symmetric normalization makes *S*ᵀ*S* similar to a stochastic
//! matrix, with eigenvalues in \[−αβ . . αβ\], guaranteeing convergence to a
//! unique stationary point for α, β ∈ (0 . . 1). The convergence rate
//! depends on the second largest eigenvalue of *S*ᵀ*S* in magnitude.
//!
//! # The algorithm
//!
//! 1. Symmetrically normalize *W*:  *S* = *Dᵤ*⁻½ *W* *Dₚ*⁻½
//!    (precomputed as per-node factors 1/√*dᵢ*, 1/√*dⱼ*).
//! 2. Initialize ranks from the preference vector.
//! 3. **while** the stopping criterion is not met **do**
//!    - **for** each *pⱼ* (target node):
//!      *pⱼ* ← α · (1/√*dⱼ*) · ∑_{*i*→*j*} (1/√*dᵢ*) · *uᵢ*  +  (1 − α) · *pⱼ*⁰ ;
//!    - **for** each *uᵢ* (source node):
//!      *uᵢ* ← β · (1/√*dᵢ*) · ∑_{*i*→*j*} (1/√*dⱼ*) · *pⱼ*  +  (1 − β) · *uᵢ*⁰ ;
//! 4. **return** **p** and **u**.
//!
//! Note that the target nodes are updated first (Phase 1), and then the
//! source nodes are updated using the _new_ target scores (Phase 2).
//!
//! # Parallelism
//!
//! Unlike [Gauss–Seidel PageRank](super::pagerank), where reads and writes
//! may overlap on the same array, BiRank's two-phase structure guarantees
//! that the read and write sets are _completely disjoint_ within each phase:
//!
//! - **Phase 1** (target *P* nodes): reads from source indices
//!   [0 . . `num_sources`), writes to target indices [`num_sources` . . *n*).
//! - **Phase 2** (source *U* nodes): reads from target indices
//!   [`num_sources` . . *n*), writes to source indices [0 . . `num_sources`).
//!
//! This means there are no data races at all—neither benign nor harmful—making
//! the [`SyncCell`] usage sound by construction. Each thread grabs a chunk of
//! nodes from an [`AtomicUsize`] cursor and processes them independently.
//!
//! # Stopping criteria
//!
//! The [`run`](BiRank::run) method accepts a composable [`Predicate`] that is
//! evaluated after each iteration. The predicate receives the current
//! iteration number and the ℓ₁ norm of the rank-vector change:
//!
//! > ‖**x**⁽ᵗ⁾ − **x**⁽ᵗ⁻¹⁾‖₁
//!
//! where **x** = (**u**, **p**) is the concatenated rank vector.
//!
//! This module re-exports the [predicates](preds) from the
//! [`pagerank`](super::pagerank) module, as they share the same
//! [`PredParams`] structure.
//!
//! # References
//!
//! Xiangnan He, Ming Gao, Min-Yen Kan, and Dingxian Wang. [BiRank: Towards
//! Ranking on Bipartite
//! Graphs](https://doi.org/10.1109/TKDE.2017.2750166). *IEEE
//! Transactions on Knowledge and Data Engineering*, 29(1):57–71, 2017.
//!
//! [BiRank]: https://doi.org/10.1109/TKDE.2017.2750166
//! [`Predicate`]: predicates::Predicate
//! [`PredParams`]: preds::PredParams
//! [`SyncCell`]: sync_cell_slice::SyncCell
//! [`AtomicUsize`]: std::sync::atomic::AtomicUsize

pub use super::pagerank::preds;

use super::pagerank::UniformPreference;
use dsi_progress_logger::{ConcurrentProgressLog, ProgressLog, no_logging};
use kahan::KahanSum;
use lender::prelude::*;
use predicates::Predicate;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use sync_cell_slice::SyncSlice;
use value_traits::slices::SliceByValue;
use webgraph::traits::RandomAccessGraph;
use webgraph::utils::Granularity;

/// Computes BiRank scores for a bipartite graph using parallel power
/// iteration.
///
/// For details about the algorithm, see the [module-level
/// documentation](self).
///
/// The struct is configured via setters and then executed via
/// [`run`](Self::run). After completion the rank vector is available via the
/// [`rank`](Self::rank) method, where `rank[i]` for *i* < `num_sources` is
/// the score of source node *i* (*uᵢ*), and `rank[j]` for
/// *j* ≥ `num_sources` is the score of target node *j* (*pⱼ*).
///
/// Note that the [`preference`](Self::preference) setter consumes `self`
/// because the preference type may differ from the current one; all internal
/// state (including cached 1/√*d* values) is preserved.
///
/// If you compute multiple variants of BiRank on the same graph, please reuse
/// this structure, as it caches the inverse square-root degrees of nodes.
///
/// # Examples
///
/// Default BiRank (α = β = 0.85) on a small bipartite graph:
///
/// ```
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph_algo::rank::birank::{BiRank, preds};
///
/// // U = {0, 1, 2}, P = {3, 4, 5}
/// // Arcs: 0→3, 0→4, 1→3, 2→4, 2→5
/// let mut graph = VecGraph::empty(6);
/// graph.add_arcs([(0, 3), (0, 4), (1, 3), (2, 4), (2, 5)]);
///
/// let mut transpose = VecGraph::empty(6);
/// transpose.add_arcs([(3, 0), (4, 0), (3, 1), (4, 2), (5, 2)]);
///
/// let mut br = BiRank::new(&graph, &transpose, 3);
/// br.run(preds::L1Norm::try_from(1E-9).unwrap());
///
/// assert_eq!(br.rank().len(), 6);
/// ```
pub struct BiRank<
    'a,
    G: RandomAccessGraph + Sync,
    H: RandomAccessGraph + Sync,
    V: SliceByValue<Value = f64> = UniformPreference,
> {
    graph: &'a G,
    transpose: &'a H,
    num_sources: usize,
    alpha: f64,
    beta: f64,
    preference: V,
    inv_sqrt_degrees: Option<Box<[f64]>>,
    granularity: Granularity,
    norm_delta: f64,

    rank: Box<[f64]>,
    iteration: usize,
}

impl<G: RandomAccessGraph + Sync, H: RandomAccessGraph + Sync, V: SliceByValue<Value = f64>>
    std::fmt::Debug for BiRank<'_, G, H, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BiRank")
            .field("num_sources", &self.num_sources)
            .field("alpha", &self.alpha)
            .field("beta", &self.beta)
            .field("granularity", &self.granularity)
            .field("norm_delta", &self.norm_delta)
            .field("iteration", &self.iteration)
            .finish_non_exhaustive()
    }
}

impl<'a, G: RandomAccessGraph + Sync, H: RandomAccessGraph + Sync> BiRank<'a, G, H> {
    /// Creates a new BiRank computation with uniform preference.
    ///
    /// # Arguments
    ///
    /// * `graph` — the bipartite graph with arcs from source nodes
    ///   [0 . . `num_sources`) to target nodes [`num_sources` . . *n*).
    /// * `transpose` — the transpose of `graph`.
    /// * `num_sources` — the number of source nodes (|*U*|).
    ///
    /// # Panics
    ///
    /// Panics if `graph` and `transpose` have different numbers of nodes, or
    /// if `num_sources` exceeds the number of nodes.
    pub fn new(graph: &'a G, transpose: &'a H, num_sources: usize) -> Self {
        let n = graph.num_nodes();
        assert_eq!(
            n,
            transpose.num_nodes(),
            "Graph and transpose must have the same number of nodes ({n} vs {})",
            transpose.num_nodes()
        );
        assert!(
            num_sources <= n,
            "num_sources ({num_sources}) exceeds the number of nodes ({n})"
        );
        Self {
            graph,
            transpose,
            num_sources,
            alpha: 0.85,
            beta: 0.85,
            preference: UniformPreference::new(n),
            inv_sqrt_degrees: None,
            granularity: Granularity::default(),
            norm_delta: f64::INFINITY,
            rank: vec![0.0; n].into_boxed_slice(),
            iteration: 0,
        }
    }
}

impl<'a, G: RandomAccessGraph + Sync, H: RandomAccessGraph + Sync, V: SliceByValue<Value = f64>>
    BiRank<'a, G, H, V>
{
    /// Sets the damping factor α for the target (*P*) nodes.
    ///
    /// Controls the balance between graph structure and the query vector
    /// **p**⁰. When α = 1 the ranking is purely structural; when α = 0 the
    /// target scores are fixed to **p**⁰.
    ///
    /// # Panics
    ///
    /// Panics if `alpha` is not in [0 . . 1].
    pub fn alpha(&mut self, alpha: f64) -> &mut Self {
        assert!(
            (0.0..=1.0).contains(&alpha),
            "Alpha must be in [0 . . 1], got {alpha}"
        );
        self.alpha = alpha;
        self
    }

    /// Sets the damping factor β for the source (*U*) nodes.
    ///
    /// Controls the balance between graph structure and the query vector
    /// **u**⁰. When β = 1 the ranking is purely structural; when β = 0 the
    /// source scores are fixed to **u**⁰.
    ///
    /// # Panics
    ///
    /// Panics if `beta` is not in [0 . . 1].
    pub fn beta(&mut self, beta: f64) -> &mut Self {
        assert!(
            (0.0..=1.0).contains(&beta),
            "Beta must be in [0 . . 1], got {beta}"
        );
        self.beta = beta;
        self
    }

    /// Sets the preference (query) vector.
    ///
    /// The preference vector has length *n* (the total number of nodes).
    /// Entries [0 . . `num_sources`) serve as **u**⁰ (query vector for source
    /// nodes) and entries [`num_sources` . . *n*) serve as **p**⁰ (query
    /// vector for target nodes).
    ///
    /// The preference vector is any [`SliceByValue<Value =
    /// f64>`](SliceByValue): for example, a `&[f64]`, a `Vec<f64>`, or a
    /// functional/implicit implementation such as [`UniformPreference`].
    ///
    /// This method consumes `self` because the preference type may differ
    /// from the current one; all internal state (including cached 1/√*d*
    /// values) is preserved.
    ///
    /// # Panics
    ///
    /// Panics if the length of the vector does not match the number of nodes.
    pub fn preference<W: SliceByValue<Value = f64>>(self, preference: W) -> BiRank<'a, G, H, W> {
        let n = self.graph.num_nodes();
        assert_eq!(
            preference.len(),
            n,
            "Preference vector length ({}) does not match the number of nodes ({n})",
            preference.len()
        );
        BiRank {
            graph: self.graph,
            transpose: self.transpose,
            num_sources: self.num_sources,
            alpha: self.alpha,
            beta: self.beta,
            preference,
            inv_sqrt_degrees: self.inv_sqrt_degrees,
            granularity: self.granularity,
            norm_delta: self.norm_delta,
            rank: self.rank,
            iteration: self.iteration,
        }
    }

    /// Sets the parallel task granularity.
    ///
    /// The granularity expresses how many
    /// [nodes](Granularity::node_granularity) will be passed to a Rayon task
    /// at a time.
    pub fn granularity(&mut self, granularity: Granularity) -> &mut Self {
        self.granularity = granularity;
        self
    }

    /// Returns the rank vector.
    ///
    /// After calling [`run`](Self::run), entries [0 . . `num_sources`) contain
    /// the scores *uᵢ* of source (*U*) nodes and entries
    /// [`num_sources` . . *n*) the scores *pⱼ* of target (*P*) nodes.
    pub fn rank(&self) -> &[f64] {
        &self.rank
    }

    /// Returns the number of iterations performed by the last call to
    /// [`run`](Self::run).
    pub const fn iterations(&self) -> usize {
        self.iteration
    }

    /// Returns the ℓ₁ norm of the rank-vector change after the last
    /// iteration, that is, ‖**x**⁽ᵗ⁾ − **x**⁽ᵗ⁻¹⁾‖₁.
    pub const fn norm_delta(&self) -> f64 {
        self.norm_delta
    }
}

impl<G: RandomAccessGraph + Sync, H: RandomAccessGraph + Sync, V: SliceByValue<Value = f64> + Sync>
    BiRank<'_, G, H, V>
{
    /// Runs the BiRank computation until the given predicate is satisfied.
    pub fn run(&mut self, predicate: impl Predicate<preds::PredParams>) {
        self.run_with_logging(predicate, no_logging![], no_logging![]);
    }

    /// Runs the BiRank computation until the given predicate is satisfied,
    /// logging progress.
    ///
    /// `pl` is a sequential [`ProgressLog`] used for degree computation and
    /// iteration counting. `cpl` is a [`ConcurrentProgressLog`] used for
    /// node-level progress inside each iteration phase.
    pub fn run_with_logging(
        &mut self,
        predicate: impl Predicate<preds::PredParams>,
        pl: &mut impl ProgressLog,
        cpl: &mut impl ConcurrentProgressLog,
    ) {
        let n = self.graph.num_nodes();
        let num_u = self.num_sources;
        let num_p = n - num_u;

        if n == 0 || num_u == 0 || num_p == 0 {
            return;
        }

        log::info!("Alpha: {}", self.alpha);
        log::info!("Beta: {}", self.beta);
        log::info!("Source set size (|U|): {}", num_u);
        log::info!("Target set size (|P|): {}", num_p);
        log::info!("Stopping criterion: {}", predicate);

        self.iteration = 0;

        // Initialize rank with preference vector
        for i in 0..n {
            // SAFETY: i < n == self.preference.len()
            self.rank[i] = unsafe { self.preference.get_value_unchecked(i) };
        }

        // Precompute 1/√dᵢ for each node (cached across runs).
        // For source nodes dᵢ = outdegree in graph; for target nodes
        // dⱼ = outdegree in transpose (= indegree in graph).
        let inv_sqrt_degrees = self.inv_sqrt_degrees.get_or_insert_with(|| {
            let mut inv_sqrt_d = vec![0.0; n].into_boxed_slice();

            pl.item_name("node");
            pl.expected_updates(Some(n));
            pl.start("Computing inverse square-root degrees...");

            for i in 0..num_u {
                let d = self.graph.outdegree(i);
                if d > 0 {
                    inv_sqrt_d[i] = 1.0 / (d as f64).sqrt();
                }
                pl.light_update();
            }
            for j in num_u..n {
                let d = self.transpose.outdegree(j);
                if d > 0 {
                    inv_sqrt_d[j] = 1.0 / (d as f64).sqrt();
                }
                pl.light_update();
            }

            pl.done();
            inv_sqrt_d
        });

        let node_granularity = self
            .granularity
            .node_granularity(n, Some(self.graph.num_arcs()))
            .max(1);

        pl.item_name("iteration");
        pl.expected_updates(None);
        pl.start(format!(
            "Computing BiRank (alpha={}, beta={}, granularity={node_granularity})...",
            self.alpha, self.beta
        ));

        loop {
            let norm_delta_accum = Mutex::new(0.0f64);
            let rank_sync = self.rank.as_sync_slice();

            // Phase 1: update target (P) nodes.
            //
            // pⱼ ← α · (1/√dⱼ) · ∑_{i→j} (1/√dᵢ) · uᵢ  +  (1 − α) · pⱼ⁰
            //
            // Reads from source indices [0 .. num_u), writes to target
            // indices [num_u .. n). The two sets are disjoint: no data
            // races are possible.
            {
                let p_cursor = AtomicUsize::new(0);

                cpl.item_name("node");
                cpl.expected_updates(Some(num_p));
                cpl.start(format!(
                    "Iteration {} phase 1 (target nodes)...",
                    self.iteration + 1
                ));

                rayon::broadcast(|_| {
                    let mut local_cpl = cpl.clone();
                    let mut local_delta: KahanSum<f64> = KahanSum::new();

                    loop {
                        let start = p_cursor.fetch_add(node_granularity, Ordering::Relaxed);
                        if start >= num_p {
                            break;
                        }
                        let len = node_granularity.min(num_p - start);

                        for_![(j, succ) in self.transpose.iter_from(start + num_u).take(len) {
                            // SAFETY: threads write to disjoint target-node
                            // ranges and read only from source nodes, which
                            // are not modified in this phase.
                            unsafe {
                                let mut sigma: KahanSum<f64> = KahanSum::new();
                                for i in succ {
                                    sigma += inv_sqrt_degrees[i] * rank_sync[i].get();
                                }

                                let v_j = self.preference.get_value_unchecked(j);
                                let new_rank = self.alpha
                                    * inv_sqrt_degrees[j]
                                    * sigma.sum()
                                    + (1.0 - self.alpha) * v_j;

                                local_delta += (new_rank - rank_sync[j].get()).abs();
                                rank_sync[j].set(new_rank);
                            }
                        }];

                        local_cpl.update_with_count(len);
                    }

                    *norm_delta_accum.lock().unwrap() += local_delta.sum();
                });

                cpl.done();
            }

            // Phase 2: update source (U) nodes.
            //
            // uᵢ ← β · (1/√dᵢ) · ∑_{i→j} (1/√dⱼ) · pⱼ  +  (1 − β) · uᵢ⁰
            //
            // Reads from target indices [num_u .. n) (just updated in
            // phase 1), writes to source indices [0 .. num_u). The two
            // sets are disjoint: no data races are possible.
            {
                let u_cursor = AtomicUsize::new(0);

                cpl.item_name("node");
                cpl.expected_updates(Some(num_u));
                cpl.start(format!(
                    "Iteration {} phase 2 (source nodes)...",
                    self.iteration + 1
                ));

                rayon::broadcast(|_| {
                    let mut local_cpl = cpl.clone();
                    let mut local_delta: KahanSum<f64> = KahanSum::new();

                    loop {
                        let start = u_cursor.fetch_add(node_granularity, Ordering::Relaxed);
                        if start >= num_u {
                            break;
                        }
                        let len = node_granularity.min(num_u - start);

                        for_![(i, succ) in self.graph.iter_from(start).take(len) {
                            // SAFETY: threads write to disjoint source-node
                            // ranges and read only from target nodes, which
                            // are not modified in this phase.
                            unsafe {
                                let mut sigma: KahanSum<f64> = KahanSum::new();
                                for j in succ {
                                    sigma += inv_sqrt_degrees[j] * rank_sync[j].get();
                                }

                                let v_i = self.preference.get_value_unchecked(i);
                                let new_rank = self.beta
                                    * inv_sqrt_degrees[i]
                                    * sigma.sum()
                                    + (1.0 - self.beta) * v_i;

                                local_delta += (new_rank - rank_sync[i].get()).abs();
                                rank_sync[i].set(new_rank);
                            }
                        }];

                        local_cpl.update_with_count(len);
                    }

                    *norm_delta_accum.lock().unwrap() += local_delta.sum();
                });

                cpl.done();
            }

            self.norm_delta = *norm_delta_accum.lock().unwrap();
            self.iteration += 1;

            log::info!(
                "Iteration {}: norm delta = {}",
                self.iteration,
                self.norm_delta
            );

            pl.update_and_display();

            if predicate.eval(&preds::PredParams {
                iteration: self.iteration,
                norm_delta: self.norm_delta,
            }) {
                break;
            }
        }

        pl.done();
    }
}
