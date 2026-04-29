/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Parallel Gauss–Seidel PageRank.
//!
//! This implementation uses two vectors of doubles (one for the current
//! approximation, the other for the inverses of outdegrees) and,
//! experimentally, converges faster than other implementations. Moreover, it
//! scales linearly with the number of cores.
//!
//! **Warning**: since we need to enumerate the _predecessors_ of a node, you
//! must pass to the [constructor] the **transpose** of the
//! graph.
//!
//! # The formula
//!
//! There are two main formulae for PageRank in the literature. The first one,
//! which we call _weakly preferential_, patches all dangling nodes by adding a
//! uniform transition towards all other nodes. The second one, which we call
//! _strongly preferential_, patches all dangling nodes by adding transitions
//! weighted following the preference vector **v**. We can consider the two
//! formulae together, letting **u** be a stochastic vector that is uniform in
//! the weak case and coincides with **v** in the strong case.
//!
//! If we denote with *P* the row-normalized adjacency matrix of the graph
//! (with zero rows for dangling nodes), with **d** the characteristic vector of
//! dangling nodes, and with α the damping factor, the generic equation is
//!
//! > **x** = **x** ( α *P*  +  α **d**ᵀ **u**  +  (1 − α) **1**ᵀ **v** )
//!
//! which, distributing, yields
//!
//! > **x** = (1 − α) **v** ( *I* − α (*P* + **d**ᵀ **u**) )⁻¹,
//!
//! to which we can apply the Gauss–Seidel method.
//!
//! The [`mode`] setter selects among three variants:
//!
//! - [`StronglyPreferential`] (the default):
//!   **u** = **v**, so the preference vector doubles as the dangling-node
//!   distribution.
//! - [`WeaklyPreferential`]: **u** = **1**/*n*, so
//!   dangling nodes distribute their rank uniformly regardless of the
//!   preference vector.
//! - [`PseudoRank`]: **u** = **0**, zeroing out the
//!   dangling-node contribution entirely and yielding a non-stochastic vector
//!   sometimes called _pseudorank_.
//!
//! # The Gauss–Seidel method
//!
//! The formula above can be rewritten as the linear system
//!
//! > **x** ( *I*  −  α (*P* + **d**ᵀ **u**) )  =  (1 − α) **v**
//!
//! that is, **x** *M* = **b** where *M* = *I* − α (*P* + **d**ᵀ **u**) and
//! **b** = (1 − α) **v**. The [Gauss–Seidel method] solves this system by
//! updating a _single_ vector in place:
//!
//! > *xᵢ*⁽*ᵗ* ⁺ ¹⁾ = ( *bᵢ* − ∑*ⱼ*<sub><</sub>*ᵢ* *mᵢⱼ*·*xⱼ*⁽*ᵗ* ⁺ ¹⁾ − ∑*ⱼ*<sub>></sub>*ᵢ* *mᵢⱼ*·*xⱼ*⁽*ᵗ*⁾ ) / *mᵢᵢ*.
//!
//! Substituting the expressions for *M* and **b** we obtain the update rule
//!
//! > *xᵢ*⁽*ᵗ* ⁺ ¹⁾ = ( (1 − α) *vᵢ* + α ( ∑*ⱼ*<sub><</sub>*ᵢ* (*pⱼᵢ* + *uᵢ* *dⱼ*) *xⱼ*⁽*ᵗ* ⁺ ¹⁾ + ∑*ⱼ*<sub>></sub>*ᵢ* (*pⱼᵢ* + *uᵢ* *dⱼ*) *xⱼ*⁽*ᵗ*⁾ ) ) / (1 − α *pᵢᵢ* − α *uᵢ* *dᵢ*).
//!
//! We can rearrange these sums into two separate contributions: one from nodes
//! *j* → *i* (predecessors of *i*) and one from dangling nodes. Non-dangling
//! nodes that are not predecessors of *i* give no contribution. The
//! Gauss–Seidel method can thus be implemented as follows:
//!
//! 1. initialize **x** as the preference vector (or 1/*n* if uniform);
//! 2. while the stopping criterion is not met, for each
//!    *i* = 0, 1, …, *n* − 1:
//!    - σ = 0;
//!    - for each *j* → *i* with *j* ≠ *i*:  σ += *pⱼᵢ* · *xⱼ*;
//!    - for each dangling *j*:  σ += *uᵢ* · *xⱼ*;
//!    - *xᵢ* = ( (1 − α) *vᵢ* + α σ ) / (1 − α *pᵢᵢ* − α *dᵢ* *uᵢ*).
//!
//! Here *uᵢ* = 1/*n* for the weakly preferential variant, or *vᵢ* for the
//! strongly preferential one.
//!
//! ## The *B*/*A* optimization
//!
//! The inner loop over all dangling nodes can be avoided by maintaining two
//! running totals: *B* (the accumulated rank of dangling nodes _before_ index
//! *i*) and *A* (the accumulated rank of dangling nodes from index *i* on):
//!
//! 1. initialize **x** as the preference vector;
//! 2. *B* = 0; *A* = ∑ over dangling *j* of *xⱼ*;
//! 3. while the stopping criterion is not met, for each
//!    *i* = 0, 1, …, *n* − 1:
//!    - σ = 0;
//!    - for each *j* → *i* with *j* ≠ *i*:  σ += *pⱼᵢ* · *xⱼ*;
//!    - σ += (*A* + *B* − *dᵢ* · *xᵢ*) · *uᵢ*;
//!    - σ = ( (1 − α) *vᵢ* + α σ ) / (1 − α *pᵢᵢ* − α *dᵢ* *uᵢ*);
//!    - if *i* is dangling: *B* += σ; *A* −= *xᵢ*;
//!    - *xᵢ* = σ.
//!
//! # Parallelism
//!
//! Technically, the iteration performed by this implementation is _not_ a true
//! Gauss–Seidel iteration: we simply start a number of threads, and each
//! thread updates a value using a Gauss–Seidel-like rule. As a result, each
//! update uses some old and some new values: in other words, the _regular
//! splitting_ *M* − *N* = *I* − α (*P* + **u**ᵀ **d**) associated with each
//! update is always different (in a true Gauss–Seidel iteration, *M* is upper
//! triangular and *N* is strictly lower triangular). Nonetheless, it is easy to
//! check that *M* is still (up to permutation) upper triangular and invertible,
//! independently of the specific update sequence.
//!
//! The rank vector is shared among threads via [`SyncCell`]. Each thread grabs
//! a chunk of nodes from an [`AtomicUsize`] cursor and updates ranks in place.
//!
//! To avoid excessive synchronization, the dangling rank (the sum of the
//! rank of dangling nodes) is computed at the end of each iteration and
//! used unchanged throughout the next one. This corresponds to permuting the
//! array so that dangling nodes come out last.
//!
//! # Stopping Criteria
//!
//! The [`run`] method accepts a composable
//! [`Predicate`] that is evaluated after each iteration.
//! The predicate receives the current iteration number and a _norm delta_—an
//! upper bound on the ℓ₁ error between the current approximation and the true
//! PageRank vector, computed as
//!
//! > α / (1 − α) · ‖**x**⁽ᵗ⁾ − **x**⁽ᵗ⁻¹⁾‖₁
//!
//! This idea arose in discussions with David Gleich.
//!
//! [constructor]: PageRank::new
//! [`mode`]: PageRank::mode
//! [`StronglyPreferential`]: Mode::StronglyPreferential
//! [`WeaklyPreferential`]: Mode::WeaklyPreferential
//! [`PseudoRank`]: Mode::PseudoRank
//! [`run`]: PageRank::run
//! [`Predicate`]: predicates::Predicate
//! [Gauss–Seidel method]: https://en.wikipedia.org/wiki/Gauss%E2%80%93Seidel_method
//! [`SyncCell`]: sync_cell_slice::SyncCell
//! [`AtomicUsize`]: std::sync::atomic::AtomicUsize

pub use super::preds;

use preds::{HasIteration, HasL1Norm};

/// Carries the data passed to stopping predicates by [`PageRank`].
///
/// Implements [`HasIteration`] and [`HasL1Norm`]. The ℓ₁ norm delta
/// is an upper bound on the ℓ₁ error, computed as
/// α / (1 − α) · ‖**x**⁽ᵗ⁾ − **x**⁽ᵗ⁻¹⁾‖₁ (see the [module-level
/// documentation]).
///
/// [module-level documentation]: self
#[doc(hidden)]
#[derive(Debug)]
pub struct PredParams {
    pub iteration: usize,
    pub l1_norm_delta: f64,
}

impl HasIteration for PredParams {
    fn iteration(&self) -> usize {
        self.iteration
    }
}

impl HasL1Norm for PredParams {
    fn l1_norm(&self) -> f64 {
        self.l1_norm_delta
    }
}

/// Selects the PageRank variant to compute.
///
/// See the [module-level documentation] for the mathematical details.
///
/// [module-level documentation]: self
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Uses the preference vector **v** as the dangling-node distribution
    /// (**u** = **v**). This is the default.
    #[default]
    StronglyPreferential,
    /// Uses a uniform dangling-node distribution (**u** = **1**/*n*) regardless
    /// of the preference vector.
    WeaklyPreferential,
    /// Zeroes out the dangling-node contribution (**u** = **0**), yielding in
    /// the case there are dangling nodes a non-stochastic vector which however
    /// is identical to the [strongly preferential]
    /// variant modulo normalization.
    ///
    /// [strongly preferential]: Mode::StronglyPreferential
    PseudoRank,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::StronglyPreferential => f.write_str("strongly preferential"),
            Mode::WeaklyPreferential => f.write_str("weakly preferential"),
            Mode::PseudoRank => f.write_str("pseudorank"),
        }
    }
}

use dsi_progress_logger::{ProgressLog, no_logging};
use kahan::KahanSum;
use lender::prelude::*;
use predicates::Predicate;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator,
};

use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use sync_cell_slice::SyncSlice;
use value_traits::slices::SliceByValue;
use webgraph::traits::RandomAccessGraph;
use webgraph::utils::Granularity;

/// A functional [`SliceByValue`] that returns 1/*n* for all indices,
/// representing the uniform distribution over *n* nodes.
///
/// Used as the default preference vector for [`PageRank`].
pub struct UniformPreference {
    n: usize,
    inv_n: f64,
}

impl UniformPreference {
    /// Creates a new uniform preference vector of length `n`.
    pub fn new(n: usize) -> Self {
        Self {
            n,
            inv_n: if n == 0 { 0.0 } else { 1.0 / n as f64 },
        }
    }
}

impl SliceByValue for UniformPreference {
    type Value = f64;
    fn len(&self) -> usize {
        self.n
    }
    unsafe fn get_value_unchecked(&self, _index: usize) -> f64 {
        self.inv_n
    }
}

/// Computes PageRank using a parallel Gauss-Seidel iteration.
///
/// For details about the algorithm used, see the [module-level
/// documentation].
///
/// The struct is configured via setters and then executed via
/// [`run`]. After completion the rank vector is available via the
/// [`rank`] method.
///
/// Note that the [`preference`] setter consumes `self`
/// because the preference type may differ from the current one; all internal
/// state (including cached inverse outdegrees) is preserved.
///
/// The constructor takes the _transpose_ of the graph, because the algorithm
/// needs to iterate over the predecessors of each node.
///
/// If you compute multiple variants of PageRank on the same graph, please reuse
/// this structure, as it caches the inverse outdegrees of the graph.
///
/// [module-level documentation]: self
/// [`run`]: Self::run
/// [`rank`]: Self::rank
/// [`preference`]: Self::preference
///
/// # Examples
///
/// Default PageRank (strongly preferential, α = 0.85) on a small graph:
///
/// ```
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph_algo::rank::pagerank::{PageRank, preds};
///
/// // Build the transpose of a 5-node graph:
/// //   0 → 1, 0 → 2, 1 → 2, 2 → 0, 3 → 0, 4 → 3
/// let mut gt = VecGraph::empty(5);
/// gt.add_arcs([(1, 0), (2, 0), (2, 1), (0, 2), (0, 3), (3, 4)]);
///
/// let mut pr = PageRank::new(&gt);
/// pr.run(preds::L1Norm::try_from(1E-9).unwrap());
///
/// assert_eq!(pr.rank().len(), 5);
/// assert!((pr.rank().iter().sum::<f64>() - 1.0).abs() < 1E-9);
/// ```
///
/// Weakly preferential PageRank with a custom preference vector:
///
/// ```
/// use webgraph::graphs::vec_graph::VecGraph;
/// use webgraph_algo::rank::pagerank::{Mode, PageRank, preds};
///
/// let mut gt = VecGraph::empty(5);
/// gt.add_arcs([(1, 0), (2, 0), (2, 1), (0, 2), (0, 3), (3, 4)]);
///
/// // Custom preference: favor node 0
/// let pref: &[f64] = &[0.5, 0.2, 0.1, 0.1, 0.1];
///
/// let mut pr = PageRank::new(&gt).preference(pref);
/// pr.alpha(0.9).mode(Mode::WeaklyPreferential);
/// pr.run(preds::L1Norm::try_from(1E-9).unwrap());
///
/// // Node 0 has the highest rank
/// assert!(pr.rank()[0] > pr.rank()[1]);
/// assert_eq!(pr.rank().len(), 5);
/// assert!((pr.rank().iter().sum::<f64>() - 1.0).abs() < 1E-9);
/// ```
pub struct PageRank<
    'a,
    G: RandomAccessGraph + Sync,
    V: SliceByValue<Value = f64> = UniformPreference,
> {
    transpose: &'a G,
    alpha: f64,
    inv_outdegrees: Option<Box<[f64]>>,
    preference: V,
    mode: Mode,
    granularity: Granularity,
    norm_delta: f64,

    rank: Box<[f64]>,
    iteration: usize,
}

impl<G: RandomAccessGraph + Sync, V: SliceByValue<Value = f64>> std::fmt::Debug
    for PageRank<'_, G, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageRank")
            .field("alpha", &self.alpha)
            .field("mode", &self.mode)
            .field("granularity", &self.granularity)
            .field("norm_delta", &self.norm_delta)
            .field("iteration", &self.iteration)
            .finish_non_exhaustive()
    }
}

impl<'a, G: RandomAccessGraph + Sync> PageRank<'a, G> {
    /// Creates a new PageRank computation with uniform preference.
    ///
    /// This constructor takes the _transpose_ of the graph, because the algorithm
    /// needs to iterate over the predecessors of each node.
    pub fn new(transpose: &'a G) -> Self {
        let n = transpose.num_nodes();
        Self {
            transpose,
            alpha: 0.85,
            inv_outdegrees: None,
            preference: UniformPreference::new(n),
            mode: Mode::default(),
            granularity: Granularity::default(),
            norm_delta: f64::INFINITY,
            rank: vec![0.0; n].into_boxed_slice(),
            iteration: 0,
        }
    }
}

impl<'a, G: RandomAccessGraph + Sync, V: SliceByValue<Value = f64>> PageRank<'a, G, V> {
    /// Sets the damping factor α.
    ///
    /// # Panics
    ///
    /// Panics if `alpha` is not in the interval [0 . . 1).
    pub fn alpha(&mut self, alpha: f64) -> &mut Self {
        assert!(
            // Note that 0.0..1.0 is [0.0..1.0) in mathematical notation
            (0.0..1.0).contains(&alpha),
            "The damping factor must be in [0 . . 1), got {alpha}"
        );
        self.alpha = alpha;
        self
    }

    /// Sets the preference (personalization) vector.
    ///
    /// The preference vector is any [`SliceByValue<Value =
    /// f64>`](SliceByValue): for example, a `&[f64]`, a `Vec<f64>`, or a
    /// functional/implicit implementation such as [`UniformPreference`].
    ///
    /// When set, the preference vector is also used as the dangling-node
    /// distribution in [`StronglyPreferential`] mode.
    ///
    /// [`StronglyPreferential`]: Mode::StronglyPreferential
    ///
    /// This method consumes `self` because the preference type may differ
    /// from the current one; all internal state (including cached inverse
    /// outdegrees) is preserved.
    ///
    /// # Panics
    ///
    /// Panics if the length of the vector does not match the number of nodes.
    /// In test mode, we also check for stochasticity (nonnegative entries
    /// summing to 1 within a tolerance of 1E-6) and panic if the check fails.
    pub fn preference<W: SliceByValue<Value = f64>>(self, preference: W) -> PageRank<'a, G, W> {
        let n = self.transpose.num_nodes();
        assert_eq!(
            preference.len(),
            n,
            "Preference vector length ({}) does not match the number of nodes ({n})",
            preference.len()
        );
        #[cfg(test)]
        PageRank::<G, W>::assert_stochastic(&preference, "preference");
        PageRank {
            transpose: self.transpose,
            alpha: self.alpha,
            inv_outdegrees: self.inv_outdegrees,
            preference,
            mode: self.mode,
            granularity: self.granularity,
            norm_delta: self.norm_delta,
            rank: self.rank,
            iteration: self.iteration,
        }
    }

    /// Sets the PageRank [mode].
    ///
    /// [mode]: Mode
    pub fn mode(&mut self, mode: Mode) -> &mut Self {
        self.mode = mode;
        self
    }

    /// Sets the parallel task granularity.
    ///
    /// The granularity expresses how many [nodes] will be passed to a Rayon
    /// task at a time.
    ///
    /// [nodes]: Granularity::node_granularity
    pub fn granularity(&mut self, granularity: Granularity) -> &mut Self {
        self.granularity = granularity;
        self
    }

    /// Returns the rank vector.
    ///
    /// After calling [`run`], this contains the computed PageRank
    /// values.
    ///
    /// [`run`]: Self::run
    pub fn rank(&self) -> &[f64] {
        &self.rank
    }

    /// Returns the number of iterations performed by the last call to
    /// [`run`].
    ///
    /// [`run`]: Self::run
    pub const fn iterations(&self) -> usize {
        self.iteration
    }

    /// Returns the norm delta after the last iteration.
    ///
    /// This is an upper bound on the L₁ error between the current
    /// approximation and the true PageRank, computed as
    /// α / (1 − α) · ‖*x*(*t*) − *x*(*t* − 1)‖₁.
    pub const fn norm_delta(&self) -> f64 {
        self.norm_delta
    }

    /// Checks that a vector is stochastic (all entries nonnegative and summing
    /// to 1 within a tolerance of 1E-6).
    #[cfg(test)]
    fn assert_stochastic(v: &impl SliceByValue<Value = f64>, name: &str) {
        for i in 0..v.len() {
            let x = v.index_value(i);
            assert!(
                x >= 0.0,
                "The {name} vector has a negative entry at index {i}: {x}"
            );
        }
        let sum: f64 = (0..v.len()).map(|i| v.index_value(i)).sum();
        assert!(
            (sum - 1.0).abs() < 1E-6,
            "The {name} vector is not stochastic (sum = {sum})"
        );
    }
}

impl<'a, G: RandomAccessGraph + Sync, V: SliceByValue<Value = f64> + Sync> PageRank<'a, G, V> {
    /// Runs the PageRank computation until the given predicate is satisfied.
    pub fn run(&mut self, predicate: impl Predicate<PredParams>) {
        self.run_with_logging(predicate, no_logging![]);
    }

    /// Runs the PageRank computation until the given predicate is satisfied,
    /// logging progress.
    ///
    /// A concurrent progress logger is derived internally via
    /// [`ProgressLog::concurrent`]; `display_memory` is disabled on it because
    /// sysinfo can deadlock in concurrent contexts.
    pub fn run_with_logging(
        &mut self,
        predicate: impl Predicate<PredParams>,
        pl: &mut impl ProgressLog,
    ) {
        let n = self.transpose.num_nodes();
        if n == 0 {
            return;
        }

        log::info!("Mode: {}", self.mode);
        log::info!("Alpha: {}", self.alpha);
        log::info!("Stopping criterion: {}", predicate);

        self.iteration = 0;
        let inv_n = 1.0 / n as f64;

        // Fill rank with preference vector
        for i in 0..n {
            // SAFETY: i < n == self.preference.len()
            self.rank[i] = unsafe { self.preference.get_value_unchecked(i) };
        }

        let inv_outdegrees = self.inv_outdegrees.get_or_insert_with(|| {
            // Phase 1: Compute outdegrees from the transpose, then convert to
            // inverse outdegrees in place.

            let mut counts = vec![0.0; n].into_boxed_slice();

            pl.item_name("node");
            pl.expected_updates(n);
            pl.start("Computing outdegrees...");
            for_![(_, succ) in self.transpose.iter() {
                for j in succ {
                    counts[j] += 1.0;
                }
                pl.light_update();
            }];
            pl.done();
            pl.info(format_args!("Inverting outdegrees..."));
            counts
                .par_iter_mut()
                .with_min_len(sux::RAYON_MIN_LEN)
                .for_each(|c| {
                    if *c != 0.0 {
                        *c = 1.0 / *c;
                    }
                });

            counts
        });

        // Phase 2: Compute initial dangling rank
        pl.info(format_args!("Computing initial dangling rank..."));
        let (dangling_count, dangling_rank) = inv_outdegrees
            .par_iter()
            .with_min_len(sux::RAYON_MIN_LEN)
            .enumerate()
            .filter(|&(_, &inv_d)| inv_d == 0.0)
            .fold(
                || (0usize, KahanSum::<f64>::new()),
                |(count, dangling_rank), (i, _)| (count + 1, dangling_rank + self.rank[i]),
            )
            .reduce(
                || (0usize, KahanSum::<f64>::new()),
                |(count0, rank0), (count1, rank1)| (count0 + count1, rank0 + rank1),
            );
        let mut dangling_rank = dangling_rank.sum();
        log::info!("{} dangling nodes", dangling_count);
        log::info!("Initial dangling rank: {}", dangling_rank);

        let node_granularity = self
            .granularity
            .node_granularity(n, Some(self.transpose.num_arcs()))
            .max(1);

        // display_memory uses sysinfo, which can deadlock in concurrent contexts
        let mut cpl = pl.concurrent();
        cpl.display_memory(false);

        // Phase 3: Iteration loop
        pl.item_name("iteration");
        pl.expected_updates(None);
        pl.start(format!(
            "Computing PageRank (alpha={}, granularity={node_granularity})...",
            self.alpha
        ));

        loop {
            let norm_delta_accum = Mutex::new(0.0f64);
            let dangling_rank_accum = Mutex::new(0.0f64);
            let node_cursor = AtomicUsize::new(0);

            let rank_sync = self.rank.as_sync_slice();

            cpl.item_name("node");
            cpl.expected_updates(n);
            cpl.start(format!("Iteration {}...", self.iteration + 1));

            rayon::broadcast(|_| {
                let mut local_cpl = cpl.clone();
                let mut local_norm: KahanSum<f64> = KahanSum::new();
                let mut local_dangling: KahanSum<f64> = KahanSum::new();

                loop {
                    let start = node_cursor.fetch_add(node_granularity, Ordering::Relaxed);
                    if start >= n {
                        break;
                    }
                    let len = node_granularity.min(n - start);

                    // Use sequential iteration for efficient access
                    // to compressed graphs.
                    for_![(i, succ) in self.transpose.iter_from(start).take(len) {
                        // SAFETY: each thread processes disjoint ranges of nodes.
                        // Reads from other nodes' ranks are benign data races
                        // (Gauss-Seidel semantics).
                        unsafe {
                            // Accumulate contributions from predecessors
                            // (successors in transpose)
                            let mut sigma: KahanSum<f64> = KahanSum::new();
                            let mut has_loop = false;

                            for j in succ {
                                if j == i {
                                    has_loop = true;
                                } else {
                                    sigma += rank_sync[j].get() * inv_outdegrees[j];
                                }
                            }

                            // Preference and dangling distribution for node i
                            // SAFETY: i < n == self.preference.len()
                            let v_i = self.preference.get_value_unchecked(i);
                            // u_i = v_i in strongly preferential mode,
                            // u_i = 1/n in weakly preferential mode.
                            let u_i = match self.mode {
                                Mode::StronglyPreferential => v_i,
                                Mode::WeaklyPreferential => inv_n,
                                Mode::PseudoRank => 0.0, // unused, but avoids branching
                            };

                            // Compute self-loop correction and self dangling rank
                            let (self_dangling_rank, self_loop_factor) = if inv_outdegrees[i] == 0.0
                            {
                                // Dangling node
                                let sdr = rank_sync[i].get();
                                let slf = if self.mode == Mode::PseudoRank {
                                    1.0
                                } else {
                                    1.0 - self.alpha * u_i
                                };
                                (sdr, slf)
                            } else {
                                // Non-dangling node
                                let slf = if has_loop {
                                    1.0 - self.alpha * inv_outdegrees[i]
                                } else {
                                    1.0
                                };
                                (0.0, slf)
                            };

                            // Add dangling rank contribution
                            if self.mode != Mode::PseudoRank {
                                sigma += (dangling_rank - self_dangling_rank) * u_i;
                            }

                            let new_rank = ((1.0 - self.alpha) * v_i + self.alpha * sigma.sum())
                                / self_loop_factor;

                            // Accumulate dangling rank for next iteration
                            if inv_outdegrees[i] == 0.0 {
                                local_dangling += new_rank;
                            }

                            // Accumulate norm delta
                            local_norm += (new_rank - rank_sync[i].get()).abs();

                            // Update rank in place
                            rank_sync[i].set(new_rank);
                        }
                    }];

                    local_cpl.update_with_count(len);
                }

                // Combine thread-local accumulators
                *norm_delta_accum.lock().unwrap() += local_norm.sum();
                *dangling_rank_accum.lock().unwrap() += local_dangling.sum();
            });

            cpl.done();

            // Update dangling rank for next iteration
            dangling_rank = *dangling_rank_accum.lock().unwrap();

            // Bound on 𝓁₁ error
            self.norm_delta = *norm_delta_accum.lock().unwrap() * self.alpha / (1.0 - self.alpha);

            self.iteration += 1;

            log::info!(
                "Iteration {}: norm delta = {}",
                self.iteration,
                self.norm_delta
            );

            pl.update_and_display();

            if predicate.eval(&PredParams {
                iteration: self.iteration,
                l1_norm_delta: self.norm_delta,
            }) {
                break;
            }
        }

        pl.done();
    }
}
