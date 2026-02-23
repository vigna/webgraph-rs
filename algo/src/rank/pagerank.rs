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
//! must pass to the [constructor](PageRank::new) the **transpose** of the
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
//! The [`mode`](PageRank::mode) setter selects among three variants:
//!
//! - [`StronglyPreferential`](Mode::StronglyPreferential) (the default):
//!   **u** = **v**, so the preference vector doubles as the dangling-node
//!   distribution.
//! - [`WeaklyPreferential`](Mode::WeaklyPreferential): **u** = **1**/*n*, so
//!   dangling nodes distribute their rank uniformly regardless of the
//!   preference vector.
//! - [`PseudoRank`](Mode::PseudoRank): **u** = **0**, zeroing out the
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
//! > *xᵢ*⁽*ᵗ* ⁺ ¹⁾ = ( *bᵢ* − ∑_(*j*<*i*) *mᵢⱼ*·*xⱼ*⁽*ᵗ* ⁺ ¹⁾ − ∑_(*j*>*i*) *mᵢⱼ*·*xⱼ*⁽*ᵗ*⁾ ) / *mᵢᵢ*.
//!
//! Substituting the expressions for *M* and **b** we obtain the update rule
//!
//! > *xᵢ*⁽*ᵗ* ⁺ ¹⁾ = ( (1 − α) *vᵢ* + α ( ∑_(*j*<*i*) (*pⱼᵢ* + *uᵢ* *dⱼ*) *xⱼ*⁽*ᵗ* ⁺ ¹⁾ + ∑_(*j*>*i*) (*pⱼᵢ* + *uᵢ* *dⱼ*) *xⱼ*⁽*ᵗ*⁾ ) ) / (1 − α *pᵢᵢ* − α *uᵢ* *dᵢ*).
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
//! The [`run`](PageRank::run) method accepts a composable
//! [`Predicate`] that is evaluated after each iteration.
//! The predicate receives the current iteration number and a _norm delta_—an
//! upper bound on the ℓ₁ error between the current approximation and the true
//! PageRank vector, computed as
//!
//! > α / (1 − α) · ‖**x**⁽ᵗ⁾ − **x**⁽ᵗ⁻¹⁾‖₁
//!
//! This idea arose in discussions with David Gleich.
//!
//! [Gauss–Seidel method]: https://en.wikipedia.org/wiki/Gauss%E2%80%93Seidel_method
//! [`SyncCell`]: sync_cell_slice::SyncCell
//! [`AtomicUsize`]: std::sync::atomic::AtomicUsize

pub mod preds {
    //! Predicates implementing stopping conditions.
    //!
    //! The implementation of [PageRank](super::PageRank) requires a
    //! [predicate](Predicate) to stop the algorithm. This module provides a few
    //! such predicates: they evaluate to true if the computation should be
    //! stopped.
    //!
    //! You can combine the predicates using the `and` and `or` methods provided
    //! by the [`Predicate`] trait.
    //!
    //! # Examples
    //! ```
    //! # fn main() -> Result<(), Box<dyn std::error::Error>> {
    //! use predicates::prelude::*;
    //! use webgraph_algo::rank::pagerank::preds::{L1Norm, MaxIter};
    //!
    //! let mut predicate = L1Norm::try_from(1E-6)?.boxed();
    //! predicate = predicate.or(MaxIter::from(100)).boxed();
    //! #     Ok(())
    //! # }
    //! ```

    use anyhow::ensure;
    use predicates::{Predicate, reflection::PredicateReflection};
    use std::fmt::Display;

    #[doc(hidden)]
    /// This structure is passed to stopping predicates to provide the
    /// information that is needed to evaluate them.
    #[derive(Debug)]
    pub struct PredParams {
        pub iteration: usize,
        pub norm_delta: f64,
    }

    /// Stops after at most the provided number of iterations.
    #[derive(Debug, Clone)]
    pub struct MaxIter {
        max_iter: usize,
    }

    impl MaxIter {
        pub const DEFAULT_MAX_ITER: usize = usize::MAX;
    }

    impl From<usize> for MaxIter {
        fn from(max_iter: usize) -> Self {
            MaxIter { max_iter }
        }
    }

    impl Default for MaxIter {
        fn default() -> Self {
            Self::from(Self::DEFAULT_MAX_ITER)
        }
    }

    impl Display for MaxIter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("(max iter: {})", self.max_iter))
        }
    }

    impl PredicateReflection for MaxIter {}

    impl Predicate<PredParams> for MaxIter {
        fn eval(&self, pred_params: &PredParams) -> bool {
            pred_params.iteration >= self.max_iter
        }
    }

    /// Stops when the norm of the difference between successive approximations
    /// falls below a given threshold.
    ///
    /// The threshold represents an upper bound on the 𝓁₁ error, approximated
    /// by α / (1 − α) · ‖*x*(*t*) − *x*(*t* − 1)‖₁ where *x*(*t*) is
    /// the rank vector at iteration *t*. This idea arose in discussions with
    /// David Gleich.
    #[derive(Debug, Clone)]
    pub struct L1Norm {
        threshold: f64,
    }

    impl L1Norm {
        pub const DEFAULT_THRESHOLD: f64 = 1E-6;
    }

    impl TryFrom<Option<f64>> for L1Norm {
        type Error = anyhow::Error;
        fn try_from(threshold: Option<f64>) -> anyhow::Result<Self> {
            Ok(match threshold {
                Some(threshold) => {
                    ensure!(!threshold.is_nan());
                    ensure!(threshold > 0.0, "The threshold must be positive");
                    L1Norm { threshold }
                }
                None => Self::default(),
            })
        }
    }

    impl TryFrom<f64> for L1Norm {
        type Error = anyhow::Error;
        fn try_from(threshold: f64) -> anyhow::Result<Self> {
            Some(threshold).try_into()
        }
    }

    impl Default for L1Norm {
        fn default() -> Self {
            Self::try_from(Self::DEFAULT_THRESHOLD).unwrap()
        }
    }

    impl Display for L1Norm {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("(norm: {})", self.threshold))
        }
    }

    impl PredicateReflection for L1Norm {}
    impl Predicate<PredParams> for L1Norm {
        fn eval(&self, pred_params: &PredParams) -> bool {
            pred_params.norm_delta <= self.threshold
        }
    }
}

/// Selects the PageRank variant to compute.
///
/// See the [module-level documentation](self) for the mathematical details.
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
    /// is identical to the [strongly preferential](Mode::StronglyPreferential)
    /// variant modulo normalization.
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

use dsi_progress_logger::{ConcurrentProgressLog, ProgressLog, no_logging};
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
use webgraph::traits::RandomAccessGraph;
use webgraph::utils::Granularity;

/// Computes PageRank using a parallel Gauss-Seidel iteration.
///
/// The struct is configured via setters and then executed via
/// [`run`](Self::run). After completion the rank vector is available via the
/// [`rank`](Self::rank) method.
///
/// The constructor takes the _transpose_ of the graph, because the algorithm
/// needs to iterate over the predecessors of each node.
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
/// let pref = [0.5, 0.2, 0.1, 0.1, 0.1];
///
/// let mut pr = PageRank::new(&gt);
/// pr.alpha(0.9)
///     .preference(Some(&pref))
///     .mode(Mode::WeaklyPreferential);
/// pr.run(preds::L1Norm::try_from(1E-9).unwrap());
///
/// // Node 0 has the highest rank
/// assert!(pr.rank()[0] > pr.rank()[1]);
/// assert_eq!(pr.rank().len(), 5);
/// assert!((pr.rank().iter().sum::<f64>() - 1.0).abs() < 1E-9);
/// ```
pub struct PageRank<'a, G: RandomAccessGraph + Sync> {
    transpose: &'a G,
    alpha: f64,
    inv_outdegrees: Option<Box<[f64]>>,
    preference: Option<&'a [f64]>,
    mode: Mode,
    granularity: Granularity,
    norm_delta: f64,

    rank: Box<[f64]>,
    iteration: usize,
}

impl<G: RandomAccessGraph + Sync> std::fmt::Debug for PageRank<'_, G> {
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
    /// Creates a new PageRank computation.
    ///
    /// This constructor takes the _transpose_ of the graph, because the algorithm
    /// needs to iterate over the predecessors of each node.
    pub fn new(transpose: &'a G) -> Self {
        let n = transpose.num_nodes();
        let rank = vec![0.0; n].into_boxed_slice();
        Self {
            transpose,
            alpha: 0.85,
            inv_outdegrees: None,
            preference: None,
            mode: Mode::default(),
            granularity: Granularity::default(),
            norm_delta: f64::INFINITY,
            rank,
            iteration: 0,
        }
    }

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
    /// When set, the preference vector is also used as the dangling-node
    /// distribution in [`StronglyPreferential`](Mode::StronglyPreferential)
    /// mode.
    ///
    /// Pass `None` to revert to the uniform preference (1/*n*).
    ///
    /// # Panics
    ///
    /// Panics if the length of the vector does not match the number of nodes.
    /// In test mode, we also check for stochasticity (nonnegative entries
    /// summing to 1 within a tolerance of 1E-6) and panic if the check fails.
    pub fn preference(&mut self, preference: Option<&'a [f64]>) -> &mut Self {
        if let Some(v) = preference {
            let n = self.transpose.num_nodes();
            assert_eq!(
                v.len(),
                n,
                "Preference vector length ({}) does not match the number of nodes ({n})",
                v.len()
            );
            #[cfg(test)]
            Self::assert_stochastic(v, "preference");
        }
        self.preference = preference;
        self
    }

    /// Sets the PageRank [mode](Mode).
    pub fn mode(&mut self, mode: Mode) -> &mut Self {
        self.mode = mode;
        self
    }

    /// Sets the parallel task granularity.
    ///
    /// The granularity expresses how many
    /// [nodes](Granularity::node_granularity) will be passed to a Rayon task at
    /// a time.
    pub fn granularity(&mut self, granularity: Granularity) -> &mut Self {
        self.granularity = granularity;
        self
    }

    /// Returns the rank vector.
    ///
    /// After calling [`run`](Self::run), this contains the computed PageRank
    /// values.
    pub fn rank(&self) -> &[f64] {
        &self.rank
    }

    /// Returns the number of iterations performed by the last call to
    /// [`run`](Self::run).
    pub fn iterations(&self) -> usize {
        self.iteration
    }

    /// Returns the norm delta after the last iteration.
    ///
    /// This is an upper bound on the L₁ error between the current
    /// approximation and the true PageRank, computed as
    /// α / (1 − α) · ‖*x*(*t*) − *x*(*t* − 1)‖₁.
    pub fn norm_delta(&self) -> f64 {
        self.norm_delta
    }

    /// Runs the PageRank computation until the given predicate is satisfied.
    pub fn run(&mut self, predicate: impl Predicate<preds::PredParams>) {
        self.run_with_logging(predicate, no_logging![], no_logging![]);
    }

    /// Runs the PageRank computation until the given predicate is satisfied,
    /// logging progress.
    ///
    /// `pl` is a sequential [`ProgressLog`] used for outdegree computation and
    /// iteration counting. `cpl` is a [`ConcurrentProgressLog`] used for
    /// node-level progress inside each iteration. Their options will be preserved,
    /// making thus possible to customize the logs.
    ///
    /// It is possible to specify either `pl` or `cpl` as
    /// [`no_logging![]`](dsi_progress_logger::no_logging) if you don't want to log
    /// the corresponding part of the computation, albeit having the latter one
    /// and not the first one will lead to confusing logs.
    pub fn run_with_logging(
        &mut self,
        predicate: impl Predicate<preds::PredParams>,
        pl: &mut impl ProgressLog,
        cpl: &mut impl ConcurrentProgressLog,
    ) {
        let n = self.transpose.num_nodes();
        if n == 0 {
            return;
        }

        log::info!("Mode: {}", self.mode);
        log::info!("Alpha: {}", self.alpha);
        log::info!(
            "Preference: {}",
            if self.preference.is_some() {
                "custom"
            } else {
                "uniform"
            }
        );
        log::info!("Stopping criterion: {}", predicate);

        self.iteration = 0;
        let inv_n = 1.0 / n as f64;

        // Fill rank with preference vector
        match self.preference {
            Some(v) => self.rank.copy_from_slice(v),
            None => self.rank.fill(inv_n),
        }

        let inv_outdegrees = self.inv_outdegrees.get_or_insert_with(|| {
            // Phase 1: Compute outdegrees from the transpose, then convert to
            // inverse outdegrees in place.

            let mut counts = vec![0.0; n].into_boxed_slice();

            pl.item_name("node");
            pl.expected_updates(Some(n));
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
            cpl.expected_updates(Some(n));
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
                            let v_i = match self.preference {
                                Some(v) => v[i],
                                None => inv_n,
                            };
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

            if predicate.eval(&preds::PredParams {
                iteration: self.iteration,
                norm_delta: self.norm_delta,
            }) {
                break;
            }
        }

        pl.done();
    }

    /// Checks that a vector is stochastic (all entries nonnegative and summing
    /// to 1 within a tolerance of 1E-6).
    #[cfg(test)]
    fn assert_stochastic(v: &[f64], name: &str) {
        for (i, &x) in v.iter().enumerate() {
            assert!(
                x >= 0.0,
                "The {name} vector has a negative entry at index {i}: {x}"
            );
        }
        let sum: f64 = v.iter().sum();
        assert!(
            (sum - 1.0).abs() < 1E-6,
            "The {name} vector is not stochastic (sum = {sum})"
        );
    }
}
