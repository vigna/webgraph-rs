/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Computes an approximation of the neighborhood function, of the size of the
//! reachable sets, and of (discounted) positive geometric centralities of a
//! graph using HyperBall.
//!
//! HyperBall is an algorithm computing by dynamic programming an approximation
//! of the sizes of the balls of growing radius around the nodes of a graph.
//! Starting from these data, it can approximate the _neighborhood function_ of
//! a graph (i.e., the function returning for each _t_ the number of pairs of
//! nodes at distance at most _t_), the number of nodes reachable from each
//! node, Bavelas's closeness centrality, Lin's index, and _harmonic centrality_
//! (studied by Paolo Boldi and Sebastiano Vigna in "[Axioms for
//! Centrality]", _Internet Math._, 10(3-4):222–262, 2014). HyperBall can also
//! compute _discounted centralities_, in which the _discount_ assigned to a
//! node is some specified function of its distance. All centralities are
//! computed in their _positive_ version (i.e., using distance _from_ the
//! source: see below how to compute the more usual, and useful, _negative_
//! version).
//!
//! HyperBall has been described by Paolo Boldi and Sebastiano Vigna in
//! "[In-Core Computation of Geometric Centralities with HyperBall: A Hundred
//! Billion Nodes and Beyond][HyperBall paper]", _Proc. of 2013 IEEE 13th
//! International Conference on Data Mining Workshops (ICDMW 2013)_, IEEE, 2013,
//! and it is a generalization of the method described in "[HyperANF:
//! Approximating the Neighborhood Function of Very Large Graphs on a
//! Budget][HyperANF paper]", by Paolo Boldi, Marco Rosa, and Sebastiano Vigna,
//! _Proceedings of the 20th international conference on World Wide Web_, pages
//! 625–634, ACM, 2011.
//!
//! Incidentally, HyperBall (actually, HyperANF) has been used to show that
//! Facebook has just [four degrees of separation].
//!
//! # Algorithm
//!
//! At step _t_, for each node we (approximately) keep track (using
//! [HyperLogLog counters]) of the set of nodes at distance at most _t_. At
//! each iteration, the sets associated with the successors of each node are
//! merged, thus obtaining the new sets. A crucial component in making this
//! process efficient and scalable is the usage of broadword programming to
//! implement the merge phase, which requires maximizing in parallel the list of
//! registers associated with each successor.
//!
//! Using the approximate sets, for each _t_ we estimate the number of pairs of
//! nodes (_x_, _y_) such that the distance from _x_ to _y_ is at most _t_.
//! Since during the computation we are also in possession of the number of
//! nodes at distance _t_ − 1, we can also perform computations using the
//! number of nodes at distance _exactly_ _t_ (e.g., centralities).
//!
//! # Systolic Computation
//!
//! If you additionally pass the _transpose_ of your graph, when three quarters
//! of the nodes stop changing their value HyperBall will switch to a _systolic_
//! computation: using the transpose, when a node changes it will signal back to
//! its predecessors that at the next iteration they could change. At the next
//! scan, only the successors of signalled nodes will be scanned. In particular,
//! when a very small number of nodes is modified by an iteration, HyperBall
//! will switch to a systolic _local_ mode, in which all information about
//! modified nodes is kept in (traditional) dictionaries, rather than being
//! represented as arrays of booleans. This strategy makes the last phases of
//! the computation orders of magnitude faster, and makes in practice the
//! running time of HyperBall proportional to the theoretical bound
//! _O_(_m_ log _n_), where _n_ is the number of nodes and _m_ is the number of
//! arcs of the graph. Note that graphs with a large diameter require a
//! correspondingly large number of iterations, and these iterations will have
//! to pass over all nodes if you do not provide the transpose.
//!
//! # Stopping Criterion
//!
//! Deciding when to stop iterating is a rather delicate issue. The only safe
//! way is to iterate until no counter is modified, and systolic (local)
//! computation makes this goal easily attainable. However, in some cases one
//! can assume that the graph is not pathological, and stop when the relative
//! increment of the number of pairs goes below some threshold.
//!
//! # Computing Centralities
//!
//! Note that usually one is interested in the _negative_ version of a
//! centrality measure, that is, the version that depends on the _incoming_
//! arcs. HyperBall can compute only _positive_ centralities: if you are
//! interested (as it usually happens) in the negative version, you must pass to
//! HyperBall the _transpose_ of the graph (and if you want to run in systolic
//! mode, the original graph, which is the transpose of the transpose). Note
//! that the neighborhood function of the transpose is identical to the
//! neighborhood function of the original graph, so the exchange does not alter
//! its computation.
//!
//! # Node Weights
//!
//! HyperBall can manage to a certain extent a notion of _node weight_ in its
//! computation of centralities. Weights must be nonnegative integers, and the
//! initialization phase requires generating a random integer for each unit of
//! overall weight, as weights are simulated by loading the counter of a node
//! with multiple elements. Combining this feature with discounts, one can
//! compute _discounted-gain centralities_ as defined in the [HyperBall paper].
//!
//! # Performance
//!
//! Most of the memory goes into storing HyperLogLog registers. By tuning the
//! number of registers per counter, you can modify the memory allocated for
//! them. Note that you can only choose a number of registers per counter that
//! is a power of two, so your latitude in adjusting the memory used for
//! registers is somewhat limited.
//!
//! By default, two full arrays of counters are kept in RAM (the "ping-pong"
//! pattern). If memory is tight, you can use the _external_ mode (see
//! [`HyperBallBuilder::with_hyper_log_log_external`] and
//! [`HyperBallBuilder::with_hyper_log_log8_external`]), which keeps only one
//! array in RAM and writes modified counters to an anonymous memory-mapped
//! region during each iteration. After the iteration, the modified counters are
//! scattered back into the in-memory array. This halves the counter memory
//! at the cost of additional I/O after each iteration. In practice, after the
//! first few iterations only a small fraction of counters change, so the
//! overhead is modest.
//!
//! If there are several available cores, the iterations will be _decomposed_
//! into relatively small tasks (small blocks of nodes) and each task will be
//! assigned to the first available core. Since all tasks are completely
//! independent, this behavior ensures a very high degree of parallelism. Be
//! careful, however, because this feature requires a graph with a reasonably
//! fast random access (e.g., in the case of short reference chains in a
//! [`BvGraph`] and a good choice of the granularity.
//!
//! [`BvGraph`]: webgraph::prelude::BvGraph
//!
//! [Axioms for Centrality]: <http://vigna.di.unimi.it/papers.php#BoVAC>
//! [HyperBall paper]: <http://vigna.di.unimi.it/papers.php#BoVHB>
//! [HyperANF paper]: <http://vigna.di.unimi.it/papers.php#BoRoVHANF>
//! [four degrees of separation]: <http://vigna.di.unimi.it/papers.php#BBRFDS>
//! [HyperLogLog counters]: <https://docs.rs/card-est-array/latest/card_est_array/impls/struct.HyperLogLog.html>

use anyhow::{Context, Result, bail, ensure};
use card_est_array::impls::{
    HyperLogLog, HyperLogLog8, HyperLogLog8Builder, HyperLogLogBuilder, SliceEstimatorArray,
    SyncSliceEstimatorArray,
};
use card_est_array::traits::{
    AsSyncArray, EstimationLogic, EstimatorArray, EstimatorArrayMut, EstimatorMut,
    MergeEstimationLogic, SliceEstimationLogic, SyncEstimatorArray, Word,
};
use crossbeam_utils::CachePadded;
use dsi_progress_logger::{ConcurrentProgressLog, ProgressLog};
use kahan::KahanSum;
use lender::prelude::*;
use rayon::prelude::*;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::sync::{Mutex, atomic::*};
use sux::traits::AtomicBitVecOps;
use sux::{bits::AtomicBitVec, traits::Succ};
use sync_cell_slice::{SyncCell, SyncSlice};
use webgraph::traits::{RandomAccessGraph, SequentialLabeling};
use webgraph::utils::Granularity;

/// Write-only view of an [`OutputStore`], used during parallel iterations.
///
/// # Safety
///
/// Implementations must ensure that concurrent calls to [`set`](Self::set)
/// with distinct indices do not cause data races.
pub unsafe trait SyncOutputStore<L: EstimationLogic + ?Sized>: Sync {
    /// Stores `content` as the backend of node `index`.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no two threads call `set` with the
    /// same `index` concurrently.
    unsafe fn set(&self, index: usize, content: &L::Backend);
}

unsafe impl<L: SliceEstimationLogic<W> + Sync, W: Word, S: AsRef<[SyncCell<W>]> + Sync>
    SyncOutputStore<L> for SyncSliceEstimatorArray<L, W, S>
{
    #[inline(always)]
    unsafe fn set(&self, index: usize, content: &L::Backend) {
        // SAFETY: forwarded from caller's safety guarantee (no concurrent
        // access to the same index).
        unsafe { SyncEstimatorArray::set(self, index, content) };
    }
}

/// Stores iteration results and commutes them back into the current state.
///
/// Two implementations are provided:
///
/// - An in-memory implementation on [`SliceEstimatorArray`] that uses
///   the existing ping-pong pattern (O(1) swap).
/// - A spill-to-disk implementation ([`SpillStore`]) that writes `(node,
///   backend)` records to an mmap'd log and scatters them back in
///   [`commute`](Self::commute).
pub trait OutputStore<L: EstimationLogic + ?Sized, A> {
    /// The thread-safe view used during parallel iterations.
    type OutputStore<'a>: SyncOutputStore<L>
    where
        Self: 'a;

    /// Returns a thread-safe view for use inside `rayon::broadcast`.
    fn as_sync(&mut self) -> Self::OutputStore<'_>;

    /// Moves the results of the last iteration into `curr`.
    fn commute(&mut self, curr: &mut A);

    /// Resets this store to a clean state.
    fn clear(&mut self);

    /// Returns the number of bytes of RAM used by this store.
    fn mem_usage(&self) -> usize;
}

impl<L: SliceEstimationLogic<W> + Clone + Sync, W: Word, S: AsRef<[W]> + AsMut<[W]>>
    OutputStore<L, Self> for SliceEstimatorArray<L, W, S>
{
    type OutputStore<'a>
        = SyncSliceEstimatorArray<L, W, &'a [SyncCell<W>]>
    where
        Self: 'a;

    #[inline(always)]
    fn as_sync(&mut self) -> Self::OutputStore<'_> {
        AsSyncArray::as_sync_array(self)
    }

    #[inline(always)]
    fn commute(&mut self, curr: &mut Self) {
        std::mem::swap(self, curr);
    }

    #[inline(always)]
    fn clear(&mut self) {
        EstimatorArrayMut::clear(self);
    }

    fn mem_usage(&self) -> usize {
        std::mem::size_of_val(self.as_ref())
    }
}

/// An [`OutputStore`] that spills `(node, backend)` records to an anonymous
/// memory-mapped region instead of keeping a second full estimator array in
/// RAM.
///
/// During the parallel phase, each call to [`SyncOutputStore::set`] atomically
/// reserves space in the mmap and writes the node index followed by the
/// backend words. During [`commute`](OutputStore::commute), the log is
/// scattered back into the current estimator array in parallel, and the write
/// cursor is reset.
pub struct SpillStore<W: Word> {
    mmap: mmap_rs::MmapMut,
    offset: CachePadded<AtomicUsize>,
    backend_len: usize,
    record_size: usize,
    _marker: std::marker::PhantomData<W>,
}

// SAFETY: concurrent access is coordinated through the atomic `offset`
// field; each record is written to a disjoint region of the mmap.
unsafe impl<W: Word> Sync for SpillStore<W> {}

impl<W: Word> SpillStore<W> {
    /// Creates a new spill store sized for `num_nodes` estimators, each
    /// with a backend of `backend_len` words of type `W`.
    pub fn new(num_nodes: usize, backend_len: usize) -> Self {
        let record_size = std::mem::size_of::<usize>() + backend_len * std::mem::size_of::<W>();
        let total = num_nodes * record_size;
        // Round up to page size.
        let page = mmap_rs::MmapOptions::page_size();
        let total = total.next_multiple_of(page);
        let mmap = mmap_rs::MmapOptions::new(total)
            .expect("mmap size should be valid")
            .map_mut()
            .expect("anonymous mmap should succeed");
        Self {
            mmap,
            offset: CachePadded::new(AtomicUsize::new(0)),
            backend_len,
            record_size,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Thread-safe view into a [`SpillStore`], returned by
/// [`OutputStore::as_sync`].
pub struct SyncSpillView<'a, W: Word> {
    store: &'a SpillStore<W>,
}

unsafe impl<W: Word> Sync for SyncSpillView<'_, W> {}

unsafe impl<L: SliceEstimationLogic<W> + Sync, W: Word> SyncOutputStore<L>
    for SyncSpillView<'_, W>
{
    unsafe fn set(&self, index: usize, content: &L::Backend) {
        debug_assert_eq!(content.len(), self.store.backend_len);
        let pos = self
            .store
            .offset
            .fetch_add(self.store.record_size, Ordering::Relaxed);
        // SAFETY: the caller guarantees no two threads write the same
        // index, and each fetch_add reserves a disjoint region.
        unsafe {
            let dst = self.store.mmap.as_ptr().add(pos) as *mut u8;
            std::ptr::copy_nonoverlapping(
                std::ptr::from_ref(&index) as *const u8,
                dst,
                std::mem::size_of::<usize>(),
            );
            std::ptr::copy_nonoverlapping(
                content.as_ptr() as *const u8,
                dst.add(std::mem::size_of::<usize>()),
                self.store.backend_len * std::mem::size_of::<W>(),
            );
        }
    }
}

impl<L: SliceEstimationLogic<W> + Clone + Sync, W: Word, S: AsRef<[W]> + AsMut<[W]>>
    OutputStore<L, SliceEstimatorArray<L, W, S>> for SpillStore<W>
{
    type OutputStore<'a>
        = SyncSpillView<'a, W>
    where
        Self: 'a;

    fn as_sync(&mut self) -> SyncSpillView<'_, W> {
        SyncSpillView { store: self }
    }

    fn commute(&mut self, curr: &mut SliceEstimatorArray<L, W, S>) {
        let num_records = self.offset.load(Ordering::Relaxed) / self.record_size;
        if num_records == 0 {
            return;
        }
        let record_size = self.record_size;
        let backend_len = self.backend_len;
        let log = &self.mmap[..num_records * record_size];
        let curr_sync = curr.as_sync_array();
        let chunk_size = 1024 * record_size;
        log.par_chunks(chunk_size).for_each(|chunk| {
            for record in chunk.chunks_exact(record_size) {
                let node = usize::from_ne_bytes(
                    record[..std::mem::size_of::<usize>()].try_into().unwrap(),
                );
                let backend_bytes = &record[std::mem::size_of::<usize>()..];
                // SAFETY: each node appears at most once in the log, so
                // there are no data races across parallel chunks.
                unsafe {
                    let backend =
                        std::slice::from_raw_parts(backend_bytes.as_ptr() as *const W, backend_len);
                    SyncEstimatorArray::set(&curr_sync, node, backend);
                }
            }
        });
        self.offset.store(0, Ordering::Relaxed);
    }

    fn clear(&mut self) {
        self.offset.store(0, Ordering::Relaxed);
    }

    fn mem_usage(&self) -> usize {
        0
    }
}

/// A builder for [`HyperBall`].
///
/// # Creating a Builder
///
/// There are three constructors, depending on the type of graph and
/// cardinality estimator:
///
/// - [`with_hyper_log_log`]: the most common entry
///   point—it creates a builder using [`HyperLogLog`] counters, requiring
///   only the base-2 logarithm of the number of registers per counter
///   (`log2m`). Higher values of `log2m` give more precise estimates at the
///   cost of more memory;
/// - [`new`]: creates a builder from two pre-built estimator
///   arrays and a graph (without its transpose);
/// - [`with_transpose`]: same, but also accepts the
///   transpose of the graph, enabling [systolic
///   computation](super::hyperball#systolic-computation).
///
/// # Configuration
///
/// After creation, the builder can be configured using the following
/// methods:
///
/// - [`sum_of_distances`] — enables the computation of the sum of distances
///   from each node (needed for closeness, Lin, and Nieminen centrality);
/// - [`sum_of_inverse_distances`] — enables the computation of harmonic
///   centrality;
/// - [`discount_function`] — adds a custom discount function;
/// - [`granularity`] — sets the granularity for the parallel iterations;
/// - [`weights`] — sets optional nonnegative integer node weights.
///
/// Finally, call [`build`] to obtain a [`HyperBall`] instance, and then
/// [`run`] or [`run_until_done`] to perform the actual computation.
///
/// [`with_hyper_log_log`]: Self::with_hyper_log_log
/// [`new`]: Self::new
/// [`with_transpose`]: Self::with_transpose
/// [`sum_of_distances`]: Self::sum_of_distances
/// [`sum_of_inverse_distances`]: Self::sum_of_inverse_distances
/// [`discount_function`]: Self::discount_function
/// [`granularity`]: Self::granularity
/// [`weights`]: Self::weights
/// [`build`]: Self::build
/// [`run`]: HyperBall::run
/// [`run_until_done`]: HyperBall::run_until_done
///
/// # Examples
///
/// ```
/// # use webgraph::graphs::vec_graph::VecGraph;
/// # use webgraph::traits::SequentialLabeling;
/// # use webgraph_algo::distances::hyperball::*;
/// # use dsi_progress_logger::no_logging;
/// # use rand::SeedableRng;
/// let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
/// let dcf = graph.build_dcf();
///
/// // Build and run HyperBall (neighborhood function only)
/// let rng = rand::rngs::SmallRng::seed_from_u64(0);
/// let mut hyperball = HyperBallBuilder::with_hyper_log_log(
///     &graph, None::<&VecGraph>, &dcf, 6, None,
/// )?.build(no_logging![]);
/// hyperball.run_until_done(rng, no_logging![])?;
///
/// let nf = hyperball.neighborhood_function()?;
/// assert!(nf.len() >= 4);
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// To compute harmonic centrality, enable it on the builder:
///
/// ```
/// # use webgraph::graphs::vec_graph::VecGraph;
/// # use webgraph::traits::SequentialLabeling;
/// # use webgraph_algo::distances::hyperball::*;
/// # use dsi_progress_logger::no_logging;
/// # use rand::SeedableRng;
/// # let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
/// # let dcf = graph.build_dcf();
/// # let rng = rand::rngs::SmallRng::seed_from_u64(0);
/// let mut hyperball = HyperBallBuilder::with_hyper_log_log(
///     &graph, None::<&VecGraph>, &dcf, 6, None,
/// )?
/// .sum_of_inverse_distances(true)
/// .build(no_logging![]);
/// hyperball.run_until_done(rng, no_logging![])?;
///
/// let centralities = hyperball.harmonic_centralities()?;
/// assert_eq!(centralities.len(), graph.num_nodes());
/// # Ok::<(), anyhow::Error>(())
/// ```
pub struct HyperBallBuilder<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
    L: MergeEstimationLogic<Item = G1::Label>,
    A: EstimatorArrayMut<L>,
    N: OutputStore<L, A> = A,
> {
    /// A graph.
    graph: &'a G1,
    /// The transpose of `graph`, if any.
    transpose: Option<&'a G2>,
    /// The outdegree cumulative function of the graph.
    cumul_outdegree: &'a D,
    /// Whether to compute the sum of distances (e.g., for closeness centrality).
    do_sum_of_dists: bool,
    /// Whether to compute the sum of inverse distances (e.g., for harmonic centrality).
    do_sum_of_inv_dists: bool,
    /// Custom discount functions whose sum should be computed.
    discount_functions: Vec<Box<dyn Fn(usize) -> f64 + Send + Sync + 'a>>,
    /// The granularity of parallel tasks.
    granularity: Granularity,
    /// Integer weights for the nodes, if any.
    weights: Option<&'a [usize]>,
    /// The estimator array (read side).
    array_0: A,
    /// The output store (write side).
    array_1: N,
    _marker: std::marker::PhantomData<L>,
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
>
    HyperBallBuilder<
        'a,
        G1,
        G2,
        D,
        HyperLogLog<usize, BuildHasherDefault<DefaultHasher>>,
        SliceEstimatorArray<HyperLogLog<usize, BuildHasherDefault<DefaultHasher>>>,
    >
{
    /// A builder for [`HyperBall`] using a specified [`EstimationLogic`].
    ///
    /// # Arguments
    /// * `graph` - the graph to analyze.
    /// * `transpose` - optionally, the transpose of `graph`. If [`None`], no
    ///   systolic iterations will be performed by the resulting [`HyperBall`].
    /// * `cumul_outdeg` - the outdegree cumulative function of the graph.
    /// * `log2m` - the base-2 logarithm of the number *m* of register per
    ///   HyperLogLog cardinality estimator.
    /// * `weights` - the weights to use. If [`None`] every node is assumed to be
    ///   of weight equal to 1.
    pub fn with_hyper_log_log(
        graph: &'a G1,
        transposed: Option<&'a G2>,
        cumul_outdeg: &'a D,
        log2m: u32,
        weights: Option<&'a [usize]>,
    ) -> Result<Self> {
        let num_elements = weights.map_or(graph.num_nodes(), |w| w.iter().sum());
        let logic = HyperLogLogBuilder::new(num_elements)
            .log2_num_regs(log2m)
            .build()?;
        let array_0 = SliceEstimatorArray::new(logic.clone(), graph.num_nodes());
        let array_1 = SliceEstimatorArray::new(logic, graph.num_nodes());
        Self::from_parts(graph, transposed, cumul_outdeg, weights, array_0, array_1)
    }
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
>
    HyperBallBuilder<
        'a,
        G1,
        G2,
        D,
        HyperLogLog8<usize, BuildHasherDefault<DefaultHasher>>,
        SliceEstimatorArray<HyperLogLog8<usize, BuildHasherDefault<DefaultHasher>>, u8>,
    >
{
    /// Creates a builder for [`HyperBall`] using [`HyperLogLog8`] counters
    /// (byte-sized registers with SIMD-accelerated merges).
    ///
    /// This is an alternative to [`with_hyper_log_log`] that trades ~33%
    /// extra space for significantly faster merge operations.
    ///
    /// [`with_hyper_log_log`]: HyperBallBuilder::with_hyper_log_log
    ///
    /// # Arguments
    /// * `graph` - the graph to analyze.
    /// * `transpose` - optionally, the transpose of `graph`. If [`None`], no
    ///   systolic iterations will be performed by the resulting [`HyperBall`].
    /// * `cumul_outdeg` - the outdegree cumulative function of the graph.
    /// * `log2m` - the base-2 logarithm of the number *m* of registers per
    ///   HyperLogLog counter.
    /// * `weights` - the weights to use. If [`None`] every node is assumed to be
    ///   of weight equal to 1.
    pub fn with_hyper_log_log8(
        graph: &'a G1,
        transposed: Option<&'a G2>,
        cumul_outdeg: &'a D,
        log2m: u32,
        weights: Option<&'a [usize]>,
    ) -> Result<Self> {
        let logic = HyperLogLog8Builder::new()
            .log2_num_regs(log2m)
            .build::<usize>();
        let array_0 = SliceEstimatorArray::new(logic.clone(), graph.num_nodes());
        let array_1 = SliceEstimatorArray::new(logic, graph.num_nodes());
        Self::from_parts(graph, transposed, cumul_outdeg, weights, array_0, array_1)
    }
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
>
    HyperBallBuilder<
        'a,
        G1,
        G2,
        D,
        HyperLogLog<usize, BuildHasherDefault<DefaultHasher>>,
        SliceEstimatorArray<HyperLogLog<usize, BuildHasherDefault<DefaultHasher>>>,
        SpillStore<usize>,
    >
{
    /// Creates a builder for [`HyperBall`] using [`HyperLogLog`] counters
    /// with an external (spill-to-disk) output store.
    ///
    /// Only one estimator array is kept in RAM; iteration results are
    /// written to an anonymous memory-mapped region and scattered back
    /// after each iteration.
    ///
    /// # Arguments
    /// * `graph` - the graph to analyze.
    /// * `transpose` - optionally, the transpose of `graph`.
    /// * `cumul_outdeg` - the outdegree cumulative function of the graph.
    /// * `log2m` - the base-2 logarithm of the number of registers per
    ///   HyperLogLog counter.
    /// * `weights` - optional nonnegative integer node weights.
    pub fn with_hyper_log_log_external(
        graph: &'a G1,
        transposed: Option<&'a G2>,
        cumul_outdeg: &'a D,
        log2m: u32,
        weights: Option<&'a [usize]>,
    ) -> Result<Self> {
        let num_elements = weights.map_or(graph.num_nodes(), |w| w.iter().sum());
        let logic = HyperLogLogBuilder::new(num_elements)
            .log2_num_regs(log2m)
            .build()?;
        let backend_len = logic.backend_len();
        let array_0 = SliceEstimatorArray::new(logic, graph.num_nodes());
        let array_1 = SpillStore::new(graph.num_nodes(), backend_len);
        Self::from_parts(graph, transposed, cumul_outdeg, weights, array_0, array_1)
    }
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
>
    HyperBallBuilder<
        'a,
        G1,
        G2,
        D,
        HyperLogLog8<usize, BuildHasherDefault<DefaultHasher>>,
        SliceEstimatorArray<HyperLogLog8<usize, BuildHasherDefault<DefaultHasher>>, u8>,
        SpillStore<u8>,
    >
{
    /// Creates a builder for [`HyperBall`] using [`HyperLogLog8`] counters
    /// with an external (spill-to-disk) output store.
    ///
    /// # Arguments
    /// * `graph` - the graph to analyze.
    /// * `transpose` - optionally, the transpose of `graph`.
    /// * `cumul_outdeg` - the outdegree cumulative function of the graph.
    /// * `log2m` - the base-2 logarithm of the number of registers per
    ///   HyperLogLog counter.
    /// * `weights` - optional nonnegative integer node weights.
    pub fn with_hyper_log_log8_external(
        graph: &'a G1,
        transposed: Option<&'a G2>,
        cumul_outdeg: &'a D,
        log2m: u32,
        weights: Option<&'a [usize]>,
    ) -> Result<Self> {
        let logic = HyperLogLog8Builder::new()
            .log2_num_regs(log2m)
            .build::<usize>();
        let backend_len = logic.backend_len();
        let array_0 = SliceEstimatorArray::new(logic, graph.num_nodes());
        let array_1 = SpillStore::new(graph.num_nodes(), backend_len);
        Self::from_parts(graph, transposed, cumul_outdeg, weights, array_0, array_1)
    }
}

impl<
    'a,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
    G: RandomAccessGraph + Sync,
    L: MergeEstimationLogic<Item = G::Label> + PartialEq,
    A: EstimatorArrayMut<L> + OutputStore<L, A>,
> HyperBallBuilder<'a, G, G, D, L, A>
{
    /// Creates a new builder with default parameters.
    ///
    /// # Arguments
    /// * `graph` - the graph to analyze.
    /// * `cumul_outdeg` - the outdegree cumulative function of the graph.
    /// * `array_0` - a first array of estimators.
    /// * `array_1` - a second array of estimators of the same length and with the same logic of
    ///   `array_0`.
    pub fn new(graph: &'a G, cumul_outdeg: &'a D, array_0: A, array_1: A) -> Self {
        assert!(array_0.logic() == array_1.logic(), "Incompatible logic");
        assert_eq!(
            graph.num_nodes(),
            array_0.len(),
            "array_0 should have length {}. Got {}",
            graph.num_nodes(),
            array_0.len()
        );
        assert_eq!(
            graph.num_nodes(),
            array_1.len(),
            "array_1 should have length {}. Got {}",
            graph.num_nodes(),
            array_1.len()
        );
        Self {
            graph,
            transpose: None,
            cumul_outdegree: cumul_outdeg,
            do_sum_of_dists: false,
            do_sum_of_inv_dists: false,
            discount_functions: Vec::new(),
            granularity: Self::DEFAULT_GRANULARITY,
            weights: None,
            array_0,
            array_1,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
    L: MergeEstimationLogic<Item = G1::Label>,
    A: EstimatorArrayMut<L> + OutputStore<L, A>,
> HyperBallBuilder<'a, G1, G2, D, L, A>
{
    /// Creates a new builder with default parameters using also the transpose.
    ///
    /// # Arguments
    /// * `graph` - the graph to analyze.
    /// * `transpose` - the transpose of `graph`.
    /// * `cumul_outdeg` - the outdegree cumulative function of the graph.
    /// * `array_0` - a first array of estimators.
    /// * `array_1` - a second array of estimators of the same length and with
    ///   the same logic of `array_0`.
    pub fn with_transpose(
        graph: &'a G1,
        transpose: &'a G2,
        cumul_outdeg: &'a D,
        array_0: A,
        array_1: A,
    ) -> Self {
        assert_eq!(
            graph.num_nodes(),
            array_0.len(),
            "array_0 should have len {}. Got {}",
            graph.num_nodes(),
            array_0.len()
        );
        assert_eq!(
            graph.num_nodes(),
            array_1.len(),
            "array_1 should have len {}. Got {}",
            graph.num_nodes(),
            array_1.len()
        );
        assert_eq!(
            transpose.num_nodes(),
            graph.num_nodes(),
            "the transpose should have same number of nodes of the graph ({}). Got {}.",
            graph.num_nodes(),
            transpose.num_nodes()
        );
        assert_eq!(
            transpose.num_arcs(),
            graph.num_arcs(),
            "the transpose should have same number of arcs of the graph ({}). Got {}.",
            graph.num_arcs(),
            transpose.num_arcs()
        );
        Self {
            graph,
            transpose: Some(transpose),
            cumul_outdegree: cumul_outdeg,
            do_sum_of_dists: false,
            do_sum_of_inv_dists: false,
            discount_functions: Vec::new(),
            granularity: Self::DEFAULT_GRANULARITY,
            weights: None,
            array_0,
            array_1,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
    L: MergeEstimationLogic<Item = G1::Label>,
    A: EstimatorArrayMut<L>,
    N: OutputStore<L, A>,
> HyperBallBuilder<'a, G1, G2, D, L, A, N>
{
    const DEFAULT_GRANULARITY: Granularity = Granularity::Nodes(16 * 1024);

    fn from_parts(
        graph: &'a G1,
        transposed: Option<&'a G2>,
        cumul_outdeg: &'a D,
        weights: Option<&'a [usize]>,
        array_0: A,
        array_1: N,
    ) -> Result<Self> {
        if let Some(w) = weights {
            ensure!(
                w.len() == graph.num_nodes(),
                "weights should have length equal to the graph's number of nodes"
            );
        }
        Ok(Self {
            graph,
            transpose: transposed,
            cumul_outdegree: cumul_outdeg,
            do_sum_of_dists: false,
            do_sum_of_inv_dists: false,
            discount_functions: Vec::new(),
            granularity: Self::DEFAULT_GRANULARITY,
            weights,
            array_0,
            array_1,
            _marker: std::marker::PhantomData,
        })
    }

    /// Sets whether to compute the sum of distances.
    pub fn sum_of_distances(mut self, do_sum_of_distances: bool) -> Self {
        self.do_sum_of_dists = do_sum_of_distances;
        self
    }

    /// Sets whether to compute the sum of inverse distances.
    pub fn sum_of_inverse_distances(mut self, do_sum_of_inverse_distances: bool) -> Self {
        self.do_sum_of_inv_dists = do_sum_of_inverse_distances;
        self
    }

    /// Sets the base granularity used in the parallel phases of the iterations.
    pub fn granularity(mut self, granularity: Granularity) -> Self {
        self.granularity = granularity;
        self
    }

    /// Sets optional weights for the nodes of the graph.
    ///
    /// # Arguments
    /// * `weights` - weights to use for the nodes. If [`None`], every node is
    ///   assumed to be of weight equal to 1.
    pub fn weights(mut self, weights: Option<&'a [usize]>) -> Self {
        if let Some(w) = weights {
            assert_eq!(w.len(), self.graph.num_nodes());
        }
        self.weights = weights;
        self
    }

    /// Adds a new discount function whose sum over all spheres should be
    /// computed.
    pub fn discount_function(
        mut self,
        discount_function: impl Fn(usize) -> f64 + Send + Sync + 'a,
    ) -> Self {
        self.discount_functions.push(Box::new(discount_function));
        self
    }

    /// Removes all custom discount functions.
    pub fn no_discount_function(mut self) -> Self {
        self.discount_functions.clear();
        self
    }
}

impl<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
    L: MergeEstimationLogic<Item = G1::Label> + Sync + std::fmt::Display,
    A: EstimatorArrayMut<L>,
    N: OutputStore<L, A>,
> HyperBallBuilder<'a, G1, G2, D, L, A, N>
{
    /// Builds a [`HyperBall`] instance.
    ///
    /// # Arguments
    ///
    /// * `pl` - A progress logger.
    pub fn build(self, pl: &mut impl ConcurrentProgressLog) -> HyperBall<'a, G1, G2, D, L, A, N> {
        let num_nodes = self.graph.num_nodes();

        let sum_of_distances = if self.do_sum_of_dists {
            pl.debug(format_args!("Initializing sum of distances"));
            Some(vec![0.0; num_nodes])
        } else {
            pl.debug(format_args!("Skipping sum of distances"));
            None
        };
        let sum_of_inverse_distances = if self.do_sum_of_inv_dists {
            pl.debug(format_args!("Initializing sum of inverse distances"));
            Some(vec![0.0; num_nodes])
        } else {
            pl.debug(format_args!("Skipping sum of inverse distances"));
            None
        };

        let mut discounted_centralities = Vec::new();
        pl.debug(format_args!(
            "Initializing {} discount functions",
            self.discount_functions.len()
        ));
        for _ in self.discount_functions.iter() {
            discounted_centralities.push(vec![0.0; num_nodes]);
        }

        pl.info(format_args!("Initializing bit vectors"));
        let estimator_modified = AtomicBitVec::new(num_nodes);
        let modified_result_estimator = AtomicBitVec::new(num_nodes);
        let must_be_checked = AtomicBitVec::new(num_nodes);
        let next_must_be_checked = AtomicBitVec::new(num_nodes);

        pl.info(format_args!(
            "Using estimation logic {}",
            self.array_0.logic()
        ));

        pl.info(format_args!(
            "Running {} thread(s)",
            rayon::current_num_threads()
        ));

        // Compute memory usage (not counting the graph itself)
        let estimator_bytes = std::mem::size_of_val(self.array_0.get_backend(0)) * num_nodes
            + self.array_1.mem_usage();
        let mut total_bytes = estimator_bytes;
        if sum_of_distances.is_some() {
            total_bytes += num_nodes * std::mem::size_of::<f32>();
        }
        if sum_of_inverse_distances.is_some() {
            total_bytes += num_nodes * std::mem::size_of::<f32>();
        }
        total_bytes += discounted_centralities.len() * num_nodes * std::mem::size_of::<f32>();
        // 4 AtomicBitVec of num_nodes bits
        total_bytes += 5 * num_nodes.div_ceil(usize::BITS as usize) * usize::BITS as usize / 8;

        pl.info(format_args!(
            "HyperBall memory usage: {}B [not counting graph(s)]",
            webgraph::utils::humanize(total_bytes as f64)
        ));

        HyperBall {
            graph: self.graph,
            transposed: self.transpose,
            weight: self.weights,
            granularity: self.granularity,
            curr_state: self.array_0,
            next_state: self.array_1,
            completed: false,
            neighborhood_function: Vec::new(),
            last: 0.0,
            relative_increment: 0.0,
            sum_of_dists: sum_of_distances,
            sum_of_inv_dists: sum_of_inverse_distances,
            discounted_centralities,
            iteration_context: IterationContext {
                cumul_outdeg: self.cumul_outdegree,
                iteration: 0,
                current_nf: Mutex::new(0.0),
                node_granularity: 0,
                node_cursor: AtomicUsize::new(0).into(),
                arc_cursor: Mutex::new((0, 0)),
                visited_arcs: AtomicU64::new(0).into(),
                modified_estimators: AtomicU64::new(0).into(),
                systolic: false,
                local: false,
                pre_local: false,
                local_checklist: Vec::new(),
                local_next_must_be_checked: Mutex::new(Vec::new()),
                must_be_checked,
                next_must_be_checked,
                curr_modified: estimator_modified,
                next_modified: modified_result_estimator,
                discount_functions: self.discount_functions,
            },
            _marker: std::marker::PhantomData,
        }
    }
}

/// Data used by [`parallel_task`].
///
/// These variables are used by the threads running [`parallel_task`]. They
/// must be isolated in a field because we need to be able to borrow
/// exclusively [`HyperBall::next_state`], while sharing references to the
/// data contained here and to the [`HyperBall::curr_state`].
///
/// [`parallel_task`]: HyperBall::parallel_task
struct IterationContext<'a, G1: SequentialLabeling, D> {
    /// The cumulative list of outdegrees.
    cumul_outdeg: &'a D,
    /// The number of the current iteration.
    iteration: usize,
    /// The value of the neighborhood function computed during the current iteration.
    current_nf: Mutex<f64>,
    /// The node granularity for the current iteration: each task will try to
    /// process at least this number of nodes.
    node_granularity: usize,
    /// A cursor scanning the nodes to process during local computations.
    node_cursor: CachePadded<AtomicUsize>,
    /// A cursor scanning the nodes and arcs to process during non-local
    /// computations.
    arc_cursor: Mutex<(usize, u64)>,
    /// The number of arcs visited during the current iteration.
    visited_arcs: CachePadded<AtomicU64>,
    /// The number of estimators modified during the current iteration.
    modified_estimators: CachePadded<AtomicU64>,
    /// `true` if we started a systolic computation.
    systolic: bool,
    /// `true` if we started a local computation.
    local: bool,
    /// `true` if we are preparing a local computation (systolic is `true` and less than 1% nodes were modified).
    pre_local: bool,
    /// If [`local`] is `true`, the sorted list of nodes that
    /// should be scanned.
    ///
    /// [`local`]: Self::local
    local_checklist: Vec<G1::Label>,
    /// If [`pre_local`] is `true`, the set of nodes that
    /// should be scanned on the next iteration.
    ///
    /// [`pre_local`]: Self::pre_local
    local_next_must_be_checked: Mutex<Vec<G1::Label>>,
    /// Used in systolic iterations to keep track of nodes to check.
    must_be_checked: AtomicBitVec,
    /// Used in systolic iterations to keep track of nodes to check in the next
    /// iteration.
    next_must_be_checked: AtomicBitVec,
    /// Whether each estimator has been modified during the previous iteration.
    curr_modified: AtomicBitVec,
    /// Whether each estimator has been modified during the current iteration.
    next_modified: AtomicBitVec,
    /// Custom discount functions whose sum should be computed.
    discount_functions: Vec<Box<dyn Fn(usize) -> f64 + Send + Sync + 'a>>,
}

impl<G1: SequentialLabeling, D> IterationContext<'_, G1, D> {
    /// Resets the iteration context
    fn reset(&mut self, node_granularity: usize) {
        self.node_granularity = node_granularity;
        self.node_cursor.store(0, Ordering::Relaxed);
        *self.arc_cursor.lock().unwrap() = (0, 0);
        self.visited_arcs.store(0, Ordering::Relaxed);
        self.modified_estimators.store(0, Ordering::Relaxed);
    }
}

/// An algorithm that computes an approximation of the neighborhood function,
/// of the size of the reachable sets, and of (discounted) positive geometric
/// centralities of a graph.
pub struct HyperBall<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64>,
    L: MergeEstimationLogic<Item = G1::Label> + Sync,
    A: EstimatorArrayMut<L>,
    N: OutputStore<L, A> = A,
> {
    /// The graph to analyze.
    graph: &'a G1,
    /// The transpose of [`Self::graph`], if any.
    transposed: Option<&'a G2>,
    /// An optional slice of nonnegative node weights.
    weight: Option<&'a [usize]>,
    /// The granularity of parallel tasks.
    granularity: Granularity,
    /// The current state (read side).
    curr_state: A,
    /// The output store (write side).
    next_state: N,
    /// `true` if the computation is over.
    completed: bool,
    /// The neighborhood function.
    neighborhood_function: Vec<f64>,
    /// The value computed by the last iteration.
    last: f64,
    /// The relative increment of the neighborhood function for the last
    /// iteration.
    relative_increment: f64,
    /// The sum of the distances from every given node, if requested.
    sum_of_dists: Option<Vec<f32>>,
    /// The sum of inverse distances from each given node, if requested.
    sum_of_inv_dists: Option<Vec<f32>>,
    /// The overall discount centrality for every discount function.
    discounted_centralities: Vec<Vec<f32>>,
    /// Context used in a single iteration.
    iteration_context: IterationContext<'a, G1, D>,
    _marker: std::marker::PhantomData<L>,
}

impl<
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64> + Sync,
    L: MergeEstimationLogic<Item = usize> + Sync,
    A: EstimatorArrayMut<L> + Sync,
    N: OutputStore<L, A>,
> HyperBall<'_, G1, G2, D, L, A, N>
where
    L::Backend: PartialEq,
{
    /// Runs HyperBall.
    ///
    /// # Arguments
    ///
    /// * `upper_bound` - an upper bound to the number of iterations.
    ///
    /// * `threshold` - a value that will be used to stop the computation by
    ///   relative increment if the neighborhood function is being computed. If
    ///   [`None`] the computation will stop when no estimators are modified.
    ///
    /// * `pl` - A progress logger.
    pub fn run(
        &mut self,
        upper_bound: usize,
        threshold: Option<f64>,
        rng: impl rand::RngExt,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        let upper_bound = std::cmp::min(upper_bound, self.graph.num_nodes());

        self.init(rng, pl)
            .with_context(|| "Could not initialize estimator")?;

        pl.item_name("iteration");
        pl.expected_updates(None);
        pl.start(format!(
            "Running HyperBall for a maximum of {} iterations and a threshold of {:?}",
            upper_bound, threshold
        ));

        for i in 0..upper_bound {
            self.iterate(pl)
                .with_context(|| format!("Could not perform iteration {}", i + 1))?;

            pl.update_and_display();

            if self
                .iteration_context
                .modified_estimators
                .load(Ordering::Relaxed)
                == 0
            {
                pl.info(format_args!(
                    "Terminating HyperBall after {} iteration(s) by stabilization",
                    i + 1
                ));
                break;
            }

            if let Some(t) = threshold {
                if i > 3 && self.relative_increment < (1.0 + t) {
                    pl.info(format_args!("Terminating HyperBall after {} iteration(s) by relative bound on the neighborhood function", i + 1));
                    break;
                }
            }
        }

        pl.done();

        Ok(())
    }

    /// Runs HyperBall until no estimators are modified.
    ///
    /// # Arguments
    ///
    /// * `upper_bound` - an upper bound to the number of iterations.
    ///
    /// * `pl` - A progress logger.
    #[inline(always)]
    pub fn run_until_stable(
        &mut self,
        upper_bound: usize,
        rng: impl rand::RngExt,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        self.run(upper_bound, None, rng, pl)
            .with_context(|| "Could not complete run_until_stable")
    }

    /// Runs HyperBall until no estimators are modified with no upper bound on the
    /// number of iterations.
    ///
    /// # Arguments
    ///
    /// * `pl` - A progress logger.
    #[inline(always)]
    pub fn run_until_done(
        &mut self,
        rng: impl rand::RngExt,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        self.run_until_stable(usize::MAX, rng, pl)
            .with_context(|| "Could not complete run_until_done")
    }

    #[inline(always)]
    fn ensure_iteration(&self) -> Result<()> {
        ensure!(
            self.iteration_context.iteration > 0,
            "HyperBall was not run. Please call HyperBall::run before accessing computed fields."
        );
        Ok(())
    }

    /// Returns the neighborhood function computed by this instance.
    pub fn neighborhood_function(&self) -> Result<&[f64]> {
        self.ensure_iteration()?;
        Ok(&self.neighborhood_function)
    }

    /// Returns the sum of distances computed by this instance if requested.
    pub fn sum_of_distances(&self) -> Result<&[f32]> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_dists {
            Ok(distances)
        } else {
            bail!(
                "Sum of distances were not requested: use builder.sum_of_distances(true) while building HyperBall to compute them"
            )
        }
    }

    /// Returns the harmonic centralities (sum of inverse distances) computed by this instance if requested.
    pub fn harmonic_centralities(&self) -> Result<&[f32]> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_inv_dists {
            Ok(distances)
        } else {
            bail!(
                "Sum of inverse distances were not requested: use builder.sum_of_inverse_distances(true) while building HyperBall to compute them"
            )
        }
    }

    /// Returns the discounted centralities of the specified index computed by this instance.
    ///
    /// # Arguments
    /// * `index` - the index of the requested discounted centrality.
    pub fn discounted_centrality(&self, index: usize) -> Result<&[f32]> {
        self.ensure_iteration()?;
        let d = self.discounted_centralities.get(index);
        if let Some(distances) = d {
            Ok(distances)
        } else {
            bail!("Discount centrality of index {} does not exist", index)
        }
    }

    /// Computes and returns the closeness centralities from the sum of distances computed by this instance.
    pub fn closeness_centrality(&self) -> Result<Box<[f32]>> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_dists {
            Ok(distances
                .iter()
                .map(|&d| if d == 0.0 { 0.0 } else { d.recip() })
                .collect())
        } else {
            bail!(
                "Sum of distances were not requested: use builder.sum_of_distances(true) while building HyperBall to compute closeness centrality"
            )
        }
    }

    /// Computes and returns the Lin centralities from the sum of distances computed by this instance.
    ///
    /// Note that Lin's index for isolated nodes is by (our) definition one (it's smaller than any other node).
    pub fn lin_centrality(&self) -> Result<Box<[f32]>> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_dists {
            let logic = self.curr_state.logic();
            Ok(distances
                .iter()
                .enumerate()
                .map(|(node, &d)| {
                    if d == 0.0 {
                        1.0
                    } else {
                        let count = logic.estimate(self.curr_state.get_backend(node));
                        (count * count / d as f64) as f32
                    }
                })
                .collect())
        } else {
            bail!(
                "Sum of distances were not requested: use builder.sum_of_distances(true) while building HyperBall to compute Lin centrality"
            )
        }
    }

    /// Computes and returns the Nieminen centralities from the sum of distances computed by this instance.
    pub fn nieminen_centrality(&self) -> Result<Box<[f32]>> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_dists {
            let logic = self.curr_state.logic();
            Ok(distances
                .iter()
                .enumerate()
                .map(|(node, &d)| {
                    let count = logic.estimate(self.curr_state.get_backend(node));
                    ((count * count) - d as f64) as f32
                })
                .collect())
        } else {
            bail!(
                "Sum of distances were not requested: use builder.sum_of_distances(true) while building HyperBall to compute Nieminen centrality"
            )
        }
    }

    /// Reads from the internal estimator array and estimates the number of nodes
    /// reachable from the specified node.
    ///
    /// # Arguments
    /// * `node` - the index of the node to compute reachable nodes from.
    pub fn reachable_nodes_from(&self, node: usize) -> Result<f64> {
        self.ensure_iteration()?;
        Ok(self
            .curr_state
            .logic()
            .estimate(self.curr_state.get_backend(node)))
    }

    /// Reads from the internal estimator array and estimates the number of nodes reachable
    /// from every node of the graph.
    ///
    /// `hyperball.reachable_nodes().unwrap()[i]` is equal to `hyperball.reachable_nodes_from(i).unwrap()`.
    pub fn reachable_nodes(&self) -> Result<Box<[f32]>> {
        self.ensure_iteration()?;
        let logic = self.curr_state.logic();
        Ok((0..self.graph.num_nodes())
            .map(|n| logic.estimate(self.curr_state.get_backend(n)) as f32)
            .collect())
    }
}

impl<
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: for<'b> Succ<Input = u64, Output<'b> = u64> + Sync,
    L: EstimationLogic<Item = usize> + MergeEstimationLogic + Sync,
    A: EstimatorArrayMut<L> + Sync,
    N: OutputStore<L, A>,
> HyperBall<'_, G1, G2, D, L, A, N>
where
    L::Backend: PartialEq,
{
    /// Performs a new iteration of HyperBall.
    ///
    /// # Arguments
    /// * `pl` - A progress logger.
    fn iterate(&mut self, pl: &mut impl ConcurrentProgressLog) -> Result<()> {
        let ic = &mut self.iteration_context;

        pl.info(format_args!("Performing iteration {}", ic.iteration + 1));

        // Alias the number of modified estimators, nodes and arcs
        let num_nodes = self.graph.num_nodes() as u64;
        let num_arcs = self.graph.num_arcs();
        let modified_estimators = ic.modified_estimators.load(Ordering::Relaxed);

        // Let us record whether the previous computation was systolic or local
        let prev_was_systolic = ic.systolic;
        let prev_was_local = ic.local;

        // If less than one fourth of the nodes have been modified, and we have
        // the transpose, it is time to pass to a systolic computation
        ic.systolic =
            self.transposed.is_some() && ic.iteration > 0 && modified_estimators < num_nodes / 4;

        // Non-systolic computations add up the values of all estimators.
        //
        // Systolic computations modify the last value by compensating for each
        // modified estimators.
        *ic.current_nf.lock().unwrap() = if ic.systolic { self.last } else { 0.0 };

        // If we completed the last iteration in pre-local mode, we MUST run in
        // local mode
        ic.local = ic.pre_local;

        // We run in pre-local mode if we are systolic and few nodes where
        // modified.
        ic.pre_local = ic.systolic
            && modified_estimators
                < ((num_nodes as u128 * num_nodes as u128) / (num_arcs as u128 * 10)) as u64;

        if ic.systolic {
            pl.info(format_args!(
                "Starting systolic iteration (local: {}, pre_local: {})",
                ic.local, ic.pre_local
            ));
        } else {
            pl.info(format_args!("Starting standard iteration"));
        }

        if prev_was_local {
            for &node in ic.local_checklist.iter() {
                ic.next_modified.set(node, false, Ordering::Relaxed);
            }
        } else {
            ic.next_modified.fill(false, Ordering::Relaxed);
        }

        if ic.local {
            // In case of a local computation, we convert the set of
            // must-be-checked for the next iteration into a check list
            rayon::join(
                || ic.local_checklist.clear(),
                || {
                    let mut local_next_must_be_checked =
                        ic.local_next_must_be_checked.lock().unwrap();
                    local_next_must_be_checked.par_sort_unstable();
                    local_next_must_be_checked.dedup();
                },
            );
            std::mem::swap(
                &mut ic.local_checklist,
                &mut ic.local_next_must_be_checked.lock().unwrap(),
            );
        } else if ic.systolic {
            rayon::join(
                || {
                    // Systolic, non-local computations store the could-be-modified set implicitly into Self::next_must_be_checked.
                    ic.next_must_be_checked.fill(false, Ordering::Relaxed);
                },
                || {
                    // If the previous computation wasn't systolic, we must assume that all registers could have changed.
                    if !prev_was_systolic {
                        ic.must_be_checked.fill(true, Ordering::Relaxed);
                    }
                },
            );
        }

        let mut node_granularity = ic.node_granularity;
        let num_threads = rayon::current_num_threads();

        if num_threads > 1 && !ic.local {
            if ic.iteration > 0 {
                node_granularity = f64::min(
                    std::cmp::max(1, num_nodes as usize / num_threads) as _,
                    node_granularity as f64
                        * (num_nodes as f64 / std::cmp::max(1, modified_estimators) as f64),
                ) as usize;
            }
            pl.info(format_args!(
                "Adaptive node granularity for this iteration: {}",
                node_granularity
            ));
        }

        ic.reset(node_granularity);

        let mut arc_pl = pl.dup();
        arc_pl.item_name("arc");
        arc_pl.expected_updates(if ic.local { None } else { Some(num_arcs as _) });
        arc_pl.start("Scanning arcs...");
        {
            let next_state_sync = self.next_state.as_sync();
            let sum_of_dists = self.sum_of_dists.as_mut().map(|x| x.as_sync_slice());
            let sum_of_inv_dists = self.sum_of_inv_dists.as_mut().map(|x| x.as_sync_slice());

            let discounted_centralities = &self
                .discounted_centralities
                .iter_mut()
                .map(|s| s.as_sync_slice())
                .collect::<Vec<_>>();
            rayon::broadcast(|c| {
                let mut arc_pl = arc_pl.clone();
                Self::parallel_task(
                    self.graph,
                    self.transposed,
                    &self.curr_state,
                    &next_state_sync,
                    ic,
                    sum_of_dists,
                    sum_of_inv_dists,
                    discounted_centralities,
                    &mut arc_pl,
                    c,
                )
            });
        }

        arc_pl.done_with_count(ic.visited_arcs.load(Ordering::Relaxed) as usize);
        let modified_estimators = ic.modified_estimators.load(Ordering::Relaxed);

        pl.info(format_args!(
            "Modified estimators: {}/{} ({:.3}%)",
            modified_estimators,
            self.graph.num_nodes(),
            (modified_estimators as f64 / self.graph.num_nodes() as f64) * 100.0
        ));

        self.next_state.commute(&mut self.curr_state);
        std::mem::swap(&mut ic.curr_modified, &mut ic.next_modified);

        if ic.systolic {
            std::mem::swap(&mut ic.must_be_checked, &mut ic.next_must_be_checked);
        }

        let mut current_nf_mut = ic.current_nf.lock().unwrap();
        self.last = *current_nf_mut;
        // We enforce monotonicity--non-monotonicity can only be caused by
        // approximation errors
        let &last_output = self
            .neighborhood_function
            .as_slice()
            .last()
            .expect("Should always have at least 1 element");
        if *current_nf_mut < last_output {
            *current_nf_mut = last_output;
        }
        self.relative_increment = *current_nf_mut / last_output;

        pl.info(format_args!(
            "Pairs: {} ({}%)",
            *current_nf_mut,
            (*current_nf_mut * 100.0) / (num_nodes * num_nodes) as f64
        ));
        pl.info(format_args!(
            "Absolute increment: {}",
            *current_nf_mut - last_output
        ));
        pl.info(format_args!(
            "Relative increment: {}",
            self.relative_increment
        ));

        self.neighborhood_function.push(*current_nf_mut);

        ic.iteration += 1;

        Ok(())
    }

    /// Processes a single node during an iteration.
    ///
    /// The method is generic over the successor iterator type, making it
    /// possible to use either random-access successors (local iterations)
    /// or sequentially-decoded successors (non-local iterations) without
    /// duplicating the processing logic.
    ///
    /// Returns `(visited_arcs, modified_estimators)`.
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn process_node<I: IntoIterator<Item = usize>>(
        node: usize,
        successors: I,
        transpose: Option<&(impl RandomAccessGraph + Sync)>,
        curr_state: &impl EstimatorArray<L>,
        next_state: &impl SyncOutputStore<L>,
        ic: &IterationContext<'_, G1, D>,
        sum_of_dists: Option<&[SyncCell<f32>]>,
        sum_of_inv_dists: Option<&[SyncCell<f32>]>,
        discounted_centralities: &[&[SyncCell<f32>]],
        do_centrality: bool,
        next_estimator: &mut L::Estimator<'_>,
        helper: &mut L::Helper,
        arc_pl: &mut impl ConcurrentProgressLog,
        neighborhood_function_delta: &mut KahanSum<f64>,
    ) -> (u64, u64) {
        let logic = curr_state.logic();
        let prev_estimator = curr_state.get_backend(node);
        let mut visited_arcs = 0u64;
        let mut modified_estimators = 0u64;

        // The three cases in which we enumerate successors:
        // 1) A non-systolic computation (we don't know anything, so we enumerate).
        // 2) A systolic, local computation (the node is by definition to be
        //    checked, as it comes from the local check list).
        // 3) A systolic, non-local computation in which the node should be checked.
        if !ic.systolic || ic.local || ic.must_be_checked[node] {
            next_estimator.set(prev_estimator);
            let mut modified = false;
            for succ in successors {
                if succ != node && ic.curr_modified[succ] {
                    visited_arcs += 1;
                    arc_pl.light_update();
                    if !modified {
                        modified = true;
                    }
                    logic.merge_with_helper(
                        next_estimator.as_mut(),
                        curr_state.get_backend(succ),
                        helper,
                    );
                }
            }

            let mut post = f64::NAN;
            let estimator_modified = modified && next_estimator.as_ref() != prev_estimator;

            // We need the estimator value only if the iteration is standard (as we're going to
            // compute the neighborhood function cumulating actual values, and not deltas) or
            // if the estimator was actually modified (as we're going to cumulate the neighborhood
            // function delta, or at least some centrality).
            if !ic.systolic || estimator_modified {
                post = logic.estimate(next_estimator.as_ref())
            }
            if !ic.systolic {
                *neighborhood_function_delta += post;
            }

            if estimator_modified && (ic.systolic || do_centrality) {
                let pre = logic.estimate(prev_estimator);
                if ic.systolic {
                    *neighborhood_function_delta += -pre;
                    *neighborhood_function_delta += post;
                }

                if do_centrality {
                    let delta = post - pre;
                    // Note that this code is executed only for distances > 0
                    if delta > 0.0 {
                        if let Some(distances) = sum_of_dists {
                            let new_value = delta * (ic.iteration + 1) as f64;
                            // SAFETY: each node is accessed exactly once per iteration.
                            unsafe {
                                distances[node]
                                    .set((distances[node].get() as f64 + new_value) as f32)
                            };
                        }
                        if let Some(distances) = sum_of_inv_dists {
                            let new_value = delta / (ic.iteration + 1) as f64;
                            // SAFETY: each node is accessed exactly once per iteration.
                            unsafe {
                                distances[node]
                                    .set((distances[node].get() as f64 + new_value) as f32)
                            };
                        }
                        for (func, distances) in ic
                            .discount_functions
                            .iter()
                            .zip(discounted_centralities.iter())
                        {
                            let new_value = delta * func(ic.iteration + 1);
                            // SAFETY: each node is accessed exactly once per iteration.
                            unsafe {
                                distances[node]
                                    .set((distances[node].get() as f64 + new_value) as f32)
                            };
                        }
                    }
                }
            }

            if estimator_modified {
                // We keep track of modified estimators in the result. Note that we must
                // add the current node to the must-be-checked set for the next
                // local iteration if it is modified, as it might need a copy to
                // the result array at the next iteration.
                if ic.pre_local {
                    ic.local_next_must_be_checked.lock().unwrap().push(node);
                }
                ic.next_modified.set(node, true, Ordering::Relaxed);

                if ic.systolic {
                    debug_assert!(transpose.is_some());
                    // In systolic computations we must keep track of
                    // which estimators must be checked on the next
                    // iteration. If we are preparing a local
                    // computation, we do this explicitly, by adding the
                    // predecessors of the current node to a set.
                    // Otherwise, we do this implicitly, by setting the
                    // corresponding entry in an array.

                    // SAFETY: ic.systolic is true, so transpose is Some
                    let transpose = unsafe { transpose.unwrap_unchecked() };
                    if ic.pre_local {
                        let mut local_next_must_be_checked =
                            ic.local_next_must_be_checked.lock().unwrap();
                        for succ in transpose.successors(node) {
                            local_next_must_be_checked.push(succ);
                        }
                    } else {
                        for succ in transpose.successors(node) {
                            ic.next_must_be_checked.set(succ, true, Ordering::Relaxed);
                        }
                    }
                }

                modified_estimators += 1;
            }

            // SAFETY: each node is accessed exactly once per iteration.
            unsafe {
                next_state.set(node, next_estimator.as_ref());
            }
        } else {
            // Even if we cannot possibly have changed our value, still our copy
            // in the result vector might need to be updated because it does not
            // reflect our current value.
            if ic.curr_modified[node] {
                // SAFETY: each node is accessed exactly once per iteration.
                unsafe {
                    next_state.set(node, prev_estimator);
                }
            }
        }

        (visited_arcs, modified_estimators)
    }

    /// The parallel operations to be performed each iteration.
    ///
    /// # Arguments:
    /// * `graph` - the graph to analyze.
    /// * `transpose` - optionally, the transpose of `graph`. If [`None`], no
    ///   systolic iterations will be performed.
    /// * `curr_state` - the current state of the estimators.
    /// * `next_state` - the next state of the estimators (to be computed).
    /// * `ic` - the iteration context.
    #[allow(clippy::too_many_arguments)]
    fn parallel_task(
        graph: &(impl RandomAccessGraph + Sync),
        transpose: Option<&(impl RandomAccessGraph + Sync)>,
        curr_state: &impl EstimatorArray<L>,
        next_state: &impl SyncOutputStore<L>,
        ic: &IterationContext<'_, G1, D>,
        sum_of_dists: Option<&[SyncCell<f32>]>,
        sum_of_inv_dists: Option<&[SyncCell<f32>]>,
        discounted_centralities: &[&[SyncCell<f32>]],
        arc_pl: &mut impl ConcurrentProgressLog,
        _broadcast_context: rayon::BroadcastContext,
    ) {
        let node_granularity = ic.node_granularity;
        let target_arcs = ((graph.num_arcs() as f64 * node_granularity as f64)
            / graph.num_nodes() as f64)
            .ceil() as u64;
        let do_centrality = sum_of_dists.is_some()
            || sum_of_inv_dists.is_some()
            || !ic.discount_functions.is_empty();
        let node_upper_limit = if ic.local {
            ic.local_checklist.len()
        } else {
            graph.num_nodes()
        };
        let mut visited_arcs = 0;
        let mut modified_estimators = 0;
        let arc_upper_limit = graph.num_arcs();

        // During standard iterations, cumulates the neighborhood function for the nodes scanned
        // by this thread. During systolic iterations, cumulates the *increase* of the
        // neighborhood function for the nodes scanned by this thread.
        let mut neighborhood_function_delta = KahanSum::new_with_value(0.0);
        let mut helper = curr_state.logic().new_helper();
        let logic = curr_state.logic();
        let mut next_estimator = logic.new_estimator();

        loop {
            // Get work
            let (start, end) = if ic.local {
                let start = std::cmp::min(
                    ic.node_cursor.fetch_add(1, Ordering::Relaxed),
                    node_upper_limit,
                );
                let end = std::cmp::min(start + 1, node_upper_limit);
                (start, end)
            } else {
                let mut arc_balanced_cursor = ic.arc_cursor.lock().unwrap();
                let (mut next_node, mut next_arc) = *arc_balanced_cursor;
                if next_node >= node_upper_limit {
                    (node_upper_limit, node_upper_limit)
                } else {
                    let start = next_node;
                    let target = next_arc + target_arcs;
                    if target >= arc_upper_limit {
                        next_node = node_upper_limit;
                    } else {
                        (next_node, next_arc) = ic.cumul_outdeg.succ(target).unwrap();
                    }
                    let end = next_node;
                    *arc_balanced_cursor = (next_node, next_arc);
                    (start, end)
                }
            };

            if start == node_upper_limit {
                break;
            }

            // Do work: local iterations use random access, non-local
            // iterations use sequential decoding via iter_from() for
            // much faster access to compressed graphs.
            if ic.local {
                for i in start..end {
                    let node = ic.local_checklist[i];
                    let (va, me) = Self::process_node(
                        node,
                        graph.successors(node),
                        transpose,
                        curr_state,
                        next_state,
                        ic,
                        sum_of_dists,
                        sum_of_inv_dists,
                        discounted_centralities,
                        do_centrality,
                        &mut next_estimator,
                        &mut helper,
                        arc_pl,
                        &mut neighborhood_function_delta,
                    );
                    visited_arcs += va;
                    modified_estimators += me;
                }
            } else {
                for_![(node, successors) in graph.iter_from(start).take(end - start) {
                    let (va, me) = Self::process_node(
                        node,
                        successors,
                        transpose,
                        curr_state,
                        next_state,
                        ic,
                        sum_of_dists,
                        sum_of_inv_dists,
                        discounted_centralities,
                        do_centrality,
                        &mut next_estimator,
                        &mut helper,
                        arc_pl,
                        &mut neighborhood_function_delta,
                    );
                    visited_arcs += va;
                    modified_estimators += me;
                }]
            }
        }

        *ic.current_nf.lock().unwrap() += neighborhood_function_delta.sum();
        ic.visited_arcs.fetch_add(visited_arcs, Ordering::Relaxed);
        ic.modified_estimators
            .fetch_add(modified_estimators, Ordering::Relaxed);
    }

    /// Initializes HyperBall.
    fn init(
        &mut self,
        mut rng: impl rand::RngExt,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        pl.start("Initializing estimators...");
        pl.info(format_args!("Clearing all registers..."));

        self.curr_state.clear();
        self.next_state.clear();

        pl.info(format_args!("Initializing registers"));
        if let Some(w) = &self.weight {
            pl.info(format_args!("Loading weights"));
            for (i, &node_weight) in w.iter().enumerate() {
                let mut estimator = self.curr_state.get_estimator_mut(i);
                for _ in 0..node_weight {
                    estimator.add(&(rng.random::<u64>() as usize));
                }
            }
        } else {
            (0..self.graph.num_nodes()).for_each(|i| {
                self.curr_state.get_estimator_mut(i).add(i);
            });
        }

        self.completed = false;

        let ic = &mut self.iteration_context;
        ic.iteration = 0;
        ic.systolic = false;
        ic.local = false;
        ic.pre_local = false;
        ic.reset(
            self.granularity
                .node_granularity(self.graph.num_nodes(), Some(self.graph.num_arcs())),
        );

        pl.debug(format_args!("Initializing distances"));
        if let Some(distances) = &mut self.sum_of_dists {
            distances.fill(0.0);
        }
        if let Some(distances) = &mut self.sum_of_inv_dists {
            distances.fill(0.0);
        }
        pl.debug(format_args!("Initializing centralities"));
        for centralities in self.discounted_centralities.iter_mut() {
            centralities.fill(0.0);
        }

        self.last = self.graph.num_nodes() as f64;
        pl.debug(format_args!("Initializing neighborhood function"));
        self.neighborhood_function.clear();
        self.neighborhood_function.push(self.last);

        pl.debug(format_args!("Initializing modified estimators"));
        ic.curr_modified.fill(true, Ordering::Relaxed);

        pl.done();

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use card_est_array::traits::{EstimatorArray, MergeEstimator};
    use dsi_progress_logger::no_logging;
    use epserde::deser::{Deserialize, Flags};
    use rand::SeedableRng;
    use webgraph::{
        prelude::{BvGraph, DCF},
        traits::SequentialLabeling,
    };

    /// Generates a parallel-vs-sequential HyperBall comparison test for a
    /// given estimation logic. The macro is needed because the backend word
    /// type (`usize` vs `u8`) differs between `HyperLogLog` and
    /// `HyperLogLog8`, preventing a single generic function. The
    macro_rules! cnr_2000_test {
        ($name:ident, $make_logic:expr) => {
            #[cfg_attr(feature = "slow_tests", test)]
            #[cfg_attr(not(feature = "slow_tests"), allow(dead_code))]
            fn $name() -> Result<()> {
                #[cfg(target_pointer_width = "64")]
                let basename = "../data/cnr-2000";
                #[cfg(not(target_pointer_width = "64"))]
                let basename = "../data/cnr-2000_32/cnr-2000";

                #[cfg(target_pointer_width = "64")]
                let basename_t = "../data/cnr-2000-t";
                #[cfg(not(target_pointer_width = "64"))]
                let basename_t = "../data/cnr-2000_32/cnr-2000-t";

                let graph = BvGraph::with_basename(basename).load()?;
                let transpose = BvGraph::with_basename(basename_t).load()?;
                let cumulative =
                    unsafe { DCF::load_mmap(basename.to_owned() + ".dcf", Flags::empty()) }?;
                let num_nodes = graph.num_nodes();

                let logic = ($make_logic)(num_nodes)?;

                let seq_bits = SliceEstimatorArray::new(logic.clone(), num_nodes);
                let seq_result_bits = SliceEstimatorArray::new(logic.clone(), num_nodes);
                let par_bits = SliceEstimatorArray::new(logic.clone(), num_nodes);
                let par_result_bits = SliceEstimatorArray::new(logic, num_nodes);

                let mut hyperball = HyperBallBuilder::with_transpose(
                    &graph,
                    &transpose,
                    cumulative.uncase(),
                    par_bits,
                    par_result_bits,
                )
                .build(no_logging![]);

                // Sequential reference implementation
                struct SeqState<A> {
                    curr: A,
                    next: A,
                }

                let mut seq = SeqState {
                    curr: seq_bits,
                    next: seq_result_bits,
                };

                // Init
                for i in 0..num_nodes {
                    seq.curr.get_estimator_mut(i).add(&i);
                }

                let mut modified_estimators = num_nodes as u64;
                let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
                hyperball.init(&mut rng, no_logging![])?;

                while modified_estimators != 0 {
                    hyperball.iterate(no_logging![])?;

                    // Sequential iterate
                    for i in 0..num_nodes {
                        let mut estimator = seq.next.get_estimator_mut(i);
                        estimator.set(seq.curr.get_backend(i));
                        for succ in graph.successors(i) {
                            estimator.merge(seq.curr.get_backend(succ));
                        }
                    }
                    std::mem::swap(&mut seq.curr, &mut seq.next);

                    modified_estimators = hyperball
                        .iteration_context
                        .modified_estimators
                        .load(Ordering::Relaxed);

                    // Compare per-node backends
                    for i in 0..num_nodes {
                        assert_eq!(
                            hyperball.next_state.get_backend(i),
                            seq.next.get_backend(i),
                            "next_state mismatch at node {i}"
                        );
                        assert_eq!(
                            hyperball.curr_state.get_backend(i),
                            seq.curr.get_backend(i),
                            "curr_state mismatch at node {i}"
                        );
                    }
                }

                Ok(())
            }
        };
    }

    cnr_2000_test!(test_cnr_2000, |n| HyperLogLogBuilder::new(n)
        .log2_num_regs(6)
        .build());

    cnr_2000_test!(test_cnr_2000_hll8, |_| Ok::<_, anyhow::Error>(
        HyperLogLog8Builder::new().log2_num_regs(6).build::<usize>()
    ));

    macro_rules! cnr_2000_external_test {
        ($name:ident, $make_builder:expr) => {
            #[cfg_attr(feature = "slow_tests", test)]
            #[cfg_attr(not(feature = "slow_tests"), allow(dead_code))]
            fn $name() -> Result<()> {
                #[cfg(target_pointer_width = "64")]
                let basename = "../data/cnr-2000";
                #[cfg(not(target_pointer_width = "64"))]
                let basename = "../data/cnr-2000_32/cnr-2000";

                #[cfg(target_pointer_width = "64")]
                let basename_t = "../data/cnr-2000-t";
                #[cfg(not(target_pointer_width = "64"))]
                let basename_t = "../data/cnr-2000_32/cnr-2000-t";

                let graph = BvGraph::with_basename(basename).load()?;
                let transpose = BvGraph::with_basename(basename_t).load()?;
                let cumulative =
                    unsafe { DCF::load_mmap(basename.to_owned() + ".dcf", Flags::empty()) }?;
                let num_nodes = graph.num_nodes();

                let logic = ($make_builder)(&graph, &transpose, cumulative.uncase(), num_nodes)?;
                let (mut hyperball, seq_logic) = logic;

                let seq_bits = SliceEstimatorArray::new(seq_logic.clone(), num_nodes);
                let seq_result_bits = SliceEstimatorArray::new(seq_logic, num_nodes);

                struct SeqState<A> {
                    curr: A,
                    next: A,
                }

                let mut seq = SeqState {
                    curr: seq_bits,
                    next: seq_result_bits,
                };

                for i in 0..num_nodes {
                    seq.curr.get_estimator_mut(i).add(&i);
                }

                let mut modified_estimators = num_nodes as u64;
                let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
                hyperball.init(&mut rng, no_logging![])?;

                while modified_estimators != 0 {
                    hyperball.iterate(no_logging![])?;

                    for i in 0..num_nodes {
                        let mut estimator = seq.next.get_estimator_mut(i);
                        estimator.set(seq.curr.get_backend(i));
                        for succ in graph.successors(i) {
                            estimator.merge(seq.curr.get_backend(succ));
                        }
                    }
                    std::mem::swap(&mut seq.curr, &mut seq.next);

                    modified_estimators = hyperball
                        .iteration_context
                        .modified_estimators
                        .load(Ordering::Relaxed);

                    for i in 0..num_nodes {
                        assert_eq!(
                            hyperball.curr_state.get_backend(i),
                            seq.curr.get_backend(i),
                            "curr_state mismatch at node {i}"
                        );
                    }
                }

                Ok(())
            }
        };
    }

    cnr_2000_external_test!(test_cnr_2000_external, |graph, transpose, dcf, n| {
        let logic = HyperLogLogBuilder::new(n)
            .log2_num_regs(6)
            .build::<usize>()?;
        let seq_logic = logic.clone();
        let hb = HyperBallBuilder::with_hyper_log_log_external(
            graph,
            Some(transpose),
            dcf,
            6,
            None,
        )?
        .build(no_logging![]);
        Ok::<_, anyhow::Error>((hb, seq_logic))
    });

    cnr_2000_external_test!(test_cnr_2000_hll8_external, |graph, transpose, dcf, _n| {
        let logic = HyperLogLog8Builder::new().log2_num_regs(6).build::<usize>();
        let seq_logic = logic.clone();
        let hb = HyperBallBuilder::with_hyper_log_log8_external(
            graph,
            Some(transpose),
            dcf,
            6,
            None,
        )?
        .build(no_logging![]);
        Ok::<_, anyhow::Error>((hb, seq_logic))
    });

    #[test]
    fn test_spill_store_vs_in_memory() -> Result<()> {
        use webgraph::graphs::vec_graph::VecGraph;
        let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
        let dcf = graph.build_dcf();

        let mut rng_mem = rand::rngs::SmallRng::seed_from_u64(0);
        let mut rng_ext = rand::rngs::SmallRng::seed_from_u64(0);

        let mut hb_mem =
            HyperBallBuilder::with_hyper_log_log(&graph, None::<&VecGraph>, &dcf, 6, None)?
                .build(no_logging![]);

        let mut hb_ext = HyperBallBuilder::with_hyper_log_log_external(
            &graph,
            None::<&VecGraph>,
            &dcf,
            6,
            None,
        )?
        .build(no_logging![]);

        hb_mem.run_until_done(&mut rng_mem, no_logging![])?;
        hb_ext.run_until_done(&mut rng_ext, no_logging![])?;

        let nf_mem = hb_mem.neighborhood_function()?;
        let nf_ext = hb_ext.neighborhood_function()?;

        assert_eq!(nf_mem.len(), nf_ext.len());
        for (m, e) in nf_mem.iter().zip(nf_ext.iter()) {
            assert!(
                (m - e).abs() < 1e-6,
                "neighborhood function mismatch: {m} vs {e}"
            );
        }

        for i in 0..graph.num_nodes() {
            assert_eq!(
                hb_mem.curr_state.get_backend(i),
                hb_ext.curr_state.get_backend(i),
                "backend mismatch at node {i}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_spill_store_hll8() -> Result<()> {
        use webgraph::graphs::vec_graph::VecGraph;
        let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0), (1, 3)]);
        let dcf = graph.build_dcf();

        let mut rng_mem = rand::rngs::SmallRng::seed_from_u64(42);
        let mut rng_ext = rand::rngs::SmallRng::seed_from_u64(42);

        let mut hb_mem =
            HyperBallBuilder::with_hyper_log_log8(&graph, None::<&VecGraph>, &dcf, 6, None)?
                .build(no_logging![]);

        let mut hb_ext = HyperBallBuilder::with_hyper_log_log8_external(
            &graph,
            None::<&VecGraph>,
            &dcf,
            6,
            None,
        )?
        .build(no_logging![]);

        hb_mem.run_until_done(&mut rng_mem, no_logging![])?;
        hb_ext.run_until_done(&mut rng_ext, no_logging![])?;

        let nf_mem = hb_mem.neighborhood_function()?;
        let nf_ext = hb_ext.neighborhood_function()?;

        assert_eq!(nf_mem.len(), nf_ext.len());
        for (m, e) in nf_mem.iter().zip(nf_ext.iter()) {
            assert!(
                (m - e).abs() < 1e-6,
                "neighborhood function mismatch: {m} vs {e}"
            );
        }

        Ok(())
    }
}
