/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{bail, ensure, Context, Result};
use counter_array::impls::{HyperLogLog, HyperLogLogBuilder, SliceCounterArray};
use counter_array::traits::{
    AsSyncArray, CounterArray, CounterArrayMut, CounterLogic, CounterMut, MergeCounterLogic,
    SyncCounterArray,
};
use dsi_progress_logger::ConcurrentProgressLog;
use kahan::KahanSum;
use rand::random;
use rayon::{prelude::*, ThreadPool};
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::sync::{atomic::*, Mutex};
use sux::{bits::AtomicBitVec, traits::Succ};
use sync_cell_slice::{SyncCell, SyncSlice};
use webgraph::traits::{RandomAccessGraph, SequentialLabeling};
use webgraph::utils::Granularity;

/// A builder for [`HyperBall`].
///
/// After creating a builder with [`HyperBallBuilder::new`] you can configure it
/// using setters such as [`HyperBallBuilder`] its methods, then call
/// [`HyperBallBuilder::build`] on it to create a [`HyperBall`] instance.
pub struct HyperBallBuilder<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: Succ<Input = usize, Output = usize>,
    L: MergeCounterLogic<Item = G1::Label>,
    A: CounterArrayMut<L>,
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
    discount_functions: Vec<Box<dyn Fn(usize) -> f64 + Sync + 'a>>,
    /// The arc granularity.
    arc_granularity: usize,
    /// Integer weights for the nodes, if any.
    weights: Option<&'a [usize]>,
    /// A first array of counters.
    array_0: A,
    /// A second array of counters of the same length and with the same logic of
    /// `array_0`.
    array_1: A,
    _marker: std::marker::PhantomData<L>,
}

impl<
        'a,
        G1: RandomAccessGraph + Sync,
        G2: RandomAccessGraph + Sync,
        D: Succ<Input = usize, Output = usize>,
    >
    HyperBallBuilder<
        'a,
        G1,
        G2,
        D,
        HyperLogLog<G1::Label, BuildHasherDefault<DefaultHasher>, usize>,
        SliceCounterArray<
            HyperLogLog<G1::Label, BuildHasherDefault<DefaultHasher>, usize>,
            usize,
            Box<[usize]>,
        >,
    >
{
    /// A builder for [`HyperBall`] using a specified [`CounterLogic`].
    ///
    /// # Arguments
    /// * `graph`: the graph to analyze.
    /// * `transpose`: optionally, the transpose of `graph`. If [`None`], no
    ///   systolic iterations will be performed by the resulting [`HyperBall`].
    /// * `cumul_outdeg`: the outdegree cumulative function of the graph.
    /// * `log2m`: the base-2 logarithm of the number *m* of register per
    ///   HyperLogLog counter.
    /// * `weights`: the weights to use. If [`None`] every node is assumed to be
    ///   of weight equal to 1.
    /// * `mmap_options`: the options to use for the backend of the counter
    ///   arrays as a [`TempMmapOptions`].
    pub fn with_hyper_log_log(
        graph: &'a G1,
        transposed: Option<&'a G2>,
        cumul_outdeg: &'a D,
        log2m: usize,
        weights: Option<&'a [usize]>,
    ) -> Result<Self> {
        let num_elements = if let Some(w) = weights {
            ensure!(
                w.len() == graph.num_nodes(),
                "weights should have length equal to the graph's number of nodes"
            );
            w.iter().sum()
        } else {
            graph.num_nodes()
        };

        let logic = HyperLogLogBuilder::new(num_elements)
            .log_2_num_reg(log2m)
            .build()
            .with_context(|| "Could not build HyperLogLog logic")?;

        let array_0 = SliceCounterArray::new(logic.clone(), graph.num_nodes());
        let array_1 = SliceCounterArray::new(logic, graph.num_nodes());

        Ok(Self {
            graph,
            transpose: transposed,
            cumul_outdegree: cumul_outdeg,
            do_sum_of_dists: false,
            do_sum_of_inv_dists: false,
            discount_functions: Vec::new(),
            arc_granularity: Self::DEFAULT_GRANULARITY,
            weights,
            array_0,
            array_1,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<
        'a,
        D: Succ<Input = usize, Output = usize>,
        G: RandomAccessGraph + Sync,
        L: MergeCounterLogic<Item = G::Label> + PartialEq,
        A: CounterArrayMut<L>,
    > HyperBallBuilder<'a, G, G, D, L, A>
{
    /// Creates a new builder with default parameters.
    ///
    /// # Arguments
    /// * `graph`: the graph to analyze.
    /// * `cumul_outdeg`: the outdegree cumulative function of the graph.
    /// * `array_0`: a first array of counters.
    /// * `array_1`: A second array of counters of the same length and with the same logic of
    ///   `array_0`.
    pub fn new(graph: &'a G, cumul_outdeg: &'a D, array_0: A, array_1: A) -> Self {
        assert!(array_0.logic() == array_1.logic(), "Incompatible logics");
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
            arc_granularity: Self::DEFAULT_GRANULARITY,
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
        D: Succ<Input = usize, Output = usize>,
        L: MergeCounterLogic<Item = G1::Label>,
        A: CounterArrayMut<L>,
    > HyperBallBuilder<'a, G1, G2, D, L, A>
{
    const DEFAULT_GRANULARITY: usize = 16 * 1024;

    /// Creates a new builder with default parameters using also the transpose.
    ///
    /// * `graph`: the graph to analyze.
    /// * `transpose`: optionally, the transpose of `graph`. If [`None`], no
    ///   systolic iterations will be performed by the resulting [`HyperBall`].
    /// * `cumul_outdeg`: the outdegree cumulative function of the graph.
    /// * `array_0`: a first array of counters.
    /// * `array_1`: A second array of counters of the same length and with the same logic of
    ///   `array_0`.
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
            "array_0 should have have len {}. Got {}",
            graph.num_nodes(),
            array_0.len()
        );
        assert_eq!(
            graph.num_nodes(),
            array_1.len(),
            "array_1 should have have len {}. Got {}",
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
            "the transpose should have same number of nodes of the graph ({}). Got {}.",
            graph.num_arcs(),
            transpose.num_arcs()
        );
        /* TODOdebug_assert!(
            check_transposed(graph, transpose),
            "the transpose should be the transpose of the graph"
        );*/
        Self {
            graph,
            transpose: Some(transpose),
            cumul_outdegree: cumul_outdeg,
            do_sum_of_dists: false,
            do_sum_of_inv_dists: false,
            discount_functions: Vec::new(),
            arc_granularity: Self::DEFAULT_GRANULARITY,
            weights: None,
            array_0,
            array_1,
            _marker: std::marker::PhantomData,
        }
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
        self.arc_granularity =
            granularity.arc_granularity(self.graph.num_nodes(), Some(self.graph.num_arcs()));
        self
    }

    /// Sets optional weights for the nodes of the graph.
    ///
    /// # Arguments
    /// * `weights`: weights to use for the nodes. If [`None`], every node is
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
        discount_function: impl Fn(usize) -> f64 + Sync + 'a,
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
        D: Succ<Input = usize, Output = usize>,
        L: MergeCounterLogic<Item = G1::Label> + Sync + std::fmt::Display,
        A: CounterArrayMut<L>,
    > HyperBallBuilder<'a, G1, G2, D, L, A>
{
    /// Builds a [`HyperBall`] instance.
    ///
    /// # Arguments
    ///
    /// * `pl`: A progress logger.
    #[allow(clippy::type_complexity)]
    pub fn build(self, pl: &mut impl ConcurrentProgressLog) -> HyperBall<'a, G1, G2, D, L, A> {
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

        pl.debug(format_args!("Initializing counter_modified bitvec"));
        let counter_modified = AtomicBitVec::new(num_nodes);

        pl.debug(format_args!("Initializing modified_result_counter bitvec"));
        let modified_result_counter = AtomicBitVec::new(num_nodes);

        pl.debug(format_args!("Initializing must_be_checked bitvec"));
        let must_be_checked = AtomicBitVec::new(num_nodes);

        pl.debug(format_args!("Initializing next_must_be_checked bitvec"));
        let next_must_be_checked = AtomicBitVec::new(num_nodes);

        pl.info(format_args!(
            "Using counter logic: {}",
            self.array_0.logic()
        ));

        HyperBall {
            graph: self.graph,
            transposed: self.transpose,
            weight: self.weights,
            granularity: self.arc_granularity,
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
                arc_granularity: 0,
                node_cursor: AtomicUsize::new(0),
                arc_cursor: Mutex::new((0, 0)),
                visited_arcs: AtomicU64::new(0),
                modified_counters: AtomicU64::new(0),
                systolic: false,
                local: false,
                pre_local: false,
                local_checklist: Vec::new(),
                local_next_must_be_checked: Mutex::new(Vec::new()),
                must_be_checked,
                next_must_be_checked,
                curr_modified: counter_modified,
                next_modified: modified_result_counter,
                discount_functions: self.discount_functions,
            },
            _marker: std::marker::PhantomData,
        }
    }
}

/// Data used by [`parallel_task`](HyperBall::parallel_task).
///
/// These variables are used by the threads running
/// [`parallel_task`](HyperBall::parallel_task). They must be isolated in a
/// field because we need to be able to borrow exclusively
/// [`HyperBall::next_state`], while sharing references to the data contained
/// here and to the [`HyperBall::curr_state`].
struct IterationContext<'a, G1: SequentialLabeling, D> {
    /// The cumulative list of outdegrees.
    cumul_outdeg: &'a D,
    /// The number of the current iteration.
    iteration: usize,
    /// The value of the neighborhood function computed during the current iteration.
    current_nf: Mutex<f64>,
    /// The arc granularity: each task will try to process at least this number
    /// of arcs.
    arc_granularity: usize,
    /// A cursor scanning the nodes to process during local computations.
    node_cursor: AtomicUsize,
    /// A cursor scanning the nodes and arcs to process during non-local
    /// computations.
    arc_cursor: Mutex<(usize, usize)>,
    /// The number of arcs visited during the current iteration.
    visited_arcs: AtomicU64,
    /// The number of counters modified during the current iteration.
    modified_counters: AtomicU64,
    /// `true` if we started a systolic computation.
    systolic: bool,
    /// `true` if we started a local computation.
    local: bool,
    /// `true` if we are preparing a local computation (systolic is `true` and less than 1% nodes were modified).
    pre_local: bool,
    /// If [`local`](Self::local) is `true`, the sorted list of nodes that
    /// should be scanned.
    local_checklist: Vec<G1::Label>,
    /// If [`pre_local`](Self::pre_local) is `true`, the set of nodes that
    /// should be scanned on the next iteration.
    local_next_must_be_checked: Mutex<Vec<G1::Label>>,
    /// Used in systolic iterations to keep track of nodes to check.
    must_be_checked: AtomicBitVec,
    /// Used in systolic iterations to keep track of nodes to check in the next
    /// iteration.
    next_must_be_checked: AtomicBitVec,
    /// Whether each counter has been modified during the previous iteration.
    curr_modified: AtomicBitVec,
    /// Whether each counter has been modified during the current iteration.
    next_modified: AtomicBitVec,
    /// Custom discount functions whose sum should be computed.
    discount_functions: Vec<Box<dyn Fn(usize) -> f64 + Sync + 'a>>,
}

impl<G1: SequentialLabeling, D> IterationContext<'_, G1, D> {
    /// Resets the iteration context
    fn reset(&mut self, granularity: usize) {
        self.arc_granularity = granularity;
        self.node_cursor.store(0, Ordering::Relaxed);
        *self.arc_cursor.lock().unwrap() = (0, 0);
        self.visited_arcs.store(0, Ordering::Relaxed);
        self.modified_counters.store(0, Ordering::Relaxed);
    }
}

/// An algorithm that computes an approximation of the neighborhood function,
/// of the size of the reachable sets, and of (discounted) positive geometric
/// centralities of a graph.
pub struct HyperBall<
    'a,
    G1: RandomAccessGraph + Sync,
    G2: RandomAccessGraph + Sync,
    D: Succ<Input = usize, Output = usize>,
    L: MergeCounterLogic<Item = G1::Label> + Sync,
    A: CounterArrayMut<L>,
> {
    /// The graph to analyze.
    graph: &'a G1,
    /// The transpose of [`Self::graph`], if any.
    transposed: Option<&'a G2>,
    /// An optional slice of nonnegative node weights.
    weight: Option<&'a [usize]>,
    /// The base number of nodes per task. TODO.
    granularity: usize,
    /// The previous state.
    curr_state: A,
    /// The next state.
    next_state: A,
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
    sum_of_dists: Option<Vec<f64>>,
    /// The sum of inverse distances from each given node, if requested.
    sum_of_inv_dists: Option<Vec<f64>>,
    /// The overall discount centrality for every [`Self::discount_functions`].
    discounted_centralities: Vec<Vec<f64>>,
    /// Context used in a single iteration.
    iteration_context: IterationContext<'a, G1, D>,
    _marker: std::marker::PhantomData<L>,
}

impl<
        G1: RandomAccessGraph + Sync,
        G2: RandomAccessGraph + Sync,
        D: Succ<Input = usize, Output = usize> + Sync,
        L: MergeCounterLogic<Item = usize> + Sync,
        A: CounterArrayMut<L> + Sync + AsSyncArray<L>,
    > HyperBall<'_, G1, G2, D, L, A>
where
    L::Backend: PartialEq,
{
    /// Runs HyperBall.
    ///
    /// # Arguments
    ///
    /// * `upper_bound`: an upper bound to the number of iterations.
    ///
    /// * `threshold`: a value that will be used to stop the computation by
    ///   relative increment if the neighborhood function is being computed. If
    ///   [`None`] the computation will stop when no counters are modified.
    ///
    /// * `thread_pool`: The thread pool to use for parallel computation.
    ///
    /// * `pl`: A progress logger.
    pub fn run(
        &mut self,
        upper_bound: usize,
        threshold: Option<f64>,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        let upper_bound = std::cmp::min(upper_bound, self.graph.num_nodes());

        self.init(thread_pool, pl)
            .with_context(|| "Could not initialize approximator")?;

        pl.item_name("iteration");
        pl.expected_updates(None);
        pl.start(format!(
            "Running Hyperball for a maximum of {} iterations and a threshold of {:?}",
            upper_bound, threshold
        ));

        for i in 0..upper_bound {
            self.iterate(thread_pool, pl)
                .with_context(|| format!("Could not perform iteration {}", i + 1))?;

            pl.update_and_display();

            if self
                .iteration_context
                .modified_counters
                .load(Ordering::Relaxed)
                == 0
            {
                pl.info(format_args!(
                    "Terminating appoximation after {} iteration(s) by stabilisation",
                    i + 1
                ));
                break;
            }

            if let Some(t) = threshold {
                if i > 3 && self.relative_increment < (1.0 + t) {
                    pl.info(format_args!("Terminating approximation after {} iteration(s) by relative bound on the neighborhood function", i + 1));
                    break;
                }
            }
        }

        pl.done();

        Ok(())
    }

    /// Runs HyperBall until no counters are modified.
    ///
    /// # Arguments
    ///
    /// * `upper_bound`: an upper bound to the number of iterations.
    ///
    /// * `thread_pool`: The thread pool to use for parallel computation.
    ///
    /// * `pl`: A progress logger.
    #[inline(always)]
    pub fn run_until_stable(
        &mut self,
        upper_bound: usize,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        self.run(upper_bound, None, thread_pool, pl)
            .with_context(|| "Could not complete run_until_stable")
    }

    /// Runs HyperBall until no counters are modified with no upper bound on the
    /// number of iterations.
    ///
    /// # Arguments
    ///
    /// * `thread_pool`: The thread pool to use for parallel computation.
    ///
    /// * `pl`: A progress logger.
    #[inline(always)]
    pub fn run_until_done(
        &mut self,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        self.run_until_stable(usize::MAX, thread_pool, pl)
            .with_context(|| "Could not complete run_until_done")
    }

    #[inline(always)]
    fn ensure_iteration(&self) -> Result<()> {
        ensure!(
            self.iteration_context.iteration > 0,
            "HyperBall was not run. Please call HyperBall::run before accessing computed fields"
        );
        Ok(())
    }

    /// Returns the neighborhood function computed by this instance.
    pub fn neighborhood_function(&self) -> Result<Vec<f64>> {
        self.ensure_iteration()?;
        Ok(self.neighborhood_function.clone())
    }

    /// Returns the sum of distances computed by this instance if requested.
    pub fn sum_of_distances(&self) -> Result<&[f64]> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_dists {
            // TODO these are COPIES
            Ok(distances)
        } else {
            bail!("Sum of distances were not requested. Use builder.with_sum_of_distances(true) while building HyperBall to compute them")
        }
    }

    /// Returns the harmonic centralities (sum of inverse distances) computed by this instance if requested.
    pub fn harmonic_centralities(&self) -> Result<&[f64]> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_inv_dists {
            Ok(distances)
        } else {
            bail!("Sum of inverse distances were not requested. Use builder.with_sum_of_inverse_distances(true) while building HyperBall to compute them")
        }
    }

    /// Returns the discounted centralities of the specified index computed by this instance.
    ///
    /// # Arguments
    /// * `index`: the index of the requested discounted centrality.
    pub fn discounted_centrality(&self, index: usize) -> Result<&[f64]> {
        self.ensure_iteration()?;
        let d = self.discounted_centralities.get(index);
        if let Some(distances) = d {
            Ok(&distances)
        } else {
            bail!("Discount centrality of index {} does not exist", index)
        }
    }

    /// Computes and returns the closeness centralities from the sum of distances computed by this instance.
    pub fn closeness_centrality(&self) -> Result<Vec<f64>> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_dists {
            Ok(distances
                .iter()
                .map(|&d| if d == 0.0 { 0.0 } else { d.recip() })
                .collect())
        } else {
            bail!("Sum of distances were not requested. Use builder.with_sum_of_distances(true) while building HyperBall to compute closeness centrality.")
        }
    }

    /// Computes and returns the lin centralities from the sum of distances computed by this instance.
    ///
    /// Note that lin's index for isolated nodes is by (our) definition one (it's smaller than any other node).
    pub fn lin_centrality(&self) -> Result<Vec<f64>> {
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
                        let count = logic.count(self.curr_state.get_backend(node));
                        count * count / d
                    }
                })
                .collect())
        } else {
            bail!("Sum of distances were not requested. Use builder.with_sum_of_distances(true) while building HyperBall to compute lin centrality")
        }
    }

    /// Computes and returns the Nieminen centralities from the sum of distances computed by this instance.
    pub fn nieminen_centrality(&self) -> Result<Vec<f64>> {
        self.ensure_iteration()?;
        if let Some(distances) = &self.sum_of_dists {
            let logic = self.curr_state.logic();
            Ok(distances
                .iter()
                .enumerate()
                .map(|(node, &d)| {
                    let count = logic.count(self.curr_state.get_backend(node));
                    (count * count) - d
                })
                .collect())
        } else {
            bail!("Sum of distances were not requested. Use builder.with_sum_of_distances(true) while building HyperBall to compute lin centrality")
        }
    }

    /// Reads from the internal counter array and estimates the number of nodes
    /// reachable from the specified node.
    ///
    /// # Arguments
    /// * `node`: the index of the node to compute reachable nodes from.
    pub fn reachable_nodes_from(&self, node: usize) -> Result<f64> {
        self.ensure_iteration()?;
        Ok(self
            .curr_state
            .logic()
            .count(self.curr_state.get_backend(node)))
    }

    /// Reads from the internal counter array and estimates the number of nodes reachable
    /// from every node of the graph.
    ///
    /// `hyperball.reachable_nodes().unwrap()[i]` is equal to `hyperball.reachable_nodes_from(i).unwrap()`.
    pub fn reachable_nodes(&self) -> Result<Vec<f64>> {
        self.ensure_iteration()?;
        let logic = self.curr_state.logic();
        Ok((0..self.graph.num_nodes())
            .map(|n| logic.count(self.curr_state.get_backend(n)))
            .collect())
    }
}

impl<
        G1: RandomAccessGraph + Sync,
        G2: RandomAccessGraph + Sync,
        D: Succ<Input = usize, Output = usize> + Sync,
        L: CounterLogic<Item = usize> + MergeCounterLogic + Sync,
        A: CounterArrayMut<L> + Sync + AsSyncArray<L>,
    > HyperBall<'_, G1, G2, D, L, A>
where
    L::Backend: PartialEq,
{
    /// Performs a new iteration of HyperBall.
    ///
    /// # Arguments
    /// * `thread_pool`: The thread pool to use for parallel computation.
    /// * `pl`: A progress logger.
    fn iterate(
        &mut self,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        let ic = &mut self.iteration_context;

        pl.info(format_args!("Performing iteration {}", ic.iteration + 1));

        // Alias the number of modified counters, nodes and arcs
        let num_nodes = self.graph.num_nodes() as u64;
        let num_arcs = self.graph.num_arcs();
        let modified_counters = ic.modified_counters.load(Ordering::Relaxed);

        // Let us record whether the previous computation was systolic or local
        let prev_was_systolic = ic.systolic;
        let prev_was_local = ic.local;

        // If less than one fourth of the nodes have been modified, and we have
        // the transpose, it is time to pass to a systolic computation
        ic.systolic =
            self.transposed.is_some() && ic.iteration > 0 && modified_counters < num_nodes / 4;

        // Non-systolic computations add up the values of all counter
        //
        // Systolic computations modify the last value by compensating for each
        // modified counter
        *ic.current_nf.lock().unwrap() = if ic.systolic { self.last } else { 0.0 };

        // If we completed the last iteration in pre-local mode, we MUST run in
        // local mode
        ic.local = ic.pre_local;

        // We run in pre-local mode if we are systolic and few nodes where
        // modified.
        ic.pre_local = ic.systolic && modified_counters < (num_nodes * num_nodes) / (num_arcs * 10);

        if ic.systolic {
            pl.info(format_args!(
                "Starting systolic iteration (local: {}, pre_local: {})",
                ic.local, ic.pre_local
            ));
        } else {
            pl.info(format_args!("Starting standard iteration"));
        }

        pl.info(format_args!("Preparing modified_result_counter"));
        if prev_was_local {
            for &node in ic.local_checklist.iter() {
                ic.next_modified.set(node, false, Ordering::Relaxed);
            }
        } else {
            thread_pool.install(|| ic.next_modified.fill(false, Ordering::Relaxed));
        }

        if ic.local {
            pl.info(format_args!("Preparing local checklist"));
            // In case of a local computation, we convert the set of
            // must-be-checked for the next iteration into a check list
            thread_pool.join(
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
            pl.info(format_args!("Preparing systolic flags"));
            thread_pool.join(
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

        let mut granularity = ic.arc_granularity;
        let num_threads = thread_pool.current_num_threads();

        if num_threads > 1 && !ic.local {
            if ic.iteration > 0 {
                granularity = f64::min(
                    std::cmp::max(1, num_nodes as usize / num_threads) as _,
                    granularity as f64
                        * (num_nodes as f64 / std::cmp::max(1, modified_counters) as f64),
                ) as usize;
            }
            pl.info(format_args!(
                "Adaptive granularity for this iteration: {}",
                granularity
            ));
        }

        ic.reset(granularity);

        pl.item_name("arc");
        pl.expected_updates(if ic.local { None } else { Some(num_arcs as _) });
        pl.start("Starting parallel execution");
        {
            let next_state_sync = self.next_state.as_sync_array();
            let sum_of_dists = match &mut self.sum_of_dists {
                None => None,
                Some(x) => Some(x.as_sync_slice()),
            };

            let sum_of_inv_dists = match &mut self.sum_of_inv_dists {
                None => None,
                Some(x) => Some(x.as_sync_slice()),
            };
            let discounted_centralities = &self
                .discounted_centralities
                .iter_mut()
                .map(|s| s.as_sync_slice())
                .collect::<Vec<_>>();
            thread_pool.broadcast(|c| {
                Self::parallel_task(
                    self.graph,
                    self.transposed,
                    &self.curr_state,
                    &next_state_sync,
                    ic,
                    sum_of_dists,
                    sum_of_inv_dists,
                    discounted_centralities,
                    c,
                )
            });
        }

        pl.done_with_count(ic.visited_arcs.load(Ordering::Relaxed) as usize);
        let modified_counters = ic.modified_counters.load(Ordering::Relaxed);

        pl.info(format_args!(
            "Modified counters: {}/{} ({:.3}%)",
            modified_counters,
            self.graph.num_nodes(),
            (modified_counters as f64 / self.graph.num_nodes() as f64) * 100.0
        ));

        std::mem::swap(&mut self.curr_state, &mut self.next_state);
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

    /// The parallel operations to be performed each iteration.
    ///
    /// # Arguments:
    /// * `graph`: the graph to analyze.
    /// * `transpose`: optionally, the transpose of `graph`. If [`None`], no
    ///   systolic iterations will be performed.
    /// * `curr_state`: the current state of the counters.
    /// * `next_state`: the next state of the counters (to be computed).
    /// * `ic`: the iteration context.
    fn parallel_task(
        graph: &(impl RandomAccessGraph + Sync),
        transpose: Option<&(impl RandomAccessGraph + Sync)>,
        curr_state: &impl CounterArray<L>,
        next_state: &impl SyncCounterArray<L>,
        ic: &IterationContext<'_, G1, D>,
        sum_of_dists: Option<&[SyncCell<f64>]>,
        sum_of_inv_dists: Option<&[SyncCell<f64>]>,
        discounted_centralities: &[&[SyncCell<f64>]],
        _broadcast_context: rayon::BroadcastContext,
    ) {
        let node_granularity = ic.arc_granularity;
        let arc_granularity = ((graph.num_arcs() as f64 * node_granularity as f64)
            / graph.num_nodes() as f64)
            .ceil() as usize;
        let do_centrality = sum_of_dists.is_some()
            || sum_of_inv_dists.is_some()
            || !ic.discount_functions.is_empty();
        let node_upper_limit = if ic.local {
            ic.local_checklist.len()
        } else {
            graph.num_nodes()
        };
        let mut visited_arcs = 0;
        let mut modified_counters = 0;
        let arc_upper_limit = graph.num_arcs();

        // During standard iterations, cumulates the neighborhood function for the nodes scanned
        // by this thread. During systolic iterations, cumulates the *increase* of the
        // neighborhood function for the nodes scanned by this thread.
        let mut neighborhood_function_delta = KahanSum::new_with_value(0.0);
        let mut helper = curr_state.logic().new_helper();
        let logic = curr_state.logic();
        let mut next_counter = logic.new_counter();

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
                    let target = next_arc + arc_granularity;
                    if target as u64 >= arc_upper_limit {
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

            // Do work
            for i in start..end {
                let node = if ic.local { ic.local_checklist[i] } else { i };

                let prev_counter = curr_state.get_backend(node);

                // The three cases in which we enumerate successors:
                // 1) A non-systolic computation (we don't know anything, so we enumerate).
                // 2) A systolic, local computation (the node is by definition to be checked, as it comes from the local check list).
                // 3) A systolic, non-local computation in which the node should be checked.
                if !ic.systolic || ic.local || ic.must_be_checked[node] {
                    next_counter.set(prev_counter);
                    let mut modified = false;
                    for succ in graph.successors(node) {
                        if succ != node && ic.curr_modified[succ] {
                            visited_arcs += 1;
                            if !modified {
                                modified = true;
                            }
                            logic.merge_with_helper(
                                next_counter.as_mut(),
                                curr_state.get_backend(succ),
                                &mut helper,
                            );
                        }
                    }

                    let mut post = f64::NAN;
                    let counter_modified = modified && next_counter.as_ref() != prev_counter;

                    // We need the counter value only if the iteration is standard (as we're going to
                    // compute the neighborhood function cumulating actual values, and not deltas) or
                    // if the counter was actually modified (as we're going to cumulate the neighborhood
                    // function delta, or at least some centrality).
                    if !ic.systolic || counter_modified {
                        post = logic.count(next_counter.as_ref())
                    }
                    if !ic.systolic {
                        neighborhood_function_delta += post;
                    }

                    if counter_modified && (ic.systolic || do_centrality) {
                        let pre = logic.count(prev_counter);
                        if ic.systolic {
                            neighborhood_function_delta += -pre;
                            neighborhood_function_delta += post;
                        }

                        if do_centrality {
                            let delta = post - pre;
                            // Note that this code is executed only for distances > 0
                            if delta > 0.0 {
                                if let Some(distances) = sum_of_dists {
                                    let new_value = delta * (ic.iteration + 1) as f64;
                                    unsafe {
                                        distances[node].set(distances[node].get() + new_value)
                                    };
                                }
                                if let Some(distances) = sum_of_inv_dists {
                                    let new_value = delta / (ic.iteration + 1) as f64;
                                    unsafe {
                                        distances[node].set(distances[node].get() + new_value)
                                    };
                                }
                                for (func, distances) in ic
                                    .discount_functions
                                    .iter()
                                    .zip(discounted_centralities.iter())
                                {
                                    let new_value = delta * func(ic.iteration + 1);
                                    unsafe {
                                        distances[node].set(distances[node].get() + new_value)
                                    };
                                }
                            }
                        }
                    }

                    if counter_modified {
                        // We keep track of modified counters in the result. Note that we must
                        // add the current node to the must-be-checked set for the next
                        // local iteration if it is modified, as it might need a copy to
                        // the result array at the next iteration.
                        if ic.pre_local {
                            ic.local_next_must_be_checked.lock().unwrap().push(node);
                        }
                        ic.next_modified.set(node, true, Ordering::Relaxed);

                        if ic.systolic {
                            debug_assert!(transpose.is_some());
                            // In systolic computations we must keep track of which counters must
                            // be checked on the next iteration. If we are preparing a local computation,
                            // we do this explicitly, by adding the predecessors of the current
                            // node to a set. Otherwise, we do this implicitly, by setting the
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

                        modified_counters += 1;
                    }

                    unsafe {
                        next_state.set(node, next_counter.as_ref());
                    }
                } else {
                    // Even if we cannot possibly have changed our value, still our copy
                    // in the result vector might need to be updated because it does not
                    // reflect our current value.
                    if ic.curr_modified[node] {
                        unsafe {
                            next_state.set(node, prev_counter);
                        }
                    }
                }
            }
        }

        *ic.current_nf.lock().unwrap() += neighborhood_function_delta.sum();
        ic.visited_arcs.fetch_add(visited_arcs, Ordering::Relaxed);
        ic.modified_counters
            .fetch_add(modified_counters, Ordering::Relaxed);
    }

    /// Initializes HyperBall.
    fn init(
        &mut self,
        thread_pool: &ThreadPool,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<()> {
        pl.start("Initializing approximator");
        pl.info(format_args!("Clearing all registers"));

        self.curr_state.clear();
        self.next_state.clear();

        pl.info(format_args!("Initializing registers"));
        if let Some(w) = &self.weight {
            pl.info(format_args!("Loading weights"));
            for (i, &node_weight) in w.iter().enumerate() {
                let mut counter = self.curr_state.get_counter_mut(i);
                for _ in 0..node_weight {
                    counter.add(&(random::<u64>() as usize));
                }
            }
        } else {
            (0..self.graph.num_nodes()).for_each(|i| {
                self.curr_state.get_counter_mut(i).add(i);
            });
        }

        self.completed = false;

        let ic = &mut self.iteration_context;
        ic.iteration = 0;
        ic.systolic = false;
        ic.local = false;
        ic.pre_local = false;
        ic.reset(self.granularity);

        pl.debug(format_args!("Initializing distances"));
        if let Some(distances) = &mut self.sum_of_dists {
            distances.fill(0_f64);
        }
        if let Some(distances) = &mut self.sum_of_inv_dists {
            distances.fill(0_f64);
        }
        pl.info(format_args!("Initializing centralities"));
        for centralities in self.discounted_centralities.iter_mut() {
            centralities.fill(0.0);
        }

        self.last = self.graph.num_nodes() as f64;
        pl.info(format_args!("Initializing neighborhood function"));
        self.neighborhood_function.clear();
        self.neighborhood_function.push(self.last);

        pl.info(format_args!("Initializing modified counters"));
        thread_pool.install(|| ic.curr_modified.fill(true, Ordering::Relaxed));

        pl.done();

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::hash::{BuildHasherDefault, DefaultHasher};

    use super::*;
    use counter_array::traits::MergeCounter;
    use dsi_progress_logger::no_logging;
    use epserde::deser::{Deserialize, Flags};
    use webgraph::{
        prelude::{BvGraph, DCF},
        traits::SequentialLabeling,
    };

    struct SeqHyperBall<'a, G: RandomAccessGraph> {
        graph: &'a G,
        curr_state: SliceCounterArray<
            HyperLogLog<G::Label, BuildHasherDefault<DefaultHasher>, usize>,
            usize,
            Box<[usize]>,
        >,
        next_state: SliceCounterArray<
            HyperLogLog<G::Label, BuildHasherDefault<DefaultHasher>, usize>,
            usize,
            Box<[usize]>,
        >,
    }

    impl<G: RandomAccessGraph> SeqHyperBall<'_, G> {
        fn init(&mut self) {
            for i in 0..self.graph.num_nodes() {
                self.curr_state.get_counter_mut(i).add(i);
            }
        }

        fn iterate(&mut self) {
            for i in 0..self.graph.num_nodes() {
                let mut counter = self.next_state.get_counter_mut(i);
                counter.set(self.curr_state.get_backend(i));
                for succ in self.graph.successors(i) {
                    counter.merge(self.curr_state.get_backend(succ));
                }
            }
            std::mem::swap(&mut self.curr_state, &mut self.next_state);
        }
    }

    #[cfg_attr(feature = "slow_tests", test)]
    #[cfg_attr(not(feature = "slow_tests"), allow(dead_code))]
    fn test_cnr_2000() -> Result<()> {
        let basename = "../data/cnr-2000";

        let graph = BvGraph::with_basename(basename).load()?;
        let transpose = BvGraph::with_basename(basename.to_owned() + "-t").load()?;
        let cumulative = DCF::load_mmap(basename.to_owned() + ".dcf", Flags::empty())?;

        let num_nodes = graph.num_nodes();

        let hyper_log_log = HyperLogLogBuilder::new(num_nodes)
            .log_2_num_reg(6)
            .build()?;

        let seq_bits = SliceCounterArray::new(hyper_log_log.clone(), num_nodes);
        let seq_result_bits = SliceCounterArray::new(hyper_log_log.clone(), num_nodes);
        let par_bits = SliceCounterArray::new(hyper_log_log.clone(), num_nodes);
        let par_result_bits = SliceCounterArray::new(hyper_log_log.clone(), num_nodes);

        let mut hyperball = HyperBallBuilder::with_transpose(
            &graph,
            &transpose,
            cumulative.as_ref(),
            par_bits,
            par_result_bits,
        )
        .build(no_logging![]);
        let mut seq_hyperball = SeqHyperBall {
            curr_state: seq_bits,
            next_state: seq_result_bits,
            graph: &graph,
        };

        let mut modified_counters = num_nodes as u64;
        let threads = thread_pool![];
        hyperball.init(&threads, no_logging![])?;
        seq_hyperball.init();

        while modified_counters != 0 {
            hyperball.iterate(&threads, no_logging![])?;
            seq_hyperball.iterate();

            modified_counters = hyperball
                .iteration_context
                .modified_counters
                .load(Ordering::Relaxed);

            assert_eq!(
                hyperball.next_state.as_ref(),
                seq_hyperball.next_state.as_ref()
            );
            assert_eq!(
                hyperball.curr_state.as_ref(),
                seq_hyperball.curr_state.as_ref()
            );
        }

        Ok(())
    }
}
