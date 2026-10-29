/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Tests for merging the labels of repeated arcs while sorting.

#![cfg(not(miri))]

mod common;

use anyhow::Result;
use dsi_bitstream::prelude::*;
use lender::for_;
use rayon::prelude::*;
use webgraph::graphs::par_sorted_graph::{ParSortedLabeledGraphConf, SortedLabeledIter};
use webgraph::graphs::vec_graph::LabeledVecGraph;
use webgraph::labels::bitstream::BitStreamStoreLabelsConf;
use webgraph::prelude::*;
use webgraph::traits::{AddLabel, CountRepeats, FixedWidth, KeepOne, MergeLabels, VecOf};
use webgraph::utils::{MemoryUsage, WithMerge};

/// Multiplicities of the arcs of `common::TEST_GRAPH_ARCS`, in order: the
/// first, a middle, and the last successor of a node are repeated, and node 3
/// has a single successor repeated three times.
const MULTIPLICITIES: [u64; 11] = [2, 1, 1, 3, 2, 1, 3, 1, 1, 2, 1];

/// Returns the copies of the arcs of the test graph, each repeated as many
/// times as its multiplicity: first one copy of each arc, in order, and then
/// the remaining copies of each arc, adjacent, with arcs in reverse order.
/// The `k`-th copy of `arc` has label `label(arc, k)`.
fn pairs(label: impl Fn((usize, usize), u64) -> u32) -> Vec<((usize, usize), u32)> {
    let arcs = common::TEST_GRAPH_ARCS.into_iter().zip(MULTIPLICITIES);
    let mut pairs: Vec<_> = arcs.clone().map(|(arc, _)| (arc, label(arc, 0))).collect();
    for (arc, m) in arcs.rev() {
        pairs.extend((1..m).map(|k| (arc, label(arc, k))));
    }
    pairs
}

/// The same label for all copies of an arc.
fn arc_label((src, dst): (usize, usize), _k: u64) -> u32 {
    (10 * src + dst) as u32
}

/// A different label for each copy of an arc.
fn copy_label((src, dst): (usize, usize), k: u64) -> u32 {
    (100 * src + 10 * dst) as u32 + k as u32
}

/// Returns the labeled arcs of `graph`, in the order in which they are
/// returned.
fn arcs<L>(graph: &impl SequentialLabeling<Label = (usize, L)>) -> Vec<((usize, usize), L)> {
    let mut arcs = vec![];
    for_!((src, succ) in graph.iter() {
        arcs.extend(succ.into_iter().map(|(dst, label)| ((src, dst), label)));
    });
    arcs
}

/// A configuration with three lenders whose sorter, using `num_threads`
/// threads, has buffers of four pairs: each partition is sorted in several
/// batches, and the repeated copies of some arcs end up in the same batch.
fn conf(num_threads: usize) -> ParSortedLabeledGraphConf {
    ParSortedLabeledGraph::config()
        .num_lenders(3)
        .memory_usage(MemoryUsage::BatchSize(4 * 3 * num_threads))
}

/// Runs `f` in a pool of two threads, so that the batches of parallel sorts
/// do not depend on the number of cores.
fn with_two_threads<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap()
        .install(f)
}

/// A sorted graph whose labels count the occurrences of each arc.
type Counted = ParSortedLabeledGraph<SortedLabeledIter<FixedWidth<u64>, WithMerge<CountRepeats>>>;

/// Returns the arcs of the test graph, each labeled by its multiplicity.
fn counted_arcs() -> Vec<((usize, usize), u64)> {
    common::TEST_GRAPH_ARCS
        .into_iter()
        .zip(MULTIPLICITIES)
        .collect()
}

/// Checks that `graph` is the test graph, with `num_nodes` nodes, labeled
/// with multiplicities, both as it is and after compressing it and loading
/// it back.
fn check_counted(num_nodes: usize, graph: Counted) -> Result<()> {
    assert_eq!(graph.num_nodes(), num_nodes);
    assert_eq!(arcs(&graph), counted_arcs());
    check_compressed_counted(num_nodes, &graph)
}

/// Checks that compressing `graph` and loading it back yields the test
/// graph, with `num_nodes` nodes, labeled with multiplicities.
fn check_compressed_counted<G>(num_nodes: usize, graph: G) -> Result<()>
where
    G: for<'a> IntoParLenders<ParLender: NodeLabelsLender<'a, Label = (usize, u64)>>,
{
    let (_dir, basename) = common::tmp_basename()?;
    BvComp::with_basename(&basename).par_comp_labeled::<BE, _, _>(
        graph,
        BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u64>::new()),
    )?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;
    let labeling = BitStreamLabelingSeq::<BE, _, _>::load(
        BvCompConf::default_labels_basename(&basename),
        FixedWidth::<u64>::new(),
    )?;
    let mut expected_graph = LabeledVecGraph::empty(num_nodes);
    expected_graph.add_arcs(counted_arcs());
    graph::eq_labeled(&expected_graph, &Zip(seq, labeling))?;
    Ok(())
}

#[test]
fn test_count_repeats_sort_pairs() -> Result<()> {
    check_counted(
        8,
        conf(1).merge_labels(CountRepeats).sort_pairs(
            8,
            FixedWidth::<u64>::new(),
            pairs(arc_label),
        )?,
    )
}

#[test]
fn test_count_repeats_par_sort_pair_iters() -> Result<()> {
    let pairs = pairs(arc_label);
    // Contiguous halves: the second contains the repeated copies, adjacent
    let (first, second) = pairs.split_at(pairs.len() / 2);
    check_counted(
        8,
        with_two_threads(|| {
            conf(2).merge_labels(CountRepeats).par_sort_pair_iters(
                8,
                FixedWidth::<u64>::new(),
                [first, second].map(|half| half.iter().copied()),
            )
        })?,
    )
}

#[test]
fn test_count_repeats_par_sort_pairs() -> Result<()> {
    check_counted(
        8,
        with_two_threads(|| {
            conf(2).merge_labels(CountRepeats).par_sort_pairs(
                8,
                FixedWidth::<u64>::new(),
                pairs(arc_label).into_par_iter(),
            )
        })?,
    )
}

#[test]
fn test_count_repeats_empty_partition() -> Result<()> {
    // Nodes 8 to 11 have no arcs, so the last partition gets no arcs
    check_counted(
        12,
        conf(1).merge_labels(CountRepeats).sort_pairs(
            12,
            FixedWidth::<u64>::new(),
            pairs(arc_label),
        )?,
    )
}

/// The number of occurrences of an arc; it does not implement [`Clone`].
#[derive(Debug, Default)]
struct Occurrences(u64);

/// Serializes and deserializes [`Occurrences`] using 64 bits.
#[derive(Debug, Clone, Copy)]
struct OccurrencesSD;

impl<E: Endianness, BW: BitWrite<E>> BitSerializer<E, BW> for OccurrencesSD {
    type SerType = Occurrences;

    fn serialize(&self, value: &Occurrences, bitstream: &mut BW) -> Result<usize, BW::Error> {
        bitstream.write_bits(value.0, 64)
    }

    fn name(&self) -> String {
        "OccurrencesSD".to_string()
    }
}

impl<E: Endianness, BR: BitRead<E>> BitDeserializer<E, BR> for OccurrencesSD {
    type DeserType = Occurrences;

    fn deserialize(&self, bitstream: &mut BR) -> Result<Occurrences, BR::Error> {
        Ok(Occurrences(bitstream.read_bits(64)?))
    }

    fn name(&self) -> String {
        "OccurrencesSD".to_string()
    }
}

/// Counts the occurrences of each arc, like [`CountRepeats`], but using
/// states that do not implement [`Clone`].
#[derive(Debug, Clone, Copy)]
struct CountOccurrences;

impl MergeLabels for CountOccurrences {
    type State = Occurrences;
    type Output = u64;

    fn merge(&self, state: &mut Occurrences, other: Occurrences) {
        state.0 += other.0;
    }

    fn finish(&self, state: Occurrences) -> u64 {
        state.0
    }
}

impl AddLabel<u32> for CountOccurrences {
    fn add(&self, state: &mut Occurrences, _label: u32) {
        state.0 += 1;
    }
}

#[test]
fn test_count_non_clone_states() -> Result<()> {
    let graph =
        conf(1)
            .merge_labels(CountOccurrences)
            .sort_pairs(8, OccurrencesSD, pairs(arc_label))?;
    // States are not Clone, so the graph can only be consumed by value
    check_compressed_counted(8, graph)
}

/// Collects the labels of each arc into a sorted boxed slice.
#[derive(Debug, Clone, Copy)]
struct Names;

impl MergeLabels for Names {
    type State = Vec<u32>;
    type Output = Box<[u32]>;

    fn init(&self, state: &mut Vec<u32>) {
        state.clear();
    }

    fn merge(&self, state: &mut Vec<u32>, mut other: Vec<u32>) {
        state.append(&mut other);
    }

    fn finish(&self, mut state: Vec<u32>) -> Box<[u32]> {
        state.sort_unstable();
        state.into_boxed_slice()
    }
}

impl AddLabel<u32> for Names {
    fn add(&self, state: &mut Vec<u32>, label: u32) {
        state.push(label);
    }
}

#[test]
fn test_collect_names() -> Result<()> {
    let graph = conf(1).merge_labels(Names).sort_pairs(
        8,
        VecOf(FixedWidth::<u32>::new()),
        pairs(copy_label),
    )?;
    let expected: Vec<(_, Box<[u32]>)> = common::TEST_GRAPH_ARCS
        .into_iter()
        .zip(MULTIPLICITIES)
        .map(|(arc, m)| (arc, (0..m).map(|k| copy_label(arc, k)).collect()))
        .collect();
    // Twice, as iterating clones the iterators and their states
    assert_eq!(arcs(&graph), expected);
    assert_eq!(arcs(&graph), expected);
    Ok(())
}

#[test]
fn test_no_merge_keeps_repeats() -> Result<()> {
    let mut expected = pairs(copy_label);
    let graph = conf(1).sort_pairs(8, FixedWidth::<u32>::new(), expected.clone())?;
    let mut sorted = arcs(&graph);
    // The order of repeated arcs is unspecified
    sorted.sort_unstable();
    expected.sort_unstable();
    assert_eq!(sorted, expected);
    Ok(())
}

/// Returns the arcs of the test graph, each labeled by [`arc_label`].
fn arcs_once() -> Vec<((usize, usize), u32)> {
    common::TEST_GRAPH_ARCS
        .into_iter()
        .map(|arc| (arc, arc_label(arc, 0)))
        .collect()
}

#[test]
fn test_dedup_keeps_one() -> Result<()> {
    let graph = conf(1)
        .dedup()
        .sort_pairs(8, FixedWidth::<u32>::new(), pairs(arc_label))?;
    assert_eq!(arcs(&graph), arcs_once());
    Ok(())
}

#[test]
fn test_keep_one() -> Result<()> {
    let graph = conf(1).merge_labels(KeepOne::new()).sort_pairs(
        8,
        FixedWidth::<u32>::new(),
        pairs(arc_label),
    )?;
    assert_eq!(arcs(&graph), arcs_once());
    Ok(())
}
