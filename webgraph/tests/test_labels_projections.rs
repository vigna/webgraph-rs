/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod common;

use anyhow::Result;
use common::vec_graph;
use lender::*;
use webgraph::{
    graphs::vec_graph::{LabeledVecGraph, VecGraph},
    labels::{Left, LeftSucc, Right, RightSucc, Zip},
    traits::{
        RandomAccessGraph, RandomAccessLabeling, SequentialLabeling,
        graph::{self, UnitLabelGraph},
        labels,
    },
};

/// A labeled graph whose labels are 10 times the successors plus the source.
fn labeled_graph() -> LabeledVecGraph<u32> {
    LabeledVecGraph::from_arcs(
        [(0, 1), (0, 2), (1, 0), (2, 1)].map(|(s, d)| ((s, d), (10 * d + s) as u32)),
    )
}

/// Returns the labels of each node of a random-access labeling.
fn all_labels<L: RandomAccessLabeling>(l: &L) -> Vec<Vec<L::Label>> {
    (0..l.num_nodes())
        .map(|node| l.labels(node).into_iter().collect())
        .collect()
}

#[test]
fn test_left_projection() -> Result<()> {
    let left = Left(labeled_graph());
    labels::eq_sorted(&left, &vec_graph(3, [(0, 1), (0, 2), (1, 0), (2, 1)]))?;
    labels::check_impl(&left)?;
    assert_eq!(left.get_num_arcs(), Some(4));
    assert_eq!(left.num_arcs(), 4);
    assert_eq!(
        (0..3).map(|node| left.outdegree(node)).collect::<Vec<_>>(),
        [2, 1, 1]
    );
    graph::eq(
        &VecGraph::from_lender(left.iter_from(1)),
        &vec_graph(3, [(1, 0), (2, 1)]),
    )?;
    Ok(())
}

#[test]
fn test_right_projection() -> Result<()> {
    let right = Right(labeled_graph());
    labels::check_impl(&right)?;
    assert_eq!(all_labels(&right), [vec![10, 20], vec![1], vec![12]]);
    assert_eq!(right.get_num_arcs(), Some(4));
    assert_eq!(right.num_arcs(), 4);
    let mut iter = right.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!((node, succ.into_iter().collect::<Vec<_>>()), (1, vec![1]));
    Ok(())
}

#[test]
fn test_projection_lenders() {
    for len in [2, 3] {
        let mut graph = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
        if len == 3 {
            graph.add_node(2);
        }
        let left = Left(&graph);
        let right = Right(&graph);
        let mut left = left.iter();
        let mut right = right.iter();
        for remaining in (0..=len).rev() {
            assert_eq!(left.size_hint(), (remaining, Some(remaining)));
            assert_eq!(ExactSizeLender::len(&right), remaining);
            assert_eq!(ExactSizeLender::is_empty(&left), remaining == 0);
            assert_eq!(left.next().is_some(), remaining > 0);
            assert_eq!(right.next().is_some(), remaining > 0);
        }
    }
}

#[test]
fn test_projection_successors() {
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30), (4, 40)];

    let mut left = LeftSucc(pairs.clone().into_iter());
    assert_eq!(left.len(), 4);
    assert_eq!(left.next_back(), Some(4));
    assert_eq!(left.next(), Some(1));
    // nth_back(1) skips 3
    assert_eq!(left.nth_back(1), Some(2));
    assert_eq!(left.next(), None);

    let mut right = RightSucc(pairs.into_iter());
    assert_eq!(right.len(), 4);
    assert_eq!(right.next_back(), Some(40));
    assert_eq!(right.next(), Some(10));
    // nth_back(0) is next_back()
    assert_eq!(right.nth_back(0), Some(30));
    assert_eq!(right.next(), Some(20));
    assert_eq!(right.next(), None);
}

#[test]
fn test_projection_successor_size_hint() {
    // Regression: LeftSucc/RightSucc implemented ExactSizeIterator::len but
    // inherited the default (0, None) size_hint, violating the
    // ExactSizeIterator contract.
    let mut left = LeftSucc(vec![(1usize, 10usize), (2, 20)].into_iter());
    assert_eq!(left.size_hint(), (2, Some(2)));
    assert_eq!(left.next(), Some(1));
    assert_eq!(left.size_hint(), (1, Some(1)));

    let mut right = RightSucc(vec![(1usize, 10usize), (2, 20)].into_iter());
    assert_eq!(right.size_hint(), (2, Some(2)));
    assert_eq!(right.next(), Some(10));
    assert_eq!(right.size_hint(), (1, Some(1)));
}

#[test]
fn test_zip() -> Result<()> {
    let g = common::test_graph();
    let labeled = LabeledVecGraph::<u32>::from_arcs(
        common::TEST_GRAPH_ARCS.map(|(s, d)| ((s, d), (10 * s + d) as u32)),
    );
    let z = Zip(&g, &labeled);
    // Zip forwards (and cross-checks) the hints of its components
    assert_eq!(z.get_num_arcs(), Some(11));
    assert_eq!(z.num_arcs(), 11);
    labels::check_impl(&z)?;
    let expected: Vec<Vec<_>> = (0..g.num_nodes())
        .map(|node| {
            g.successors(node)
                .map(|succ| (succ, (succ, (10 * node + succ) as u32)))
                .collect()
        })
        .collect();
    assert_eq!(all_labels(&z), expected);
    // Regression: Zip did not forward get_num_arcs, so build_dcf and
    // arc-based granularity failed even though the count was known.
    assert_eq!(z.build_dcf().len(), g.num_nodes() + 1);
    Ok(())
}

#[test]
#[should_panic(expected = "different number of labels")]
fn test_zip_mismatched_label_counts_panic() {
    // Regression: mismatched per-node label counts were silently truncated
    // to the shorter side in release builds.
    let g = VecGraph::from_arcs([(0, 1), (0, 2)]);
    let mut l = LabeledVecGraph::<u32>::empty(3);
    l.add_arc(0, 1, 10);
    let z = Zip(&g, &l);
    let mut iter = z.iter();
    while let Some((_node, labels)) = iter.next() {
        let _ = labels.count();
    }
}

#[test]
fn test_unit_label_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let u = UnitLabelGraph(&g);
    labels::check_impl(&u)?;
    assert_eq!(u.num_arcs(), 4);
    assert_eq!(u.outdegree(0), 2);
    assert_eq!(
        all_labels(&u),
        [vec![(1, ()), (2, ())], vec![(2, ())], vec![(0, ())]]
    );
    let mut iter = u.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!((node, succ.collect::<Vec<_>>()), (1, vec![(2, ())]));
    Ok(())
}

#[test]
fn test_assume_sorted_lender() {
    use webgraph::traits::labels::AssumeSortedLender;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    // SAFETY: VecGraph lenders are already sorted
    let mut sorted = unsafe { AssumeSortedLender::new(g.iter()) };
    let (node, succ) = sorted.next().unwrap();
    assert_eq!((node, succ.collect::<Vec<_>>()), (0, vec![1]));
    sorted.next();
    assert_eq!(sorted.size_hint(), (1, Some(1)));
}

#[test]
fn test_split_iters_into_sorted_labeled_graph() -> Result<()> {
    use webgraph::graphs::par_sorted_graph::ParSortedLabeledGraph;
    use webgraph::utils::SplitIters;

    let iter1 = vec![((0_usize, 1_usize), ()), ((1, 0), ())];
    let iter2 = vec![((2_usize, 3_usize), ()), ((3, 2), ())];
    let sorted: ParSortedLabeledGraph<_> = SplitIters::new(
        vec![0, 2, 4].into_boxed_slice(),
        vec![iter1.into_iter(), iter2.into_iter()].into_boxed_slice(),
    )
    .into();
    assert_eq!(sorted.num_nodes(), 4);
    let mut arcs = vec![];
    for_!((node, succ) in sorted.iter() {
        arcs.extend(succ.into_iter().map(|(s, _label)| (node, s)));
    });
    assert_eq!(arcs, [(0, 1), (1, 0), (2, 3), (3, 2)]);
    Ok(())
}

#[test]
fn test_eq_sorted_labeled() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 20)]);
    labels::eq_sorted(&g, &g.clone())?;
    // Different label
    let other = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 2), 30)]);
    assert!(labels::eq_sorted(&g, &other).is_err());
    Ok(())
}
