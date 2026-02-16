/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use lender::*;
use webgraph::{
    graphs::vec_graph::{LabeledVecGraph, VecGraph},
    labels::{Left, Right, Zip},
    traits::{RandomAccessLabeling, SequentialLabeling, graph::UnitLabelGraph},
};

#[test]
fn test_left_projection() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 2), 30)]);
    let left = Left(g);
    assert_eq!(left.num_nodes(), 3);

    let mut iter = left.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1, 2]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![2]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), Vec::<usize>::new());
    Ok(())
}

#[test]
fn test_right_projection() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 2), 30)]);
    let right = Right(g);
    assert_eq!(right.num_nodes(), 3);

    let mut iter = right.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![10, 20]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![30]);

    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), Vec::<u32>::new());
    Ok(())
}

#[test]
fn test_left_random_access() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    let left = Left(g);
    assert_eq!(left.num_arcs(), 3);
    assert_eq!(left.outdegree(0), 2);
    assert_eq!(left.outdegree(1), 1);
    assert_eq!(
        RandomAccessLabeling::labels(&left, 0)
            .into_iter()
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        RandomAccessLabeling::labels(&left, 1)
            .into_iter()
            .collect::<Vec<_>>(),
        vec![0]
    );
    Ok(())
}

#[test]
fn test_right_random_access() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    let right = Right(g);
    assert_eq!(right.num_arcs(), 3);
    assert_eq!(right.outdegree(0), 2);
    assert_eq!(
        RandomAccessLabeling::labels(&right, 0)
            .into_iter()
            .collect::<Vec<_>>(),
        vec![10, 20]
    );
    Ok(())
}

#[test]
fn test_zip_labeling() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let z = Zip(g0, g1);
    assert_eq!(z.num_nodes(), 3);

    let mut iter = z.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    let s: Vec<_> = succ.collect();
    assert_eq!(s, vec![(1, 1), (2, 2)]);
    Ok(())
}

#[test]
fn test_zip_random_access() -> Result<()> {
    let g0 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let g1 = VecGraph::from_arcs([(0, 1), (0, 2), (1, 0)]);
    let z = Zip(g0, g1);
    assert_eq!(z.num_arcs(), 3);
    assert_eq!(z.outdegree(0), 2);
    let succs: Vec<_> = RandomAccessLabeling::labels(&z, 0).collect();
    assert_eq!(succs, vec![(1, 1), (2, 2)]);
    let succs: Vec<_> = RandomAccessLabeling::labels(&z, 1).collect();
    assert_eq!(succs, vec![(0, 0)]);
    Ok(())
}

#[test]
fn test_unit_label_graph() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let u = UnitLabelGraph(g);
    assert_eq!(u.num_nodes(), 3);
    assert_eq!(u.num_arcs(), 3);

    let succs: Vec<_> = RandomAccessLabeling::labels(&u, 0).collect();
    assert_eq!(succs, vec![(1, ()), (2, ())]);

    let mut iter = u.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(1, ()), (2, ())]);
    Ok(())
}

#[test]
fn test_unit_label_graph_iter_from() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 0)]);
    let lg = UnitLabelGraph(&g);
    // iter_from(1) starts from node 1
    let mut iter = lg.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(2, ())]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.collect::<Vec<_>>(), vec![(0, ())]);
    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn test_unit_label_graph_num_arcs() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let lg = UnitLabelGraph(&g);
    assert_eq!(RandomAccessLabeling::num_arcs(&lg), 3);
    assert_eq!(RandomAccessLabeling::outdegree(&lg, 0), 1);
    Ok(())
}

#[test]
fn test_left_projection_iter_from() -> Result<()> {
    let g =
        LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30), ((2, 1), 40)]);
    let left = Left(g);
    // iter_from(1) should start at node 1
    let mut iter = left.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![0]);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 2);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1]);
    assert!(iter.next().is_none());
    Ok(())
}

#[test]
fn test_right_projection_iter_from() -> Result<()> {
    let g =
        LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30), ((2, 1), 40)]);
    let right = Right(g);
    let mut iter = right.iter_from(1);
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 1);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![30]);
    Ok(())
}

#[test]
fn test_left_projection_num_arcs_outdegree() -> Result<()> {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((0, 2), 20), ((1, 0), 30)]);
    let left = Left(g);
    assert_eq!(RandomAccessLabeling::num_arcs(&left), 3);
    assert_eq!(RandomAccessLabeling::outdegree(&left, 0), 2);
    assert_eq!(RandomAccessLabeling::outdegree(&left, 1), 1);
    assert_eq!(RandomAccessLabeling::outdegree(&left, 2), 0);
    Ok(())
}

#[test]
fn test_zip_num_arcs_hint() {
    let g1 = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let g2 = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let z = Zip(&g1, &g2);
    // Zip does not override num_arcs_hint, so it returns None
    assert_eq!(z.num_arcs_hint(), None);
}

#[test]
fn test_left_num_arcs_hint() {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let left = Left(g);
    assert_eq!(left.num_arcs_hint(), Some(2));
}

#[test]
fn test_right_num_arcs_hint() {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let right = Right(g);
    assert_eq!(right.num_arcs_hint(), Some(2));
}

#[test]
fn test_assume_sorted_lender() -> Result<()> {
    use webgraph::traits::labels::AssumeSortedLender;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let lender = g.iter();
    // SAFETY: VecGraph lenders are already sorted
    let mut sorted = unsafe { AssumeSortedLender::new(lender) };
    let (node, succ) = sorted.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.collect::<Vec<_>>(), vec![1]);
    let (node, _succ) = sorted.next().unwrap();
    assert_eq!(node, 1);
    // Check size_hint
    let (min, max) = sorted.size_hint();
    assert_eq!(min, 1);
    assert_eq!(max, Some(1));
    Ok(())
}

#[test]
fn test_left_random_access_double_ended() -> Result<()> {
    use webgraph::labels::proj::LeftIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30)];
    let mut iter = LeftIntoIter(pairs.into_iter());
    assert_eq!(iter.next_back(), Some(3));
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next_back(), Some(2));
    assert_eq!(iter.next(), None);
    Ok(())
}

#[test]
fn test_right_random_access_double_ended() -> Result<()> {
    use webgraph::labels::proj::RightIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30)];
    let mut iter = RightIntoIter(pairs.into_iter());
    assert_eq!(iter.next_back(), Some(30));
    assert_eq!(iter.next(), Some(10));
    // nth_back(0) = next_back()
    assert_eq!(iter.nth_back(0), Some(20));
    assert_eq!(iter.next(), None);
    Ok(())
}

#[test]
fn test_left_exact_size_iterator() {
    use webgraph::labels::proj::LeftIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30)];
    let iter = LeftIntoIter(pairs.into_iter());
    assert_eq!(iter.len(), 3);
}

#[test]
fn test_right_exact_size_iterator() {
    use webgraph::labels::proj::RightIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20)];
    let iter = RightIntoIter(pairs.into_iter());
    assert_eq!(iter.len(), 2);
}

#[test]
fn test_left_nth_back() -> Result<()> {
    use webgraph::labels::proj::LeftIntoIter;
    let pairs: Vec<(usize, u32)> = vec![(1, 10), (2, 20), (3, 30), (4, 40)];
    let mut iter = LeftIntoIter(pairs.into_iter());
    // nth_back(1) skips 4, returns 3
    assert_eq!(iter.nth_back(1), Some(3));
    assert_eq!(iter.next_back(), Some(2));
    Ok(())
}

#[test]
fn test_left_size_hint() {
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let left = Left(g);
    let lender = left.iter();
    let (min, max) = lender.size_hint();
    assert_eq!(min, 2);
    assert_eq!(max, Some(2));
}

#[test]
fn test_left_iterator_is_empty() {
    use lender::ExactSizeLender;
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10)]);
    let left = Left(g);
    let mut lender = left.iter();
    assert!(!lender.is_empty());
    // LabeledVecGraph has 2 nodes (0 and 1), so the lender has 2 entries
    assert_eq!(lender.len(), 2);
    let _ = lender.next();
    assert_eq!(lender.len(), 1);
    let _ = lender.next();
    assert!(lender.is_empty());
    assert_eq!(lender.len(), 0);
}

#[test]
fn test_right_iterator_is_empty() {
    use lender::ExactSizeLender;
    let g = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
    let right = Right(g);
    let mut lender = right.iter();
    assert!(!lender.is_empty());
    assert_eq!(lender.len(), 2);
    let _ = lender.next();
    assert_eq!(lender.len(), 1);
    let _ = lender.next();
    assert!(lender.is_empty());
}

// ── Tests from test_coverage.rs ──

#[test]
fn test_left_projection_iter() -> Result<()> {
    use webgraph::labels::Left;
    let g = webgraph::graphs::vec_graph::LabeledVecGraph::<u32>::from_arcs([
        ((0, 1), 10),
        ((0, 2), 20),
        ((1, 0), 30),
    ]);
    let left = Left(g);
    let mut iter = left.iter();
    let (node, succ) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(succ.into_iter().collect::<Vec<_>>(), vec![1, 2]);
    Ok(())
}

#[test]
fn test_right_projection_iter() -> Result<()> {
    use webgraph::labels::Right;
    let g = webgraph::graphs::vec_graph::LabeledVecGraph::<u32>::from_arcs([
        ((0, 1), 10),
        ((0, 2), 20),
        ((1, 0), 30),
    ]);
    let right = Right(g);
    let mut iter = right.iter();
    let (node, labels) = iter.next().unwrap();
    assert_eq!(node, 0);
    assert_eq!(labels.into_iter().collect::<Vec<_>>(), vec![10, 20]);
    Ok(())
}

#[test]
fn test_split_iters_into_labeled_lenders() -> Result<()> {
    use webgraph::utils::SplitIters;

    let boundaries: Box<[usize]> = vec![0, 2, 4].into_boxed_slice();
    let iter1 = vec![((0_usize, 1_usize), ()), ((1, 0), ())];
    let iter2 = vec![((2_usize, 3_usize), ()), ((3, 2), ())];
    #[allow(clippy::type_complexity)]
    let iters: Box<[Vec<((usize, usize), ())>]> = vec![iter1, iter2].into_boxed_slice();

    let split = SplitIters::new(boundaries, iters);
    // Convert to Iter lenders via From impl for labeled pairs
    let lenders: Vec<webgraph::graphs::arc_list_graph::Iter<(), _>> = split.into();
    assert_eq!(lenders.len(), 2);
    Ok(())
}

#[test]
fn test_labeled_graph_zip() -> Result<()> {
    use webgraph::labels::Zip;
    let g = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2)]);
    let labels = webgraph::graphs::vec_graph::LabeledVecGraph::<u32>::from_arcs([
        ((0, 1), 10),
        ((1, 2), 20),
    ]);
    let zipped = Zip(&g, &labels);
    assert_eq!(zipped.num_nodes(), 3);
    let mut iter = zipped.iter();
    let (_node, succ) = iter.next().unwrap();
    let s: Vec<_> = succ.into_iter().collect();
    assert_eq!(s, vec![(1, (1, 10))]);
    Ok(())
}

#[test]
fn test_eq_sorted_labeled() -> Result<()> {
    use webgraph::traits::labels;
    let g1 = webgraph::graphs::vec_graph::LabeledVecGraph::<u32>::from_arcs([
        ((0, 1), 10),
        ((1, 2), 20),
    ]);
    let g2 = webgraph::graphs::vec_graph::LabeledVecGraph::<u32>::from_arcs([
        ((0, 1), 10),
        ((1, 2), 20),
    ]);
    labels::eq_sorted(&g1, &g2)?;
    Ok(())
}

#[test]
fn test_eq_sorted_labeled_mismatch() {
    use webgraph::traits::labels;
    let g1 = webgraph::graphs::vec_graph::LabeledVecGraph::<u32>::from_arcs([
        ((0, 1), 10),
        ((1, 2), 20),
    ]);
    let g2 = webgraph::graphs::vec_graph::LabeledVecGraph::<u32>::from_arcs([
        ((0, 1), 10),
        ((1, 2), 30), // Different label
    ]);
    assert!(labels::eq_sorted(&g1, &g2).is_err());
}
