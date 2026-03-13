/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_bitstream::prelude::BE;
use lender::*;
use webgraph::{graphs::vec_graph::VecGraph, prelude::*, transform};

// ── transpose ──

#[test]
fn test_transpose() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let t = transform::transpose(&g, MemoryUsage::default())?;
    let t = VecGraph::from_lender(&t);
    assert_eq!(t.num_nodes(), 3);

    let s0: Vec<_> = t.successors(0).collect();
    assert!(s0.is_empty());

    let s1: Vec<_> = t.successors(1).collect();
    assert_eq!(s1, vec![0]);

    let s2: Vec<_> = t.successors(2).collect();
    assert_eq!(s2, vec![0, 1]);
    Ok(())
}

#[test]
fn test_transpose_round_trip() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 3), (3, 0)]);
    let t = transform::transpose(&g, MemoryUsage::BatchSize(2))?;
    let t = VecGraph::from_lender(&t);
    let tt = transform::transpose(&t, MemoryUsage::BatchSize(2))?;
    let tt = VecGraph::from_lender(&tt);
    webgraph::traits::graph::eq(&g, &tt)?;
    Ok(())
}

#[test]
fn test_transpose_split() -> Result<()> {
    use webgraph::transform::transpose_split;

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    let split = transpose_split(&seq, MemoryUsage::BatchSize(2), None)?;
    assert_eq!(split.boundaries[0], 0);
    assert_eq!(*split.boundaries.last().unwrap(), 3);
    // Verify transposed content: (0,1),(1,2),(2,0) transposed is (2,0),(0,1),(1,2)
    let lenders: Vec<_> = split.into();
    let mut arcs = vec![];
    for lender in lenders {
        for_!((node, succs) in lender {
            for succ in succs {
                arcs.push((node, succ));
            }
        });
    }
    arcs.sort();
    assert_eq!(arcs, vec![(0, 2), (1, 0), (2, 1)]);
    Ok(())
}

#[test]
fn test_transpose_split_bvgraph() -> Result<()> {
    use webgraph::traits::SequentialLabeling;
    let basename = std::path::Path::new("../data/cnr-2000");
    let graph = BvGraph::with_basename(basename).load()?;
    let num_nodes = graph.num_nodes();

    let split = transform::transpose_split(&graph, MemoryUsage::BatchSize(100_000), None)?;

    assert_eq!(*split.boundaries.first().unwrap(), 0);
    assert_eq!(*split.boundaries.last().unwrap(), num_nodes);

    let lenders: Vec<_> = split.into();
    assert!(!lenders.is_empty());

    let mut total_arcs = 0u64;
    for lender in lenders {
        for_!((_node, succs) in lender {
            for _succ in succs {
                total_arcs += 1;
            }
        });
    }
    // Transpose should have same number of arcs
    assert_eq!(total_arcs, graph.num_arcs());
    Ok(())
}

#[test]
fn test_transpose_labeled() -> Result<()> {
    use webgraph::transform::transpose_labeled;
    use webgraph::utils::DefaultBatchCodec;

    let g = webgraph::graphs::vec_graph::LabeledVecGraph::<()>::from_arcs([
        ((0, 1), ()),
        ((0, 2), ()),
        ((1, 2), ()),
    ]);
    let t = transpose_labeled(
        &g,
        MemoryUsage::BatchSize(2),
        <DefaultBatchCodec>::default(),
    )?;
    assert_eq!(t.num_nodes(), 3);
    let mut iter = t.iter();
    while let Some((node, succ)) = iter.next() {
        let mut succs: Vec<_> = succ.into_iter().map(|(n, _)| n).collect();
        succs.sort();
        match node {
            0 => assert!(succs.is_empty()),
            1 => assert_eq!(succs, vec![0]),    // transposed 0->1
            2 => assert_eq!(succs, vec![0, 1]), // transposed 0->2, 1->2
            _ => unreachable!("unexpected node {}", node),
        }
    }
    Ok(())
}

// ── permute ──

#[test]
fn test_permute() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let perm = [2, 0, 1]; // 0->2, 1->0, 2->1
    let p = transform::permute(&g, &perm, MemoryUsage::default())?;
    let p = VecGraph::from_lender(&p);
    assert_eq!(p.num_nodes(), 3);

    // node 0 maps to 2, arcs (0,1) -> (2,0), (0,2) -> (2,1)
    assert_eq!(p.successors(2).collect::<Vec<_>>(), vec![0, 1]);
    // node 1 maps to 0, arc (1,2) -> (0,1)
    assert_eq!(p.successors(0).collect::<Vec<_>>(), vec![1]);
    // node 2 maps to 1, no outgoing arcs
    assert_eq!(p.successors(1).collect::<Vec<_>>(), Vec::<usize>::new());
    Ok(())
}

#[test]
fn test_permute_identity() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let id = [0, 1, 2];
    let p = transform::permute(&g, &id, MemoryUsage::BatchSize(2))?;
    let p = VecGraph::from_lender(&p);
    webgraph::traits::graph::eq(&g, &p)?;
    Ok(())
}

#[test]
fn test_permute_reverse() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let perm = [3, 2, 1, 0]; // reverse
    let p = transform::permute(&g, &perm, MemoryUsage::BatchSize(2))?;
    let p = VecGraph::from_lender(&p);
    assert_eq!(p.num_nodes(), 4);
    // Arc (0,1) -> (3,2), (1,2) -> (2,1), (2,3) -> (1,0)
    assert_eq!(p.successors(0).collect::<Vec<_>>(), Vec::<usize>::new());
    assert_eq!(p.successors(1).collect::<Vec<_>>(), vec![0]);
    assert_eq!(p.successors(2).collect::<Vec<_>>(), vec![1]);
    assert_eq!(p.successors(3).collect::<Vec<_>>(), vec![2]);
    Ok(())
}

#[test]
fn test_permute_size_mismatch() {
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let perm = vec![0, 1]; // 2 elements but graph has 3 nodes
    let result = transform::permute(&g, &perm, MemoryUsage::BatchSize(2));
    assert!(result.is_err());
}

#[test]
fn test_permute_split() -> Result<()> {
    use webgraph::transform::permute_split;

    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    let perm = vec![2, 0, 1];
    let p = permute_split(&seq, &perm, MemoryUsage::BatchSize(2), None)?;
    // Collect all arcs via From conversion
    let lenders: Vec<_> = p.into();
    let mut arcs = vec![];
    for lender in lenders {
        for_!((node, succs) in lender {
            for succ in succs {
                arcs.push((node, succ));
            }
        });
    }
    arcs.sort();
    // Original (0,1) -> (2,0), (0,2) -> (2,1), (1,2) -> (0,1)
    assert_eq!(arcs, vec![(0, 1), (2, 0), (2, 1)]);
    Ok(())
}

// ── symmetrize ──

#[test]
fn test_symmetrize_no_loops() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (0, 0)]); // includes self-loop
    let s = transform::symmetrize::<true>(&g, MemoryUsage::default())?;
    let s = VecGraph::from_lender(&s);
    assert_eq!(s.num_nodes(), 3);

    // Self-loop should be removed, all edges bidirectional
    let mut s0: Vec<_> = s.successors(0).collect();
    s0.sort();
    assert_eq!(s0, vec![1]);
    let mut s1: Vec<_> = s.successors(1).collect();
    s1.sort();
    assert_eq!(s1, vec![0, 2]);
    let mut s2: Vec<_> = s.successors(2).collect();
    s2.sort();
    assert_eq!(s2, vec![1]);
    Ok(())
}

#[test]
fn test_symmetrize_keep_loops() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (0, 0)]); // includes self-loop
    let s = transform::symmetrize::<false>(&g, MemoryUsage::default())?;
    let s = VecGraph::from_lender(&s);
    assert_eq!(s.num_nodes(), 3);

    // Self-loop should be kept, all edges bidirectional
    let mut s0: Vec<_> = s.successors(0).collect();
    s0.sort();
    assert_eq!(s0, vec![0, 1]);
    let mut s1: Vec<_> = s.successors(1).collect();
    s1.sort();
    assert_eq!(s1, vec![0, 2]);
    let mut s2: Vec<_> = s.successors(2).collect();
    s2.sort();
    assert_eq!(s2, vec![1]);
    Ok(())
}

#[test]
fn test_symmetrize_with_batch_size() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 0)]);
    let s = transform::symmetrize::<true>(&g, MemoryUsage::BatchSize(2))?;
    let s = VecGraph::from_lender(&s);
    assert_eq!(s.num_nodes(), 4);
    // Each node in a 4-cycle has exactly 2 neighbors after symmetrization
    assert_eq!(s.successors(0).collect::<Vec<_>>(), vec![1, 3]);
    assert_eq!(s.successors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(s.successors(2).collect::<Vec<_>>(), vec![1, 3]);
    assert_eq!(s.successors(3).collect::<Vec<_>>(), vec![0, 2]);
    Ok(())
}

#[test]
fn test_symmetrize_sorted() -> Result<()> {
    use webgraph::transform::symmetrize_sorted;

    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    let s = symmetrize_sorted::<true, _>(&seq, MemoryUsage::BatchSize(2))?;
    let s = VecGraph::from_lender(s.iter());
    // Every edge becomes bidirectional, no self-loops
    assert_eq!(s.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(s.successors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(s.successors(2).collect::<Vec<_>>(), vec![0, 1]);
    Ok(())
}

#[test]
fn test_symmetrize_sorted_split() -> Result<()> {
    use webgraph::transform::symmetrize_sorted_split;

    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    let s = symmetrize_sorted_split::<true, _>(&seq, MemoryUsage::BatchSize(2), None)?;
    let lenders: Vec<_> = s.into();
    let mut arcs = vec![];
    for lender in lenders {
        for_!((node, succs) in lender {
            for succ in succs {
                arcs.push((node, succ));
            }
        });
    }
    arcs.sort();
    assert_eq!(arcs, vec![(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)]);
    Ok(())
}

#[test]
fn test_symmetrize_sorted_split_with_loops() -> Result<()> {
    use webgraph::transform::symmetrize_sorted_split;

    // Graph with self-loop
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 2)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    let s = symmetrize_sorted_split::<false, _>(&seq, MemoryUsage::BatchSize(2), None)?;
    let lenders: Vec<_> = s.into();
    let mut arcs = vec![];
    for lender in lenders {
        for_!((node, succs) in lender {
            for succ in succs {
                arcs.push((node, succ));
            }
        });
    }
    arcs.sort();
    // Self-loop preserved, all edges bidirectional
    assert_eq!(arcs, vec![(0, 1), (1, 0), (1, 2), (2, 1), (2, 2)]);
    Ok(())
}

#[test]
fn test_symmetrize_split_no_loops() -> Result<()> {
    use webgraph::transform::symmetrize_split;

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    let s = symmetrize_split::<true, _>(&seq, MemoryUsage::BatchSize(2), None)?;
    // Collect all arcs and verify symmetrization
    let lenders: Vec<_> = s.into();
    let mut arcs = vec![];
    for lender in lenders {
        for_!((node, succs) in lender {
            for succ in succs {
                arcs.push((node, succ));
            }
        });
    }
    arcs.sort();
    // 3-cycle symmetrized: each node has exactly 2 neighbors, 6 arcs total
    assert_eq!(arcs, vec![(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)]);
    Ok(())
}

// ── map ──

#[test]
fn test_map() -> Result<()> {
    // Graph: 0->1, 0->2, 1->2, 2->3
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2), (2, 3)]);
    // Map: 0->0, 1->0, 2->1, 3->1 (merges 0,1 into 0 and 2,3 into 1)
    let m = [0, 0, 1, 1];
    let mapped = transform::map(&g, &m, 2, MemoryUsage::default())?;
    let mapped = VecGraph::from_lender(&mapped);
    assert_eq!(mapped.num_nodes(), 2);
    // Arcs: (0,0),(0,1),(0,1),(1,1) → deduped: (0,0),(0,1),(1,1)
    assert_eq!(mapped.successors(0).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(mapped.successors(1).collect::<Vec<_>>(), vec![1]);
    Ok(())
}

#[test]
fn test_map_identity() -> Result<()> {
    let g = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let id = [0, 1, 2];
    let mapped = transform::map(&g, &id, 3, MemoryUsage::BatchSize(2))?;
    let mapped = VecGraph::from_lender(&mapped);
    webgraph::traits::graph::eq(&g, &mapped)?;
    Ok(())
}

#[test]
fn test_map_enlarges() -> Result<()> {
    // Map to a larger node space
    let g = VecGraph::from_arcs([(0, 1), (1, 0)]);
    let m = [5, 10];
    let mapped = transform::map(&g, &m, 11, MemoryUsage::default())?;
    let mapped = VecGraph::from_lender(&mapped);
    assert_eq!(mapped.num_nodes(), 11);
    assert_eq!(mapped.successors(5).collect::<Vec<_>>(), vec![10]);
    assert_eq!(mapped.successors(10).collect::<Vec<_>>(), vec![5]);
    Ok(())
}

#[test]
fn test_map_size_mismatch() {
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let m = vec![0, 1]; // 2 elements but graph has 3 nodes
    let result = transform::map(&g, &m, 3, MemoryUsage::default());
    assert!(result.is_err());
}

#[test]
fn test_map_split() -> Result<()> {
    use webgraph::transform::map_split;

    // Graph: 0->1, 0->2, 1->2 with map 0->2, 1->0, 2->1 (a permutation)
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    let m = vec![2, 0, 1];
    let s = map_split(&seq, &m, 3, MemoryUsage::BatchSize(2), None)?;
    let lenders: Vec<_> = s.into();
    let mut arcs = vec![];
    for lender in lenders {
        for_!((node, succs) in lender {
            for succ in succs {
                arcs.push((node, succ));
            }
        });
    }
    arcs.sort();
    // Original (0,1) -> (2,0), (0,2) -> (2,1), (1,2) -> (0,1)
    assert_eq!(arcs, vec![(0, 1), (2, 0), (2, 1)]);
    Ok(())
}

#[test]
fn test_map_split_shrinks() -> Result<()> {
    use webgraph::transform::map_split;

    // Graph: 0->1, 1->2, 2->0; map merges 1 and 2 into node 1
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp.path();
    BvComp::with_basename(path).comp_graph::<BE>(&graph)?;
    let seq = BvGraphSeq::with_basename(path).endianness::<BE>().load()?;
    let m = vec![0, 1, 1]; // 0->0, 1->1, 2->1
    let s = map_split(&seq, &m, 2, MemoryUsage::BatchSize(2), None)?;
    let lenders: Vec<_> = s.into();
    let mut arcs = vec![];
    for lender in lenders {
        for_!((node, succs) in lender {
            for succ in succs {
                arcs.push((node, succ));
            }
        });
    }
    arcs.sort();
    // (0,1)->(0,1), (1,2)->(1,1), (2,0)->(1,0) → deduped: (0,1),(1,0),(1,1)
    assert_eq!(arcs, vec![(0, 1), (1, 0), (1, 1)]);
    Ok(())
}
