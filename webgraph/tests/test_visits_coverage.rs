/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for visit algorithms (BFS and DFS).

use anyhow::Result;
use webgraph::graphs::vec_graph::VecGraph;
use webgraph::prelude::*;
use webgraph::visits::Sequential;

/// Canonical test graph (8 nodes, 11 arcs).
///
/// ```text
///   0 ──→ 1 ──→ 3 ──→ 6 ──→ 2
///   │     │           ↑     ↑ │
///   │     ├──→ 4 ─────┘     │ │
///   │     └──→ 5 ────→ 7    │ │
///   │              │         │ │
///   └──→ 2 ──→ 4  └─→ 6    │ │
///        ↑                   │ │
///        └───────────────────┘ │
///              (cycle: 6→2)    │
/// ```
///
/// - Outdegree 0: node 7 (sink)
/// - Outdegree 1: nodes 2, 3, 4, 6
/// - Outdegree 2: nodes 0, 5
/// - Outdegree 3: node 1
/// - Indegree 0: node 0 (source)
/// - Indegree 1: nodes 1, 3, 5, 7
/// - Indegree 2: nodes 2, 4
/// - Indegree 3: node 6
/// - Cycle: 2 → 4 → 6 → 2
fn test_graph() -> VecGraph {
    VecGraph::from_arcs([
        (0, 1),
        (0, 2),
        (1, 3),
        (1, 4),
        (1, 5),
        (2, 4),
        (3, 6),
        (4, 6),
        (5, 6),
        (5, 7),
        (6, 2),
    ])
}

// ── DFS SeqNoPred ──

#[test]
fn test_dfs_previsit() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::depth_first;
    let g = test_graph();
    let mut visited = vec![];
    depth_first::SeqNoPred::new(&g)
        .visit([0], |event| {
            if let depth_first::EventNoPred::Previsit { node, .. } = event {
                visited.push(node);
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    assert_eq!(visited.len(), 8);
    assert_eq!(visited[0], 0);
    Ok(())
}

#[test]
fn test_dfs_early_termination() -> Result<()> {
    use webgraph::visits::depth_first;
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visited = vec![];
    let result = depth_first::SeqNoPred::new(&g).visit([0], |event| {
        if let depth_first::EventNoPred::Previsit { node, .. } = event {
            visited.push(node);
            if node == 2 {
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(())
    });
    assert!(result.is_break());
    assert_eq!(visited, vec![0, 1, 2]);
    Ok(())
}

#[test]
fn test_dfs_no_pred_reset() -> Result<()> {
    use webgraph::visits::depth_first;
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = depth_first::SeqNoPred::new(&g);
    // Use callback visit
    use no_break::NoBreak;
    visit
        .visit([0], |_event| std::ops::ControlFlow::Continue(()))
        .continue_value_no_break();
    // Reset and visit again
    visit.reset();
    visit
        .visit([0], |_event| std::ops::ControlFlow::Continue(()))
        .continue_value_no_break();
    Ok(())
}

#[test]
fn test_dfs_seq_no_pred_multiple_roots() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    // Disconnected: 0->1, 2->3
    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let mut visit = depth_first::SeqNoPred::new(&graph);
    let mut init_roots = vec![];
    let mut visited = vec![];
    visit
        .visit([0, 2], |event| {
            match event {
                depth_first::EventNoPred::Init { root } => init_roots.push(root),
                depth_first::EventNoPred::Previsit { node, .. } => visited.push(node),
                _ => {}
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(init_roots, vec![0, 2]);
    assert_eq!(visited.len(), 4);
    Ok(())
}

#[test]
fn test_dfs_seq_no_pred_filter() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    // 0->1->2->3
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = depth_first::SeqNoPred::new(&graph);
    let mut visited = vec![];
    visit
        .visit_filtered(
            [0],
            |event| {
                if let depth_first::EventNoPred::Previsit { node, .. } = event {
                    visited.push(node);
                }
                Continue(())
            },
            |args: depth_first::FilterArgsNoPred| args.node != 2,
        )
        .continue_value_no_break();

    assert!(visited.contains(&0));
    assert!(visited.contains(&1));
    assert!(!visited.contains(&2));
    Ok(())
}

#[test]
fn test_dfs_seq_no_pred_visit_with() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = depth_first::SeqNoPred::new(&graph);
    let mut visited = Vec::new();
    visit
        .visit_with([0], &mut visited, |visited, event| {
            if let depth_first::EventNoPred::Previsit { node, root: _, .. } = event {
                visited.push(node);
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(visited, vec![0, 1, 2]);
    Ok(())
}

#[test]
fn test_dfs_seq_no_pred_visit_filtered_with() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = depth_first::SeqNoPred::new(&graph);
    let mut visited = Vec::new();
    visit
        .visit_filtered_with(
            [0],
            &mut visited,
            |visited, event| {
                if let depth_first::EventNoPred::Previsit { node, .. } = event {
                    visited.push(node);
                }
                Continue(())
            },
            |_, args: depth_first::FilterArgsNoPred| args.node != 2,
        )
        .continue_value_no_break();

    assert!(visited.contains(&0));
    assert!(visited.contains(&1));
    assert!(!visited.contains(&2));
    Ok(())
}

#[test]
fn test_dfs_seq_no_pred_all_events() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    let graph = test_graph();
    let mut visit = depth_first::SeqNoPred::new(&graph);

    let mut previsits = Vec::new();
    let mut revisits = Vec::new();
    let mut had_done = false;
    visit
        .visit([0], |event| {
            match event {
                depth_first::EventNoPred::Previsit { node, .. } => previsits.push(node),
                depth_first::EventNoPred::Revisit { node, .. } => revisits.push(node),
                depth_first::EventNoPred::Done { .. } => had_done = true,
                _ => {}
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(previsits.len(), 8);
    assert!(had_done);
    // 11 arcs − 7 tree edges = 4 non-tree edges, each generating a revisit
    assert_eq!(revisits.len(), 4);
    Ok(())
}

// ── DFS SeqPred ──

#[test]
fn test_dfs_order_disconnected() -> Result<()> {
    use webgraph::visits::depth_first;
    // Two disconnected components: 0->1, 2->3
    let g = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let nodes: Vec<_> = depth_first::SeqPred::new(&g)
        .into_iter()
        .map(|e| e.node)
        .collect();
    // Should visit all 4 nodes
    assert_eq!(nodes.len(), 4);
    assert!(nodes.contains(&0));
    assert!(nodes.contains(&1));
    assert!(nodes.contains(&2));
    assert!(nodes.contains(&3));
    Ok(())
}

#[test]
fn test_dfs_pred_cycle() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::depth_first;
    let g = test_graph();
    let mut previsited = vec![];
    let mut revisited = vec![];
    depth_first::SeqPred::new(&g)
        .visit(0..g.num_nodes(), |event| {
            match event {
                depth_first::EventPred::Previsit { node, .. } => previsited.push(node),
                depth_first::EventPred::Revisit { node, .. } => revisited.push(node),
                _ => {}
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    assert_eq!(previsited.len(), 8);
    // 11 arcs − 7 tree edges = 4 non-tree edges, each generating a revisit
    assert_eq!(revisited.len(), 4);
    Ok(())
}

#[test]
fn test_dfs_postvisit() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::depth_first;
    let g = test_graph();
    let mut postvisited = vec![];
    depth_first::SeqPred::new(&g)
        .visit(0..g.num_nodes(), |event| {
            if let depth_first::EventPred::Postvisit { node, .. } = event {
                postvisited.push(node);
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    assert_eq!(postvisited.len(), 8);
    // Sink (7) must be postvisited before source (0)
    let pos_7 = postvisited.iter().position(|&n| n == 7).unwrap();
    let pos_0 = postvisited.iter().position(|&n| n == 0).unwrap();
    assert!(pos_7 < pos_0);
    Ok(())
}

#[test]
fn test_dfs_interrupted_visit_stack() -> Result<()> {
    use webgraph::visits::depth_first;
    // Linear chain: 0->1->2->3->4
    let g = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visit = depth_first::SeqPred::new(&g);
    // Visit and interrupt at node 3
    let interrupted_node = visit.visit([0], |event| {
        if let depth_first::EventPred::Previsit { node, .. } = event {
            if node == 3 {
                return std::ops::ControlFlow::Break(node);
            }
        }
        std::ops::ControlFlow::Continue(())
    });
    assert_eq!(interrupted_node, std::ops::ControlFlow::Break(3));
    // After interruption at node 3 on chain 0->1->2->3->4, the stack
    // yields the parents of nodes on the visit path (excluding the root),
    // in reverse order. The interrupted node (3) was never pushed.
    let stack_nodes: Vec<usize> = visit.stack().collect();
    assert_eq!(stack_nodes, vec![1, 0]);
    Ok(())
}

#[test]
fn test_dfs_reset() -> Result<()> {
    use webgraph::visits::depth_first;
    let g = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = depth_first::SeqPred::new(&g);
    // Do a full visit
    let _: Vec<_> = (&mut visit).into_iter().collect();
    // Reset
    visit.reset();
    // Visit again - should work the same
    let nodes: Vec<_> = (&mut visit).into_iter().map(|e| e.node).collect();
    assert_eq!(nodes, vec![0, 1, 2]);
    Ok(())
}

#[test]
fn test_dfs_seq_pred_filter() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    // 0->1->2->3
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = depth_first::SeqPred::new(&graph);
    let mut visited = vec![];
    visit
        .visit_filtered(
            [0],
            |event| {
                if let depth_first::EventPred::Previsit { node, .. } = event {
                    visited.push(node);
                }
                Continue(())
            },
            |args: depth_first::FilterArgsPred| args.node != 2,
        )
        .continue_value_no_break();

    assert!(visited.contains(&0));
    assert!(visited.contains(&1));
    assert!(!visited.contains(&2));
    Ok(())
}

#[test]
fn test_dfs_seq_pred_visit_with() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = depth_first::SeqPred::new(&graph);
    let mut parents = vec![usize::MAX; 3];
    visit
        .visit_with([0], &mut parents, |parents, event| {
            if let depth_first::EventPred::Previsit { node, parent, .. } = event {
                parents[node] = parent;
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(parents[0], 0); // root's parent is itself
    assert_eq!(parents[1], 0);
    assert_eq!(parents[2], 1);
    Ok(())
}

#[test]
fn test_dfs_seq_pred_visit_filtered_with() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = depth_first::SeqPred::new(&graph);
    let mut visited = Vec::new();
    visit
        .visit_filtered_with(
            [0],
            &mut visited,
            |visited, event| {
                if let depth_first::EventPred::Previsit { node, .. } = event {
                    visited.push(node);
                }
                Continue(())
            },
            |_, args: depth_first::FilterArgsPred| args.node != 2,
        )
        .continue_value_no_break();

    assert!(visited.contains(&0));
    assert!(visited.contains(&1));
    assert!(!visited.contains(&2));
    Ok(())
}

#[test]
fn test_dfs_with_callbacks() {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    let graph = test_graph();
    let mut visit = depth_first::SeqPred::new(&graph);

    let mut preorder = vec![];
    let mut postorder = vec![];
    visit
        .visit(0..graph.num_nodes(), |event| {
            match event {
                depth_first::EventPred::Previsit { node, .. } => preorder.push(node),
                depth_first::EventPred::Postvisit { node, .. } => postorder.push(node),
                _ => {}
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(preorder.len(), 8);
    assert_eq!(postorder.len(), 8);
}

// ── DFS SeqPath ──

#[test]
fn test_dfs_path_on_stack() -> Result<()> {
    use std::ops::ControlFlow::{Break, Continue};
    use webgraph::visits::{Sequential, depth_first};

    let graph = test_graph();
    let mut visit = depth_first::SeqPath::new(&graph);
    let mut found_cycle = false;
    let result = visit.visit([0], |event| {
        if let depth_first::EventPred::Revisit { on_stack, .. } = event {
            if on_stack {
                found_cycle = true;
                return Break("cycle");
            }
        }
        Continue(())
    });
    assert!(result.is_break());
    assert!(found_cycle);
    Ok(())
}

#[test]
fn test_dfs_seq_path_postvisit_and_done() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, depth_first};

    let graph = test_graph();
    let mut visit = depth_first::SeqPath::new(&graph);
    let mut got_done = false;
    let mut postvisit_order = vec![];
    visit
        .visit([0], |event| {
            match event {
                depth_first::EventPred::Postvisit { node, .. } => {
                    postvisit_order.push(node);
                }
                depth_first::EventPred::Done { .. } => {
                    got_done = true;
                }
                _ => {}
            }
            Continue(())
        })
        .continue_value_no_break();

    assert!(got_done);
    assert_eq!(postvisit_order.len(), 8);
    Ok(())
}

// ── DFS Order ──

#[test]
fn test_dfs_order_iterator() {
    use webgraph::visits::depth_first;

    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let mut visit = depth_first::SeqPred::new(&graph);
    let order = depth_first::DfsOrder::new(&mut visit);

    let events: Vec<_> = order.collect();
    // All 4 nodes should be visited
    assert_eq!(events.len(), 4);
    // First event: root node 0
    assert_eq!(events[0].node, 0);
    assert_eq!(events[0].depth, 0);
    assert_eq!(events[0].root, 0);
}

#[test]
fn test_dfs_order_disconnected_graph() {
    use webgraph::visits::depth_first;

    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let mut visit = depth_first::SeqPred::new(&graph);
    let order = depth_first::DfsOrder::new(&mut visit);

    let events: Vec<_> = order.collect();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].root, 0);
}

#[test]
fn test_dfs_order_exact_size() {
    use webgraph::visits::depth_first;

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = depth_first::SeqPred::new(&graph);
    let order = depth_first::DfsOrder::new(&mut visit);
    assert_eq!(order.len(), 3);
}

#[test]
fn test_dfs_order_deep_path() {
    use webgraph::visits::depth_first;

    // Linear path: 0->1->2->3->4
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visit = depth_first::SeqPred::new(&graph);
    let order = depth_first::DfsOrder::new(&mut visit);

    let events: Vec<_> = order.collect();
    // Should visit nodes in DFS order (linear path)
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e.node, i);
        assert_eq!(e.depth, i);
    }
}

// ── BFS Seq ──

#[test]
fn test_bfs_order_distances() -> Result<()> {
    use webgraph::visits::breadth_first;
    let g = test_graph();
    let events: Vec<_> = (&mut breadth_first::Seq::new(&g)).into_iter().collect();

    assert_eq!(events.len(), 8);
    let mut distances = vec![usize::MAX; 8];
    for e in &events {
        distances[e.node] = e.distance;
    }
    assert_eq!(distances, vec![0, 1, 1, 2, 2, 2, 3, 3]);
    Ok(())
}

#[test]
fn test_bfs_disconnected() -> Result<()> {
    use webgraph::visits::breadth_first;
    // Two components: {0,1} and {2,3}
    let g = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let events: Vec<_> = (&mut breadth_first::Seq::new(&g)).into_iter().collect();
    assert_eq!(events.len(), 4);

    // First two events should be root 0 and distance 0/1
    assert_eq!(events[0].root, 0);
    assert_eq!(events[0].distance, 0);
    assert_eq!(events[1].root, 0);
    assert_eq!(events[1].distance, 1);

    // Next two should be from root 2
    assert_eq!(events[2].root, 2);
    assert_eq!(events[2].distance, 0);
    assert_eq!(events[3].root, 2);
    assert_eq!(events[3].distance, 1);
    Ok(())
}

#[test]
fn test_bfs_visit_callback() -> Result<()> {
    use no_break::NoBreak;
    use webgraph::visits::breadth_first;
    let g = test_graph();
    let mut distances = vec![usize::MAX; g.num_nodes()];
    breadth_first::Seq::new(&g)
        .visit([0], |event| {
            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                distances[node] = distance;
            }
            std::ops::ControlFlow::Continue(())
        })
        .continue_value_no_break();
    assert_eq!(distances, vec![0, 1, 1, 2, 2, 2, 3, 3]);
    Ok(())
}

#[test]
fn test_bfs_into_iter_order() -> Result<()> {
    use webgraph::visits::breadth_first;
    let g = test_graph();
    let mut visit = breadth_first::Seq::new(&g);
    let events: Vec<_> = (&mut visit).into_iter().collect();
    assert_eq!(events.len(), 8);
    // BFS visits level by level: distances must be non-decreasing
    for w in events.windows(2) {
        assert!(w[0].distance <= w[1].distance);
    }
    // Check level membership
    assert_eq!(events[0].node, 0);
    assert_eq!(events[0].distance, 0);
    for e in &events[1..3] {
        assert_eq!(e.distance, 1);
    }
    for e in &events[3..6] {
        assert_eq!(e.distance, 2);
    }
    for e in &events[6..8] {
        assert_eq!(e.distance, 3);
    }
    Ok(())
}

#[test]
fn test_bfs_seq_filter() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let mut visited = vec![];
    visit
        .visit_filtered(
            [0],
            |event| {
                if let breadth_first::EventPred::Visit { node, .. } = event {
                    visited.push(node);
                }
                Continue(())
            },
            |args: breadth_first::FilterArgsPred| args.node != 2,
        )
        .continue_value_no_break();

    assert!(visited.contains(&0));
    assert!(visited.contains(&1));
    assert!(!visited.contains(&2));
    Ok(())
}

#[test]
fn test_bfs_seq_with_init() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let mut distances = vec![0_usize; 3];
    visit
        .visit_with([0], &mut distances, |dists, event| {
            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                dists[node] = distance;
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(distances, vec![0, 1, 2]);
    Ok(())
}

#[test]
fn test_bfs_seq_frontier_sizes() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    let graph = test_graph();
    let mut visit = breadth_first::Seq::new(&graph);
    let mut frontier_sizes = vec![];
    visit
        .visit([0], |event| {
            if let breadth_first::EventPred::FrontierSize { distance, size } = event {
                frontier_sizes.push((distance, size));
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(frontier_sizes, vec![(0, 1), (1, 2), (2, 3), (3, 2)]);
    Ok(())
}

#[test]
fn test_bfs_seq_multiple_roots() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    // Disconnected: 0->1, 2->3
    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let mut visited = vec![];
    visit
        .visit([0, 2], |event| {
            if let breadth_first::EventPred::Visit { node, .. } = event {
                visited.push(node);
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(visited.len(), 4);
    Ok(())
}

#[test]
fn test_bfs_seq_revisit() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    let graph = test_graph();
    let mut visit = breadth_first::Seq::new(&graph);
    let mut revisited = vec![];
    visit
        .visit([0], |event| {
            if let breadth_first::EventPred::Revisit { node, .. } = event {
                revisited.push(node);
            }
            Continue(())
        })
        .continue_value_no_break();

    // Convergence revisits (node 4 from 2→4, node 6 from 4→6 and 5→6)
    // and cycle revisit (node 2 from 6→2)
    assert!(revisited.contains(&4));
    assert!(revisited.contains(&6));
    assert!(revisited.contains(&2));
    Ok(())
}

#[test]
fn test_bfs_seq_reset() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::Seq::new(&graph);

    let mut count1 = 0;
    visit
        .visit([0], |event| {
            if let breadth_first::EventPred::Visit { .. } = event {
                count1 += 1;
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(count1, 3);

    visit.reset();
    let mut count2 = 0;
    visit
        .visit([0], |event| {
            if let breadth_first::EventPred::Visit { .. } = event {
                count2 += 1;
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(count2, 3);
    Ok(())
}

#[test]
fn test_bfs_seq_visit_filtered_with() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let mut visited = Vec::new();
    visit
        .visit_filtered_with(
            [0],
            &mut visited,
            |visited, event| {
                if let breadth_first::EventPred::Visit { node, .. } = event {
                    visited.push(node);
                }
                Continue(())
            },
            |_, args: breadth_first::FilterArgsPred| args.distance <= 1,
        )
        .continue_value_no_break();

    assert_eq!(visited, vec![0, 1]);
    Ok(())
}

#[test]
fn test_bfs_seq_early_termination() -> Result<()> {
    use std::ops::ControlFlow::{Break, Continue};
    use webgraph::visits::{Sequential, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visit = breadth_first::Seq::new(&graph);

    let mut visited = Vec::new();
    let result = visit.visit([0], |event| {
        if let breadth_first::EventPred::Visit { node, .. } = event {
            visited.push(node);
            if node == 2 {
                return Break("found target");
            }
        }
        Continue(())
    });

    assert!(result.is_break());
    assert!(visited.contains(&0));
    assert!(visited.contains(&1));
    assert!(visited.contains(&2));
    // Node 3 and 4 should not be visited
    assert!(!visited.contains(&3));
    assert!(!visited.contains(&4));
    Ok(())
}

#[test]
fn test_bfs_seq_init_done_events() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let mut had_init = false;
    let mut had_done = false;
    visit
        .visit([0], |event| {
            match event {
                breadth_first::EventPred::Init {} => had_init = true,
                breadth_first::EventPred::Done {} => had_done = true,
                _ => {}
            }
            Continue(())
        })
        .continue_value_no_break();

    assert!(had_init);
    assert!(had_done);
    Ok(())
}

#[test]
fn test_bfs_disconnected_graph() {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use webgraph::visits::{Sequential, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);

    // Visit from node 0 - should only reach nodes 0, 1
    let mut visited_from_0 = vec![];
    visit
        .visit([0], |event| {
            if let breadth_first::EventPred::Visit { node, .. } = event {
                visited_from_0.push(node);
            }
            Continue(())
        })
        .continue_value_no_break();
    visited_from_0.sort();
    assert_eq!(visited_from_0, vec![0, 1]);

    // Reset and visit from node 2 - should reach 2, 3
    visit.reset();
    let mut visited_from_2 = vec![];
    visit
        .visit([2], |event| {
            if let breadth_first::EventPred::Visit { node, .. } = event {
                visited_from_2.push(node);
            }
            Continue(())
        })
        .continue_value_no_break();
    visited_from_2.sort();
    assert_eq!(visited_from_2, vec![2, 3]);
}

// ── BFS Order ──

#[test]
fn test_bfs_order_iterator() {
    use webgraph::visits::breadth_first;

    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let order = breadth_first::BfsOrder::new(&mut visit);

    let events: Vec<_> = order.collect();
    // All 4 nodes should be visited
    assert_eq!(events.len(), 4);
    // First event: root node 0
    assert_eq!(events[0].node, 0);
    assert_eq!(events[0].distance, 0);
}

#[test]
fn test_bfs_order_disconnected_graph() {
    use webgraph::visits::breadth_first;

    // Graph with two components: {0,1} and {2,3}
    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let order = breadth_first::BfsOrder::new(&mut visit);

    let events: Vec<_> = order.collect();
    // All 4 nodes should be visited (BfsOrder discovers all components)
    assert_eq!(events.len(), 4);
    // First root should be 0
    assert_eq!(events[0].root, 0);
}

#[test]
fn test_bfs_order_exact_size() {
    use webgraph::visits::breadth_first;

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let order = breadth_first::BfsOrder::new(&mut visit);
    assert_eq!(order.len(), 3);
}

#[test]
fn test_bfs_order_from_roots() -> Result<()> {
    use webgraph::visits::breadth_first;

    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let mut visit = breadth_first::Seq::new(&graph);
    let order = visit.iter_from_roots([2, 0])?;

    let events: Vec<_> = order.collect();
    assert_eq!(events.len(), 4);
    // First visited node should be from root 2
    assert_eq!(events[0].node, 2);
    assert_eq!(events[0].distance, 0);
    Ok(())
}

// ── BFS ParFairNoPred ──

#[test]
fn test_bfs_par_fair_no_pred() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use sync_cell_slice::SyncSlice;
    use webgraph::visits::{Parallel, breadth_first};

    let graph = test_graph();
    let mut visit = breadth_first::ParFairNoPred::new(&graph);
    let mut d = [0_usize; 8];
    let d_sync = d.as_sync_slice();
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventNoPred::Visit { node, distance, .. } = event {
                unsafe { d_sync[node].set(distance) };
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(d, [0, 1, 1, 2, 2, 2, 3, 3]);
    Ok(())
}

#[test]
fn test_bfs_par_fair_reset() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::ParFairNoPred::new(&graph);

    let count = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventNoPred::Visit { .. } = event {
                count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(count.load(Ordering::Relaxed), 3);

    visit.reset();
    let count2 = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventNoPred::Visit { .. } = event {
                count2.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(count2.load(Ordering::Relaxed), 3);
    Ok(())
}

#[test]
fn test_bfs_par_fair_with_granularity() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use sync_cell_slice::SyncSlice;
    use webgraph::utils::Granularity;
    use webgraph::visits::{Parallel, breadth_first};

    let graph = test_graph();
    let mut visit = breadth_first::ParFairNoPred::with_granularity(&graph, Granularity::Nodes(2));
    let mut d = [0_usize; 8];
    let d_sync = d.as_sync_slice();
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventNoPred::Visit { node, distance, .. } = event {
                unsafe { d_sync[node].set(distance) };
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(d, [0, 1, 1, 2, 2, 2, 3, 3]);
    Ok(())
}

#[test]
fn test_bfs_par_fair_no_pred_filter() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    // 0->1->2->3
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = breadth_first::ParFairNoPred::new(&graph);
    let visit_count = AtomicUsize::new(0);
    visit
        .par_visit_filtered(
            [0],
            |event| {
                if let breadth_first::EventNoPred::Visit { .. } = event {
                    visit_count.fetch_add(1, Ordering::Relaxed);
                }
                Continue(())
            },
            |args: breadth_first::FilterArgsNoPred| args.node != 2,
        )
        .continue_value_no_break();

    // Should visit 0, 1 but not 2 or 3
    assert_eq!(visit_count.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn test_bfs_par_fair_no_pred_visit_with() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::ParFairNoPred::new(&graph);
    let count = AtomicUsize::new(0);
    visit
        .par_visit_with([0], &count, |count, event| {
            if let breadth_first::EventNoPred::Visit { .. } = event {
                count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(count.load(Ordering::Relaxed), 3);
    Ok(())
}

#[test]
fn test_bfs_par_fair_no_pred_revisit() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    // 0->1, 0->2, 1->2 (revisit at 2)
    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 2)]);
    let mut visit = breadth_first::ParFairNoPred::new(&graph);
    let revisit_count = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventNoPred::Revisit { .. } = event {
                revisit_count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();

    // Exactly 1 revisit: edge 1→2 finds node 2 already discovered from 0
    assert_eq!(revisit_count.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn test_bfs_par_fair_no_pred_multiple_roots() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    // Disconnected: 0->1, 2->3
    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let mut visit = breadth_first::ParFairNoPred::new(&graph);
    let visit_count = AtomicUsize::new(0);
    visit
        .par_visit([0, 2], |event| {
            if let breadth_first::EventNoPred::Visit { .. } = event {
                visit_count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(visit_count.load(Ordering::Relaxed), 4);
    Ok(())
}

#[test]
fn test_bfs_par_fair_no_pred_visit_filtered_with() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = breadth_first::ParFairNoPred::new(&graph);
    let count = AtomicUsize::new(0);
    visit
        .par_visit_filtered_with(
            [0],
            &count,
            |count, event| {
                if let breadth_first::EventNoPred::Visit { .. } = event {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                Continue(())
            },
            |_, args: breadth_first::FilterArgsNoPred| args.distance <= 1,
        )
        .continue_value_no_break();

    // Only nodes at distance 0 and 1 should be visited (0, 1)
    assert_eq!(count.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn test_bfs_par_fair_early_termination() -> Result<()> {
    use std::ops::ControlFlow::{Break, Continue};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visit = breadth_first::ParFairNoPred::new(&graph);

    let result: std::ops::ControlFlow<&str, ()> = visit.par_visit([0], |event| {
        if let breadth_first::EventNoPred::Visit { node, .. } = event {
            if node == 2 {
                return Break("found");
            }
        }
        Continue(())
    });

    assert!(result.is_break());
    Ok(())
}

// ── BFS ParFairPred ──

#[test]
fn test_bfs_par_fair_pred() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use sync_cell_slice::SyncSlice;
    use webgraph::visits::{Parallel, breadth_first};

    let graph = test_graph();
    let mut visit = breadth_first::ParFairPred::new(&graph);
    let mut d = [0_usize; 8];
    let d_sync = d.as_sync_slice();
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                unsafe { d_sync[node].set(distance) };
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(d, [0, 1, 1, 2, 2, 2, 3, 3]);
    Ok(())
}

#[test]
fn test_bfs_par_fair_pred_revisit() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    // Cycle: 0->1->2->0
    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let mut visit = breadth_first::ParFairPred::new(&graph);
    let revisit_count = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventPred::Revisit { .. } = event {
                revisit_count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();

    // Exactly 1 revisit: back-edge 2→0 finds node 0 already visited
    assert_eq!(revisit_count.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn test_bfs_par_fair_pred_filter() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = breadth_first::ParFairPred::new(&graph);
    let visit_count = AtomicUsize::new(0);
    visit
        .par_visit_filtered(
            [0],
            |event| {
                if let breadth_first::EventPred::Visit { .. } = event {
                    visit_count.fetch_add(1, Ordering::Relaxed);
                }
                Continue(())
            },
            |args: breadth_first::FilterArgsPred| args.node != 2,
        )
        .continue_value_no_break();

    assert_eq!(visit_count.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn test_bfs_par_fair_pred_multiple_roots() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (2, 3)]);
    let mut visit = breadth_first::ParFairPred::new(&graph);
    let visit_count = AtomicUsize::new(0);
    visit
        .par_visit([0, 2], |event| {
            if let breadth_first::EventPred::Visit { .. } = event {
                visit_count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(visit_count.load(Ordering::Relaxed), 4);
    Ok(())
}

#[test]
fn test_bfs_par_fair_pred_reset() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::ParFairPred::new(&graph);

    let count = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventPred::Visit { .. } = event {
                count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(count.load(Ordering::Relaxed), 3);

    visit.reset();
    let count2 = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventPred::Visit { .. } = event {
                count2.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(count2.load(Ordering::Relaxed), 3);
    Ok(())
}

#[test]
fn test_bfs_par_fair_pred_visit_filtered_with() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = breadth_first::ParFairPred::new(&graph);
    let count = AtomicUsize::new(0);
    visit
        .par_visit_filtered_with(
            [0],
            &count,
            |count, event| {
                if let breadth_first::EventPred::Visit { .. } = event {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                Continue(())
            },
            |_, args: breadth_first::FilterArgsPred| args.distance <= 1,
        )
        .continue_value_no_break();

    assert_eq!(count.load(Ordering::Relaxed), 2);
    Ok(())
}

// ── BFS ParLowMem ──

#[test]
fn test_bfs_par_low_mem() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use sync_cell_slice::SyncSlice;
    use webgraph::visits::{Parallel, breadth_first};

    let graph = test_graph();
    let mut visit = breadth_first::ParLowMem::new(&graph);
    let mut d = [0_usize; 8];
    let d_sync = d.as_sync_slice();
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventPred::Visit { node, distance, .. } = event {
                unsafe { d_sync[node].set(distance) };
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(d, [0, 1, 1, 2, 2, 2, 3, 3]);
    Ok(())
}

#[test]
fn test_bfs_par_low_mem_filter() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3)]);
    let mut visit = breadth_first::ParLowMem::new(&graph);
    let visit_count = AtomicUsize::new(0);
    visit
        .par_visit_filtered(
            [0],
            |event| {
                if let breadth_first::EventPred::Visit { .. } = event {
                    visit_count.fetch_add(1, Ordering::Relaxed);
                }
                Continue(())
            },
            |args: breadth_first::FilterArgsPred| args.node != 2,
        )
        .continue_value_no_break();

    assert_eq!(visit_count.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn test_bfs_par_low_mem_reset() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2)]);
    let mut visit = breadth_first::ParLowMem::new(&graph);

    let count = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventPred::Visit { .. } = event {
                count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(count.load(Ordering::Relaxed), 3);

    visit.reset();
    let count2 = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventPred::Visit { .. } = event {
                count2.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();
    assert_eq!(count2.load(Ordering::Relaxed), 3);
    Ok(())
}

#[test]
fn test_bfs_par_low_mem_filtered() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (1, 2), (2, 3), (3, 4)]);
    let mut visit = breadth_first::ParLowMem::new(&graph);
    let count = AtomicUsize::new(0);
    visit
        .par_visit_filtered_with(
            [0],
            &count,
            |count, event| {
                if let breadth_first::EventPred::Visit { .. } = event {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                Continue(())
            },
            |_, args: breadth_first::FilterArgsPred| args.distance <= 2,
        )
        .continue_value_no_break();

    // Only nodes at distance 0, 1, 2 should be visited (0, 1, 2)
    assert_eq!(count.load(Ordering::Relaxed), 3);
    Ok(())
}

#[test]
fn test_bfs_par_low_mem_with_granularity() -> Result<()> {
    use no_break::NoBreak;
    use std::ops::ControlFlow::Continue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use webgraph::utils::Granularity;
    use webgraph::visits::{Parallel, breadth_first};

    let graph = VecGraph::from_arcs([(0, 1), (0, 2), (1, 3), (2, 3)]);
    let mut visit = breadth_first::ParLowMem::with_granularity(&graph, Granularity::Nodes(1));
    let count = AtomicUsize::new(0);
    visit
        .par_visit([0], |event| {
            if let breadth_first::EventPred::Visit { .. } = event {
                count.fetch_add(1, Ordering::Relaxed);
            }
            Continue(())
        })
        .continue_value_no_break();

    assert_eq!(count.load(Ordering::Relaxed), 4);
    Ok(())
}
