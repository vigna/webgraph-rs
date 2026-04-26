/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Tests for the drop safety of [`IntoPairs`] and [`IntoLabeledPairs`].
//!
//! These structs use `transmute` to extend the lifetime of an iterator
//! obtained from a boxed lender. Correct drop order (iterator before lender)
//! is ensured by declaring `current_iter` before `lender` in the struct, since
//! Rust drops fields in declaration order. These tests exercise partial
//! consumption followed by drop, which is the scenario where wrong drop order
//! would cause the iterator to outlive the lender.
//!
//! Run under miri to verify the unsafe code:
//! ```sh
//! cargo +nightly miri test --lib -p webgraph test_into_pairs_drop
//! ```

use webgraph::graphs::arc_list_graph::ArcListGraph;
use webgraph::prelude::*;

/// Tests `into_pairs` with `ArcListGraph`, whose lender's `Succ` iterator
/// holds `&mut NodeLabels<...>` — a direct mutable borrow into the lender.
/// Each call to `Succ::next()` reads and writes through that reference,
/// making this the strongest test for drop-order correctness.
#[test]
fn test_into_pairs_arc_list_graph() {
    let graph = ArcListGraph::new(4, [(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let mut pairs = graph.iter().into_pairs();
    assert_eq!(pairs.next(), Some((0, 1)));
    assert_eq!(pairs.next(), Some((0, 2)));
    assert_eq!(pairs.next(), Some((1, 3)));
    // Drop with current_iter still populated (node 2's successors not started)
    drop(pairs);
}

/// Tests `into_pairs` when the inner iterator is partially consumed within
/// a node's successor list.
#[test]
fn test_into_pairs_partial_node() {
    let graph = ArcListGraph::new(3, [(0, 1), (0, 2), (0, 3), (1, 2), (2, 0)]);
    let mut pairs = graph.iter().into_pairs();
    assert_eq!(pairs.next(), Some((0, 1)));
    // Node 0 still has successors 2 and 3 — drop mid-node
    drop(pairs);
}

/// Tests `into_labeled_pairs` with a labeled `ArcListGraph`.
#[test]
fn test_into_labeled_pairs_arc_list_graph() {
    let graph = ArcListGraph::new_labeled(
        4,
        [
            ((0, 1), 10u32),
            ((0, 2), 20),
            ((1, 3), 30),
            ((2, 3), 40),
            ((3, 0), 50),
        ],
    );
    let mut pairs = graph.iter().into_labeled_pairs();
    assert_eq!(pairs.next(), Some(((0, 1), 10)));
    assert_eq!(pairs.next(), Some(((0, 2), 20)));
    // Drop with remaining successors
    drop(pairs);
}

/// Full consumption sanity check — verifies the complete output.
#[test]
fn test_into_pairs_full_consumption() {
    let graph = ArcListGraph::new(4, [(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
    let pairs: Vec<_> = graph.iter().into_pairs().collect();
    assert_eq!(pairs, [(0, 1), (0, 2), (1, 3), (2, 3), (3, 0)]);
}
