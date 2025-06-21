/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::mem::transmute;

use epserde::{deser::Deserialize, ser::Serialize};
use webgraph::{
    graphs::csr_graph::{CompressedCsrGraph, CompressedCsrSortedGraph, CsrSortedGraph},
    prelude::{CsrGraph, VecGraph},
    traits::{graph, labels},
};

#[cfg(feature = "serde")]
#[test]
fn test_serde() -> anyhow::Result<()> {
    use webgraph::graphs::vec_graph::VecGraph;
    let arcs = [(0, 1), (0, 2), (1, 2)];
    let g = VecGraph::from_arcs(arcs);

    let csr = CsrGraph::from_seq_graph(&g);
    let res = serde_json::to_string(&csr)?;
    let json: CsrGraph = serde_json::from_str(&res)?;
    graph::eq(&csr, &json)?;

    let csr = CsrSortedGraph::from_seq_graph(&g);
    let res = serde_json::to_string(&csr)?;
    let json: CsrGraph = serde_json::from_str(&res)?;
    graph::eq(&csr, &json)?;

    Ok(())
}

#[test]
fn test_epserde() -> anyhow::Result<()> {
    let arcs = [(0, 1), (0, 2), (1, 2)];
    let g = VecGraph::from_arcs(arcs);

    let csr = CsrGraph::from_seq_graph(&g);
    let mut file = std::io::Cursor::new(vec![]);
    csr.serialize(&mut file)?;
    let data = file.into_inner();
    // This is presently needed because of limitations of the borrow checker
    let data = unsafe { transmute::<&'_ [u8], &'static [u8]>(&data) };
    let eps = <CsrGraph>::deserialize_eps(&data)?;
    graph::eq(&csr, &eps)?;

    let csr = CsrSortedGraph::from_seq_graph(&g);
    let mut file = std::io::Cursor::new(vec![]);
    csr.serialize(&mut file)?;
    let data = file.into_inner();
    // This is presently needed because of limitations of the borrow checker
    let data = unsafe { transmute::<&'_ [u8], &'static [u8]>(&data) };
    let eps = <CsrSortedGraph>::deserialize_eps(&data)?;
    graph::eq(&csr, &eps)?;

    let csr = CompressedCsrGraph::from_graph(&g);
    let mut file = std::io::Cursor::new(vec![]);
    csr.serialize(&mut file)?;
    let data = file.into_inner();
    // This is presently needed because of limitations of the borrow checker
    let data = unsafe { transmute::<&'_ [u8], &'static [u8]>(&data) };
    let eps = <CompressedCsrGraph>::deserialize_eps(&data)?;
    //graph::eq(&csr, &eps)?;

    let csr = CompressedCsrSortedGraph::from_graph(&g);
    let mut file = std::io::Cursor::new(vec![]);
    csr.serialize(&mut file)?;
    let data = file.into_inner();
    // This is presently needed because of limitations of the borrow checker
    let data = unsafe { transmute::<&'_ [u8], &'static [u8]>(&data) };
    let eps = <CompressedCsrSortedGraph>::deserialize_eps(&data)?;
    //graph::eq(&csr, &eps)?;

    Ok(())
}

#[test]
fn test_csr_graph() -> anyhow::Result<()> {
    let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g = VecGraph::from_arcs(arcs.iter().copied());

    let csr = <CsrGraph>::from_seq_graph(&g);
    labels::check_impl(&csr)?;
    graph::eq(&csr, &g)?;

    let csr = CompressedCsrGraph::from_graph(&g);
    //graph::eq(&csr, &g)?;
    //labels::check_impl(&csr)?;
    Ok(())
}

#[test]
fn test_sorted() -> anyhow::Result<()> {
    let arcs = vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
    let g = VecGraph::from_arcs(arcs.iter().copied());
    // This is just to test that we implemented correctly
    // the SortedLender and SortedIterator traits.
    let csr_sorted = CsrSortedGraph::from_seq_graph(&g);
    labels::eq_sorted(&csr_sorted, &csr_sorted)?;

    let csr_comp_sorted = CompressedCsrSortedGraph::from_graph(&g);
    // labels::eq_sorted(&csr_comp_sorted, &csr_comp_sorted)?;
    Ok(())
}
