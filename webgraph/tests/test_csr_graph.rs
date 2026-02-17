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
    traits::{SequentialGraph, SortedLender, graph},
};

/// Helper function to test epserde serialization/deserialization for CSR graph types
fn test_epserde_roundtrip<T, U>(
    original: &T,
    deserializer: impl Fn(&'static [u8]) -> anyhow::Result<U>,
) -> anyhow::Result<()>
where
    T: Serialize + SequentialGraph,
    U: SequentialGraph,
    for<'a> T::Lender<'a>: SortedLender,
    for<'a> U::Lender<'a>: SortedLender,
{
    let mut file = std::io::Cursor::new(vec![]);
    unsafe { original.serialize(&mut file) }?;
    let data = file.into_inner();
    // This is presently needed because of limitations of the borrow checker
    let data = unsafe { transmute::<&'_ [u8], &'static [u8]>(&data) };
    let deserialized = deserializer(data)?;
    graph::eq(original, &deserialized)?;
    Ok(())
}

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
    test_epserde_roundtrip(&csr, |data| {
        Ok(unsafe { <CsrGraph>::deserialize_eps(data) }?)
    })?;

    let csr = CsrSortedGraph::from_seq_graph(&g);
    test_epserde_roundtrip(&csr, |data| {
        Ok(unsafe { <CsrSortedGraph>::deserialize_eps(data) }?)
    })?;

    let csr = CompressedCsrGraph::try_from_graph(&g)?;

    test_epserde_roundtrip(&csr, |data| {
        Ok(unsafe { <CompressedCsrGraph>::deserialize_eps(data) }?)
    })?;

    let csr = CompressedCsrSortedGraph::try_from_graph(&g)?;
    test_epserde_roundtrip(&csr, |data| {
        Ok(unsafe { <CompressedCsrSortedGraph>::deserialize_eps(data) }?)
    })?;

    Ok(())
}
