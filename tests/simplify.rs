#![cfg(feature = "slow_tests")]

use anyhow::Result;
use dsi_bitstream::traits::BigEndian;
use mmap_rs::MmapFlags;
use std::path::{Path, PathBuf};
use sux::traits::bit_field_slice::BitFieldSlice;
use tempfile::Builder;
use webgraph::cli::main as cli_main;
use webgraph::graphs::bvgraph::{GRAPH_EXTENSION, OFFSETS_EXTENSION, PROPERTIES_EXTENSION};
use webgraph::prelude::Left;
use webgraph::prelude::{JavaPermutation, VecGraph};
use webgraph::traits::{RandomAccessGraph, SequentialGraph, SequentialLabeling};

const TEST_GRAPH: &str = "tests/data/cnr-2000";

/// check that the simplified graph is correct
fn check_simplification<G, P>(ground_truth: G, path: P) -> anyhow::Result<()>
where
    P: AsRef<Path>,
    G: SequentialGraph + RandomAccessGraph,
{
    let simplified = webgraph::graphs::bvgraph::BVGraphSeq::with_basename(path.as_ref())
        .endianness::<BigEndian>()
        .load()?;

    assert_eq!(ground_truth.num_nodes(), simplified.num_nodes());

    lender::for_!((src, succ) in simplified {
        let succs = succ.into_iter().collect::<Vec<_>>();
        let expected = ground_truth.successors(src).into_iter().collect::<Vec<_>>();
        assert_eq!(
            expected,
            succs,
            "Node {} has different successors",
            src
        );
    });

    std::fs::remove_file(dbg!(path.as_ref().with_extension(GRAPH_EXTENSION)))?;
    std::fs::remove_file(dbg!(path.as_ref().with_extension(OFFSETS_EXTENSION)))?;
    std::fs::remove_file(dbg!(path.as_ref().with_extension(PROPERTIES_EXTENSION)))?;

    Ok(())
}

#[test]
fn simplify() -> Result<()> {
    let copy_basename = PathBuf::from(TEST_GRAPH);
    let tmp_dir = Builder::new().prefix("simplify_check").tempdir()?;
    let graph_name = copy_basename.file_stem().unwrap();
    let basename = tmp_dir.path().join(graph_name).display().to_string();

    // copy the graph files to the temporary directory
    for extension in [GRAPH_EXTENSION, PROPERTIES_EXTENSION, OFFSETS_EXTENSION] {
        std::fs::copy(
            dbg!(copy_basename.with_extension(extension)),
            dbg!(tmp_dir.path().join(graph_name).with_extension(extension)),
        )?;
    }

    cli_main(vec!["webgraph", "build", "ef", &basename])?;

    // Load the original graph
    let original = webgraph::graphs::bvgraph::BVGraph::with_basename(&basename)
        .endianness::<BigEndian>()
        .load()?;

    // build the simplified version
    let mut simplified = VecGraph::new();
    for src in 0..original.num_nodes() {
        simplified.add_node(src);
        for dst in original.successors(src) {
            if dst == src {
                continue;
            }
            simplified.add_node(dst);
            simplified.add_arc(src, dst);
            simplified.add_arc(dst, src);
        }
    }
    let simplified = Left(simplified);

    // base simplify
    cli_main(vec![
        "webgraph",
        "transform",
        "simplify",
        &basename,
        &format!("{}-simple", basename),
    ])?;

    // check that the simplified graph is correct
    check_simplification(&simplified, format!("{}-simple", basename))?;

    log::info!("Transpose the graph");
    cli_main(vec![
        "webgraph",
        "transform",
        "transpose",
        &basename,
        &format!("{}-t", basename),
    ])?;

    // simplify with transposed
    cli_main(vec![
        "webgraph",
        "transform",
        "simplify",
        &basename,
        &format!("{}-simple", basename),
        "--transposed",
        &format!("{}-t", basename),
    ])?;

    check_simplification(&simplified, format!("{}-simple", basename))?;

    log::info!("Create a random permutation for the graph");
    cli_main(vec![
        "webgraph",
        "perm",
        "rand",
        &basename,
        &format!("{}.perm", basename),
    ])?;

    let mut simplified_permuted = VecGraph::new();
    let permutation =
        JavaPermutation::mmap(&format!("{}.perm", basename), MmapFlags::RANDOM_ACCESS)?;
    for src in 0..original.num_nodes() {
        let p_src = permutation.get(src);
        simplified_permuted.add_node(p_src);
        for dst in original.successors(src) {
            if dst == src {
                continue;
            }
            let p_dst = permutation.get(dst);
            simplified_permuted.add_node(p_dst);
            simplified_permuted.add_arc(p_src, p_dst);
            simplified_permuted.add_arc(p_dst, p_src);
        }
    }
    let simplified_permuted = Left(simplified_permuted);

    cli_main(vec![
        "webgraph",
        "transform",
        "simplify",
        &basename,
        &format!("{}-simple", basename),
        "--permutation",
        &format!("{}.perm", basename),
    ])?;

    check_simplification(&simplified_permuted, format!("{}-simple", basename))?;

    log::info!("Simplify with transpose and permutation");
    cli_main(vec![
        "webgraph",
        "transform",
        "simplify",
        &basename,
        &format!("{}-simple", basename),
        "--transposed",
        &format!("{}-t", basename),
        "--permutation",
        &format!("{}.perm", basename),
    ])?;

    check_simplification(&simplified_permuted, format!("{}-simple", basename))?;

    log::info!("Create the Elias Fano for transposed");
    cli_main(vec!["webgraph", "build", "ef", &format!("{}-t", basename)])?;

    log::info!("Simplify with transpose and permutation and efs");
    cli_main(vec![
        "webgraph",
        "transform",
        "simplify",
        &basename,
        &format!("{}-simple", basename),
        "--transposed",
        &format!("{}-t", basename),
        "--permutation",
        &format!("{}.perm", basename),
    ])?;
    check_simplification(&simplified_permuted, format!("{}-simple", basename))?;

    Ok(())
}
