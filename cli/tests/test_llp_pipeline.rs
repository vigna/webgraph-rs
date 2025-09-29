#![cfg(feature = "slow_tests")]
use anyhow::Result;
use dsi_bitstream::traits::BigEndian;
use mmap_rs::MmapFlags;
use std::path::PathBuf;
use sux::traits::bit_field_slice::BitFieldSlice;
use tempfile::Builder;
use webgraph::graphs::bvgraph::{GRAPH_EXTENSION, OFFSETS_EXTENSION, PROPERTIES_EXTENSION};
use webgraph::prelude::JavaPermutation;
use webgraph::traits::{RandomAccessGraph, RandomAccessLabeling, SequentialLabeling};
use webgraph_cli::cli_main;
use webgraph_cli::init_env_logger;

const TEST_GRAPH: &str = "../data/cnr-2000";
const EXPECTED_BITS_PER_ARC: f64 = 2.897;

#[test]
fn test_llp_pipeline() -> Result<()> {
    init_env_logger()?;
    let copy_basename = PathBuf::from(TEST_GRAPH);
    let tmp_dir = Builder::new().prefix("LLPPipeline").tempdir()?;
    let graph_name = copy_basename.file_stem().unwrap();
    let basename = tmp_dir.path().join(graph_name).display().to_string();

    // copy the graph files to the temporary directory
    for extension in [GRAPH_EXTENSION, PROPERTIES_EXTENSION] {
        std::fs::copy(
            copy_basename.with_extension(extension),
            tmp_dir.path().join(graph_name).with_extension(extension),
        )?;
    }

    log::info!("Step 0: Built the Offsets");
    cli_main(vec!["webgraph", "build", "offsets", &basename])?;
    log::info!("Step 1: Creates the Elias Fano");
    cli_main(vec!["webgraph", "build", "ef", &basename])?;
    log::info!("Step 2: Run a BFS traversal to get the initial permutation");
    cli_main(vec![
        "webgraph",
        "perm",
        "bfs",
        &basename,
        &format!("{}.bfs", basename),
    ])?;
    log::info!("Step 3: Creates a simplified view of the graph with the BFS permutation");
    cli_main(vec![
        "webgraph",
        "transform",
        "simplify",
        &basename,
        &format!("{}-simple", basename),
        "--permutation",
        &format!("{}.bfs", basename),
    ])?;
    // re-build the offsets and compare with original
    let simpl_offsets = format!("{}-simple.{}", basename, OFFSETS_EXTENSION);
    let simpl_offsets_orig = format!("{}-simple.{}.orig", basename, OFFSETS_EXTENSION);
    std::fs::rename(&simpl_offsets, &simpl_offsets_orig)?;
    cli_main(vec![
        "webgraph",
        "build",
        "offsets",
        &format!("{}-simple", basename),
    ])?;
    // check that the offsets are the same
    let original_offsets = std::fs::read(&simpl_offsets_orig)?;
    let simplified_offsets = std::fs::read(simpl_offsets)?;
    assert_eq!(
        original_offsets, simplified_offsets,
        "Offsets do not match after simplification"
    );
    std::fs::remove_file(simpl_offsets_orig)?;

    log::info!("Step 4: Creates the Elias Fano for the simplified graph");
    cli_main(vec![
        "webgraph",
        "build",
        "ef",
        &format!("{}-simple", basename),
    ])?;
    log::info!("Step 5: Creates the Degrees Cumulative Function");
    cli_main(vec![
        "webgraph",
        "build",
        "dcf",
        &format!("{}-simple", basename),
    ])?;
    log::info!("Step 6: Run LLP to get the final permutation");
    cli_main(vec![
        "webgraph",
        "run",
        "llp",
        &format!("{}-simple", basename),
        &format!("{}.llp", basename),
    ])?;
    log::info!("Step 7: Compose the two permutations");
    cli_main(vec![
        "webgraph",
        "perm",
        "comp",
        &format!("{}.composed", basename),
        &format!("{}.bfs", basename),
        &format!("{}.llp", basename),
    ])?;
    log::info!("Step 8: Apply both permutations to the original graph");
    cli_main(vec![
        "webgraph",
        "to",
        "bvgraph",
        &basename,
        &format!("{}-final", basename),
        "--permutation",
        &format!("{}.composed", basename),
    ])?;

    // re-build the offsets and compare with original
    let final_offsets = format!("{}-final.{}", basename, OFFSETS_EXTENSION);
    let final_offsets_orig = format!("{}-final.{}.orig", basename, OFFSETS_EXTENSION);
    std::fs::rename(&final_offsets, &final_offsets_orig)?;
    cli_main(vec![
        "webgraph",
        "build",
        "offsets",
        &format!("{}-final", basename),
    ])?;
    // check that the offsets are the same
    let original_offsets = std::fs::read(&final_offsets_orig)?;
    let finalified_offsets = std::fs::read(final_offsets)?;
    assert_eq!(
        original_offsets, finalified_offsets,
        "Offsets do not match after finalification"
    );
    std::fs::remove_file(final_offsets_orig)?;

    log::info!("Step 9: Creates the final Elias Fano");
    cli_main(vec![
        "webgraph",
        "build",
        "ef",
        &format!("{}-final", basename),
    ])?;

    // Load the created graph, and check that it is the same as the original
    let original = webgraph::graphs::bvgraph::BvGraph::with_basename(TEST_GRAPH)
        .endianness::<BigEndian>()
        .load()?;

    let final_graph =
        webgraph::graphs::bvgraph::BvGraph::with_basename(format!("{}-final", basename))
            .endianness::<BigEndian>()
            .load()?;

    assert_eq!(original.num_nodes(), final_graph.num_nodes());
    assert_eq!(original.num_arcs(), final_graph.num_arcs());

    let permutation =
        JavaPermutation::mmap(format!("{}.composed", basename), MmapFlags::RANDOM_ACCESS)?;

    for node in 0..original.num_nodes() {
        assert_eq!(
            original.outdegree(node),
            final_graph.outdegree(permutation.get(node))
        );
        let mut original_succ = original
            .successors(node)
            .map(|succ| permutation.get(succ))
            .collect::<Vec<usize>>();
        original_succ.sort_unstable();
        let final_succ = final_graph
            .successors(permutation.get(node))
            .collect::<Vec<usize>>();
        assert_eq!(original_succ, final_succ);
    }

    log::info!(
        "Loading graph at {}",
        format!("{}-final.{}", basename, GRAPH_EXTENSION)
    );
    let graph_size = std::fs::metadata(format!("{}-final.{}", basename, GRAPH_EXTENSION))?.len();
    let bits_per_arc = 8.0 * graph_size as f64 / original.num_arcs() as f64;
    log::info!("Graph size: {} bytes", graph_size);
    log::info!("Bits per arc: {}", bits_per_arc);
    assert!(
        bits_per_arc < 1.05 * EXPECTED_BITS_PER_ARC, //  allow 5% error
        "The graph is too large: {:.3} bits per arc, the expected is {:.3}",
        bits_per_arc,
        EXPECTED_BITS_PER_ARC
    );

    Ok(())
}
