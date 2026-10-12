#![cfg(feature = "slow_tests")]

mod common;

use anyhow::Result;
use common::check_rebuilt_offsets;
use dsi_bitstream::traits::BigEndian;
use dsi_progress_logger::no_logging;
use tempfile::Builder;
use webgraph::graphs::bvgraph::{GRAPH_EXTENSION, PROPERTIES_EXTENSION};
use webgraph::traits::{RandomAccessLabeling, graph};
use webgraph::transform::permute_seq;
use webgraph::utils::MemoryUsage;
use webgraph_cli::IntSliceFormat;
use webgraph_cli::cli_main;
use webgraph_cli::init_env_logger;

const EXPECTED_BITS_PER_ARC: f64 = 2.897;

#[test]
fn test_llp_pipeline() -> Result<()> {
    init_env_logger()?;
    let copy_basename = common::cnr_2000_basename();
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
        "symmetrize",
        &basename,
        &format!("{}-simple", basename),
        "--no-loops",
        "--permutation",
        &format!("{}.bfs", basename),
    ])?;
    // Check the offsets against those rebuilt from the graph
    check_rebuilt_offsets(&format!("{basename}-simple"))?;

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
        "--seed",
        "0",
        "--no-perm",
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

    // Check the offsets against those rebuilt from the graph
    check_rebuilt_offsets(&format!("{basename}-final"))?;

    log::info!("Step 9: Creates the final Elias Fano");
    cli_main(vec![
        "webgraph",
        "build",
        "ef",
        &format!("{}-final", basename),
    ])?;

    // Check that the final graph is the original graph permuted with the
    // composed permutation
    let original = webgraph::graphs::bvgraph::BvGraph::with_basename(&copy_basename)
        .endianness::<BigEndian>()
        .load()?;
    let final_graph =
        webgraph::graphs::bvgraph::BvGraph::with_basename(format!("{}-final", basename))
            .endianness::<BigEndian>()
            .load()?;
    let permutation = IntSliceFormat::default().load(format!("{}.composed", basename))?;
    let permuted = permute_seq(
        &original,
        &permutation,
        MemoryUsage::from_perc(10.0),
        no_logging![],
    )?;
    graph::eq(&permuted, &final_graph)?;

    log::info!("Loading graph at {}-final.{}", basename, GRAPH_EXTENSION);
    let graph_size = std::fs::metadata(format!("{}-final.{}", basename, GRAPH_EXTENSION))?.len();
    let bits_per_arc = 8.0 * graph_size as f64 / original.num_arcs() as f64;
    log::info!("Graph size: {} bytes", graph_size);
    log::info!("Bits per arc: {}", bits_per_arc);
    assert!(
        bits_per_arc < 1.10 * EXPECTED_BITS_PER_ARC, //  allow 10% error
        "The graph is too large: {:.3} bits per arc, the expected is {:.3}",
        bits_per_arc,
        EXPECTED_BITS_PER_ARC
    );

    Ok(())
}
