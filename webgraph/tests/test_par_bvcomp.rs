/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![cfg(not(miri))]

mod common;

use std::path::PathBuf;

use anyhow::Result;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use log::info;
use webgraph::prelude::*;

#[test]
fn test_par_bvcomp() -> Result<()> {
    env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init()?;

    _test_par_bvcomp(&common::cnr_2000_basename())?;
    Ok(())
}

fn _test_par_bvcomp(basename: &std::path::Path) -> Result<()> {
    let comp_flags = CompFlags::default();
    let tmp_basename: PathBuf = format!("{}-par", basename.display()).into();

    // load the graph
    let graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(basename)
        .endianness::<BE>()
        .load()?;

    let mut graph_filename = PathBuf::from(basename);
    graph_filename.set_extension(GRAPH_EXTENSION);

    let expected_size = graph_filename.metadata()?.len();
    for thread_num in 1..10 {
        log::info!("Testing with {} threads", thread_num);
        // create a threadpool and make the compression use it, this way
        // we can test with different number of threads
        let start = std::time::Instant::now();
        // recompress the graph in parallel
        BvCompConf::new(&tmp_basename)
            .comp_flags(comp_flags)
            .par_comp::<BE, _>(&graph)?;

        log::info!("The compression took: {}s", start.elapsed().as_secs_f64());

        let found_size = std::fs::File::open(tmp_basename.with_extension(GRAPH_EXTENSION))?
            .metadata()?
            .len();

        if (found_size as f64) > (expected_size as f64) * 1.2 {
            panic!(
                "The compressed graph is too big: {} > {}",
                found_size, expected_size
            );
        }

        let comp_graph =
            webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&tmp_basename)
                .endianness::<BE>()
                .load()?;

        info!("Checking that the newly compressed graph is equivalent to the original one...");
        graph::eq(&graph, &comp_graph)?;

        let offsets_path = tmp_basename.with_extension(OFFSETS_EXTENSION);
        let mut offsets_reader = buf_bit_reader::from_path::<BE, u32>(&offsets_path)?;
        let mut pr = ProgressLogger::default();
        pr.display_memory(true)
            .item_name("node")
            .expected_updates(graph.num_nodes());
        pr.start("Checking that the generated offsets are correct...");

        let mut offset = 0;
        for (real_offset, _degree) in comp_graph.offset_deg_iter().by_ref() {
            let gap_offset = offsets_reader.read_gamma().unwrap();
            offset += gap_offset;
            assert_eq!(offset, real_offset);
            pr.light_update();
        }
        pr.done();

        // cancel the file at the end
        std::fs::remove_file(tmp_basename.with_extension(GRAPH_EXTENSION))?;
        std::fs::remove_file(tmp_basename.with_extension(OFFSETS_EXTENSION))?;
        std::fs::remove_file(tmp_basename.with_extension(PROPERTIES_EXTENSION))?;
        log::info!("\n");
    }

    Ok(())
}

#[test]
fn test_par_comp_empty_interior_chunk_ok() -> Result<()> {
    use lender::prelude::*;
    use webgraph::graphs::par_graphs::ParGraph;
    // Regression: a valid IntoParLenders implementation with an empty
    // non-tail segment sent no message for it, and the ordered merge
    // silently wrote a truncated graph while stamping the full node count
    // in the properties. Empty segments are now reported explicitly and the
    // merge produces the full graph.
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("interior");
    let pg = ParGraph::with_cutpoints(graph.clone(), vec![0, 0, 3]);
    BvComp::with_basename(&basename).par_comp::<BE, _>(&pg)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    let mut arcs = vec![];
    let mut iter = seq.iter();
    while let Some((node, succ)) = iter.next() {
        for s in succ {
            arcs.push((node, s));
        }
    }
    assert_eq!(arcs, vec![(0, 1), (1, 2), (2, 0)]);
    Ok(())
}

#[test]
fn test_par_comp_worker_error_propagates() -> Result<()> {
    use webgraph::graphs::vec_graph::LabeledVecGraph;
    use webgraph::traits::{StoreLabels, StoreLabelsConf};
    // Regression: worker I/O and label-store failures were unwrapped inside
    // the rayon scope, panicking instead of returning the contextual error.
    struct FailingStore;
    impl StoreLabels for FailingStore {
        type Label = u32;
        fn init(&mut self) -> Result<()> {
            Ok(())
        }
        fn push_node(&mut self) -> Result<()> {
            Ok(())
        }
        fn push_label(&mut self, _label: &u32) -> Result<()> {
            anyhow::bail!("label storage failed")
        }
        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
        fn label_written_bits(&self) -> u64 {
            0
        }
        fn offsets_written_bits(&self) -> u64 {
            0
        }
    }
    struct FailingConf;
    impl StoreLabelsConf for FailingConf {
        type StoreLabels = FailingStore;
        fn new_storage(
            &self,
            _labels_path: &std::path::Path,
            _offsets_path: &std::path::Path,
        ) -> Result<FailingStore> {
            Ok(FailingStore)
        }
        fn init_concat(
            &mut self,
            _labels_path: &std::path::Path,
            _offsets_path: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        fn concat_part(
            &mut self,
            _labels_path: &std::path::Path,
            _labels_written_bits: u64,
            _offsets_path: &std::path::Path,
            _offsets_written_bits: u64,
        ) -> Result<()> {
            Ok(())
        }
        fn flush_concat(&mut self) -> Result<()> {
            Ok(())
        }
        fn label_serializer_name(&self) -> String {
            "()".into()
        }
    }

    let graph = LabeledVecGraph::from_arcs([((0, 1), 10u32), ((1, 2), 20)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("failing");
    let res = BvComp::with_basename(&basename).par_comp_labeled::<BE, _, _>(&graph, FailingConf);
    let err = match res {
        Err(e) => e,
        Ok(_) => anyhow::bail!("expected error, got Ok"),
    };
    assert!(
        format!("{err:#}").contains("label storage failed"),
        "unexpected error chain: {err:#}"
    );
    Ok(())
}

#[test]
fn test_par_comp_empty_tail_chunk_ok() -> Result<()> {
    use webgraph::graphs::par_graphs::ParGraph;
    // Empty tail segments are benign: everything up to num_nodes is covered.
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("tail");
    let pg = ParGraph::with_cutpoints(graph, vec![0, 3, 3]);
    BvComp::with_basename(&basename).par_comp::<BE, _>(&pg)?;
    let seq = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .mode::<LoadMem>()
        .load()?;
    assert_eq!(seq.num_nodes(), 3);
    Ok(())
}

#[test]
fn test_par_comp_preserves_custom_tmp_dir() -> Result<()> {
    // Regression: cleanup used to remove_dir_all the caller-supplied
    // temporary directory, deleting any preexisting content.
    let graph = webgraph::graphs::vec_graph::VecGraph::from_arcs([(0, 1), (1, 2), (2, 0)]);
    let tmp = tempfile::tempdir()?;
    let custom = tmp.path().join("scratch");
    std::fs::create_dir_all(&custom)?;
    let sentinel = custom.join("sentinel.txt");
    std::fs::write(&sentinel, "keep me")?;
    let basename = tmp.path().join("graph");
    BvComp::with_basename(&basename)
        .tmp_dir(&custom)
        .par_comp::<BE, _>(&graph)?;
    assert!(sentinel.exists(), "sentinel was removed");
    // The per-compression work subdirectory has been cleaned up.
    assert_eq!(
        std::fs::read_dir(&custom)?.count(),
        1,
        "leftover temporary entries in the custom dir"
    );
    Ok(())
}
