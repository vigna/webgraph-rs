/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![cfg(not(miri))]

mod common;

use anyhow::Result;
use common::{roundtrip, test_graph};
use dsi_bitstream::prelude::*;
use webgraph::graphs::bvgraph::check_offsets;
use webgraph::graphs::par_graphs::ParGraph;
use webgraph::prelude::*;

#[test]
fn test_par_bvcomp() -> Result<()> {
    common::init_logger();
    let basename = common::cnr_2000_basename();
    let graph = BvGraphSeq::with_basename(&basename)
        .endianness::<BE>()
        .load()?;
    let expected_size = basename.with_extension(GRAPH_EXTENSION).metadata()?.len();
    let (_dir, tmp_basename) = common::tmp_basename()?;

    for num_threads in 1..10 {
        log::info!("Testing with {num_threads} threads");
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()?
            .install(|| BvComp::with_basename(&tmp_basename).par_comp::<BE, _>(&graph))?;

        let found_size = tmp_basename
            .with_extension(GRAPH_EXTENSION)
            .metadata()?
            .len();
        assert!(
            (found_size as f64) <= (expected_size as f64) * 1.2,
            "The compressed graph is too big: {found_size} > {expected_size}"
        );

        let comp_graph = BvGraphSeq::with_basename(&tmp_basename)
            .endianness::<BE>()
            .load()?;
        graph::eq(&graph, &comp_graph)?;
        assert!(check_offsets(&comp_graph, &tmp_basename)?);
    }

    Ok(())
}

#[test]
fn test_par_comp_empty_chunks_ok() -> Result<()> {
    // Regression: a valid IntoParLenders implementation with an empty
    // non-tail segment sent no message for it, and the ordered merge
    // silently wrote a truncated graph while stamping the full node count
    // in the properties. Empty segments are now reported explicitly and the
    // merge produces the full graph. Empty tail segments are benign.
    let graph = test_graph();
    for cutpoints in [vec![0, 0, 8], vec![0, 3, 3, 8], vec![0, 8, 8]] {
        let par_graph = ParGraph::with_cutpoints(graph.clone(), cutpoints);
        roundtrip::<BE>(&graph, |basename| {
            BvComp::with_basename(basename).par_comp::<BE, _>(&par_graph)
        })?;
    }
    Ok(())
}

#[test]
fn test_par_comp_worker_error_propagates() -> Result<()> {
    use common::FailingStore;
    use std::marker::PhantomData;
    use std::path::Path;
    use webgraph::graphs::vec_graph::LabeledVecGraph;
    use webgraph::traits::StoreLabelsConf;
    // Regression: worker I/O and label-store failures were unwrapped inside
    // the rayon scope, panicking instead of returning the contextual error.
    struct FailingConf;
    impl StoreLabelsConf for FailingConf {
        type StoreLabels = FailingStore<u32>;
        fn new_storage(
            &self,
            _labels_path: &Path,
            _offsets_path: &Path,
        ) -> Result<Self::StoreLabels> {
            Ok(FailingStore(PhantomData))
        }
        fn init_concat(&mut self, _labels_path: &Path, _offsets_path: &Path) -> Result<()> {
            Ok(())
        }
        fn concat_part(
            &mut self,
            _labels_path: &Path,
            _labels_written_bits: u64,
            _offsets_path: &Path,
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
    let (_dir, basename) = common::tmp_basename()?;
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
fn test_par_comp_preserves_custom_tmp_dir() -> Result<()> {
    // Regression: cleanup used to remove_dir_all the caller-supplied
    // temporary directory, deleting any preexisting content.
    let graph = test_graph();
    let tmp = tempfile::tempdir()?;
    let custom = tmp.path().join("scratch");
    std::fs::create_dir_all(&custom)?;
    let sentinel = custom.join("sentinel.txt");
    std::fs::write(&sentinel, "keep me")?;
    roundtrip::<BE>(&graph, |basename| {
        BvComp::with_basename(basename)
            .tmp_dir(&custom)
            .par_comp::<BE, _>(&graph)
    })?;
    assert!(sentinel.exists(), "sentinel was removed");
    // The per-compression work subdirectory has been cleaned up.
    assert_eq!(
        std::fs::read_dir(&custom)?.count(),
        1,
        "leftover temporary entries in the custom dir"
    );
    Ok(())
}
