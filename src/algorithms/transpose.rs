use crate::bvgraph::{parallel_compress_sequential_iter, CompFlags};
use crate::prelude::COOIterToGraph;
use crate::traits::SequentialGraph;
use crate::utils::SortPairs;
use anyhow::Result;
use dsi_progress_logger::ProgressLogger;
use std::path::Path;

/// Create and compress the transposed version of the graph
pub fn transpose<G: SequentialGraph, P: AsRef<Path>>(
    graph: &G,
    batch_size: usize,
    dst_basename: P,
    compression_flags: CompFlags,
) -> Result<()> {
    let dst_basename = dst_basename.as_ref();
    let dir = tempfile::tempdir()?;
    let mut sorted = <SortPairs<()>>::new(batch_size, dir.into_path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name = "node";
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Creating batches...");

    for (node, succ) in graph.iter_nodes() {
        for s in succ {
            sorted.push(s, node, ())?;
        }
        pl.light_update();
    }
    let sorted = COOIterToGraph::new(
        graph.num_nodes(),
        sorted.iter()?.map(|(src, dst, _)| (src, dst)),
    );
    pl.done();

    parallel_compress_sequential_iter(
        dst_basename,
        sorted.iter_nodes(),
        graph.num_nodes(),
        compression_flags,
    )?;
    Ok(())
}
