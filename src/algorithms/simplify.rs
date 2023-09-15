use crate::prelude::COOIterToGraph;
use crate::traits::SequentialGraph;
use crate::utils::{BatchIterator, DedupSortedIter, KMergeIters, SortPairs};
use anyhow::Result;
use dsi_progress_logger::ProgressLogger;

/// Make the graph undirected and remove selfloops
#[allow(clippy::type_complexity)]
pub fn simplify<G: SequentialGraph>(
    graph: G,
    batch_size: usize,
) -> Result<
    COOIterToGraph<
        DedupSortedIter<
            core::iter::Filter<
                core::iter::Map<
                    KMergeIters<BatchIterator>,
                    fn((usize, usize, ())) -> (usize, usize),
                >,
                fn(&(usize, usize)) -> bool,
            >,
        >,
    >,
> {
    let dir = tempfile::tempdir()?;
    let mut sorted = SortPairs::new(batch_size, dir.into_path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name = "node";
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Creating batches...");
    // create batches of sorted edges
    for (src, succ) in graph.iter_nodes() {
        for dst in succ {
            if src != dst {
                sorted.push(src, dst)?;
                sorted.push(dst, src)?;
            }
        }
        pl.light_update();
    }
    // merge the batches
    let map: fn((usize, usize, ())) -> (usize, usize) = |(src, dst, _)| (src, dst);
    let filter: fn(&(usize, usize)) -> bool = |(src, dst)| src != dst;
    let iter = DedupSortedIter::new(sorted.iter()?.map(map).filter(filter));
    let sorted = COOIterToGraph::new(graph.num_nodes(), iter);
    pl.done();

    Ok(sorted)
}
