use crate::prelude::{COOIterToGraph, COOIterToLabelledGraph, SortPairsPayload};
use crate::traits::{LabelledIterator, LabelledSequentialGraph, SequentialGraph};
use crate::utils::{BatchIterator, KMergeIters, SortPairs};
use anyhow::Result;
use dsi_progress_logger::ProgressLogger;

/// Make the graph undirected and remove selfloops
#[allow(clippy::type_complexity)]
pub fn simplify<G: SequentialGraph>(
    graph: G,
    batch_size: usize,
) -> Result<
    COOIterToGraph<
        std::iter::Map<
            KMergeIters<(), BatchIterator<()>>,
            fn((usize, usize, ())) -> (usize, usize),
        >,
    >,
> {
    let dir = tempfile::tempdir()?;
    let mut sorted = <SortPairs<()>>::new(batch_size, dir.into_path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name = "node";
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Creating batches...");
    // create batches of sorted edges
    for (src, succ) in graph.iter_nodes() {
        for dst in succ {
            if src != dst {
                sorted.push(src, dst, ())?;
                sorted.push(dst, src, ())?;
            }
        }
        pl.light_update();
    }
    // merge the batches
    let map: fn((usize, usize, ())) -> (usize, usize) = |(src, dst, _)| (src, dst);
    let sorted = COOIterToGraph::new(graph.num_nodes(), sorted.iter()?.map(map));
    pl.done();

    Ok(sorted)
}

/// Create transpose the graph and return a sequential graph view of it
#[allow(clippy::type_complexity)]
pub fn simplify_labelled<G: LabelledSequentialGraph>(
    graph: &G,
    batch_size: usize,
) -> Result<COOIterToLabelledGraph<KMergeIters<G::Label, BatchIterator<G::Label>>>>
where
    G::Label: SortPairsPayload + 'static,
    for<'a> G::SequentialSuccessorIter<'a>: LabelledIterator<Label = G::Label>,
{
    let dir = tempfile::tempdir()?;
    let mut sorted = <SortPairs<G::Label>>::new(batch_size, dir.into_path())?;

    let mut pl = ProgressLogger::default();
    pl.item_name = "node";
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Creating batches...");
    // create batches of sorted edges
    for (src, succ) in graph.iter_nodes() {
        for (dst, label) in succ.labelled() {
            if src != dst {
                sorted.push(src, dst, label)?;
                sorted.push(dst, src, label)?;
            }
        }
        pl.light_update();
    }

    // TODO!: how do we break ties on labels?
    let iter = sorted.iter()?;

    // merge the batches
    let sorted = COOIterToLabelledGraph::new(graph.num_nodes(), iter);
    pl.done();

    Ok(sorted)
}
