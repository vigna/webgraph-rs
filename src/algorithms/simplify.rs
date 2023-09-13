use crate::prelude::{COOIterToGraph, COOIterToLabelledGraph, SortPairsPayload};
use crate::traits::{LabelledIterator, LabelledSequentialGraph, SequentialGraph};
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
                    KMergeIters<(), BatchIterator<()>>,
                    fn((usize, usize, ())) -> (usize, usize),
                >,
                fn(&(usize, usize)) -> bool,
            >,
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
    let filter: fn(&(usize, usize)) -> bool = |(src, dst)| src != dst;
    let iter = DedupSortedIter::new(sorted.iter()?.map(map).filter(filter));
    let sorted = COOIterToGraph::new(graph.num_nodes(), iter);
    pl.done();

    Ok(sorted)
}

/// Make the graph undirected and remove selfloops
#[allow(clippy::type_complexity)]
pub fn simplify_labelled<G: LabelledSequentialGraph>(
    graph: &G,
    batch_size: usize,
) -> Result<impl SequentialGraph>
where
    G::Label: SortPairsPayload + 'static + PartialEq,
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
    let iter = DedupSortedIter::new(sorted.iter()?);

    // merge the batches
    let sorted = COOIterToLabelledGraph::new(graph.num_nodes(), iter);
    pl.done();

    Ok(sorted)
}
