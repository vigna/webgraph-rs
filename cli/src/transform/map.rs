/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::*;
use anyhow::Result;
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use std::path::PathBuf;
use tempfile::Builder;
use value_traits::slices::SliceByValue;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "map", about = "Maps a BvGraph through an arbitrary function on nodes, deduplicating arcs.", long_about = None)]
pub struct CliArgs {
    /// The basename of the source graph.
    pub src: PathBuf,
    /// The basename of the mapped graph.
    pub dst: PathBuf,

    /// The path to the map to apply to the graph.
    pub map: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the map file.
    pub fmt: IntSliceFormat,

    #[arg(long)]
    /// The number of nodes of the resulting graph; if not specified, it is
    /// computed as one plus the maximum value in the map; if specified, it must
    /// be strictly greater than the maximum value in the map.
    pub num_nodes: Option<usize>,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub memory_usage: MemoryUsageArg,

    #[clap(flatten)]
    pub ca: CompressArgs,

    #[arg(long)]
    /// Use the degree cumulative function to balance work by arcs rather than
    /// by nodes; the DCF must have been pre-built with `webgraph build dcf`.
    pub dcf: bool,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => map::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => map::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn map<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let use_dcf = args.dcf;
    let src = args.src.clone();

    let target_endianness = args.ca.endianness.clone().unwrap_or_else(|| E::NAME.into());

    let dir = Builder::new().prefix("transform_map_").tempdir()?;
    let chunk_size = args.ca.chunk_size;
    let bvgraphz = args.ca.bvgraphz;
    let mut builder = BvCompConfig::new(&args.dst)
        .with_comp_flags(args.ca.into())
        .with_tmp_dir(&dir);

    if bvgraphz {
        builder = builder.with_chunk_size(chunk_size);
    }

    let loaded = args.fmt.load(&args.map)?;
    let cli_num_nodes = args.num_nodes;
    let memory_usage = args.memory_usage.memory_usage;
    let src_basename = args.src;

    dispatch_int_slice!(loaded, |node_map| {
        // Compute the number of nodes of the resulting graph
        let max_mapped = (0..node_map.len())
            .map(|i| node_map.index_value(i))
            .max()
            .unwrap_or(0);
        let num_nodes = match cli_num_nodes {
            Some(n) => {
                anyhow::ensure!(
                    n > max_mapped,
                    "The specified number of nodes ({}) is not strictly greater than the maximum mapped node ({})",
                    n,
                    max_mapped,
                );
                n
            }
            None => max_mapped + 1,
        };

        // if the .ef file exists, we can use map_split
        if std::fs::metadata(src_basename.with_extension(EF_EXTENSION)).is_ok_and(|x| x.is_file()) {
            log::info!(".ef file found, using map split");
            let graph =
                webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&src_basename)
                    .endianness::<E>()
                    .load()?;

            thread_pool.install(|| {
                log::info!("Mapping graph with memory usage {}", memory_usage);
                let start = std::time::Instant::now();
                let sorted =
                    webgraph::transform::map_split(&graph, &node_map, num_nodes, memory_usage)?;
                log::info!(
                    "Mapped the graph. It took {:.3} seconds",
                    start.elapsed().as_secs_f64()
                );

                let pairs: Vec<_> = sorted.into();
                match target_endianness.as_str() {
                    #[cfg(any(
                        feature = "be_bins",
                        not(any(feature = "be_bins", feature = "le_bins"))
                    ))]
                    BE::NAME => builder.par_comp_lenders::<BE, _>(pairs.into_iter(), num_nodes),
                    #[cfg(any(
                        feature = "le_bins",
                        not(any(feature = "be_bins", feature = "le_bins"))
                    ))]
                    LE::NAME => builder.par_comp_lenders::<LE, _>(pairs.into_iter(), num_nodes),
                    e => anyhow::bail!("Unknown endianness: {}", e),
                }
            })?;

            return Ok(());
        }

        log::warn!(SEQ_PROC_WARN![], src_basename.display());

        let seq_graph =
            webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&src_basename)
                .endianness::<E>()
                .load()?;

        log::info!("Mapping graph with memory usage {}", memory_usage);
        let start = std::time::Instant::now();
        let sorted = webgraph::transform::map(&seq_graph, &node_map, num_nodes, memory_usage)?;
        log::info!(
            "Mapped the graph. It took {:.3} seconds",
            start.elapsed().as_secs_f64()
        );

        thread_pool.install(|| {
            let cp = crate::cutpoints(&src, sorted.num_nodes(), sorted.num_arcs_hint(), use_dcf)?;
            builder.par_comp_lenders_endianness_at(&sorted, &target_endianness, cp)
        })?;

        Ok(())
    })
}
