/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{build_info, num_threads_parser, pretty_print_elapsed, IntVectorFormat};
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{progress_logger, ProgressLog};
use std::path::PathBuf;
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::prelude::*;
use webgraph_algo::thread_pool;

use super::GlobalArgs;

#[derive(Parser, Debug)]
#[command(name = "webgraph-sccs", version=build_info::version_string())]
/// Computes the strongly connected components of a graph of given basename.
/// The resulting data is saved in files stemmed from the given basename with
/// extensions .sccs and .sccsizes.
#[doc = include_str!("common_env.txt")]
pub struct Cli {
    #[clap(flatten)]
    global_args: GlobalArgs,
    #[clap(flatten)]
    args: CliArgs,
}

#[derive(Parser, Debug)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,

    #[arg(short, long)]
    /// Compute the size of the strongly connected components.
    pub sizes: bool,

    #[arg(short, long)]
    /// Renumber components in decreasing-size order (implicitly, compute sizes).
    pub renumber: bool,

    #[arg(short = 'j', long, default_value_t = rayon::current_num_threads().max(1), value_parser = num_threads_parser)]
    /// The number of threads to use to compute the sizes of the components.
    pub num_threads: usize,

    #[arg(long, value_enum, default_value_t = IntVectorFormat::Ascii)]
    /// The storage format for components and component sizes.
    pub fmt: IntVectorFormat,
}

pub fn cli_main<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let start = std::time::Instant::now();
    let cli = Cli::parse_from(args);
    main(cli.global_args, cli.args)?;

    log::info!(
        "The command took {}",
        pretty_print_elapsed(start.elapsed().as_secs_f64())
    );

    Ok(())
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => sccs::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => sccs::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn sccs<E: Endianness>(global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MemoryFactory<E, MmapHelper<u32>>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, LoadMmap>: BitSeek,
{
    log::info!("Loading the graph from {}", args.basename.display());
    let graph = BvGraph::with_basename(&args.basename)
        .mode::<LoadMmap>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .endianness::<E>()
        .load()?;

    let mut pl = progress_logger![];
    if let Some(log_interval) = global_args.log_interval {
        pl.log_interval(log_interval);
    }

    let mut sccs = webgraph_algo::sccs::tarjan(graph, &mut pl);
    log::info!(
        "Found {} strongly connected components",
        sccs.num_components()
    );

    let path = args.basename.with_extension("sccs");
    let sizes_path = args.basename.with_extension("sccsizes");

    if args.renumber {
        log::info!("Renumbering components by decreasing size");
        let component_sizes = if args.num_threads == 1 {
            log::debug!("Using sequential algorithm");
            sccs.sort_by_size()
        } else {
            log::debug!("Using parallel algorithm with {} threads", args.num_threads);
            let thread_pool = thread_pool![args.num_threads];
            thread_pool.install(|| sccs.par_sort_by_size())
        };
        let max = component_sizes.first().copied();
        args.fmt.store_usizes(sizes_path, &component_sizes, max)?;
    } else if args.sizes {
        log::info!("Computing the sizes of the components");
        let sizes = sccs.compute_sizes();
        args.fmt.store_usizes(sizes_path, &sizes, None)?;
    };

    args.fmt
        .store_usizes(path, sccs.components(), Some(sccs.num_components()))?;

    Ok(())
}
