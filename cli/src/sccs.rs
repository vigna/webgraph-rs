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
use webgraph::thread_pool;

use super::GlobalArgs;

#[derive(Parser, Debug)]
#[command(name = "webgraph-sccs", version=build_info::version_string())]
/// Computes the strongly connected components of a graph of given basename.
///
/// Note that on shells supporting process substitution you compress the results
/// using a suitable syntax. For example on bash / zsh, you can use the path
/// `>(zstd > sccs.zstd)`.
///
/// Noteworthy environment variables:
///
/// - RUST_MIN_STACK: minimum thread stack size (in bytes); we suggest
///   RUST_MIN_STACK=8388608 (8MiB)
///
/// - TMPDIR: where to store temporary files (potentially very large ones)
///
/// - RUST_LOG: configuration for env_logger
///   <https://docs.rs/env_logger/latest/env_logger/>
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

    /// The path where to save the strongly connected components.
    pub sccs: PathBuf,

    #[arg(short = 's', long)]
    /// Compute the size of the strongly connected components and store them
    /// at the given path.
    pub sizes: Option<PathBuf>,

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
        args.fmt.store_usizes(&args.sccs, &component_sizes, max)?;
    } else if let Some(sizes_path) = args.sizes {
        log::info!("Computing the sizes of the components");
        let sizes = sccs.compute_sizes();
        args.fmt.store_usizes(sizes_path, &sizes, None)?;
    };

    args.fmt
        .store_usizes(&args.sccs, sccs.components(), Some(sccs.num_components()))?;

    Ok(())
}
