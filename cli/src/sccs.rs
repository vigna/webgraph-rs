/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{
    IntSliceFormat, LogIntervalArg, build_info, get_thread_pool, num_threads_parser,
    pretty_print_elapsed,
};
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::progress_logger;
use std::path::PathBuf;
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "webgraph-sccs", version=build_info::version_string(), max_term_width = 100, next_line_help = true, after_help = include_str!("common_env.txt"))]
/// Computes the strongly connected components of a graph.​
pub struct Cli {
    #[clap(flatten)]
    args: CliArgs,
}

#[derive(Parser, Debug)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub basename: PathBuf,

    /// Output path for the strongly connected components.​
    pub sccs: PathBuf,

    #[arg(short = 's', long)]
    /// Computes the sizes of the strongly connected components and stores them
    /// at the given path.​
    pub sizes: Option<PathBuf>,

    #[arg(short, long)]
    /// Renumbers components in decreasing-size order (implicitly, computes sizes).​
    pub renumber: bool,

    #[arg(short = 'j', long, default_value_t = rayon::current_num_threads().max(1), value_parser = num_threads_parser)]
    /// The number of threads to use to compute the sizes of the components.​
    pub num_threads: usize,

    #[arg(long, value_enum, default_value_t = IntSliceFormat::Ascii)]
    /// The storage format for components and component sizes.​
    pub fmt: IntSliceFormat,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn cli_main<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let start = std::time::Instant::now();
    let cli = Cli::parse_from(args);
    main(cli.args)?;

    log::info!(
        "The command took {}",
        pretty_print_elapsed(start.elapsed().as_secs_f64())
    );

    Ok(())
}

pub fn main(args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => sccs::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => sccs::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn sccs<E: Endianness>(args: CliArgs) -> Result<()>
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

    let mut pl = progress_logger![
        expected_updates = Some(graph.num_nodes()),
        log_interval = args.log_interval.log_interval
    ];

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
            let thread_pool = get_thread_pool(args.num_threads);
            log::debug!("Using parallel algorithm with {} threads", args.num_threads);
            thread_pool.install(|| sccs.par_sort_by_size())
        };
        if let Some(sizes_path) = &args.sizes {
            let max = component_sizes.first().copied();
            args.fmt.store(sizes_path, &component_sizes, max)?;
        }
    } else if let Some(sizes_path) = &args.sizes {
        log::info!("Computing the sizes of the components");
        let sizes = sccs.compute_sizes();
        args.fmt.store(sizes_path, &sizes, None)?;
    };

    args.fmt.store(
        &args.sccs,
        sccs.components(),
        Some(sccs.num_components() - 1),
    )?;

    Ok(())
}
