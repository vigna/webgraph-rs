/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{GranularityArgs, LogIntervalArg, NumThreadsArg};
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use log::info;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use sux::prelude::*;
use sux::traits::TryIntoUnaligned;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "dcf", about = "Builds the Elias\u{2013}Fano representation of the degree cumulative function of a graph.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the graph.​
    pub basename: PathBuf,

    #[arg(short, long)]
    /// Uses the sequential algorithm (does not need the .ef file).​
    pub sequential: bool,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub granularity: GranularityArgs,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => {
            if args.sequential {
                seq_build_dcf::<BE>(args)
            } else {
                par_build_dcf::<BE>(args)
            }
        }
        #[cfg(feature = "le_bins")]
        LE::NAME => {
            if args.sequential {
                seq_build_dcf::<LE>(args)
            } else {
                par_build_dcf::<LE>(args)
            }
        }
        e => panic!("Unknown endianness: {}", e),
    }
}

fn build_and_serialize(efb: EliasFanoBuilder<u64>, ef_path: &std::path::Path) -> Result<()> {
    let ef = efb.build();
    let ef: DCF = unsafe {
        ef.map_high_bits(|bits| {
            SelectZeroAdaptConst::<_, _, LOG2_ONES_PER_INVENTORY, LOG2_WORDS_PER_SUBINVENTORY>::new(
                SelectAdaptConst::<_, _, LOG2_ONES_PER_INVENTORY, LOG2_WORDS_PER_SUBINVENTORY>::new(
                    bits,
                ),
            )
        })
        .try_into_unaligned()?
    };

    info!("Writing to disk...");

    let mut ef_file = BufWriter::new(
        File::create(ef_path).with_context(|| format!("Could not create {}", ef_path.display()))?,
    );

    unsafe {
        ef.serialize(&mut ef_file).with_context(|| {
            format!(
                "Could not serialize degree cumulative list to {}",
                ef_path.display()
            )
        })
    }?;

    info!("Completed.");
    Ok(())
}

pub fn par_build_dcf<E: Endianness>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek + Clone + Send + Sync,
{
    let basename = args.basename;

    let has_ef =
        std::fs::metadata(basename.with_extension(EF_EXTENSION)).is_ok_and(|x| x.is_file());
    if !has_ef {
        log::warn!(SEQ_PROC_WARN![], basename.display());
        return seq_build_dcf::<E>(CliArgs {
            basename,
            sequential: true,
            num_threads: args.num_threads,
            granularity: args.granularity,
            log_interval: args.log_interval,
        });
    }

    let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
    let f = File::open(&properties_path).with_context(|| {
        format!(
            "Could not open properties file: {}",
            properties_path.display()
        )
    })?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;
    let num_arcs = map.get("arcs").unwrap().parse::<u64>()?;

    let graph = webgraph::graphs::bvgraph::random_access::BvGraph::with_basename(&basename)
        .endianness::<E>()
        .load()
        .with_context(|| format!("Could not load graph at {}", basename.display()))?;

    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);

    let node_granularity = args
        .granularity
        .into_granularity()
        .node_granularity(num_nodes, Some(num_arcs));

    let mut pl = concurrent_progress_logger![
        item_name = "node",
        display_memory = true,
        expected_updates = Some(num_nodes),
    ];
    if let Some(duration) = args.log_interval.log_interval {
        pl.log_interval(duration);
    }

    let num_threads = args.num_threads.num_threads;
    pl.start(format!(
        "Building the degree cumulative function in parallel using {} threads",
        num_threads
    ));

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, num_arcs);
    efb.push(0);

    let num_chunks = num_nodes.div_ceil(node_granularity);
    let mut cumul_deg = 0;

    thread_pool.install(|| {
        use webgraph::traits::ParMapFold;
        (0..num_chunks).par_map_fold_ord_with(
            pl.clone(),
            |pl, chunk_idx| {
                let start = chunk_idx * node_granularity;
                let end = num_nodes.min(start + node_granularity);
                let degs: Box<[usize]> = (start..end).map(|node| graph.outdegree(node)).collect();
                pl.update_with_count(end - start);
                degs
            },
            &mut efb,
            |efb, degs| {
                for &deg in degs.iter() {
                    cumul_deg += deg as u64;
                    efb.push(cumul_deg);
                }
                efb
            },
        );
    });

    pl.done();

    let ef_path = basename.with_extension(DEG_CUMUL_EXTENSION);
    build_and_serialize(efb, &ef_path)
}

pub fn seq_build_dcf<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let basename = args.basename;
    let properties_path = basename.with_extension(PROPERTIES_EXTENSION);
    let f = File::open(&properties_path).with_context(|| {
        format!(
            "Could not open properties file: {}",
            properties_path.display()
        )
    })?;
    let map = java_properties::read(BufReader::new(f))?;
    let num_nodes = map.get("nodes").unwrap().parse::<usize>()?;
    let num_arcs = map.get("arcs").unwrap().parse::<u64>()?;

    let mut efb = EliasFanoBuilder::new(num_nodes + 1, num_arcs);

    let mut pl = progress_logger![
        display_memory = true,
        item_name = "node",
        expected_updates = Some(num_nodes),
    ];
    if let Some(duration) = args.log_interval.log_interval {
        pl.log_interval(duration);
    }
    let seq_graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&basename)
        .endianness::<E>()
        .load()
        .with_context(|| format!("Could not load graph at {}", basename.display()))?;

    pl.start("Building the degree cumulative function...");
    let iter = seq_graph.offset_deg_iter();
    let mut cumul_deg = 0;

    efb.push(0);
    for (_new_offset, degree) in iter {
        cumul_deg += degree as u64;
        efb.push(cumul_deg);
        pl.light_update();
    }
    pl.done();

    let ef_path = basename.with_extension(DEG_CUMUL_EXTENSION);
    build_and_serialize(efb, &ef_path)
}
