/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{
    cli::{GranularityArgs, NumThreadsArg},
    prelude::*,
};
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "codes";

#[derive(Args, Debug)]
#[command(about = "Reads a graph and suggests the best codes to use.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub granularity: GranularityArgs,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.src)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => optimize_codes::<BE>(submatches, args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => optimize_codes::<LE>(submatches, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

/// Returns ranges of nodes to process in parallel of size `chunk_size` each,
/// with the last chunk possibly being smaller.
/// The equivalent of `std::iter::Chunks` but with a `Range` instead of a `Slice`.
pub struct Chunks {
    total: core::ops::Range<usize>,
    chunk_size: usize,
}

impl Chunks {
    pub fn new(total: core::ops::Range<usize>, chunk_size: usize) -> Self {
        Self { total, chunk_size }
    }
}

impl Iterator for Chunks {
    type Item = core::ops::Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.total.start < self.total.end {
            let end = (self.total.start + self.chunk_size).min(self.total.end);
            let range = self.total.start..end;
            self.total.start = end;
            Some(range)
        } else {
            None
        }
    }
}

pub fn optimize_codes<E: Endianness>(submatches: &ArgMatches, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let mut stats = Default::default();
    let has_ef = std::fs::metadata(args.src.with_extension("ef")).is_ok_and(|x| x.is_file());

    if has_ef {
        log::info!(
            "Analyzing codes in parallel using {} threads",
            args.num_threads.num_threads
        );
        let graph = BvGraph::with_basename(&args.src).endianness::<E>().load()?;

        let mut pl = concurrent_progress_logger![item_name = "node"];
        pl.display_memory(true)
            .expected_updates(Some(graph.num_nodes()));
        pl.start("Scanning...");

        if let Some(duration) = submatches.get_one("log-interval") {
            pl.log_interval(*duration);
        }

        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(args.num_threads.num_threads)
            .build()?;

        let granularity = args
            .granularity
            .into_granularity()
            .unwrap_or_default()
            .granularity(graph.num_nodes(), args.num_threads.num_threads);

        // TODO!: use FairChunks with the offsets EF to distribute the
        // work based on number of bits used, not nodes
        stats = Chunks::new(0..graph.num_nodes(), granularity).par_map_fold_with(
            pl.clone(),
            |pl, range| {
                let mut iter = graph
                    .offset_deg_iter_from(range.start)
                    .map_decoder(|d| StatsDecoder::new(d, Default::default()));

                for _ in (&mut iter).take(range.len()) {
                    pl.light_update();
                }

                let mut stats = Default::default();
                iter.map_decoder(|d| {
                    stats = d.stats;
                    d.codes_reader // not important but we need to return something
                });
                stats
            },
            |mut acc1, acc2| {
                acc1 += &acc2;
                acc1
            },
            &thread_pool,
        );

        pl.done();
    } else {
        if args.num_threads.num_threads != 1 {
            log::info!("Analyzing codes sequentially, this might be faster if you build the Elias-Fano index using `webgraph build ef {}` which will generate file {}", args.src.display(), args.src.with_extension("ef").display());
        }

        let graph = BvGraphSeq::with_basename(args.src)
            .endianness::<E>()
            .load()?;

        let mut pl = ProgressLogger::default();
        pl.display_memory(true)
            .item_name("node")
            .expected_updates(Some(graph.num_nodes()));

        pl.start("Scanning...");

        // add the stats wrapper to the decoder
        let mut iter = graph
            .offset_deg_iter()
            .map_decoder(|d| StatsDecoder::new(d, Default::default()));
        // iterate over the graph
        for _ in iter.by_ref() {
            pl.light_update();
        }
        pl.done();
        // extract the stats
        iter.map_decoder(|d| {
            stats = d.stats;
            d.codes_reader // not important but we need to return something
        });
    }

    macro_rules! impl_best_code {
        ($new_bits:expr, $old_bits:expr, $stats:expr, $($code:ident - $old:expr),*) => {
            println!("{:>17} {:>16} {:>12} {:>8} {:>10} {:>16}",
                "Type", "Code", "Improvement", "Weight", "Bytes", "Bits",
            );
            $(
                let (_, new) = $stats.$code.best_code();
                $new_bits += new;
                $old_bits += $old;
            )*

            $(
                let (code, new) = $stats.$code.best_code();
                println!("{:>17} {:>16} {:>12} {:>8} {:>10} {:>16}",
                    stringify!($code), format!("{:?}", code),
                    format!("{:.3}%", 100.0 * ($old - new) as f64 / $old as f64),
                    format!("{:.3}", (($old - new) as f64 / ($old_bits - $new_bits) as f64)),
                    normalize(($old - new) as f64 / 8.0),
                    $old - new,
                );
            )*
        };
    }

    let mut new_bits = 0;
    let mut old_bits = 0;
    impl_best_code!(
        new_bits,
        old_bits,
        stats,
        outdegrees - stats.outdegrees.gamma,
        reference_offsets - stats.reference_offsets.unary,
        block_counts - stats.block_counts.gamma,
        blocks - stats.blocks.gamma,
        interval_counts - stats.interval_counts.gamma,
        interval_starts - stats.interval_starts.gamma,
        interval_lens - stats.interval_lens.gamma,
        first_residuals - stats.first_residuals.zeta[2],
        residuals - stats.residuals.zeta[2]
    );

    println!();
    println!(" Old bit size: {:>16}", old_bits);
    println!(" New bit size: {:>16}", new_bits);
    println!("   Saved bits: {:>16}", old_bits - new_bits);

    println!("Old byte size: {:>16}", normalize(old_bits as f64 / 8.0));
    println!("New byte size: {:>16}", normalize(new_bits as f64 / 8.0));
    println!(
        "  Saved bytes: {:>16}",
        normalize((old_bits - new_bits) as f64 / 8.0)
    );

    println!(
        "  Improvement: {:>15.3}%",
        100.0 * (old_bits - new_bits) as f64 / old_bits as f64
    );
    Ok(())
}

fn normalize(mut value: f64) -> String {
    let mut uom = ' ';
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'K';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'M';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'G';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'T';
    }
    format!("{:.3}{}", value, uom)
}
