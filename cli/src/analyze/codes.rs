/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{GlobalArgs, GranularityArgs, NumThreadsArg};
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::{dispatch::factory::CodesReaderFactoryHelper, prelude::*};
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "codes", about = "Reads a graph and suggests the best codes to use.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,

    #[clap(flatten)]
    pub granularity: GranularityArgs,

    #[clap(short = 'k', long, default_value_t = 3)]
    /// How many codes to show for each type, if k is bigger than the number of codes available
    /// all codes will be shown.
    pub top_k: usize,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => optimize_codes::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => optimize_codes::<LE>(global_args, args),
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

pub fn optimize_codes<E: Endianness>(global_args: GlobalArgs, args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    let mut stats = Default::default();
    let has_ef = std::fs::metadata(args.src.with_extension("ef")).is_ok_and(|x| x.is_file());

    // Load the compression flags from the properties file so we can compare them
    let (_, _, comp_flags) = parse_properties::<E>(args.src.with_extension(PROPERTIES_EXTENSION))?;

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

        if let Some(duration) = global_args.log_interval {
            pl.log_interval(duration);
        }

        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(args.num_threads.num_threads)
            .build()?;

        let node_granularity = args
            .granularity
            .into_granularity()
            .node_granularity(graph.num_nodes(), Some(graph.num_arcs()));

        // TODO!: use FairChunks with the offsets EF to distribute the
        // work based on number of bits used, not nodes
        stats = Chunks::new(0..graph.num_nodes(), node_granularity).par_map_fold_with(
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
            log::info!(SEQ_PROC_WARN![], args.src.display());
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

    println!("Default codes");
    compare_codes(&stats, CompFlags::default(), args.top_k);

    print!("\n\n\n");

    println!("Current codes");
    compare_codes(&stats, comp_flags, args.top_k);

    Ok(())
}

/// Gets the size in bits used by a given code.
/// This should go in dsi-bitstream eventually.
fn get_size_by_code(stats: &CodesStats, code: Codes) -> Option<u64> {
    match code {
        Codes::Unary => Some(stats.unary),
        Codes::Gamma => Some(stats.gamma),
        Codes::Delta => Some(stats.delta),
        Codes::Omega => Some(stats.omega),
        Codes::VByteBe | Codes::VByteLe => Some(stats.vbyte),
        Codes::Zeta(k) => stats.zeta.get(k - 1).copied(),
        Codes::Golomb(b) => stats.golomb.get(b as usize - 1).copied(),
        Codes::ExpGolomb(k) => stats.exp_golomb.get(k).copied(),
        Codes::Rice(k) => stats.rice.get(k).copied(),
        Codes::Pi(0) => Some(stats.gamma),   // Pi(0) is Gamma
        Codes::Pi(1) => Some(stats.zeta[1]), // Pi(1) is Zeta(2)
        Codes::Pi(k) => stats.pi.get(k - 2).copied(),
        _ => unreachable!("Code {:?} not supported", code),
    }
}

/// Prints the statistics of how much the optimal codes improve over the reference ones.
pub fn compare_codes(stats: &DecoderStats, reference: CompFlags, top_k: usize) {
    macro_rules! impl_best_code {
        ($new_bits:expr, $old_bits:expr, $stats:expr, $($code:ident -> $old:expr),*) => {
            println!("{:>17} {:>20} {:>12} {:>10} {:>10} {:>16}",
                "Type", "Code", "Improvement", "Weight", "Bytes", "Bits",
            );
            $(
                let (_, new) = $stats.$code.best_code();
                $new_bits += new;
                $old_bits += $old;
            )*

            $(
                let codes = $stats.$code.get_codes();
                let (best_code, best_size) = codes[0];

                let improvement = 100.0 * ($old - best_size) as f64 / $old as f64;
                let weight = 100.0 * ($old as f64 - best_size as f64) / ($old_bits as f64 - $new_bits as f64);

                println!("{:>17} {:>20} {:>12.3}% {:>9.3}% {:>10} {:>16}",
                    stringify!($code),
                    format!("{:?}", best_code),
                    improvement,
                    weight,
                    normalize(best_size as f64 / 8.0),
                    best_size,
                );
                for i in 1..top_k.min(codes.len()).max(1) {
                    let (code, size) = codes[i];
                    let improvement = 100.0 * ($old as f64 - size as f64) / $old as f64;
                    println!("{:>17} {:>20} {:>12.3}% {:>10.3} {:>10} {:>16}",
                        stringify!($code),
                        format!("{:?}", code),
                        improvement,
                        "",
                        normalize(size as f64 / 8.0),
                        size,
                    );
                }
                print!("\n");
            )*
        };
    }

    println!("Code optimization results against:");
    for (name, code) in [
        ("outdegrees", reference.outdegrees),
        ("reference offsets", reference.references),
        ("block counts", reference.blocks),
        ("blocks", reference.blocks),
        ("interval counts", reference.intervals),
        ("interval starts", reference.intervals),
        ("interval lengths", reference.intervals),
        ("first residuals", reference.residuals),
        ("residuals", reference.residuals),
    ] {
        println!("\t{:>18} : {:?}", name, code);
    }

    let mut new_bits = 0;
    let mut old_bits = 0;
    impl_best_code!(
        new_bits,
        old_bits,
        stats,
        outdegrees -> get_size_by_code(&stats.outdegrees, reference.outdegrees).unwrap(),
        reference_offsets -> get_size_by_code(&stats.reference_offsets, reference.references).unwrap(),
        block_counts -> get_size_by_code(&stats.block_counts, reference.blocks).unwrap(),
        blocks -> get_size_by_code(&stats.blocks, reference.blocks).unwrap(),
        interval_counts -> get_size_by_code(&stats.interval_counts, reference.intervals).unwrap(),
        interval_starts -> get_size_by_code(&stats.interval_starts, reference.intervals).unwrap(),
        interval_lens -> get_size_by_code(&stats.interval_lens, reference.intervals).unwrap(),
        first_residuals -> get_size_by_code(&stats.first_residuals, reference.residuals).unwrap(),
        residuals -> get_size_by_code(&stats.residuals, reference.residuals).unwrap()
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
