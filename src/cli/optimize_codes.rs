/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use lender::*;
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "optimize-codes";

#[derive(Args, Debug)]
#[command(about = "Reads a graph and suggests the best codes to use.", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => optimize_codes::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => optimize_codes::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

fn optimize_codes<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    let graph = BVGraphSeq::with_basename(args.basename)
        .endianness::<E>()
        .load()?
        .map_factory(StatsDecoderFactory::new);

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("node")
        .expected_updates(Some(graph.num_nodes()));

    pl.start("Scanning...");

    let mut iter = graph.iter();
    while iter.next().is_some() {
        pl.light_update();
    }
    pl.done();

    drop(iter); // This releases the decoder and updates the global stats
    let stats = graph.into_inner().stats();

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
