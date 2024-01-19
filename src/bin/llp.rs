/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{bail, Result};
use clap::Parser;
use dsi_bitstream::prelude::*;
use epserde::prelude::*;
use rayon::prelude::*;
use std::io::{BufWriter, Write};
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Performs an LLP round", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,

    /// A filename for the LLP permutation. It defaults to "{basename}.llp"
    perm: Option<String>,

    #[arg(short, long, allow_hyphen_values = true, use_value_delimiter = true, value_delimiter = ',', default_values_t = vec!["-0".to_string(), "-1".to_string(), "-2".to_string(), "-3".to_string(), "-4".to_string(), "-5".to_string(), "-6".to_string(), "-7".to_string(), "-8".to_string(), "-9".to_string(), "-10".to_string(), "0-0".to_string()])]
    /// The gammas to use in LLP, separated by commas. The format is given by a integer
    /// numerator (if missing, assumed to be one),
    /// a dash, and then a power-of-two exponent for the denominator. For example, -2 is 1/4, and 0-0 is 0.
    gammas: Vec<String>,

    #[arg(short, long, default_value_t = 100)]
    /// The maximum number of updates for a given ɣ.
    max_updates: usize,

    #[arg(short = 'r', long, default_value_t = 1000)]
    /// The size of the chunks each thread processes for the LLP.
    granularity: usize,

    #[arg(short, long, default_value_t = 100000)]
    /// The size of the cnunks each thread processes for the random permutation
    /// at the start of each iteration
    chunk_size: usize,

    #[clap(flatten)]
    num_cpus: NumCpusArg,

    #[arg(short, long, default_value_t = 0)]
    /// The seed to use for the prng
    seed: u64,

    #[arg(short = 'e', long)]
    /// Save the permutation in ε-serde format.
    epserde: bool,
}

fn llp_impl<E: Endianness + 'static + Send + Sync>(args: Args) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>:
        ZetaRead<E> + DeltaRead<E> + GammaRead<E> + BitSeek,
{
    let start = std::time::Instant::now();

    let perm = args
        .perm
        .unwrap_or_else(|| format!("{}.llp", args.basename));

    // load the graph
    let graph = webgraph::graph::bvgraph::load(&args.basename)?;

    // parse the gamma format
    let mut gammas = vec![];
    for gamma in args.gammas {
        let t: Vec<_> = gamma.split('-').collect();
        if t.len() != 2 {
            bail!("Invalid gamma: {}", gamma);
        }
        gammas.push(
            if t[0].is_empty() {
                1.0
            } else {
                t[0].parse::<usize>()? as f64
            } * (0.5_f64).powf(t[1].parse::<usize>()? as f64),
        );
    }

    gammas.sort_by(|a, b| a.total_cmp(b));

    // compute the LLP
    let labels = layered_label_propagation(
        &graph,
        gammas,
        Some(args.num_cpus.num_cpus),
        args.max_updates,
        args.chunk_size,
        args.granularity,
        0,
    )?;

    let mut llp_perm = (0..graph.num_nodes()).collect::<Vec<_>>();
    llp_perm.par_sort_by(|&a, &b| labels[a].cmp(&labels[b]));
    webgraph::algorithms::invert_in_place(llp_perm.as_mut_slice());

    log::info!("Elapsed: {}", start.elapsed().as_secs_f64());
    log::info!("Saving permutation...");

    if args.epserde {
        llp_perm.store(perm)?;
    } else {
        let mut file = std::fs::File::create(perm)?;
        let mut buf = BufWriter::new(&mut file);
        for word in llp_perm.into_iter() {
            buf.write_all(&word.to_be_bytes())?;
        }
    }
    log::info!("Completed..");
    Ok(())
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => llp_impl::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => llp_impl::<LE>(args),
        _ => panic!("Unknown endianness"),
    }
}
