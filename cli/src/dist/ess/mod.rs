/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{GlobalArgs, NumThreadsArg};
use anyhow::{ensure, Result};
use clap::{Parser, ValueEnum};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use std::path::PathBuf;
use webgraph::{graphs::bvgraph::get_endianness, prelude::BvGraph};
use webgraph_algo::distances::exact_sum_sweep::{
    All, AllForward, Diameter, Level, Radius, RadiusDiameter,
};

#[derive(Parser, Debug)]
#[command(name = "exactsumsweep", about = "Compute radius, diameter, and possibly eccentricities using the ExactSumSweep algorithm. (WORK IN PROGRESS)", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,

    /// The transposed graph of `basename`.
    pub transposed: Option<PathBuf>,

    #[arg(short, long = "symm")]
    /// If passed, we assume that the graph is symmetric.
    pub symmetric: bool,

    /// The path where to store the forward eccentricities.
    #[arg(short, long)]
    pub forward: Option<PathBuf>,

    /// The path where to store the backward eccentricities.
    #[arg(short, long)]
    pub backward: Option<PathBuf>,

    #[arg(long, value_enum)]
    pub level: LevelArg,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
/// Enum for the level of exact sum sweep to compute.
pub enum LevelArg {
    Radius,
    Diameter,
    #[clap(name = "radius-diameter")]
    RadiusDiameter,
    #[clap(name = "all-forward")]
    AllForward,
    All,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    println!("{:#4?}", args);
    ensure!(args.symmetric || args.transposed.is_some(), "You have to either pass --transposed with with the basename of the transposed graph or --symm if the graph is symmetric.");
    ensure!(
        !(args.symmetric && args.transposed.is_some()),
        "--transposed is needed only if the graph is not symmetric."
    );
    ensure!(args.forward.is_none() || matches!(args.level, LevelArg::All | LevelArg::AllForward), "You cannot only pass --forward with --level=all or --level=all-forward as the forward eccentricites won't be computed otherwise.");
    ensure!(!(args.forward.is_none() && matches!(args.level, LevelArg::All | LevelArg::AllForward)), "If --level=all or --level=all-forward, you should pass --forward to store the computed eccentricities.");
    ensure!(!(args.backward.is_some() && args.level != LevelArg::All), "You cannot only pass --backward with --level=all as the backward eccentricites won't be computed otherwise.");
    ensure!(!(args.level == LevelArg::All && args.symmetric && args.backward.is_some()), "You cannot pass --backward with --symm and --level=all as the eccentricities of a symmetric graph are the same in both directions.");

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => exact_sum_sweep::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => exact_sum_sweep::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn exact_sum_sweep<E: Endianness>(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match args.level {
        LevelArg::Radius => {
            exact_sum_sweep_level::<E, Radius>(global_args, args)?;
        }
        LevelArg::Diameter => {
            exact_sum_sweep_level::<E, Diameter>(global_args, args)?;
        }
        LevelArg::RadiusDiameter => {
            exact_sum_sweep_level::<E, RadiusDiameter>(global_args, args)?;
        }
        LevelArg::AllForward => {
            exact_sum_sweep_level::<E, AllForward>(global_args, args)?;
        }
        LevelArg::All => {
            exact_sum_sweep_level::<E, All>(global_args, args)?;
        }
    }
    Ok(())
}

pub fn exact_sum_sweep_level<E: Endianness, L: Level>(
    global_args: GlobalArgs,
    args: CliArgs,
) -> Result<()> {
    let graph = BvGraph::with_basename(&args.basename).load()?;

    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let mut pl = concurrent_progress_logger![];
    if let Some(log_interval) = global_args.log_interval {
        pl.log_interval(log_interval);
    }

    if args.symmetric {
        let _out = L::run_symm(graph, &thread_pool, &mut pl);
    } else {
        let transpose_path = args
            .transposed
            .as_ref()
            .expect("You have to pass the transposed graph if the graph is not symmetric.");
        let transpose = BvGraph::with_basename(transpose_path).load()?;
        let _out = L::run(graph, transpose, None, &thread_pool, &mut pl);
    }

    todo!("print out and serialize the eccentricities if present");
}
