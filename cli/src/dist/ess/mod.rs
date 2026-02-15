/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{GlobalArgs, IntVectorFormat, NumThreadsArg};
use anyhow::{Result, ensure};
use clap::{Parser, ValueEnum};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{ProgressLog, concurrent_progress_logger};
use std::path::PathBuf;
use webgraph::{graphs::bvgraph::get_endianness, prelude::BvGraph};
use webgraph_algo::distances::exact_sum_sweep::{
    All, AllForward, Diameter, Level, Radius, RadiusDiameter,
};

#[derive(Parser, Debug)]
#[command(name = "exact-sum-sweep", about = "Computes radius, diameter, and possibly eccentricities using the ExactSumSweep algorithm (scalar values are printed on stdout).", long_about = None)]
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

    /// The items to be computed (all-forward computes forward eccentricities, all computes both forward and backward eccentricities).
    #[arg(long, value_enum)]
    pub level: LevelArg,

    #[arg(long, value_enum, default_value_t = IntVectorFormat::Ascii)]
    /// The storage format for eccentricities.
    pub fmt: IntVectorFormat,

    #[clap(flatten)]
    pub num_threads: NumThreadsArg,
}

/// The level of exact sum sweep to compute.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
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
    ensure!(
        args.symmetric || args.transposed.is_some(),
        "You have to either pass --transposed with with the basename of the transposed graph or --symm if the graph is symmetric."
    );
    ensure!(
        !(args.symmetric && args.transposed.is_some()),
        "--transposed is needed only if the graph is not symmetric."
    );
    ensure!(
        args.forward.is_none() || matches!(args.level, LevelArg::All | LevelArg::AllForward),
        "You can only pass --forward with --level=all or --level=all-forward, as the forward eccentricities won't be computed otherwise."
    );
    ensure!(
        !(args.forward.is_none() && matches!(args.level, LevelArg::All | LevelArg::AllForward)),
        "If --level=all or --level=all-forward, you should pass --forward to store the computed eccentricities."
    );
    ensure!(
        !(args.backward.is_some() && args.level != LevelArg::All),
        "You cannot only pass --backward with --level=all as the backward eccentricities won't be computed otherwise."
    );
    ensure!(
        !(args.level == LevelArg::All && args.symmetric && args.backward.is_some()),
        "You cannot pass --backward with --symm and --level=all as the eccentricities of a symmetric graph are the same in both directions."
    );

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => exact_sum_sweep::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => exact_sum_sweep::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

/// Stores eccentricities to a file using the specified format.
fn store_eccentricities(
    eccentricities: &[usize],
    path: &PathBuf,
    fmt: IntVectorFormat,
) -> Result<()> {
    fmt.store_usizes(path, eccentricities, None)
}

pub fn exact_sum_sweep<E: Endianness>(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    let graph = BvGraph::with_basename(&args.basename).load()?;

    let thread_pool = crate::get_thread_pool(args.num_threads.num_threads);
    let mut pl = concurrent_progress_logger![];
    if let Some(log_interval) = global_args.log_interval {
        pl.log_interval(log_interval);
    }

    if args.symmetric {
        match args.level {
            LevelArg::Radius => {
                let out = thread_pool.install(|| Radius::run_symm(graph, &mut pl));
                println!("Radius: {}", out.radius);
                println!("Radial vertex: {}", out.radial_vertex);
                println!("Radius iterations: {}", out.radius_iterations);
            }
            LevelArg::Diameter => {
                let out = thread_pool.install(|| Diameter::run_symm(graph, &mut pl));
                println!("Diameter: {}", out.diameter);
                println!("Diametral vertex: {}", out.diametral_vertex);
                println!("Diameter iterations: {}", out.diameter_iterations);
            }
            LevelArg::RadiusDiameter => {
                let out = thread_pool.install(|| RadiusDiameter::run_symm(graph, &mut pl));
                println!("Radius: {}", out.radius);
                println!("Diameter: {}", out.diameter);
                println!("Radial vertex: {}", out.radial_vertex);
                println!("Diametral vertex: {}", out.diametral_vertex);
                println!("Radius iterations: {}", out.radius_iterations);
                println!("Diameter iterations: {}", out.diameter_iterations);
            }
            LevelArg::AllForward | LevelArg::All => {
                let out = thread_pool.install(|| All::run_symm(graph, &mut pl));
                println!("Radius: {}", out.radius);
                println!("Diameter: {}", out.diameter);
                println!("Radial vertex: {}", out.radial_vertex);
                println!("Diametral vertex: {}", out.diametral_vertex);
                println!("Radius iterations: {}", out.radius_iterations);
                println!("Diameter iterations: {}", out.diameter_iterations);
                println!("Iterations: {}", out.iterations);
                store_eccentricities(
                    &out.eccentricities,
                    args.forward.as_ref().unwrap(),
                    args.fmt,
                )?;
            }
        }
    } else {
        let transpose_path = args
            .transposed
            .as_ref()
            .expect("You have to pass the transposed graph if the graph is not symmetric.");
        let transpose = BvGraph::with_basename(transpose_path).load()?;

        match args.level {
            LevelArg::Radius => {
                let out = thread_pool.install(|| Radius::run(graph, transpose, None, &mut pl));
                println!("Radius: {}", out.radius);
                println!("Radial vertex: {}", out.radial_vertex);
                println!("Radius iterations: {}", out.radius_iterations);
            }
            LevelArg::Diameter => {
                let out = thread_pool.install(|| Diameter::run(graph, transpose, None, &mut pl));
                println!("Diameter: {}", out.diameter);
                println!("Diametral vertex: {}", out.diametral_vertex);
                println!("Diameter iterations: {}", out.diameter_iterations);
            }
            LevelArg::RadiusDiameter => {
                let out =
                    thread_pool.install(|| RadiusDiameter::run(graph, transpose, None, &mut pl));
                println!("Radius: {}", out.radius);
                println!("Diameter: {}", out.diameter);
                println!("Radial vertex: {}", out.radial_vertex);
                println!("Diametral vertex: {}", out.diametral_vertex);
                println!("Radius iterations: {}", out.radius_iterations);
                println!("Diameter iterations: {}", out.diameter_iterations);
            }
            LevelArg::AllForward => {
                let out = thread_pool.install(|| AllForward::run(graph, transpose, None, &mut pl));
                println!("Radius: {}", out.radius);
                println!("Diameter: {}", out.diameter);
                println!("Radial vertex: {}", out.radial_vertex);
                println!("Diametral vertex: {}", out.diametral_vertex);
                println!("Radius iterations: {}", out.radius_iterations);
                println!("Diameter iterations: {}", out.diameter_iterations);
                println!("Forward iterations: {}", out.forward_iterations);
                store_eccentricities(
                    &out.forward_eccentricities,
                    args.forward.as_ref().unwrap(),
                    args.fmt,
                )?;
            }
            LevelArg::All => {
                let out = thread_pool.install(|| All::run(graph, transpose, None, &mut pl));
                println!("Radius: {}", out.radius);
                println!("Diameter: {}", out.diameter);
                println!("Radial vertex: {}", out.radial_vertex);
                println!("Diametral vertex: {}", out.diametral_vertex);
                println!("Radius iterations: {}", out.radius_iterations);
                println!("Diameter iterations: {}", out.diameter_iterations);
                println!("Forward iterations: {}", out.forward_iterations);
                println!("All iterations: {}", out.all_iterations);
                store_eccentricities(
                    &out.forward_eccentricities,
                    args.forward.as_ref().unwrap(),
                    args.fmt,
                )?;
                if let Some(backward) = args.backward.as_ref() {
                    store_eccentricities(&out.backward_eccentricities, backward, args.fmt)?;
                }
            }
        }
    }

    Ok(())
}
