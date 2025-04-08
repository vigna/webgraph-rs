use crate::GlobalArgs;
use anyhow::{ensure, Result};
use clap::{Parser, ValueEnum};
use dsi_bitstream::prelude::*;
use std::path::PathBuf;
use webgraph::{graphs::bvgraph::get_endianness, prelude::BvGraph};

#[derive(Parser, Debug)]
#[command(name = "exactsumsweep", about = "", long_about = None)]
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
    pub level: Level,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
/// Enum for the level of exact sum sweep to compute.
pub enum Level {
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
    ensure!(!(!args.symmetric && args.transposed.is_none()), "You have to either pass --transposed with with the basename of the transposed graph or --symm if the graph is symmetric.");
    ensure!(
        !(args.symmetric && args.transposed.is_some()),
        "--transposed is needed only if the graph is not symmetric."
    );
    ensure!(!(args.forward.is_some() && !matches!(args.level, Level::All | Level::AllForward)), "You cannot only pass --forward with --level=all or --level=all-forward as the forward eccentricites won't be computed otherwise.");
    ensure!(!(args.forward.is_none() && matches!(args.level, Level::All | Level::AllForward)), "If --level=all or --level=all-forward, you should pass --forward to store the computed eccentricities.");
    ensure!(!(args.backward.is_some() && args.level != Level::All), "You cannot only pass --backward with --level=all as the backward eccentricites won't be computed otherwise.");
    ensure!(!(args.level == Level::All && args.symmetric && args.backward.is_some()), "You cannot pass --backward with --symm and --level=all as the eccentricities of a symmetric graph are the same in both directions.");

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => exact_sum_sweep::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => exact_sum_sweep::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn exact_sum_sweep<E: Endianness>(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    let graph = BvGraph::with_basename(&args.basename).load()?;

    Ok(())
}
