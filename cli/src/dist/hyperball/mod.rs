use crate::GlobalArgs;
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use std::path::PathBuf;
use webgraph::graphs::bvgraph::get_endianness;

#[derive(Parser, Debug)]
#[command(name = "hyperball", about = "", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => hyperball::<BE>(global_args, args),
        #[cfg(feature = "le_bins")]
        LE::NAME => hyperball::<LE>(global_args, args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn hyperball<E: Endianness>(_global_args: GlobalArgs, _args: CliArgs) -> Result<()> {
    todo!();
}
