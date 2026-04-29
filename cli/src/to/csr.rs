/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::LogIntervalArg;
use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::ser::Serialize;
use std::io::BufWriter;
use std::path::PathBuf;
use webgraph::graphs::bvgraph::get_endianness;
use webgraph::prelude::*;
use webgraph::utils::MmapHelper;

#[derive(Parser, Debug)]
#[command(name = "csr", about = "Converts a graph to CSR format and serializes it with ε-serde.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The basename of the source graph.​
    pub basename: PathBuf,

    /// The basename of the output CSR graph. Defaults to the source basename.​
    pub dst: Option<PathBuf>,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    match get_endianness(&args.basename)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => to_csr::<BE>(args),
        #[cfg(feature = "le_bins")]
        LE::NAME => to_csr::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn to_csr<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
{
    let mut pl = progress_logger![
        display_memory = true,
        log_interval = args.log_interval.log_interval,
    ];

    log::info!("Loading graph from {}...", args.basename.display());
    let graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.basename)
        .endianness::<E>()
        .load()?;

    pl.start("Converting to CSR...");
    let csr = CsrGraph::from_seq_graph(&graph);
    pl.done();

    drop(graph);

    let csr_path = args
        .dst
        .unwrap_or_else(|| args.basename.clone())
        .with_extension(CSR_EXTENSION);
    log::info!("Writing CSR to {}...", csr_path.display());
    let mut file = BufWriter::new(
        std::fs::File::create(&csr_path)
            .with_context(|| format!("Could not create {}", csr_path.display()))?,
    );
    unsafe {
        csr.serialize(&mut file)
            .with_context(|| format!("Could not serialize CSR to {}", csr_path.display()))
    }?;

    log::info!("Completed.");
    Ok(())
}
