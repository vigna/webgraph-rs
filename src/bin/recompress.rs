use anyhow::Result;
use clap::Parser;
use clap::ValueEnum;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use std::fs::File;
use std::io::BufWriter;
use webgraph::prelude::*;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum PrivCode {
    Unary,
    Gamma,
    Delta,
    Zeta3,
}

impl From<PrivCode> for Code {
    fn from(value: PrivCode) -> Self {
        match value {
            PrivCode::Unary => Code::Unary,
            PrivCode::Gamma => Code::Gamma,
            PrivCode::Delta => Code::Delta,
            PrivCode::Zeta3 => Code::Zeta { k: 3 },
        }
    }
}

#[derive(Parser, Debug)]
#[command(about = "Recompress a graph", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
    /// The basename for the newly compressed graph.
    new_basename: String,
    /// The compression windows
    #[clap(default_value_t = 7)]
    compression_window: usize,
    /// The minimum interval length
    #[clap(default_value_t = 4)]
    min_interval_length: usize,
    /// The maximum recursion depth for references
    #[clap(default_value_t = 3)]
    max_ref_count: usize,

    #[arg(value_enum)]
    #[clap(default_value = "gamma")]
    /// The code to use for the outdegree
    outdegrees_code: PrivCode,

    #[arg(value_enum)]
    #[clap(default_value = "unary")]
    /// The code to use for the reference offsets
    references_code: PrivCode,

    #[arg(value_enum)]
    #[clap(default_value = "gamma")]
    /// The code to use for the blocks
    blocks_code: PrivCode,

    #[arg(value_enum)]
    #[clap(default_value = "gamma")]
    /// The code to use for the intervals
    intervals_code: PrivCode,

    #[arg(value_enum)]
    #[clap(default_value = "zeta3")]
    /// The code to use for the residuals
    residuals_code: PrivCode,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let compression_flags = CompFlags {
        outdegrees: args.outdegrees_code.into(),
        references: args.references_code.into(),
        blocks: args.blocks_code.into(),
        intervals: args.intervals_code.into(),
        residuals: args.residuals_code.into(),
        min_interval_length: args.min_interval_length,
        compression_window: args.compression_window,
    };

    let seq_reader = webgraph::bvgraph::load_seq(&args.basename)?;
    let file_path = format!("{}.graph", args.new_basename);
    let writer = <DynamicCodesWriter<BE, _>>::new(
        <BufferedBitStreamWrite<BE, _>>::new(FileBackend::new(BufWriter::new(File::create(
            &file_path,
        )?))),
        &compression_flags,
    );
    let mut bvcomp = BVComp::new(
        writer,
        args.compression_window,
        args.min_interval_length,
        args.max_ref_count,
    );

    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "node";
    pr.start("Reading nodes...");
    pr.expected_updates = Some(seq_reader.num_nodes());

    for (_, iter) in seq_reader {
        bvcomp.push(iter)?;
        pr.light_update();
    }

    pr.done();
    bvcomp.flush()?;
    Ok(())
}
