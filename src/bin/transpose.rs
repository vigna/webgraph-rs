use anyhow::Result;
use clap::Parser;
use clap::ValueEnum;
use dsi_bitstream::codes::Code;
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
#[command(about = "Transpose a BVGraph", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
    /// The basename of the transposed graph.
    transpose: String,
    /// The size of a batch.
    batch_size: usize,
    /// Location for storage of temporary files
    #[arg(short = 't', long)]
    temp_dir: bool,

    #[arg(short = 'j', long)]
    /// The number of cores to use
    num_cpus: Option<usize>,
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
        max_ref_count: args.max_ref_count,
    };

    let seq_graph = webgraph::bvgraph::load_seq(&args.basename)?;

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.num_cpus.unwrap_or(rayon::max_num_threads()))
        .build()
        .unwrap()
        .install(|| {
            webgraph::algorithms::transpose(
                &seq_graph,
                args.batch_size,
                args.basename,
                compression_flags,
            )
            .unwrap();
        });

    Ok(())
}
