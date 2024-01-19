use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_bitstream::prelude::{BE, LE, NE};
use dsi_progress_logger::*;
use lender::prelude::*;
use std::io::Write;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Read a BVGraph and print the edge list `{src}\t{dst}` to stdout", long_about = None)]
struct Args {
    /// The basename of the dst.
    basename: String,

    #[arg(long, default_value_t = ',')]
    /// The index of the column containing the source node str.
    pub csv_separator: char,
}
fn to_csv<E: Endianness + 'static>(args: Args) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>:
        ZetaRead<E> + DeltaRead<E> + GammaRead<E> + BitSeek,
{
    let graph = webgraph::graph::bvgraph::load_seq::<NE, _>(&args.basename)?;
    let num_nodes = graph.num_nodes();

    // read the csv and put it inside the sort pairs
    let mut stdout = std::io::BufWriter::new(std::io::stdout().lock());
    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("nodes")
        .expected_updates(Some(num_nodes));
    pl.start("Reading BVGraph");

    for_! ( (src, succ) in graph.iter() {
        for dst in succ {
            writeln!(stdout, "{}{}{}", src, args.csv_separator, dst)?;
        }
        pl.light_update();
    });

    pl.done();
    Ok(())
}

fn main() -> Result<()> {
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    let args = Args::parse();

    match get_endianess(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => to_csv::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => to_csv::<LE>(args),
        _ => panic!("Unknown endianness"),
    }
}
