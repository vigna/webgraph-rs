use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::ProgressLogger;
use std::io::prelude::*;
use std::io::BufWriter;
use tempfile::tempdir;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Performs an LLP round", long_about = None)]
struct Args {
    /// The basename of the sourge graph.
    source: String,
    /// The basename of the destination graph.
    dest: String,
    /// The permutation.
    perm: String,

    /// How many triples to sort at once and dump on a file.
    #[arg(short, long, default_value_t = 1_000_000_000)]
    batch_size: usize,

    /// The directory where to put the temporary files needed to sort the paris
    /// this defaults to the system temporary directory as specified by the
    /// enviroment variable TMPDIR
    #[arg(short, long)]
    tmp_dir: Option<String>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    // TODO!: check that batchsize fits in memory, and that print the maximum
    // batch_size usable

    let graph = webgraph::graph::bvgraph::load(&args.source)?;
    let num_nodes = graph.num_nodes();
    let mut glob_pr = ProgressLogger::default().display_memory();
    glob_pr.item_name = "node";

    let mut perm = (0..num_nodes).collect::<Vec<_>>();
    std::fs::File::open(args.perm)?
        .read_exact(unsafe { std::mem::transmute::<&mut [usize], &mut [u8]>(&mut perm) })?;

    let bit_write = <BufferedBitStreamWrite<BE, _>>::new(<FileBackend<u64, _>>::new(
        BufWriter::new(std::fs::File::create(args.dest)?),
    ));

    let codes_writer = DynamicCodesWriter::new(
        bit_write,
        &CompFlags {
            ..Default::default()
        },
    );

    let tmpdir = tempdir().unwrap();
    let mut sort_pairs = SortPairs::new(
        args.batch_size,
        args.tmp_dir
            .unwrap_or_else(|| tmpdir.path().to_str().unwrap().to_string()),
    )
    .unwrap();

    PermutedGraph {
        graph: &graph,
        perm: &perm,
    }
    .iter_nodes()
    .for_each(|(x, succ)| {
        succ.for_each(|s| {
            sort_pairs.push(x, s, ()).unwrap();
        })
    });

    let edges = sort_pairs.iter()?.map(|(src, dst, _)| (src, dst));
    let g = COOIterToGraph::new(num_nodes, edges);

    let mut bvcomp = BVComp::new(codes_writer, 1, 4, 3, 0);
    glob_pr.expected_updates = Some(num_nodes);
    glob_pr.item_name = "node";
    glob_pr.start("Writing...");
    bvcomp.extend(g.iter_nodes())?;
    bvcomp.flush()?;
    glob_pr.done();
    Ok(())
}
