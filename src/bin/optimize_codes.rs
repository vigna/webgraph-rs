use anyhow::Result;
use clap::Parser;
use dsi_progress_logger::ProgressLogger;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Reads a graph and suggests the best codes to use.", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let mut stats = BVGraphCodesStats::default();
    let seq_reader = WebgraphSequentialIter::load_mapped_stats(&args.basename, &mut stats)?;

    let mut pr = ProgressLogger::default().display_memory();
    pr.item_name = "node".into();
    pr.start("Reading nodes...");
    pr.expected_updates = Some(seq_reader.num_nodes());

    for _ in seq_reader {
        pr.light_update();
    }

    pr.done();

    eprintln!("{:#?}", stats);

    macro_rules! impl_best_code {
        ($total_bits:expr, $stats:expr, $($code:ident),*) => {
            $(
                let (code, len) = $stats.$code.get_best_code();
                $total_bits += len;
                println!("{}: {:?} : {}", stringify!($code), code, len);
            )*
        };
    }

    let mut total_bits = 0;
    impl_best_code!(
        total_bits,
        stats,
        outdegree,
        reference_offset,
        block_count,
        blocks,
        interval_count,
        interval_start,
        interval_len,
        first_residual,
        residual
    );

    println!("Total bits: {}", total_bits);

    let mut tmp = total_bits / 8;
    let mut uom = ' ';
    if tmp > 1000 {
        tmp /= 1000;
        uom = 'K';
    }
    if tmp > 1000 {
        tmp /= 1000;
        uom = 'M';
    }
    if tmp > 1000 {
        tmp /= 1000;
        uom = 'G';
    }
    if tmp > 1000 {
        tmp /= 1000;
        uom = 'T';
    }

    println!("Total size: {}{}", tmp, uom);
    Ok(())
}
