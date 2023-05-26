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
        ($total_bits:expr, $default_bits:expr, $stats:expr, $($code:ident - $def:ident),*) => {
            println!("{:>16},{:>16},{:>12},{:>8},{:>10},{:>16}",
                "Type", "Code", "Improvement", "Weight", "Bytes", "Bits",
            );
            $(
                let (_, len) = $stats.$code.get_best_code();
                $total_bits += len;
                $default_bits += $stats.$code.$def;
            )*

            $(
                let (code, len) = $stats.$code.get_best_code();
                println!("{:>16},{:>16},{:>12},{:>8},{:>10},{:>16}",
                    stringify!($code), format!("{:?}", code),
                    format!("{:.3}", $stats.$code.$def as f64 / len as f64),
                    format!("{:.3}", (($stats.$code.$def - len) as f64 / ($default_bits - $total_bits) as f64)),
                    normalize(($stats.$code.$def - len) as f64 / 8.0),
                    $stats.$code.$def - len,
                );
            )*
        };
    }

    let mut total_bits = 0;
    let mut default_bits = 0;
    impl_best_code!(
        total_bits,
        default_bits,
        stats,
        outdegree - gamma,
        reference_offset - unary,
        block_count - gamma,
        blocks - gamma,
        interval_count - gamma,
        interval_start - gamma,
        interval_len - gamma,
        first_residual - zeta3,
        residual - zeta3
    );

    println!("  Total bits: {:>16}", total_bits);
    println!("Default bits: {:>16}", default_bits);
    println!("  Saved bits: {:>16}", default_bits - total_bits);

    println!("  Total size: {:>8}", normalize(total_bits as f64 / 8.0));
    println!("Default size: {:>8}", normalize(default_bits as f64 / 8.0));
    println!(
        "  Saved size: {:>8}",
        normalize((default_bits - total_bits) as f64 / 8.0)
    );

    println!(
        "Improvement: {:.3} times",
        default_bits as f64 / total_bits as f64
    );
    Ok(())
}

fn normalize(mut value: f64) -> String {
    let mut uom = ' ';
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'K';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'M';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'G';
    }
    if value > 1000.0 {
        value /= 1000.0;
        uom = 'T';
    }
    format!("{:.3}{}", value, uom)
}
