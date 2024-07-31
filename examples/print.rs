/*
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use clap::Parser;
use lender::for_;
use webgraph::graphs::bvgraph::BvGraphSeq;

#[derive(Parser, Debug)]
#[command(about = "Prints the arcs of a graph", long_about = None)]
struct Args {
    // The basename of the graph.
    basename: String,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // This line will load a big-endian graph (the default). To load
    // a little-endian graph, you need
    //
    // let graph = BvGraphSeq::with_basename(&args.basename).endianness::<LE>().load()?;
    let graph = BvGraphSeq::with_basename(&args.basename).load()?;

    for_!((src, succ) in graph {
        for dst in succ {
            println!("{} -> {}", src, dst);
        }
    });

    Ok(())
}
