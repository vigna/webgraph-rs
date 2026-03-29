use dsi_bitstream::prelude::*;
use webgraph::prelude::*;
use webgraph::traits::RandomAccessGraph;

fn main() {
    let basename = std::env::args().nth(1).expect("usage: degree_stats <basename>");
    let graph = BvGraph::with_basename(&basename)
        .endianness::<BE>()
        .load()
        .unwrap();
    let n = graph.num_nodes();
    let mut max_deg = 0usize;
    let mut degrees: Vec<usize> = (0..n).map(|i| {
        let d = graph.outdegree(i);
        max_deg = max_deg.max(d);
        d
    }).collect();
    degrees.sort_unstable();
    let thresholds = [32, 64, 128, 256, 512, 1024, 4096, 10000, 100000];
    println!("Graph: {} ({} nodes, max degree {})", basename, n, max_deg);
    println!("p50={} p90={} p95={} p99={} p99.9={}",
        degrees[n / 2], degrees[n * 9 / 10], degrees[n * 95 / 100],
        degrees[n * 99 / 100], degrees[n * 999 / 1000]);
    for &t in &thresholds {
        let above = degrees.iter().filter(|&&d| d > t).count();
        println!("  deg > {:>6}: {:>10} nodes ({:.3}%)", t, above, 100.0 * above as f64 / n as f64);
    }
}
