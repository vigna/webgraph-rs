/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use anyhow::Result;
use clap::Parser;
use dsi_bitstream::dispatch::factory::CodesReaderFactoryHelper;
use dsi_bitstream::prelude::*;
use itertools::Itertools;
use lender::*;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::hint::black_box;
use std::path::PathBuf;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "bvgraph", about = "Benchmarks the Rust BvGraph implementation.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub src: PathBuf,

    /// Perform a random-access test on this number of randomly selected nodes.
    #[arg(short, long)]
    pub random: Option<usize>,

    /// The number of repeats.
    #[arg(short = 'R', long, default_value = "10")]
    pub repeats: usize,

    /// In random-access tests, test just access to the first successor.
    #[arg(short = 'f', long)]
    pub first: bool,

    /// Static dispatch for speed tests (default BvGraph parameters).
    #[arg(short = 'S', long = "static")]
    pub _static: bool,

    /// Test sequential high-speed offset/degree scanning.
    #[arg(short = 'd', long)]
    pub degrees: bool,

    /// Do not test speed, but check that the sequential and random-access successor lists are the same.
    #[arg(short = 'c', long)]
    pub check: bool,

    /// Expand offsets into a slice of usize before testing random-access
    /// successor lists.
    #[arg(long)]
    pub slice: bool,
}

pub fn main(_global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    match get_endianness(&args.src)?.as_str() {
        #[cfg(feature = "be_bins")]
        BE::NAME => match args._static {
            true => bench_webgraph::<BE, Static>(args),
            false => bench_webgraph::<BE, Dynamic>(args),
        },
        #[cfg(feature = "le_bins")]
        LE::NAME => match args._static {
            true => bench_webgraph::<LE, Static>(args),
            false => bench_webgraph::<LE, Dynamic>(args),
        },
        e => panic!("Unknown endianness: {}", e),
    }
}

fn bench_random(graph: impl RandomAccessGraph, samples: usize, repeats: usize, first: bool) {
    // Random-access speed test
    for _ in 0..repeats {
        let mut rng = SmallRng::seed_from_u64(0);
        let mut c: u64 = 0;
        let num_nodes = graph.num_nodes();
        let start = std::time::Instant::now();
        if first {
            for _ in 0..samples {
                black_box(
                    graph
                        .successors(rng.random_range(0..num_nodes))
                        .into_iter()
                        .next()
                        .unwrap_or(0),
                );
                c += 1;
            }
        } else {
            for _ in 0..samples {
                c += black_box(
                    graph
                        .successors(rng.random_range(0..num_nodes))
                        .into_iter()
                        .count() as u64,
                );
            }
        }

        println!(
            "{}:    {:>20} ns/arc",
            if first { "First" } else { "Random" },
            (start.elapsed().as_secs_f64() / c as f64) * 1e9
        );
    }
}

fn bench_seq(graph: impl SequentialGraph, repeats: usize) {
    for _ in 0..repeats {
        let mut c: u64 = 0;

        let start = std::time::Instant::now();
        let mut iter = graph.iter();
        while let Some((_, succ)) = iter.next() {
            c += succ.into_iter().count() as u64;
        }
        println!(
            "Sequential:{:>20} ns/arc",
            (start.elapsed().as_secs_f64() / c as f64) * 1e9
        );

        assert_eq!(c, graph.num_arcs_hint().unwrap());
    }
}

fn bench_webgraph<E: Endianness, D: Dispatch>(args: CliArgs) -> Result<()>
where
    MmapHelper<u32>: CodesReaderFactoryHelper<E>,
    for<'a> LoadModeCodesReader<'a, E, Mmap>: BitSeek,
{
    if args.check {
        let graph = BvGraph::with_basename(&args.src).endianness::<E>().load()?;

        let seq_graph = BvGraphSeq::with_basename(&args.src)
            .endianness::<E>()
            .load()?;

        let mut deg_reader = seq_graph.offset_deg_iter();

        // Check that sequential and random-access interfaces return the same result
        for_![ (node, seq_succ) in seq_graph {
            let succ = graph.successors(node);

            assert_eq!(deg_reader.next_degree()?, seq_succ.len());
            assert_eq!(succ.collect_vec(), seq_succ.collect_vec());
        }];
    } else if args.degrees {
        let seq_graph = BvGraphSeq::with_basename(&args.src)
            .endianness::<E>()
            .load()?;

        for _ in 0..args.repeats {
            let mut deg_reader = seq_graph.offset_deg_iter();

            let mut c: u64 = 0;
            let start = std::time::Instant::now();
            for _ in 0..seq_graph.num_nodes() {
                c += black_box(deg_reader.next_degree()? as u64);
            }
            println!(
                "Degrees Only:{:>20} ns/arc",
                (start.elapsed().as_secs_f64() / c as f64) * 1e9
            );

            assert_eq!(c, seq_graph.num_arcs_hint().unwrap());
        }
    } else {
        match (
            args.random,
            std::any::TypeId::of::<D>() == std::any::TypeId::of::<Dynamic>(),
        ) {
            (Some(samples), true) => {
                if args.slice {
                    bench_random(
                        BvGraph::with_basename(&args.src)
                            .endianness::<E>()
                            .dispatch::<Dynamic>()
                            .mode::<Mmap>()
                            .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                            .load()?
                            .offsets_to_slice(),
                        samples,
                        args.repeats,
                        args.first,
                    );
                } else {
                    bench_random(
                        BvGraph::with_basename(&args.src)
                            .endianness::<E>()
                            .dispatch::<Dynamic>()
                            .mode::<Mmap>()
                            .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                            .load()?,
                        samples,
                        args.repeats,
                        args.first,
                    );
                }
            }
            (Some(samples), false) => {
                if args.slice {
                    bench_random(
                        BvGraph::with_basename(&args.src)
                            .endianness::<E>()
                            .dispatch::<Static>()
                            .mode::<Mmap>()
                            .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                            .load()?
                            .offsets_to_slice(),
                        samples,
                        args.repeats,
                        args.first,
                    );
                } else {
                    bench_random(
                        BvGraph::with_basename(&args.src)
                            .endianness::<E>()
                            .dispatch::<Static>()
                            .mode::<Mmap>()
                            .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                            .load()?,
                        samples,
                        args.repeats,
                        args.first,
                    );
                }
            }
            (None, true) => {
                bench_seq(
                    BvGraphSeq::with_basename(&args.src)
                        .endianness::<E>()
                        .dispatch::<Dynamic>()
                        .mode::<Mmap>()
                        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::SEQUENTIAL)
                        .load()?,
                    args.repeats,
                );
            }
            (None, false) => {
                bench_seq(
                    BvGraphSeq::with_basename(&args.src)
                        .endianness::<E>()
                        .dispatch::<Static>()
                        .mode::<Mmap>()
                        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::SEQUENTIAL)
                        .load()?,
                    args.repeats,
                );
            }
        }
    }
    Ok(())
}
