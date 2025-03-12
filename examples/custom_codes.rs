/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use webgraph::prelude::*;
use webgraph::utils::Converter;

#[derive(Parser, Debug)]
#[command(about = "Re-encode a graph with custom codes", long_about = None)]
struct Args {
    // The basename of the graph.
    basename: PathBuf,
    /// The destination of the graph.
    dst: PathBuf,
}

/// The custom encoder
pub struct CustomEncoder<E: Endianness, W: CodesWrite<E>> {
    pub encoder: W,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, W: CodesWrite<E>> CustomEncoder<E, W> {
    pub fn new(encoder: W) -> Self {
        Self {
            encoder,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn into_inner(self) -> W {
        self.encoder
    }
}

impl<E: Endianness, W: CodesWrite<E>> Encode for CustomEncoder<E, W> {
    type Error = W::Error;
    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_gamma(value)
    }
    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_unary(value)
    }
    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_gamma(value)
    }
    #[inline(always)]
    fn write_block(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_gamma(value)
    }
    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_gamma(value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_pi(value, 2)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_gamma(value)
    }
    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_pi(value, 3)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        self.encoder.write_pi(value, 2)
    }
    #[inline(always)]
    fn flush(&mut self) -> Result<usize, Self::Error> {
        self.encoder.flush()
    }
    #[inline(always)]
    fn start_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }
    #[inline(always)]
    fn end_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    let args = Args::parse();
    let graph = BvGraph::with_basename(&args.basename).load()?;

    // create the offsets file while we re-encode the graph so we avoid
    // having to scan the graph afterwards. We can't do an Elias-Fano yet
    // because we don't know the length of the final graph.
    let offsets_path = args.dst.with_extension(OFFSETS_EXTENSION);
    let mut offsets_writer =
        <BufBitWriter<BE, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(
            File::create(&offsets_path)
                .with_context(|| format!("Could not create {}", offsets_path.display()))?,
        )));

    // create a bitstream writer for the target graph
    let target_graph_path = args.dst.with_extension(GRAPH_EXTENSION);
    let writer = <BufBitWriter<LE, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(
        File::create(&target_graph_path)
            .with_context(|| format!("Could not create {}", target_graph_path.display()))?,
    )));
    let encoder = CustomEncoder::new(writer);

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("node")
        .expected_updates(Some(graph.num_nodes()));
    pl.start("Re-encoding...");

    // wrap the offset degrees iterator, which reads every code but doesn't
    // resolve references, so it's much faster if we only need to scan the codes
    let mut iter = graph
        .offset_deg_iter()
        .map_decoder(move |decoder| Converter {
            decoder,
            encoder,
            offset: 0,
        });
    // consume the graph iterator reading all codes and writing them to the encoder
    // through [`Converter`]
    let mut offset = 0;
    for _ in 0..graph.num_nodes() {
        // write the offset to the offsets file
        let new_offset = iter.get_decoder().offset;
        offsets_writer
            .write_gamma((new_offset - offset) as u64)
            .context("Could not write gamma")?;
        offset = new_offset;

        iter.next_degree()?; // read to next node
        pl.light_update();
    }
    // write the last offset
    let new_offset = iter.get_decoder().offset;
    offsets_writer
        .write_gamma((new_offset - offset) as u64)
        .context("Could not write gamma")?;
    pl.light_update();
    pl.done();

    log::info!(
        "Done re-encoding, the graph is {} bits ({} bytes) long.",
        iter.get_decoder().offset,
        iter.get_decoder().offset.div_ceil(8),
    );
    log::info!(
        "Now you should build elias-fano with `cargo run --release build ef '{}' {}`",
        args.dst.display(),
        graph.num_nodes()
    );
    log::info!(
        "Then you can run a BFS on it using `cargo run --release --example custom_codes_bfs '{}' {} {}`",
        args.dst.display(),
        graph.num_nodes(),
        graph.num_arcs(),
    );

    Ok(())
}
