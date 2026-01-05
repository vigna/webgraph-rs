/*
 * SPDX-FileCopyrightText: 2026 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::GlobalArgs;
use anyhow::Result;
use clap::Args;
use dsi_bitstream::prelude::*;
use dsi_bitstream::traits::BitSeek;
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;
use webgraph::prelude::*;
use webgraph::traits::SequentialLabeling;
use webgraph::utils::CircularBuffer;

#[derive(Args, Debug)]
#[command(name = "maxref", about = "Checks that the maximum reference count is respected in the given graph.", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    pub basename: PathBuf,
}

pub fn main(_global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    check_maxref(args)
}

pub fn check_maxref(args: CliArgs) -> Result<()> {
    let graph = webgraph::graphs::bvgraph::sequential::BvGraphSeq::with_basename(&args.basename)
        .endianness::<BE>()
        .load()?;

    let (_, _, comp_flags) =
        parse_properties::<BE>(args.basename.with_extension(PROPERTIES_EXTENSION))?;

    log::info!("The compression flags are: {:?}", comp_flags);

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("checking max-ref graphs")
        .expected_updates(Some(graph.num_nodes()));

    pl.start("Start comparing the graphs...");

    let mut iter = graph.offset_deg_iter().map_decoder(|d| {
        MaxRefChecker::new(
            d,
            Default::default(),
            comp_flags.compression_window,
            comp_flags.max_ref_count,
        )
    });

    for _ in iter.by_ref() {
        pl.light_update();
    }

    pl.done();
    Ok(())
}

/// A wrapper over a generic [`Decode`] that checks that the maximum reference
/// count is respected for each node in the graph.
pub struct MaxRefChecker<D: Decode> {
    pub codes_reader: D,
    pub stats: DecoderStats,
    /// A circular buffer to keep track of the reference counts of the last compression_window nodes
    pub ref_counts: CircularBuffer<usize>,
    /// The maximum reference count in the compression_flag
    pub max_ref_count: usize,
    /// The current node being processed (incremented after read_outdegree)
    pub current_node: usize,
}

impl<D: Decode> MaxRefChecker<D> {
    /// Wrap a reader
    #[inline(always)]
    pub fn new(
        codes_reader: D,
        stats: DecoderStats,
        compression_window: usize,
        max_ref_count: usize,
    ) -> Self {
        Self {
            codes_reader,
            stats,
            ref_counts: CircularBuffer::new(compression_window),
            max_ref_count,
            current_node: 0,
        }
    }
}

impl<D: Decode> BitSeek for MaxRefChecker<D>
where
    D: BitSeek,
{
    type Error = <D as BitSeek>::Error;

    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.codes_reader.bit_pos()
    }

    fn set_bit_pos(&mut self, bit_pos: u64) -> Result<(), Self::Error> {
        self.codes_reader.set_bit_pos(bit_pos)
    }
}

impl<D: Decode> Decode for MaxRefChecker<D> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        let res = self.codes_reader.read_outdegree();
        // Initialize ref_count for the current node to 0 (no references yet)
        self.ref_counts[self.current_node] = 0;
        // Increment current_node for subsequent calls
        self.current_node += 1;
        res
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        let delta = self.codes_reader.read_reference_offset();

        // The node being processed is current_node - 1 (we incremented in read_outdegree)
        let processing_node = self.current_node - 1;

        if delta > 0 {
            let referenced_node = processing_node - delta as usize;

            // ref_count is the number of hops needed to decode
            // Current node's ref_count = referenced node's ref_count + 1
            let ref_count = self.ref_counts[referenced_node] + 1;

            // Check that the chain depth does not exceed the maximum allowed value
            assert!(
                ref_count <= self.max_ref_count,
                "Node {} has ref_count {} which exceeds max_ref_count {} (references node {} with ref_count {})",
                processing_node,
                ref_count,
                self.max_ref_count,
                referenced_node,
                self.ref_counts[referenced_node]
            );

            // Store the ref_count for the current node
            self.ref_counts[processing_node] = ref_count;
        }
        // If delta == 0, ref_count stays 0 (already set in read_outdegree)

        delta
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        self.codes_reader.read_block_count()
    }

    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        self.codes_reader.read_block()
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        self.codes_reader.read_interval_count()
    }

    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        self.codes_reader.read_interval_start()
    }

    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        self.codes_reader.read_interval_len()
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        self.codes_reader.read_first_residual()
    }

    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        self.codes_reader.read_residual()
    }
}
