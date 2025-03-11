/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::collections::VecDeque;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use epserde::deser::{Deserialize, Flags, MemCase};
use lender::Lender;
use sux::traits::IndexedSeq;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Reads a graph encoded with custom codes and does a BFS on it.", long_about = None)]
struct Args {
    // The basename of the graph.
    basename: PathBuf,

    /// The number of nodes in the graph
    num_nodes: usize,

    /// The number of arcs in the graph
    num_arcs: u64,
}

/// This is the factory that we can plug in BVGraph to read the custom codes
pub struct CustomDecoderFactory<
    E: Endianness,
    F: CodeReaderFactory<E>,
    OFF: IndexedSeq<Input = usize, Output = usize>,
> {
    pub factory: F,
    // The [`MemoryCase`]` here is needed to memory-map the offsets, otherwise
    // it can just be `OFF`
    pub offsets: MemCase<OFF>,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedSeq<Input = usize, Output = usize>>
    CustomDecoderFactory<E, F, OFF>
{
    pub fn new(factory: F, offsets: MemCase<OFF>) -> Self {
        Self {
            factory,
            offsets,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedSeq<Input = usize, Output = usize>>
    RandomAccessDecoderFactory for CustomDecoderFactory<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodesRead<E, Error = F::Error> + BitSeek,
{
    type Decoder<'a>
        = CustomDecoder<E, F::CodeReader<'a>>
    where
        Self: 'a;
    fn new_decoder(&self, node: usize) -> anyhow::Result<Self::Decoder<'_>> {
        let mut code_reader = self.factory.new_reader();
        code_reader.set_bit_pos(self.offsets.get(node) as u64)?;
        Ok(CustomDecoder::new(self.factory.new_reader()))
    }
}

impl<E: Endianness, F: CodeReaderFactory<E>, OFF: IndexedSeq<Input = usize, Output = usize>>
    SequentialDecoderFactory for CustomDecoderFactory<E, F, OFF>
where
    for<'a> <F as CodeReaderFactory<E>>::CodeReader<'a>: CodesRead<E, Error = F::Error> + BitSeek,
{
    type Decoder<'a>
        = CustomDecoder<E, F::CodeReader<'a>>
    where
        Self: 'a;
    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>> {
        Ok(CustomDecoder::new(self.factory.new_reader()))
    }
}

/// This is the decoder that will decode our custom codes and give them to BVGraph
pub struct CustomDecoder<E: Endianness, R: CodesRead<E>> {
    pub decoder: R,
    _marker: std::marker::PhantomData<E>,
}

impl<E: Endianness, R: CodesRead<E>> CustomDecoder<E, R> {
    pub fn new(decoder: R) -> Self {
        Self {
            decoder,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn into_inner(self) -> R {
        self.decoder
    }
}

impl<E: Endianness, R: CodesRead<E>> Decode for CustomDecoder<E, R> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        self.decoder.read_gamma().unwrap()
    }
    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        self.decoder.read_unary().unwrap()
    }
    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        self.decoder.read_gamma().unwrap()
    }
    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        self.decoder.read_gamma().unwrap()
    }
    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        self.decoder.read_gamma().unwrap()
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        self.decoder.read_gamma().unwrap()
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        self.decoder.read_gamma().unwrap()
    }
    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        self.decoder.read_zeta3().unwrap()
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        self.decoder.read_zeta3().unwrap()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    let args = Args::parse();

    let offsets = EF::load_mmap(args.basename.with_extension(EF_EXTENSION), Flags::default())?;

    let graph = BvGraph::new(
        CustomDecoderFactory::new(
            MemoryFactory::<BE, _>::new_mmap(
                args.basename.with_extension(GRAPH_EXTENSION),
                MemoryFlags::default(),
            )?,
            offsets,
        ),
        args.num_nodes,
        args.num_arcs,
        7, // default
        4, // default
    );


    let truth = BvGraph::with_basename("/dfd/graphs/enwiki-2015").load()?;

    let mut seen = vec![false; args.num_nodes];
    let mut queue = VecDeque::new();

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("node")
        .local_speed(true)
        .expected_updates(Some(args.num_nodes));
    pl.start("Visiting graph...");

    log::info!("Checking that the graphs are equal");
    truth.iter().zip(graph.iter()).for_each(|((n1, s1), (n2, s2))| {
        assert_eq!(n1, n2);
        assert_eq!(s1.collect::<Vec<_>>(), s2.collect::<Vec<_>>(), "Error at node {}", n1);
    });
    log::info!("Graphs are equal");

    for node_id in 0..args.num_nodes {
        assert_eq!(
            graph.successors(node_id).collect::<Vec<_>>(),
            truth.successors(node_id).collect::<Vec<_>>(),
        ); 
    }

    log::info!("Graphs are randomly equal");

    for start in 0..args.num_nodes {
        if seen[start] {
            continue;
        }
        queue.push_back(start as _);
        seen[start] = true;

        while !queue.is_empty() {
            pl.light_update();
            let current_node = queue.pop_front().unwrap();
            for succ in graph.successors(current_node) {
                if !seen[succ] {
                    queue.push_back(succ);
                    seen[succ] = true;
                }
            }
        }
    }

    Ok(())
}
