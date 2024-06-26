/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::{Result, Context};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::prelude::*;
use lender::*;
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::Mutex;
use serde::{ser::SerializeMap, Deserialize, Serialize};

pub const COMMAND_NAME: &str = "counts";

#[derive(Args, Debug)]
#[command(about = "Reads a graph and compute the frequencies of each value for each code we write", long_about = None)]
pub struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    match get_endianness(&args.basename)?.as_str() {
        #[cfg(any(
            feature = "be_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        BE::NAME => build_stats::<BE>(args),
        #[cfg(any(
            feature = "le_bins",
            not(any(feature = "be_bins", feature = "le_bins"))
        ))]
        LE::NAME => build_stats::<LE>(args),
        e => panic!("Unknown endianness: {}", e),
    }
}

pub fn build_stats<E: Endianness + 'static>(args: CliArgs) -> Result<()>
where
    for<'a> BufBitReader<E, MemWordReader<u32, &'a [u32]>>: CodeRead<E> + BitSeek,
{
    // TODO!: speed it up by using random access graph if possible
    let graph = BVGraphSeq::with_basename(&args.basename)
        .endianness::<E>()
        .load()?
        .map_factory(StatsDecoderFactory::new);

    let mut pl = ProgressLogger::default();
    pl.display_memory(true)
        .item_name("node")
        .expected_updates(Some(graph.num_nodes()));

    pl.start("Scanning...");

    for (_new_offset, _degree) in graph.offset_deg_iter() {
        // decode the next nodes so we know where the next node_id starts
        pl.light_update();
    }

    let stats = graph.into_inner().stats();

    // write as a toml the gathered Counts
    let stats_file = args.basename.with_extension("counts");
    std::fs::write(
        &stats_file, 
        bincode::serialize(&stats)
            .with_context(|| format!("Could not serialize the stats to {}", stats_file.display()))?
    )?;

    Ok(())
}


/// A struct that keeps track of how many times each value occurs in each code
/// context.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Counts {
    /// The occurrences counts for outdegrees values
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    pub outdegrees: HashMap<u64, u64>,
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    /// The occurrences counts for reference_offset values
    pub reference_offsets: HashMap<u64, u64>,
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    /// The occurrences counts for block_count values
    pub block_counts: HashMap<u64, u64>,
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    /// The occurrences counts for blocks values
    pub blocks: HashMap<u64, u64>,
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    /// The occurrences counts for interval_count values
    pub interval_counts: HashMap<u64, u64>,
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    /// The occurrences counts for interval_start values
    pub interval_starts: HashMap<u64, u64>,
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    /// The occurrences counts for interval_len values
    pub interval_lens: HashMap<u64, u64>,
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    /// The occurrences counts for first_residual values
    pub first_residuals: HashMap<u64, u64>,
    #[serde(serialize_with = "hashmap_non_str_keys_serializer", deserialize_with = "hashmap_non_str_keys_deserializer")]
    /// The occurrences counts for residual values
    pub residuals: HashMap<u64, u64>,
}

pub fn hashmap_non_str_keys_serializer<S, K, V>(data: &HashMap<K, V>, serializer: S) -> Result<S::Ok, S::Error> 
where 
    S: serde::Serializer,
    V: serde::Serialize,
    K: ToString,
{
    let mut map = serializer.serialize_map(Some(data.len()))?;
    for (key, value) in data {
        map.serialize_entry(&key.to_string(), value)?;
    }
    map.end()
}

pub fn hashmap_non_str_keys_deserializer<'de, D, K, V>(deserializer: D) -> Result<HashMap<K, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    V: serde::Deserialize<'de>,
    K: std::str::FromStr + core::hash::Hash + core::cmp::Eq,
    <K as std::str::FromStr>::Err: std::fmt::Debug,
{
    let map: HashMap<String, V> = serde::Deserialize::deserialize(deserializer)?;
    let mut new_map = HashMap::new();
    for (key, value) in map {
        new_map.insert(key.parse().unwrap(), value);
    }
    Ok(new_map)
}

impl core::ops::AddAssign<&Self> for Counts {
    fn add_assign(&mut self, rhs: &Self) {
        macro_rules! sum_hashmaps {
            ($map:ident) => {
                for (key, value) in rhs.$map.iter() {
                    *self.$map.entry(*key).or_insert(0) += value;
                }
            };
        }

        sum_hashmaps!(outdegrees);
        sum_hashmaps!(reference_offsets);
        sum_hashmaps!(block_counts);
        sum_hashmaps!(blocks);
        sum_hashmaps!(interval_counts);
        sum_hashmaps!(interval_starts);
        sum_hashmaps!(interval_lens);
        sum_hashmaps!(first_residuals);
        sum_hashmaps!(residuals);
    }
}

/// A wrapper that keeps track of how many times each value occurs in each code
/// context for a [`SequentialDecoderFactory`] implementation and returns the 
/// counts.
pub struct StatsDecoderFactory<F: SequentialDecoderFactory> {
    factory: F,
    glob_stats: Mutex<Counts>,
}

impl<F> StatsDecoderFactory<F>
where
    F: SequentialDecoderFactory,
{
    pub fn new(factory: F) -> Self {
        Self {
            factory,
            glob_stats: Mutex::new(Counts::default()),
        }
    }

    /// Consume self and return the stats.
    pub fn stats(self) -> Counts {
        self.glob_stats.into_inner().unwrap()
    }
}

impl<F> From<F> for StatsDecoderFactory<F>
where
    F: SequentialDecoderFactory,
{
    #[inline(always)]
    fn from(value: F) -> Self {
        Self::new(value)
    }
}

impl<F> SequentialDecoderFactory for StatsDecoderFactory<F>
where
    F: SequentialDecoderFactory,
{
    type Decoder<'a> = StatsDecoder<'a, F>
    where
        Self: 'a;

    #[inline(always)]
    fn new_decoder(&self) -> anyhow::Result<Self::Decoder<'_>> {
        Ok(StatsDecoder::new(
            self,
            self.factory.new_decoder()?,
            Counts::default(),
        ))
    }
}

/// A wrapper over a generic [`Decode`] that keeps track of how much
/// bits each piece would take using different codes for compressions
pub struct StatsDecoder<'a, F: SequentialDecoderFactory> {
    factory: &'a StatsDecoderFactory<F>,
    codes_reader: F::Decoder<'a>,
    stats: Counts,
}

impl<'a, F: SequentialDecoderFactory> Drop for StatsDecoder<'a, F> {
    fn drop(&mut self) {
        *self.factory.glob_stats.lock().unwrap() += &self.stats;
    }
}

impl<'a, F: SequentialDecoderFactory> StatsDecoder<'a, F> {
    /// Wrap a reader
    #[inline(always)]
    pub fn new(
        factory: &'a StatsDecoderFactory<F>,
        codes_reader: F::Decoder<'a>,
        stats: Counts,
    ) -> Self {
        Self {
            factory,
            codes_reader,
            stats,
        }
    }
}

impl<'a, F: SequentialDecoderFactory> BitSeek for StatsDecoder<'a, F> 
where
    F::Decoder<'a>: BitSeek,
{
    type Error = <F::Decoder<'a> as BitSeek>::Error;

    #[inline(always)]
    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.codes_reader.bit_pos()
    }

    #[inline(always)]
    fn set_bit_pos(&mut self, bit_pos: u64) -> Result<(), Self::Error> {
        self.codes_reader.set_bit_pos(bit_pos)
    }
}

impl<'a, F: SequentialDecoderFactory> Decode for StatsDecoder<'a, F> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        let value = self.codes_reader.read_outdegree();
        *self.stats.outdegrees.entry(value).or_insert(0) += 1;
        value
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        let value = self.codes_reader.read_reference_offset();
        *self.stats.reference_offsets.entry(value).or_insert(0) += 1;
        value
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        let value = self.codes_reader.read_block_count();
        *self.stats.block_counts.entry(value).or_insert(0) += 1;
        value
    }

    #[inline(always)]
    fn read_block(&mut self) -> u64 {
        let value = self.codes_reader.read_block();
        *self.stats.blocks.entry(value).or_insert(0) += 1;
        value
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        let value = self.codes_reader.read_interval_count();
        *self.stats.interval_counts.entry(value).or_insert(0) += 1;
        value
    }

    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        let value = self.codes_reader.read_interval_start();
        *self.stats.interval_starts.entry(value).or_insert(0) += 1;
        value
    }

    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        let value = self.codes_reader.read_interval_len();
        *self.stats.interval_lens.entry(value).or_insert(0) += 1;
        value
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        let value = self.codes_reader.read_first_residual();
        *self.stats.first_residuals.entry(value).or_insert(0) += 1;
        value
    }

    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        let value = self.codes_reader.read_residual();
        *self.stats.residuals.entry(value).or_insert(0) += 1;
        value
    }
}
