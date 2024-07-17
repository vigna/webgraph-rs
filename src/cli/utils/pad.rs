/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches, ValueEnum};
use common_traits::UnsignedInt;
use log::info;
use std::{
    mem::size_of,
    path::{Path, PathBuf},
};

use crate::graphs::GRAPH_EXTENSION;

pub const COMMAND_NAME: &str = "pad";

#[derive(Args, Debug)]
#[command(about = "Zero-pad graph files to a length multiple of a word size", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
    /// The block size to align to
    #[clap(value_enum)]
    word_size: WordSize,
}

#[derive(ValueEnum, Clone, Debug, Default)]
enum WordSize {
    /// 2 bytes
    U16,
    /// 4 bytes
    U32,
    /// 8 bytes
    #[default]
    U64,
    /// 16 bytes
    U128,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    let word_size = match args.word_size {
        WordSize::U16 => size_of::<u16>(),
        WordSize::U32 => size_of::<u32>(),
        WordSize::U64 => size_of::<u64>(),
        WordSize::U128 => size_of::<u128>(),
    };

    pad(args.basename.with_extension(GRAPH_EXTENSION), word_size)
}

pub fn pad(path: impl AsRef<Path>, block_size: usize) -> Result<()> {
    let path = path.as_ref();
    let file_len = path
        .metadata()
        .with_context(|| format!("Cannot extract metadata from file {}", path.display()))?
        .len();

    let padded_len = file_len.align_to(block_size as u64);

    if file_len == padded_len {
        info!(
            "The length of file {} is already a multiple of {}",
            path.display(),
            block_size
        );
        return Ok(());
    }

    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("Cannot open file {} to pad", path.display()))?;
    file.set_len(padded_len)
        .with_context(|| format!("Cannot pad file {}", path.display()))?;
    info!(
        "File {} successfully zero-padded to a length multiple of {}",
        path.display(),
        block_size
    );

    Ok(())
}
