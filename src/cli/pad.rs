/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::{ensure, Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches, ValueEnum};
use common_traits::UnsignedInt;
use log::info;
use std::{
    mem::size_of,
    path::{Path, PathBuf},
};

pub const COMMAND_NAME: &str = "pad";

fn pad(path: impl AsRef<Path>, block_size: usize) -> Result<()> {
    let file_len = path
        .as_ref()
        .metadata()
        .with_context(|| {
            format!(
                "Cannot extract metadata from file {}",
                path.as_ref().display()
            )
        })?
        .len();
    let expected_len = file_len.align_to(block_size as u64);

    if file_len == expected_len {
        info!(
            "File {} already aligned to a block size of {} bytes",
            path.as_ref().display(),
            block_size
        );
        return Ok(());
    }

    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .open(path.as_ref())
        .with_context(|| format!("Cannot open file {} to pad", path.as_ref().display()))?;
    file.set_len(expected_len)
        .with_context(|| format!("Cannot extend file {}", path.as_ref().display()))?;
    info!(
        "File {} successfully zero-padded to align to a block size of {} bytes",
        path.as_ref().display(),
        block_size
    );

    Ok(())
}

#[derive(Args, Debug)]
#[command(about = "Zero-pad graph files to a length multiple of a block size", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
    /// The block size to align to
    #[clap(short, long, default_value_t, value_enum)]
    block_size: BlockSize,
}

#[derive(ValueEnum, Clone, Debug, Default)]
enum BlockSize {
    /// 1 byte
    U8,
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

    let block_size = match args.block_size {
        BlockSize::U8 => size_of::<u8>(),
        BlockSize::U16 => size_of::<u16>(),
        BlockSize::U32 => size_of::<u32>(),
        BlockSize::U64 => size_of::<u64>(),
        BlockSize::U128 => size_of::<u128>(),
    };
    let mut graph_filename = args.basename;
    graph_filename.set_extension("graph");

    ensure!(
        graph_filename.is_file(),
        "Cannot find graph file {}",
        graph_filename.display()
    );

    pad(&graph_filename, block_size).with_context(|| {
        format!(
            "Cannot pad file {} to a length multiple of {} bytes",
            graph_filename.display(),
            block_size
        )
    })?;
    Ok(())
}
