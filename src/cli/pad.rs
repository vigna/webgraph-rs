/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use anyhow::{ensure, Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches, ValueEnum};
use common_traits::UnsignedInt;
use log::info;
use std::{
    ffi::OsString,
    mem::size_of,
    path::{Path, PathBuf},
};

pub const COMMAND_NAME: &str = "pad";

fn pad(path: impl AsRef<Path>, block_size: u64) -> Result<()> {
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
    let expected_len = file_len.align_to(block_size);
    if file_len == expected_len {
        info!(
            "File {} already aligned to a block size of {} bytes",
            path.as_ref().display(),
            block_size
        );
    } else {
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
    }
    Ok(())
}

#[derive(Args, Debug)]
#[command(about = "Zero pad graph files to align to the specified size", long_about = None)]
struct CliArgs {
    /// The basename of the graph.
    basename: PathBuf,
    /// The block size to align to
    #[clap(short, long, default_value_t, value_enum)]
    block_size: BlockSize,
}

#[derive(ValueEnum, Clone, Debug, Default)]
enum BlockSize {
    U8,
    U16,
    U32,
    #[default]
    U64,
    U128,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    // Parse arguments
    let args = CliArgs::from_arg_matches(submatches)?;
    let block_size = match args.block_size {
        BlockSize::U8 => size_of::<u8>(),
        BlockSize::U16 => size_of::<u16>(),
        BlockSize::U32 => size_of::<u32>(),
        BlockSize::U64 => size_of::<u64>(),
        BlockSize::U128 => size_of::<u128>(),
    };
    let base_filename = args.basename.file_name().unwrap_or_default();
    let dir = match args.basename.parent() {
        Some(d) => {
            if d.to_str().is_some_and(|s| !s.is_empty()) {
                d.to_owned()
            } else {
                std::env::current_dir().with_context(|| "Cannot read current directory")?
            }
        }
        None => std::env::current_dir().with_context(|| "Cannot read current directory")?,
    };
    // dir must be an existing directory and graph_filename must be an existing file
    ensure!(
        dir.exists(),
        format!("Directory {} does not exist", dir.display())
    );
    ensure!(
        dir.is_dir(),
        format!("{} is not a directory", dir.display())
    );
    ensure!(
        suffix_path(dir.join(base_filename), ".graph").is_file(),
        format!(
            "graph file {}.graph not found in {}",
            base_filename.to_str().unwrap_or(""),
            dir.display()
        )
    );
    let paths = std::fs::read_dir(&dir)
        .with_context(|| format!("Cannot read directory {}", dir.display()))?;
    for entry in paths {
        let path = entry.with_context(|| "Cannot read fs entry")?.path();
        let base_name = path.file_stem().unwrap_or(&OsString::from("")).to_owned();
        // Pad every file that has the correct base name
        if base_filename == base_name && path.is_file() {
            pad(
                &path,
                block_size
                    .try_into()
                    .with_context(|| "Cannot convert usize to u64")?,
            )
            .with_context(|| format!("Cannot pad file {}", path.display()))?;
        }
    }
    Ok(())
}
