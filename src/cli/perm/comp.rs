/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::cli::create_parent_dir;
use crate::prelude::*;
use anyhow::{ensure, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use mmap_rs::MmapFlags;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use sux::traits::BitFieldSlice;

pub const COMMAND_NAME: &str = "comp";

#[derive(Args, Debug)]
#[command(about = "Compose multiple permutations into a single one", long_about = None)]
pub struct CliArgs {
    /// The filename of the resulting permutation in binary big-endian format.
    pub dst: PathBuf,

    /// Filenames of the permutations in binary big-endian format to compose (in order of application).
    pub perms: Vec<PathBuf>,

    #[arg(short, long)]
    /// Load and store permutations in ε-serde format.
    pub epserde: bool,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    merge_perms(submatches, CliArgs::from_arg_matches(submatches)?)
}

pub fn merge_perms(submatches: &ArgMatches, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true).item_name("indices");

    if let Some(duration) = submatches.get_one("log-interval") {
        pl.log_interval(*duration);
    }

    if args.epserde {
        let mut perm = Vec::new();
        for path in args.perms {
            let p = <Vec<usize>>::mmap(&path, Flags::RANDOM_ACCESS)?;
            perm.push(p);
        }
        let mut merged = Vec::new();

        ensure!(
            perm.iter().all(|p| p.len() == perm[0].len()),
            "All permutations must have the same length"
        );

        pl.start("Combining permutations...");
        for i in 0..perm[0].len() {
            let mut v = i;
            for p in &perm {
                v = p[v];
            }
            merged.push(v);
            pl.light_update();
        }
        pl.done();
        merged.store(&args.dst)?;
    } else {
        let mut writer = BufWriter::new(std::fs::File::create(&args.dst)?);
        let mut perm = Vec::new();
        for path in args.perms {
            let p = JavaPermutation::mmap(&path, MmapFlags::RANDOM_ACCESS)?;
            perm.push(p);
        }

        ensure!(
            perm.iter()
                .all(|p| p.as_ref().len() == perm[0].as_ref().len()),
            "All permutations must have the same length"
        );

        pl.start("Combining permutations...");
        for i in 0..perm[0].as_ref().len() {
            let mut v = i;
            for p in &perm {
                v = p.get(v);
            }
            writer.write_all(&(v as u64).to_be_bytes())?;
            pl.light_update();
        }
        pl.done();
    }
    Ok(())
}
