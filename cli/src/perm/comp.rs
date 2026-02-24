/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{GlobalArgs, create_parent_dir};
use anyhow::{Result, ensure};
use clap::Parser;
use dsi_progress_logger::prelude::*;
use epserde::prelude::*;
use mmap_rs::MmapFlags;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use value_traits::slices::SliceByValue;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "comp", about = "Compose multiple permutations into a single one", long_about = None)]
pub struct CliArgs {
    /// The filename of the resulting permutation in binary big-endian format.
    pub dst: PathBuf,

    /// Filenames of the permutations in binary big-endian format to compose (in order of application).
    pub perms: Vec<PathBuf>,

    #[arg(short, long)]
    /// Load and store permutations in ε-serde format.
    pub epserde: bool,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true).item_name("indices");

    if let Some(duration) = global_args.log_interval {
        pl.log_interval(duration);
    }

    if args.epserde {
        let mut perm = Vec::new();
        for path in args.perms {
            let p = unsafe { <Vec<usize>>::mmap(&path, Flags::RANDOM_ACCESS) }?;
            perm.push(p);
        }
        let mut merged = Vec::new();
        let len = perm[0].uncase().len();
        ensure!(
            perm.iter().all(|p| p.uncase().len() == len),
            "All permutations must have the same length"
        );

        pl.start("Combining permutations...");
        for i in 0..len {
            let mut v = i;
            for p in &perm {
                v = p.uncase()[v];
            }
            merged.push(v);
            pl.light_update();
        }
        pl.done();
        // SAFETY: the type is ε-serde serializable.
        unsafe { merged.store(&args.dst) }?;
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
                v = p.index_value(v);
            }
            writer.write_all(&(v as u64).to_be_bytes())?;
            pl.light_update();
        }
        pl.done();
    }
    Ok(())
}
