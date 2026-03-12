/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{IntSlice, IntSliceFormat, LogIntervalArg, create_parent_dir};
use anyhow::{Result, ensure};
use clap::Parser;
use dsi_progress_logger::prelude::*;
use std::path::PathBuf;
use value_traits::slices::SliceByValue;

#[derive(Parser, Debug)]
#[command(name = "comp", about = "Composes multiple permutations into a single one.", long_about = None)]
pub struct CliArgs {
    /// The filename of the resulting permutation.
    pub dst: PathBuf,

    #[arg(num_args(1..))]
    /// Filenames of the permutations to compose (in order of application).
    pub perms: Vec<PathBuf>,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the source permutation files.
    pub src_fmt: IntSliceFormat,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the destination permutation file.
    pub dst_fmt: IntSliceFormat,

    #[clap(flatten)]
    pub log_interval: LogIntervalArg,
}

fn compose<P: SliceByValue<Value = usize>>(
    perms: &[&P],
    len: usize,
    pl: &mut ProgressLogger,
) -> Vec<usize> {
    let mut merged = Vec::with_capacity(len);
    pl.start("Combining permutations...");
    for i in 0..len {
        let mut v = i;
        for p in perms {
            v = p.index_value(v);
        }
        merged.push(v);
        pl.light_update();
    }
    pl.done();
    merged
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    let mut pl = ProgressLogger::default();
    pl.display_memory(true).item_name("indices");

    if let Some(duration) = args.log_interval.log_interval {
        pl.log_interval(duration);
    }

    let mut perms: Vec<IntSlice> = Vec::new();
    for path in &args.perms {
        perms.push(args.src_fmt.load(path)?);
    }

    let len = perms[0].len();
    ensure!(
        perms.iter().all(|p| p.len() == len),
        "All permutations must have the same length"
    );

    // Dispatch on the concrete type for static dispatch in the composition
    // loop. All perms share the same variant since they are loaded with the
    // same src_fmt.
    let merged = match &perms[0] {
        IntSlice::Owned(_) => {
            let refs: Vec<_> = perms
                .iter()
                .map(|p| {
                    let IntSlice::Owned(v) = p else {
                        unreachable!()
                    };
                    v
                })
                .collect();
            compose(&refs, len, &mut pl)
        }
        #[cfg(target_pointer_width = "64")]
        IntSlice::Java(_) => {
            let refs: Vec<_> = perms
                .iter()
                .map(|p| {
                    let IntSlice::Java(j) = p else { unreachable!() };
                    j
                })
                .collect();
            compose(&refs, len, &mut pl)
        }
        IntSlice::Epserde(_) => {
            let refs: Vec<_> = perms
                .iter()
                .map(|p| {
                    let IntSlice::Epserde(m) = p else {
                        unreachable!()
                    };
                    m.uncase()
                })
                .collect();
            compose(&refs, len, &mut pl)
        }
        IntSlice::BitFieldVec(_) => {
            let refs: Vec<_> = perms
                .iter()
                .map(|p| {
                    let IntSlice::BitFieldVec(m) = p else {
                        unreachable!()
                    };
                    m.uncase()
                })
                .collect();
            compose(&refs, len, &mut pl)
        }
    };

    args.dst_fmt.store(&args.dst, &merged, None)?;

    Ok(())
}
