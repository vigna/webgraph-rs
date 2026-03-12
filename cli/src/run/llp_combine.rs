/*
 * SPDX-FileCopyrightText: 2025 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{IntSliceFormat, NumThreadsArg, get_thread_pool};
use anyhow::Result;
use clap::Parser;
use webgraph_algo::{combine_labels, labels_to_ranks};

use std::path::PathBuf;

use super::llp::store_perm;

#[derive(Parser, Debug)]
#[command(name = "llp-combine", about = "Combines the pre-computed labels from Layered Label Propagation into a permutation.", long_about = None)]
pub struct CliArgs {
    /// The folder where the LLP labels are stored in Java format
    /// (big-endian 64-bit integers).
    pub work_dir: PathBuf,

    /// A filename for the LLP permutation.
    pub perm: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.
    pub fmt: IntSliceFormat,

    /// The number of threads to use.
    #[command(flatten)]
    pub num_threads: NumThreadsArg,
}

pub fn main(args: CliArgs) -> Result<()> {
    let thread_pool = get_thread_pool(args.num_threads.num_threads);
    thread_pool.install(|| -> Result<()> {
        let labels = combine_labels(args.work_dir)?;
        log::info!("Combined labels...");
        let rank_perm = labels_to_ranks(&labels);
        log::info!("Saving permutation...");
        store_perm(&rank_perm, &args.perm, args.fmt)?;
        Ok(())
    })
}
