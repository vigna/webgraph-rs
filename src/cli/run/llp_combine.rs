/*
 * SPDX-FileCopyrightText: 2025 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{
    cli::{get_thread_pool, NumThreadsArg},
    prelude::*,
};
use anyhow::Result;
use clap::{ArgMatches, Args, Command, FromArgMatches};

use std::path::PathBuf;

pub const COMMAND_NAME: &str = "llp-combine";

use super::llp::store_perm;

#[derive(Args, Debug)]
#[command(about = "Combine the pre-compute labels from Layered Label Propagation into permutation.", long_about = None)]
pub struct CombineArgs {
    /// The folder where the LLP labels are stored.
    pub work_dir: PathBuf,

    /// A filename for the LLP permutation in binary big-endian format.
    pub perm: PathBuf,

    #[arg(short, long)]
    /// Save the permutation in ε-serde format.
    pub epserde: bool,

    /// The number of threads to use.
    #[command(flatten)]
    pub num_threads: NumThreadsArg,
}

pub fn cli(command: Command) -> Command {
    let sub_command = CombineArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0);
    command.subcommand(sub_command)
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CombineArgs::from_arg_matches(submatches)?;
    let thread_pool = get_thread_pool(args.num_threads.num_threads);
    thread_pool.install(|| -> Result<()> {
        let labels = combine_labels(args.work_dir)?;
        log::info!("Combined labels...");
        let rank_perm = labels_to_ranks(&labels);
        log::info!("Saving permutation...");
        store_perm(&rank_perm, &args.perm, args.epserde)?;
        Ok(())
    })
}
