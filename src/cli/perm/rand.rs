/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::cli::create_parent_dir;
use anyhow::{Context, Result};
use clap::{ArgMatches, Args, Command, FromArgMatches};
use dsi_progress_logger::prelude::*;
use epserde::ser::Serialize;
use rand::prelude::SliceRandom;
use std::io::prelude::*;
use std::path::PathBuf;

pub const COMMAND_NAME: &str = "rand";

#[derive(Args, Debug)]
#[command(about = "Creates a random permutation.", long_about = None)]
pub struct CliArgs {
    /// The number of elements in the permutation.
    pub len: usize,
    /// The random permutation in binary big-endian format.
    pub dst: PathBuf,

    #[arg(short = 'e', long)]
    /// Store the permutation in ε-serde format.
    pub epserde: bool,
}

pub fn cli(command: Command) -> Command {
    command.subcommand(CliArgs::augment_args(Command::new(COMMAND_NAME)).display_order(0))
}

pub fn main(submatches: &ArgMatches) -> Result<()> {
    let args = CliArgs::from_arg_matches(submatches)?;

    create_parent_dir(&args.dst)?;

    let mut rng = rand::rng();
    let mut perm = (0..args.len).collect::<Vec<_>>();
    perm.shuffle(&mut rng);

    if args.epserde {
        perm.store(&args.dst)
            .with_context(|| format!("Could not store permutation to {}", args.dst.display()))?;
    } else {
        let mut pl = ProgressLogger::default();
        pl.display_memory(true).item_name("index");
        if let Some(duration) = submatches.get_one("log-interval") {
            pl.log_interval(*duration);
        }

        let mut file =
            std::io::BufWriter::new(std::fs::File::create(&args.dst).with_context(|| {
                format!("Could not create permutation at {}", args.dst.display())
            })?);
        pl.start("Writing permutation indices...");
        for perm in perm {
            file.write_all(&perm.to_be_bytes()).with_context(|| {
                format!("Could not write permutation to {}", args.dst.display())
            })?;
            pl.light_update();
        }
        pl.done();
    }

    Ok(())
}
