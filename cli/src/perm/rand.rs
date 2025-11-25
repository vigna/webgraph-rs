/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{GlobalArgs, create_parent_dir};
use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::prelude::*;
use epserde::ser::Serialize;
use rand::prelude::SliceRandom;
use std::io::prelude::*;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "rand", about = "Creates a random permutation.", long_about = None)]
pub struct CliArgs {
    /// The number of elements in the permutation.
    pub len: usize,
    /// The random permutation in binary big-endian format.
    pub dst: PathBuf,

    #[arg(short = 'e', long)]
    /// Store the permutation in ε-serde format.
    pub epserde: bool,
}

pub fn main(global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    let mut rng = rand::rng();
    let mut perm = (0..args.len).collect::<Vec<_>>();
    perm.shuffle(&mut rng);

    if args.epserde {
        unsafe {
            perm.store(&args.dst)
                .with_context(|| format!("Could not store permutation to {}", args.dst.display()))
        }?;
    } else {
        let mut pl = ProgressLogger::default();
        pl.display_memory(true).item_name("index");
        if let Some(duration) = global_args.log_interval {
            pl.log_interval(duration);
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
