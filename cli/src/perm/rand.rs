/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{GlobalArgs, IntSliceFormat, create_parent_dir};
use anyhow::Result;
use clap::Parser;
use rand::prelude::SliceRandom;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "rand", about = "Creates a random permutation.", long_about = None)]
pub struct CliArgs {
    /// The number of elements in the permutation.
    pub len: usize,
    /// The filename of the random permutation.
    pub dst: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the permutation file.
    pub fmt: IntSliceFormat,
}

pub fn main(_global_args: GlobalArgs, args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    let mut rng = rand::rng();
    let mut perm = (0..args.len).collect::<Vec<_>>();
    perm.shuffle(&mut rng);

    args.fmt.store(&args.dst, &perm, None)?;

    Ok(())
}
