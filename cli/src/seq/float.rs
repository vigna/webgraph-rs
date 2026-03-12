/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{FloatSliceFormat, create_parent_dir};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "float", about = "Converts a float sequence between formats.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The source file path.​
    pub src: PathBuf,

    /// The destination file path.​
    pub dst: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the source file.​
    pub src_fmt: FloatSliceFormat,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the destination file.​
    pub dst_fmt: FloatSliceFormat,

    #[clap(long)]
    /// Number of decimal digits for text formats.​
    pub precision: Option<usize>,

    #[clap(long)]
    /// Treat the data as 32-bit floats instead of 64-bit.​
    pub f32: bool,
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    if args.f32 {
        let data: Vec<f32> = args.src_fmt.load(&args.src)?;
        log::info!(
            "Loaded {} f32 elements from {}",
            data.len(),
            args.src.display()
        );
        args.dst_fmt.store(&args.dst, &data, args.precision)?;
        log::info!(
            "Stored {} f32 elements to {}",
            data.len(),
            args.dst.display()
        );
    } else {
        let data: Vec<f64> = args.src_fmt.load(&args.src)?;
        log::info!(
            "Loaded {} f64 elements from {}",
            data.len(),
            args.src.display()
        );
        args.dst_fmt.store(&args.dst, &data, args.precision)?;
        log::info!(
            "Stored {} f64 elements to {}",
            data.len(),
            args.dst.display()
        );
    }

    Ok(())
}
