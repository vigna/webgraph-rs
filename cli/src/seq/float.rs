/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::FloatSliceFormat;
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "float", about = "Converts a float sequence (e.g., centralities, ranks) between formats.", long_about = None, next_line_help = true)]
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
    /// Treats the data as 32-bit floats instead of 64-bit.​
    pub f32: bool,
}

pub fn main(args: CliArgs) -> Result<()> {
    // Converts the sequence using the given float type.
    macro_rules! convert {
        ($F:ty) => {{
            let data: Vec<$F> = args.src_fmt.load(&args.src)?;
            let type_name = stringify!($F);
            log::info!(
                "Loaded {} {type_name} elements from {}",
                data.len(),
                args.src.display()
            );
            args.dst_fmt.store(&args.dst, &data, args.precision)?;
            log::info!(
                "Stored {} {type_name} elements to {}",
                data.len(),
                args.dst.display()
            );
        }};
    }

    if args.f32 {
        convert!(f32)
    } else {
        convert!(f64)
    }

    Ok(())
}
