/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::{IntSlice, IntSliceFormat, create_parent_dir};
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use value_traits::slices::SliceByValue;

#[derive(Parser, Debug)]
#[command(name = "int", about = "Converts an integer sequence between formats.", long_about = None, next_line_help = true)]
pub struct CliArgs {
    /// The source file path.​
    pub src: PathBuf,

    /// The destination file path.​
    pub dst: PathBuf,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the source file.​
    pub src_fmt: IntSliceFormat,

    #[arg(long, value_enum, default_value_t)]
    /// The format of the destination file.​
    pub dst_fmt: IntSliceFormat,
}

/// Stores a slice with its computed max.​
fn store_slice(data: &[usize], dst: &PathBuf, dst_fmt: IntSliceFormat) -> Result<()> {
    let max = data.iter().copied().max().unwrap_or(0);
    dst_fmt.store(dst, data, Some(max))
}

/// Collects a [`SliceByValue`] into a [`Vec<usize>`] and stores it.​
fn collect_and_store(
    slice: &impl SliceByValue<Value = usize>,
    dst: &PathBuf,
    dst_fmt: IntSliceFormat,
) -> Result<()> {
    let data: Vec<usize> = (0..slice.len()).map(|i| slice.index_value(i)).collect();
    let max = data.iter().copied().max().unwrap_or(0);
    dst_fmt.store(dst, &data, Some(max))
}

pub fn main(args: CliArgs) -> Result<()> {
    create_parent_dir(&args.dst)?;

    let loaded = args.src_fmt.load(&args.src)?;
    log::info!(
        "Loaded {} elements from {}",
        loaded.len(),
        args.src.display()
    );

    match &loaded {
        IntSlice::Owned(v) => store_slice(v, &args.dst, args.dst_fmt)?,
        #[cfg(target_pointer_width = "64")]
        IntSlice::Java(j) => collect_and_store(j, &args.dst, args.dst_fmt)?,
        IntSlice::Epserde(m) => store_slice(m.uncase(), &args.dst, args.dst_fmt)?,
        IntSlice::BitFieldVec(m) => collect_and_store(m.uncase(), &args.dst, args.dst_fmt)?,
    }

    log::info!("Stored {} elements to {}", loaded.len(), args.dst.display());
    Ok(())
}
