/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Command line interface structs and functions, organized by subcommands.

use std::path::{Path, PathBuf};

pub mod ascii_convert;
pub mod bench;
pub mod build;
pub mod check_ef;
pub mod convert;
pub mod from_csv;
pub mod llp;
pub mod optimize_codes;
pub mod pad;
pub mod rand_perm;
pub mod recompress;
pub mod simplify;
pub mod to_csv;
pub mod transpose;
pub mod utils;

/// Appends a string to the filename of a path.
///
/// # Panics
/// - Will panic if there is no filename.
/// - Will panic in test mode if the path has an extension.
pub fn append(path: impl AsRef<Path>, s: impl AsRef<str>) -> PathBuf {
    debug_assert!(path.as_ref().extension().is_none());
    let mut path_buf = path.as_ref().to_owned();
    let mut filename = path_buf.file_name().unwrap().to_owned();
    filename.push(s.as_ref());
    path_buf.push(filename);
    path_buf
}
