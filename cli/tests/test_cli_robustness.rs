/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use webgraph_cli::cli_main;

#[test]
fn test_build_ef_missing_nodes_property() -> Result<()> {
    // Regression: a .properties file without the 'nodes' key panicked on
    // unwrap instead of returning a contextual error.
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("bad");
    std::fs::write(basename.with_extension("properties"), "endianness = big\n")?;
    std::fs::write(basename.with_extension("graph"), b"")?;
    std::fs::write(basename.with_extension("offsets"), b"")?;
    let base = basename.display().to_string();
    let res = cli_main(vec!["webgraph", "build", "ef", &base]);
    assert!(res.is_err(), "expected error, got {res:?}");
    Ok(())
}

#[test]
fn test_unknown_endianness_is_an_error() -> Result<()> {
    // Regression: an unrecognized 'endianness' property panicked every
    // command dispatcher instead of returning an error.
    let tmp = tempfile::tempdir()?;
    let basename = tmp.path().join("weird");
    std::fs::write(
        basename.with_extension("properties"),
        "endianness = middle\nnodes = 1\narcs = 0\n",
    )?;
    std::fs::write(basename.with_extension("graph"), b"")?;
    let base = basename.display().to_string();
    let res = cli_main(vec!["webgraph", "build", "offsets", &base]);
    assert!(res.is_err(), "expected error, got {res:?}");
    Ok(())
}
