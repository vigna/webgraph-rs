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

#[test]
fn test_from_arcs_all_malformed_is_an_error() -> Result<()> {
    use clap::Parser;
    // Regression: an input where every line was skipped as malformed logged
    // an error but exited successfully, producing no graph.
    let tmp = tempfile::tempdir()?;
    let dst = tmp.path().join("out").display().to_string();
    let args =
        webgraph_cli::from::arcs::CliArgs::try_parse_from(["arcs", "--num-nodes", "3", &dst])?;
    // Spaces instead of tabs: every line is skipped as malformed.
    let res = webgraph_cli::from::arcs::from_csv(args, std::io::Cursor::new("0 1\n1 2\n"));
    assert!(res.is_err(), "expected error, got {res:?}");
    Ok(())
}

#[test]
fn test_from_arcs_lines_to_skip_ignores_comments() -> Result<()> {
    use clap::Parser;
    // Regression: --lines-to-skip counted comment lines, contrary to its
    // documentation ("Number of lines to skip, ignoring comment lines").
    let tmp = tempfile::tempdir()?;
    let dst = tmp.path().join("skip").display().to_string();
    let args = webgraph_cli::from::arcs::CliArgs::try_parse_from([
        "arcs",
        "--num-nodes",
        "3",
        "--lines-to-skip",
        "1",
        &dst,
    ])?;
    webgraph_cli::from::arcs::from_csv(
        args,
        std::io::Cursor::new("# header\n0\t1\n1\t2\n"),
    )?;
    // The first data line (0 -> 1) is skipped; only 1 -> 2 remains.
    let props = std::fs::read_to_string(format!("{dst}.properties"))?;
    assert!(props.contains("arcs=1"), "properties: {props}");
    Ok(())
}

#[test]
fn test_perm_comp_rejects_non_permutations() -> Result<()> {
    // Regression: 'perm comp' panicked on out-of-range values and silently
    // composed duplicate values into a non-permutation.
    let tmp = tempfile::tempdir()?;
    let good = tmp.path().join("good.txt");
    let out_of_range = tmp.path().join("oor.txt");
    let duplicated = tmp.path().join("dup.txt");
    let dst = tmp.path().join("out.txt");
    std::fs::write(&good, "1\n0\n")?;
    std::fs::write(&out_of_range, "0\n2\n")?;
    std::fs::write(&duplicated, "0\n0\n")?;
    let (good, out_of_range, duplicated, dst) = (
        good.display().to_string(),
        out_of_range.display().to_string(),
        duplicated.display().to_string(),
        dst.display().to_string(),
    );
    let res = cli_main(vec!["webgraph", "perm", "comp", &dst, &good, &out_of_range]);
    assert!(res.is_err(), "out-of-range accepted: {res:?}");
    let res = cli_main(vec!["webgraph", "perm", "comp", &dst, &good, &duplicated]);
    assert!(res.is_err(), "duplicate accepted: {res:?}");
    // --no-check skips the validation, composing even a non-permutation.
    cli_main(vec!["webgraph", "perm", "comp", "--no-check", &dst, &good, &duplicated])?;
    cli_main(vec!["webgraph", "perm", "comp", &dst, &good, &good])?;
    let composed = std::fs::read_to_string(&dst)?;
    assert_eq!(composed.trim(), "0\n1");
    Ok(())
}
