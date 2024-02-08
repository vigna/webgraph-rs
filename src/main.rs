/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::Command;
mod cli;

pub fn main() -> Result<()> {
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    let command = Command::new("webgraph")
        .about("Webgraph tools to build, convert, modify, and analyze webgraph files.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);

    macro_rules! impl_dispatch {
        ($command:expr, $($module:ident),*) => {{
            let command = $command;
            $(
                let command = cli::$module::cli(command);
            )*

            let matches = command.get_matches();
            match matches.subcommand() {
                $(
                    Some((cli::$module::COMMAND_NAME, sub_m)) => cli::$module::main(sub_m),
                )*
                _ => unreachable!(),
            }
        }};
    }

    impl_dispatch!(
        command,
        ascii_convert,
        bench_bvgraph,
        build_offsets,
        build_ef,
        check_ef,
        convert,
        from_csv,
        llp,
        optimize_codes,
        perm,
        rand_perm,
        recompress,
        simplify,
        to_csv,
        transpose
    )
}
