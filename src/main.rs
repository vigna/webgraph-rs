/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{value_parser, Command};
use clap_complete::shells::Shell;

use webgraph::cli;

pub fn main() -> Result<()> {
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()?;

    let command = Command::new("webgraph")
        .about("Webgraph tools to build, convert, modify, and analyze webgraph files.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("generate-completions")
                .about("Generates shell completions.")
                .arg(
                    clap::Arg::new("shell")
                        .required(true)
                        .value_parser(value_parser!(Shell)),
                ),
        );

    macro_rules! impl_dispatch {
        ($command:expr, $($module:ident),*) => {{
            let command = $command;
            $(
                let command = cli::$module::cli(command);
            )*
            let mut completion_command = command.clone();
            let matches = command.get_matches();
            match matches.subcommand() {
                Some(("generate-completions", sub_m)) => {
                    let shell = sub_m.get_one::<Shell>("shell").unwrap();
                    clap_complete::generate(
                        *shell,
                        &mut completion_command,
                        "bvgraph",
                        &mut std::io::stdout(),
                    );
                    return Ok(());
                },
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
        bench,
        build,
        check_ef,
        convert,
        from_csv,
        hyperball,
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
