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
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;

    let command = Command::new("webgraph")
        .about("Webgraph tools to build, convert, modify, and analyze webgraph files.")
        .subcommand_required(true)
        .arg_required_else_help(true)
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
            let subcommand = matches.subcommand();
            // if no command is specified, print the help message
            if subcommand.is_none() {
                completion_command.print_help().unwrap();
                return Ok(());
            }
            match subcommand.unwrap() {
                ("generate-completions", sub_m) => {
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
                    (cli::$module::COMMAND_NAME, sub_m) => cli::$module::main(sub_m),
                )*
                (command_name, _) => {
                    // this shouldn't happen as clap should catch this
                    eprintln!("Unknown command: {:?}", command_name);
                    completion_command.print_help().unwrap();
                    std::process::exit(1);
                }
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
        llp,
        optimize_codes,
        pad,
        rand_perm,
        recompress,
        simplify,
        to_csv,
        transpose
    )
}
