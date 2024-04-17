/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Stefano Zacchiroli
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{value_parser, Command};
use clap_complete::shells::Shell;

use webgraph::cli;

mod build_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));

    pub fn version_string() -> String {
        format!(
            "{}
git info: {} {} {}
build info: built for {} with {}",
            PKG_VERSION,
            GIT_VERSION.unwrap_or(""),
            GIT_COMMIT_HASH.unwrap_or(""),
            match GIT_DIRTY {
                None => "",
                Some(true) => "(dirty)",
                Some(false) => "(clean)",
            },
            TARGET,
            RUSTC_VERSION
        )
    }
}

pub fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init()?;

    let command = Command::new("webgraph")
        .about("Webgraph tools to build, convert, modify, and analyze webgraph files.")
        .version(build_info::version_string())
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
        )
        .after_help(
            "Environment (noteworthy environment variables used):
  RUST_MIN_STACK: minimum thread stack size (in bytes)
  TMPDIR: where to store temporary files (potentially very large ones)
",
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
        bfs,
        build,
        check_ef,
        convert,
        from_csv,
        llp,
        merge_perms,
        optimize_codes,
        pad,
        rand_perm,
        recompress,
        simplify,
        to_csv,
        transpose
    )
}
