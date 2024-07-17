/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use clap::{value_parser, Command};
use clap_complete::shells::Shell;

use webgraph::{build_info, cli};

pub fn main() -> Result<()> {
    let start = std::time::Instant::now();
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

    impl_dispatch!(command, analyze, bench, build, check, from, perm, run, to, trasform, utils)?;

    log::info!(
        "The command took {}",
        pretty_print_elapsed(start.elapsed().as_secs_f64())
    );

    Ok(())
}

/// Pretty print the elapsed seconds in a human readable format.
fn pretty_print_elapsed(elapsed: f64) -> String {
    let mut result = String::new();
    let mut elapsed_seconds = elapsed as u64;
    let weeks = elapsed_seconds / (60 * 60 * 24 * 7);
    elapsed_seconds %= 60 * 60 * 24 * 7;
    let days = elapsed_seconds / (60 * 60 * 24);
    elapsed_seconds %= 60 * 60 * 24;
    let hours = elapsed_seconds / (60 * 60);
    elapsed_seconds %= 60 * 60;
    let minutes = elapsed_seconds / 60;
    //elapsed_seconds %= 60;

    match weeks {
        0 => {}
        1 => result.push_str("1 week "),
        _ => result.push_str(&format!("{} weeks ", weeks)),
    }
    match days {
        0 => {}
        1 => result.push_str("1 day "),
        _ => result.push_str(&format!("{} days ", days)),
    }
    match hours {
        0 => {}
        1 => result.push_str("1 hour "),
        _ => result.push_str(&format!("{} hours ", hours)),
    }
    match minutes {
        0 => {}
        1 => result.push_str("1 minute "),
        _ => result.push_str(&format!("{} minutes ", minutes)),
    }

    result.push_str(&format!("{:.3} seconds ({}s)", elapsed % 60.0, elapsed));
    result
}
