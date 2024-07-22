/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Command line interface structs and functions, organized by subcommands.

use crate::build_info;
use anyhow::Result;
use clap::Command;
use std::path::{Path, PathBuf};

pub mod analyze;
pub mod bench;
pub mod build;
pub mod check;
pub mod common;
pub mod from;
pub mod perm;
pub mod run;
pub mod to;
pub mod transform;

pub const DEFAULT_STACK_SIZE: usize = 64 * 1024 * 1024;

/// Create a threadpool with the given number of threads and set the stack to either the env var or to
/// the default stack size `DEFAULT_STACK_SIZE`.
pub fn get_thread_pool(num_threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .stack_size(
            std::env::var("RUST_MIN_STACK")
                .map(|x| dbg!(x.parse::<usize>().unwrap()))
                .unwrap_or(crate::cli::DEFAULT_STACK_SIZE),
        )
        .build()
        .expect("Failed to create thread pool")
}

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

/// The entrypoint of the command line interface.
pub fn main<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let start = std::time::Instant::now();
    // it's ok to fail since this might be called multiple times in tests
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    let command = Command::new("webgraph")
        .about("Webgraph tools to build, convert, modify, and analyze webgraph files.")
        .version(build_info::version_string())
        .subcommand_required(true)
        .arg_required_else_help(true)
        .after_help(
            "Environment (noteworthy environment variables used):
RUST_MIN_STACK: minimum thread stack size (in bytes)
TMPDIR: where to store temporary files (potentially very large ones)
",
        );

    macro_rules! impl_dispatch {
        ($command:expr, $($module:ident),*) => {{
            let command = build::cli($command);
            $(
                let command = $module::cli(command);
            )*
            let command = command.display_order(0); // sort args alphabetically
            let mut completion_command = command.clone();
            let matches = command.get_matches_from(args);
            let subcommand = matches.subcommand();
            // if no command is specified, print the help message
            if subcommand.is_none() {
                completion_command.print_help().unwrap();
                return Ok(());
            }
            match subcommand.unwrap() {
                (build::COMMAND_NAME, sub_m) => build::main(sub_m, &mut completion_command),
                $(
                    ($module::COMMAND_NAME, sub_m) => $module::main(sub_m),
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

    impl_dispatch!(command, analyze, bench, check, from, perm, run, to, transform)?;

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
