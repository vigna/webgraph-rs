/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use crate::{build_info, pretty_print_elapsed};
use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod birank;
pub mod pagerank;

#[derive(Subcommand, Debug)]
#[command(name = "rank")]
pub enum SubCommands {
    #[clap(name = "birank", visible_alias = "br")]
    BiRank(birank::CliArgs),
    #[clap(name = "pagerank", visible_alias = "pr")]
    PageRank(pagerank::CliArgs),
}

#[derive(Parser, Debug)]
#[command(name = "webgraph-rank", version=build_info::version_string(), max_term_width = 100, after_help = include_str!("../common_env.txt"))]
/// WebGraph tools computing centrality measures.​
pub struct Cli {
    #[command(subcommand)]
    command: SubCommands,
}

pub fn cli_main<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let start = std::time::Instant::now();
    let cli = Cli::parse_from(args);
    match cli.command {
        SubCommands::BiRank(args) => {
            birank::main(args)?;
        }
        SubCommands::PageRank(args) => {
            pagerank::main(args)?;
        }
    }

    log::info!(
        "The command took {}",
        pretty_print_elapsed(start.elapsed().as_secs_f64())
    );

    Ok(())
}
