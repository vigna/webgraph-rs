/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use anyhow::Result;
use webgraph_cli::dist::cli_main;
use webgraph_cli::init_env_logger;

pub fn main() -> Result<()> {
    // Initialize the logger
    init_env_logger()?;
    // Call the main function of the CLI with cli args
    cli_main(std::env::args_os())
}
