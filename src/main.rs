/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use webgraph::cli::main as cli_main;

pub fn main() -> Result<()> {
    // Call the main function of the CLI with cli args
    cli_main(std::env::args_os())
}
