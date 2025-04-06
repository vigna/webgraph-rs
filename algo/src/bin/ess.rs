/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::Result;
use dsi_progress_logger::no_logging;
use webgraph::prelude::BvGraph;
use webgraph_algo::{
    distances::exact_sum_sweep::{self, Level},
    thread_pool,
};

fn main() -> Result<()> {
    webgraph_cli::init_env_logger()?;

    let basename = std::env::args().nth(1).unwrap();
    let graph = BvGraph::with_basename(&basename).load()?;
    let transpose = BvGraph::with_basename(basename + "-t").load()?;
    let result = exact_sum_sweep::RadiusDiameter::run(
        graph,
        transpose,
        None,
        &thread_pool![],
        no_logging![],
    );
    println!("Diameter: {}, Radius: {}", result.diameter, result.radius);
    Ok(())
}
