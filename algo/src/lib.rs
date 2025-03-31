/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![doc = include_str!("../README.md")]

#[macro_use]
pub mod utils;
mod acyclicity;
pub use acyclicity::is_acyclic;
pub mod sccs;
mod top_sort;
pub use top_sort::top_sort;
pub mod visits;

pub mod prelude {
    pub use crate::acyclicity::is_acyclic;
    pub use crate::sccs::*;
    pub use crate::thread_pool;
    pub use crate::top_sort::top_sort;
    pub use crate::visits::breadth_first;
    pub use crate::visits::depth_first;
    pub use crate::visits::*;
}
