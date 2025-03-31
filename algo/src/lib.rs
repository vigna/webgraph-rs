/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![doc = include_str!("../README.md")]

#[macro_use]
pub mod utils;
pub mod visits;

pub mod prelude {
    pub use crate::thread_pool;
    pub use crate::visits::breadth_first;
    pub use crate::visits::depth_first;
    pub use crate::visits::*;
}
