/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![doc = include_str!("../README.md")]
#![deny(unstable_features)]
#![deny(trivial_casts)]
#![deny(unconditional_recursion)]
#![deny(clippy::empty_loop)]
#![deny(unreachable_code)]
#![deny(unreachable_pub)]
#![deny(unreachable_patterns)]
#![deny(unused_macro_rules)]
#![deny(unused_doc_comments)]
#![allow(clippy::type_complexity)]

#[macro_use]
pub mod utils;
mod acyclicity;
pub use acyclicity::is_acyclic;

pub mod llp;
pub use llp::*;

pub mod rank;
pub mod sccs;
mod top_sort;
pub use top_sort::top_sort;
pub mod distances;
pub mod prelude {
    pub use crate::acyclicity::is_acyclic;
    pub use crate::distances;
    pub use crate::rank;
    pub use crate::sccs::*;
    pub use crate::top_sort::top_sort;
}
