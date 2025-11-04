/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2024 Stefano Zacchiroli
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

#[cfg(feature = "fuzz")]
pub mod fuzz;
pub mod graphs;
pub mod labels;
pub mod traits;
pub mod transform;
pub mod visits;

#[macro_use]
pub mod utils;

pub mod prelude {
    pub use crate::graphs::prelude::*;
    pub use crate::labels::*;
    pub use crate::traits::*;
    pub use crate::transform::*;
    pub use crate::utils::*;
    pub use crate::visits::breadth_first;
    pub use crate::visits::depth_first;
}
