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

pub mod algo;
#[cfg(feature = "cli")]
pub mod cli;
#[cfg(feature = "fuzz")]
pub mod fuzz;
pub mod graphs;
pub mod labels;
pub mod traits;
pub mod transform;
pub mod utils;

pub mod build_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));

    pub fn version_string() -> String {
        format!(
            "{}
git info: {} {} {}
build info: built for {} with {}",
            PKG_VERSION,
            GIT_VERSION.unwrap_or(""),
            GIT_COMMIT_HASH.unwrap_or(""),
            match GIT_DIRTY {
                None => "",
                Some(true) => "(dirty)",
                Some(false) => "(clean)",
            },
            TARGET,
            RUSTC_VERSION
        )
    }
}

pub mod prelude {
    pub use crate::algo::*;
    pub use crate::graphs::prelude::*;
    pub use crate::labels::*;
    pub use crate::traits::*;
    pub use crate::transform::*;
    pub use crate::utils::*;
}
