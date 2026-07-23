/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Bitstream-based label storage: reading ([`labeling`]) and
//! writing ([`store`]).

pub mod labeling;
pub use labeling::*;

pub mod store;
pub use store::{BitStreamStoreLabelsConf, Uncompressed, Zstd};
