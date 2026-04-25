/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Bitstream-based label storage: reading ([`BitStreamLabeling`]) and
//! writing ([`BitStreamLabelComp`]).

pub mod comp;
pub use comp::*;

pub mod labeling;
pub use labeling::*;
