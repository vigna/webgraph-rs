/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

mod bvcomp;
mod bvcompz;
mod bvcompz2;
mod bvcompla;

pub use bvcomp::*;
pub use bvcompz::*;
pub use bvcompz2::*;
pub use bvcompla::*;

mod impls;
pub use impls::{BvCompConfig, OffsetsWriter};

mod flags;
pub use flags::*;
