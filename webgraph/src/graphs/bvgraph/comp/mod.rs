/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod bvcomp;
mod bvcompz;

pub use bvcomp::*;
pub use bvcompz::*;

mod impls;
pub use impls::{BvCompConf, OffsetsWriter};

mod flags;
pub use flags::*;
