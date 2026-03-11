/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

mod bvcomp;
mod bvcompdp;
mod bvcompla;
mod bvcompz;
mod bvcompz2;

pub use bvcomp::*;
pub use bvcompdp::*;
pub use bvcompla::*;
pub use bvcompz::*;
pub use bvcompz2::*;

mod impls;
pub use impls::{BvCompConfig, OffsetsWriter};

mod flags;
pub use flags::*;
