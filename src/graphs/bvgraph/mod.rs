/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::*;

mod degrees_iter;
pub use degrees_iter::*;

pub mod sequential;
pub use sequential::*;

pub mod random_access;
pub use random_access::*;

mod bvgraph_writer;
pub use bvgraph_writer::*;

mod bvgraph_writer_par;
pub use bvgraph_writer_par::*;

mod masked_iterator;
pub use masked_iterator::*;

mod codecs;
pub use codecs::*;

mod load;
pub use load::*;

mod comp_flags;
pub use comp_flags::*;

/// The default version of EliasFano we use for the CLI
pub type EF = sux::dict::EliasFano<
    sux::rank_sel::SelectFixed2<sux::bits::CountBitVec, Vec<u64>, 8>,
    sux::bits::BitFieldVec,
>;
