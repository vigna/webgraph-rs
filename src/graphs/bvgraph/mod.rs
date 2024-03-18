/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::traits::*;

pub const GRAPH_EXTENSION: &str = "graph";
pub const PROPERTIES_EXTENSION: &str = "properties";
pub const OFFSETS_EXTENSION: &str = "offsets";
pub const EF_EXTENSION: &str = "ef";

mod offset_deg_iter;
pub use offset_deg_iter::OffsetDegIter;

pub mod sequential;
pub use sequential::BVGraphSeq;

pub mod random_access;
pub use random_access::BVGraph;

mod masked_iterator;
pub use masked_iterator::MaskedIterator;

mod codecs;
pub use codecs::*;

mod comp;
pub use comp::*;

mod load;
pub use load::*;

/// The default version of EliasFano we use for the CLI
pub type EF = sux::dict::EliasFano<
    sux::rank_sel::SelectFixed2<sux::bits::CountBitVec, Vec<u64>, 8>,
    sux::bits::BitFieldVec,
>;
