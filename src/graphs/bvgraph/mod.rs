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
pub const LABELS_EXTENSION: &str = "labels";
pub const LABELOFFSETS_EXTENSION: &str = "labeloffsets";
pub const DEG_CUMUL_EXTENSION: &str = "dcf";

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

/// The default version of EliasFano we use for the CLI.
pub type EF = sux::dict::EliasFano<
    sux::rank_sel::SelectAdapt<sux::bits::BitVec<Box<[usize]>>, Box<[usize]>>,
    sux::bits::BitFieldVec<usize, Box<[usize]>>,
>;

/// The default version of EliasFano we use for the cumulative function of degrees.
pub type DCF = sux::dict::EliasFano<
    sux::rank_sel::SelectZeroAdapt<
        sux::rank_sel::SelectAdapt<
            sux::bits::BitVec<Box<[usize]>>, 
        Box<[usize]>>, 
    Box<[usize]>>,
    sux::bits::BitFieldVec<usize, Box<[usize]>>,
>;
