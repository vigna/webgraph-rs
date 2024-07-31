/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! An implementation of the Bv format.
//!
//! The format has been described by Paolo Boldi and Sebastiano Vigna in "[The
//! WebGraph Framework I: Compression
//! Techniques](https://dl.acm.org/doi/10.1145/988672.988752)", *Proc. of the
//! Thirteenth World–Wide Web Conference*, pages 595–601, 2004, ACM Press.
//!
//! The implementation is compatible with the [Java
//! implementation](http://webgraph.di.unimi.it/), but it provides also a
//! little-endian version, too.
//!
//! The main access point to the implementation is [`BvGraph::with_basename`],
//! which provides a [`LoadConfig`] that can be further customized.

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
pub use sequential::BvGraphSeq;

pub mod random_access;
pub use random_access::BvGraph;

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
    sux::rank_sel::SelectAdaptConst<sux::bits::BitVec<Box<[usize]>>, Box<[usize]>, 12, 4>,
    sux::bits::BitFieldVec<usize, Box<[usize]>>,
>;

/// The default version of EliasFano we use for the cumulative function of degrees.
pub type DCF = sux::dict::EliasFano<
    sux::rank_sel::SelectZeroAdaptConst<
        sux::rank_sel::SelectAdaptConst<sux::bits::BitVec<Box<[usize]>>, Box<[usize]>, 12, 4>,
        Box<[usize]>,
        12,
        4,
    >,
    sux::bits::BitFieldVec<usize, Box<[usize]>>,
>;
