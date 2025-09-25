/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! An implementation of the Bv format.
//!
//! The format has been described by Paolo Boldi and Sebastiano Vigna in “[The
//!  WebGraph Framework I: Compression
//!  Techniques](http://vigna.di.unimi.it/papers.php#BoVWFI)”, in *Proc. of the
//!  13th international conference on World Wide Web*, WWW 2004, pages 595-602,
//!  ACM. [DOI
//!  10.1145/988672.988752](https://dl.acm.org/doi/10.1145/988672.988752).
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
use epserde::deser::DeserInner;
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
use sux::traits::IndexedSeq;

/// The default version of EliasFano we use for the CLI.
pub type EF = sux::dict::EliasFano<
    sux::rank_sel::SelectAdaptConst<sux::bits::BitVec<Box<[usize]>>, Box<[usize]>, 12, 4>,
    sux::bits::BitFieldVec<usize, Box<[usize]>>,
>;

/// Compound trait expressing the trait bounds for offsets.
///
/// We need [`DeserInner`] to be able to put the offsets in a [`MemCase`].
/// If you have an in-memory structure the requirement is irrelevant as
/// [`MemCase::encase`] will put the structure in a [`deserializable
/// wrapper`](epserde::deser::DeserializableWrapper).
pub trait Offsets:
    for<'a> DeserInner<DeserType<'a>: IndexedSeq<Input = usize, Output<'a> = usize>>
{
}
impl<T: for<'a> DeserInner<DeserType<'a>: IndexedSeq<Input = usize, Output<'a> = usize>>> Offsets
    for T
{
}

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
