/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! A compressed graph representation using the techniques described in "[The
//! WebGraph Framework I: Compression Techniques][BvGraph paper]", by Paolo
//! Boldi and Sebastiano Vigna, in _Proc. of the 13th international conference
//! on World Wide Web_, WWW 2004, pages 595–602, ACM.
//!
//! This module provides a flexible way to store and access graphs in compressed
//! form. A compressed graph with basename `BASENAME` is described by:
//!
//! - a _graph file_ (`BASENAME.graph`): a bitstream containing the compressed
//!   representation of the graph;
//! - a _properties file_ (`BASENAME.properties`): metadata about the graph and
//!   the compression parameters;
//! - an _offsets file_ (`BASENAME.offsets`): a bitstream of γ-coded gaps between
//!   the bit offsets of each successor list in the graph file.
//!
//! Additionally, an [Elias–Fano] representation of the offsets
//! (`BASENAME.ef`), necessary for random access, can be built using the
//! `webgraph build ef` command.
//!
//! The implementation is compatible with the [Java
//! implementation](http://webgraph.di.unimi.it/), but it provides also a
//! little-endian version.
//!
//! The main access points to the implementation are [`BvGraph::with_basename`]
//! and [`BvGraphSeq::with_basename`], which provide a [`LoadConfig`] that can
//! be further customized (e.g., selecting endianness, memory mapping, etc.).
//!
//! # The Graph File
//!
//! The graph is stored as a bitstream. The format depends on a number of
//! parameters and encodings that can be mixed orthogonally. The parameters are:
//!
//! - the _window size_, a nonnegative integer;
//! - the _maximum reference count_, a positive integer (it is meaningful only
//!   when the window is nonzero);
//! - the _minimum interval length_, an integer ≥ 2, or 0, which is interpreted
//!   as infinity.
//!
//! ## Successor lists
//!
//! The graph file is a sequence of successor lists, one for each node. The list
//! of node _x_ can be thought of as a sequence of natural numbers (even though,
//! as we will explain later, this sequence is further coded suitably as a
//! sequence of bits):
//!
//! 1. The _outdegree_ of the node; if it is zero, the list ends here.
//!
//! 2. If the window size is not zero, the _reference part_, that is:
//!    1. a nonnegative integer, the _reference_, which never exceeds the window
//!       size; if the reference is _r_, the list of successors will be specified
//!       as a modified version of the list of successors of _x_ − _r_; if _r_
//!       is 0, then the list of successors will be specified explicitly;
//!    2. if _r_ is nonzero:
//!       - a natural number β, the _block count_;
//!       - a sequence of β natural numbers *B*₁, …, *B*ᵦ, called the
//!         _copy-block list_; only the first number can be zero.
//!
//! 3. Then comes the _extra part_, specifying additional entries that the list
//!    of successors contains (or all of them, if _r_ is zero), that is:
//!    1. If the minimum interval length is finite:
//!       - an integer _i_, the _interval count_;
//!       - a sequence of _i_ pairs, whose first component is the left extreme
//!         of an interval, and whose second component is the length of the
//!         interval (the number of integers contained in it).
//!    2. Finally, the list of _residuals_, which contain all successors not
//!       specified by previous methods.
//!
//! The above data should be interpreted as follows:
//!
//! - The reference part, if present (i.e., if both the window size and the
//!   reference are positive), specifies that part of the list of successors of
//!   node _x_ − _r_ should be copied; the successors of node _x_ − _r_ that
//!   should be copied are described in the copy-block list; more precisely, one
//!   should copy the first *B*₁ entries of this list, discard the next *B*₂,
//!   copy the next *B*₃, etc. (the last remaining elements of the list of
//!   successors will be copied if β is even, and discarded if β is odd).
//!
//! - The extra part specifies additional successors (or all of them, if the
//!   reference part is absent); the extra part is not present if the number of
//!   successors that are to be copied according to the reference part already
//!   coincides with the outdegree of _x_; the successors listed in the extra
//!   part are given in two forms:
//!   - some of them are specified as belonging to (integer) intervals, if the
//!     minimum interval length is finite; the interval count indicates how many
//!     intervals, and the intervals themselves are listed as pairs (left
//!     extreme, length);
//!   - the residuals are the remaining "scattered" successors.
//!
//! ## How Successor Lists Are Coded
//!
//! The list of integers corresponding to each successor list is coded into a
//! sequence of bits. This is done in two phases: we first modify the sequence
//! so to obtain another sequence of integers (some of them might be negative).
//! Then each integer is coded, using a coding that can be specified as an
//! option; the integers that may be negative are first turned into natural
//! numbers using the standard bijection.
//!
//! 1. The outdegree of the node is left unchanged, as well as the reference and
//!    the block count.
//! 2. All blocks are decremented by 1, except for the first one.
//! 3. The interval count is left unchanged.
//! 4. All interval lengths are decremented by the minimum interval length.
//! 5. The first left extreme is expressed as its difference from _x_ (it will
//!    be negative if the first extreme is less than _x_); the remaining left
//!    extremes are expressed as their distance from the previous right extreme
//!    plus 2 (e.g., if the interval is \[5..11\] and the previous one was
//!    \[1..3\], then the left extreme 5 is expressed as 5 − (3 + 2) = 0).
//! 6. The first residual is expressed as its difference from _x_ (it will be
//!    negative if the first residual is less than _x_); the remaining residuals
//!    are expressed as decremented differences from the previous residual.
//!
//! # The Offsets File
//!
//! Since the graph is stored as a bitstream, we must have some way to know
//! where each successor list starts. This information is stored in the offset
//! file, which contains the bit offset of each successor list as a γ-coded gap
//! from the previous offset (in particular, the offset of the first successor
//! list will be zero). As a convenience, the offset file contains an additional
//! offset pointing just after the last successor list (providing, as a
//! side-effect, the actual bit length of the graph file).
//!
//! For random access, the list of offsets is stored as an [Elias–Fano]
//! representation using [ε-serde]. Building such a representation is a
//! prerequisite for random access and can be done using the `webgraph build ef`
//! command.
//!
//! [BvGraph paper]: <http://vigna.di.unimi.it/papers.php#BoVWFI>
//! [Elias–Fano]: <https://docs.rs/sux/latest/sux/dict/elias_fano/struct.EliasFano.html>
//! [ε-serde]: <https://docs.rs/epserde/latest/epserde/>

use std::path::Path;

use crate::traits::*;

pub const GRAPH_EXTENSION: &str = "graph";
pub const PROPERTIES_EXTENSION: &str = "properties";
pub const OFFSETS_EXTENSION: &str = "offsets";
pub const EF_EXTENSION: &str = "ef";
pub const LABELS_EXTENSION: &str = "labels";
pub const LABELOFFSETS_EXTENSION: &str = "labeloffsets";
pub const DEG_CUMUL_EXTENSION: &str = "dcf";

mod offset_deg_iter;
use dsi_bitstream::{
    codes::GammaRead,
    impls::{BufBitReader, WordAdapter, buf_bit_reader},
    traits::{BE, BitSeek, Endianness},
};
use epserde::deser::DeserInner;
pub use offset_deg_iter::OffsetDegIter;

pub mod sequential;
pub use sequential::BvGraphSeq;

pub mod random_access;
pub use random_access::BvGraph;

mod masked_iter;
pub use masked_iter::MaskedIter;

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
/// See the [`MemCase`](epserde::deser::MemCase) documentation for an
/// explanation as to why we bound first with [`DeserInner`] and then require
/// the bound we are interested in on the associated deserialization type.
pub trait Offsets:
    for<'a> DeserInner<DeserType<'a>: IndexedSeq<Input = usize, Output<'a> = usize>>
{
}
impl<T: for<'a> DeserInner<DeserType<'a>: IndexedSeq<Input = usize, Output<'a> = usize>>> Offsets
    for T
{
}

/// The default type we use for the cumulative function of degrees.
///
/// It provides an [indexed dictionary](sux::traits::indexed_dict::IndexedDict) with
/// [successor](sux::traits::indexed_dict::Succ) and [predecessor](sux::traits::indexed_dict::Pred) support.
///
/// This is the type returned by [`crate::traits::labels::SequentialLabeling::build_dcf`].
pub type DCF = sux::dict::EliasFano<
    sux::rank_sel::SelectZeroAdaptConst<
        sux::rank_sel::SelectAdaptConst<sux::bits::BitVec<Box<[usize]>>, Box<[usize]>, 12, 4>,
        Box<[usize]>,
        12,
        4,
    >,
    sux::bits::BitFieldVec<usize, Box<[usize]>>,
>;

/// Checks that the offsets stored in the offsets file with given
/// basename are correct for the given [`BvGraphSeq`].
pub fn check_offsets<F: for<'a> SequentialDecoderFactory<Decoder<'a>: BitSeek>>(
    graph: &BvGraphSeq<F>,
    basename: impl AsRef<Path>,
) -> anyhow::Result<bool> {
    let basename = basename.as_ref();
    let offsets_path = basename.with_extension(OFFSETS_EXTENSION);
    let mut offsets_reader = buf_bit_reader::from_path::<BE, u32>(&offsets_path)?;

    let mut offset = 0;
    for (real_offset, _degree) in graph.offset_deg_iter() {
        let gap_offset = offsets_reader.read_gamma()?;
        offset += gap_offset;
        assert_eq!(offset, real_offset);
    }
    Ok(true)
}
