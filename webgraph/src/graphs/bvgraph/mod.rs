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
//! (`BASENAME.ef`), necessary for random access, can be built
//! programmatically with [`store_ef_with_data`] / [`build_ef_with_data`] or
//! using the `webgraph build ef` command.
//!
//! The implementation is compatible with the [Java implementation], but it
//! provides also a little-endian version.
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
//! [Java implementation]: <http://webgraph.di.unimi.it/>

use std::path::Path;

use crate::traits::*;

pub const GRAPH_EXTENSION: &str = "graph";
pub const PROPERTIES_EXTENSION: &str = "properties";
pub const OFFSETS_EXTENSION: &str = "offsets";
pub const EF_EXTENSION: &str = "ef";
pub const LABELS_EXTENSION: &str = "labels";
pub const LABELS_BASENAME_SUFFIX: &str = "-labels";
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
use sux::{
    bits::{BitFieldVec, BitVec},
    dict::EliasFanoBuilder,
    prelude::{SelectAdaptConst, SelectZeroAdaptConst},
    traits::{TryIntoUnaligned, Unaligned},
};
use value_traits::slices::SliceByValue;

/// First parameter of [`SelectAdaptConst`] for [`EliasFano`] structures.
///
/// [`EliasFano`]: sux::dict::EliasFano
pub const LOG2_ONES_PER_INVENTORY: usize = 11;

/// Second parameter of [`SelectAdaptConst`] for [`EliasFano`] structures.
///
/// [`EliasFano`]: sux::dict::EliasFano
pub const LOG2_WORDS_PER_SUBINVENTORY: usize = 3;

/// The default [Elias–Fano] representation used for bit offsets.
///
/// Instances are built from a γ-coded offsets file with [`build_ef`] /
/// [`build_ef_with_data`], or built and serialized in one step with
/// [`store_ef`] / [`store_ef_with_data`].
///
/// [Elias–Fano]: sux::dict::EliasFano
pub type EF = Unaligned<
    sux::dict::EliasFano<
        u64,
        SelectAdaptConst<
            BitVec<Box<[usize]>>,
            Box<[usize]>,
            LOG2_ONES_PER_INVENTORY,
            LOG2_WORDS_PER_SUBINVENTORY,
        >,
        BitFieldVec<Box<[u64]>>,
    >,
>;

/// Builds an [`EF`] representation by reading γ-coded offset gaps from a file.
///
/// The offsets file must contain `num_nodes + 1` γ-coded gaps whose prefix sums
/// are the bit offsets of each node's data in the associated bitstream. The
/// `upper_bound` parameter is the universe of the Elias–Fano representation
/// (typically the bit-length of the bitstream).
///
/// Use [`no_logging`](dsi_progress_logger::no_logging) if no progress logging
/// is needed.
///
/// See also [`build_ef_with_data`], [`store_ef`], and [`store_ef_with_data`].
pub fn build_ef(
    num_nodes: usize,
    upper_bound: u64,
    offsets_path: impl AsRef<Path>,
    pl: &mut impl dsi_progress_logger::ProgressLog,
) -> anyhow::Result<EF> {
    let mut reader = buf_bit_reader::from_path::<BE, u32>(offsets_path.as_ref())?;
    let mut efb = EliasFanoBuilder::new(num_nodes + 1, upper_bound);
    let mut offset = 0u64;
    for _ in 0..num_nodes + 1 {
        offset += reader.read_gamma()?;
        efb.push(offset);
        pl.light_update();
    }
    let ef = efb.build();
    Ok(unsafe {
        ef.map_high_bits(
            SelectAdaptConst::<_, _, LOG2_ONES_PER_INVENTORY, LOG2_WORDS_PER_SUBINVENTORY>::new,
        )
        .try_into_unaligned()?
    })
}

/// Builds an [`EF`] representation by reading γ-coded offset gaps from a file,
/// computing the upper bound from the bit-length of a data file.
///
/// This is a convenience wrapper around [`build_ef`] that sets the upper bound
/// to `8 * file_size(data_path)`.
///
/// Use [`no_logging`](dsi_progress_logger::no_logging) if no progress logging
/// is needed.
///
/// See also [`build_ef`], [`store_ef`], and [`store_ef_with_data`].
pub fn build_ef_with_data(
    num_nodes: usize,
    data_path: impl AsRef<Path>,
    offsets_path: impl AsRef<Path>,
    pl: &mut impl dsi_progress_logger::ProgressLog,
) -> anyhow::Result<EF> {
    let file_len = 8 * std::fs::metadata(data_path.as_ref())?.len();
    build_ef(num_nodes, file_len, offsets_path, pl)
}

/// Builds and serializes an [`EF`] representation by reading γ-coded offset
/// gaps from a file.
///
/// This is a convenience wrapper around [`build_ef`] that also serializes the
/// result to `ef_path`.
///
/// See also [`build_ef`], [`build_ef_with_data`], and [`store_ef_with_data`].
pub fn store_ef(
    num_nodes: usize,
    upper_bound: u64,
    offsets_path: impl AsRef<Path>,
    ef_path: impl AsRef<Path>,
    pl: &mut impl dsi_progress_logger::ProgressLog,
) -> anyhow::Result<()> {
    let ef = build_ef(num_nodes, upper_bound, offsets_path, pl)?;
    let mut ef_file = std::io::BufWriter::new(std::fs::File::create(ef_path.as_ref())?);
    unsafe { epserde::ser::Serialize::serialize(&ef, &mut ef_file)? };
    Ok(())
}

/// Builds and serializes an [`EF`] representation by reading γ-coded offset
/// gaps from a file, computing the upper bound from the bit-length of a data
/// file.
///
/// This is a convenience wrapper around [`build_ef_with_data`] that also
/// serializes the result to `ef_path`.
///
/// See also [`build_ef`], [`build_ef_with_data`], and [`store_ef`].
pub fn store_ef_with_data(
    num_nodes: usize,
    data_path: impl AsRef<Path>,
    offsets_path: impl AsRef<Path>,
    ef_path: impl AsRef<Path>,
    pl: &mut impl dsi_progress_logger::ProgressLog,
) -> anyhow::Result<()> {
    let ef = build_ef_with_data(num_nodes, data_path, offsets_path, pl)?;
    let mut ef_file = std::io::BufWriter::new(std::fs::File::create(ef_path.as_ref())?);
    unsafe { epserde::ser::Serialize::serialize(&ef, &mut ef_file)? };
    Ok(())
}

/// Compound trait expressing the trait bounds for offsets.
///
/// See the [`MemCase`] documentation for an explanation as to why we bound
/// first with [`DeserInner`] and then require the bound we are interested in on
/// the associated deserialization type.
///
/// [`MemCase`]: epserde::deser::MemCase
pub trait Offsets: for<'a> DeserInner<DeserType<'a>: SliceByValue<Value = u64>> {}
impl<T: for<'a> DeserInner<DeserType<'a>: SliceByValue<Value = u64>>> Offsets for T {}

/// The default type for the cumulative function of degrees.
///
/// It provides an [indexed dictionary] with [successor] and [predecessor]
/// support.
///
/// Instances are built by [`SequentialLabeling::build_dcf`].
///
/// [indexed dictionary]: sux::traits::indexed_dict::IndexedDict
/// [successor]: sux::traits::indexed_dict::Succ
/// [predecessor]: sux::traits::indexed_dict::Pred
pub type DCF = Unaligned<
    sux::dict::EliasFano<
        u64,
        SelectZeroAdaptConst<
            SelectAdaptConst<
                BitVec<Box<[usize]>>,
                Box<[usize]>,
                LOG2_ONES_PER_INVENTORY,
                LOG2_WORDS_PER_SUBINVENTORY,
            >,
            Box<[usize]>,
            LOG2_ONES_PER_INVENTORY,
            LOG2_WORDS_PER_SUBINVENTORY,
        >,
        BitFieldVec<Box<[u64]>>,
    >,
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
