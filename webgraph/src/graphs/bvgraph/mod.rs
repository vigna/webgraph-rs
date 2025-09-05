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
use epserde::Epserde;
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
use sux::traits::{IndexedSeq, Types};

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

#[derive(Epserde, Debug, Clone, Copy, PartialEq, Eq)]
pub struct SliceSeq<O: PartialEq<usize> + PartialEq + Copy, A: AsRef<[O]>>(
    A,
    std::marker::PhantomData<O>,
)
where
    usize: PartialEq<O>;

impl<O: PartialEq<usize> + PartialEq + Copy, A: AsRef<[O]>> SliceSeq<O, A>
where
    usize: PartialEq<O>,
{
    pub fn new(slice: A) -> Self {
        Self(slice, std::marker::PhantomData)
    }
}

impl<O: PartialEq<usize> + PartialEq + Copy, A: AsRef<[O]>> From<A> for SliceSeq<O, A>
where
    usize: PartialEq<O>,
{
    fn from(slice: A) -> Self {
        Self::new(slice)
    }
}

impl<O: PartialEq<usize> + PartialEq + Copy, A: AsRef<[O]>> Types for SliceSeq<O, A>
where
    usize: PartialEq<O>,
{
    type Input = usize;
    type Output<'a> = O;
}

impl<O: PartialEq<usize> + PartialEq + Copy, A: AsRef<[O]>> IndexedSeq for SliceSeq<O, A>
where
    usize: PartialEq<O>,
{
    unsafe fn get_unchecked(&self, index: usize) -> Self::Output<'_> {
        unsafe { *self.0.as_ref().get_unchecked(index) }
    }

    fn len(&self) -> usize {
        self.0.as_ref().len()
    }
}
