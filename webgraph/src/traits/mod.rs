/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Traits defining the core abstractions.
//!
//! - [`graph`]: [`SequentialGraph`], [`RandomAccessGraph`], and their labeled
//!   counterparts [`LabeledSequentialGraph`], [`LabeledRandomAccessGraph`];
//! - [`labels`]: [`SequentialLabeling`], [`RandomAccessLabeling`], and the
//!   [`NodeLabelsLender`] lending protocol;
//! - [`bit_serde`]: [`BitSerializer`] / [`BitDeserializer`] and the
//!   [`FixedWidth`] implementation for label I/O;
//! - [`store`]: [`StoreLabels`] / [`StoreLabelsConfig`] for writing labels
//!   alongside graph compression;
//! - [`split`]: [`SplitLabeling`] for parallel iteration;
//! - [`par_map_fold`]: [`IntoParLenders`] for parallel graph algorithms.

/// A support trait that makes it possible to treat a pair (2-tuple) as a trait.
///
/// This approach (“traitification”) was suggested by
/// [David Henry Mantilla] as a solution to the problem of specifying that a
/// [`Lender`] should return pairs of nodes and successors, and to impose
/// conditions on the two components of the pairs. This is not possible
/// directly, as a pair is a type, not a trait.
///
/// For example, [when implementing projections] one needs to specify that the
/// label of a labeling is a pair, and in the case a component is `usize`, the
/// associated projection can be seen as a graph. To specify these constraints
/// we have to resort to traitification using the [`Pair`] trait.
///
/// The user should rarely, if ever, interact with this trait. Iterating over
/// an iterator whose output has been traitified using [`Pair`] is a bit
/// cumbersome, as the output of the iterator is a [`Pair`] and must be turned
/// into a pair using the [`into_pair`] method.
///
/// [David Henry Mantilla]: https://github.com/danielhenrymantilla/lending-iterator.rs/issues/13#issuecomment-1735475634
/// [`Lender`]: lender::Lender
/// [when implementing projections]: crate::labels::proj
/// [`into_pair`]: Pair::into_pair
pub trait Pair {
    /// The type of the first component of the [`Pair`].
    type Left;
    /// The type of the second component of the [`Pair`].
    type Right;
    /// Turns this [`Pair`] into an actual pair (i.e., a Rust 2-tuple).
    fn into_pair(self) -> (Self::Left, Self::Right);
}

impl<T, U> Pair for (T, U) {
    type Left = T;
    type Right = U;

    fn into_pair(self) -> (Self::Left, Self::Right) {
        self
    }
}

pub mod graph;
pub use graph::*;

pub mod labels;
pub use labels::*;

pub mod bit_serde;
pub use bit_serde::*;

pub mod split;
pub use split::*;

pub mod par_map_fold;
pub use par_map_fold::*;

pub mod lenders;
pub use lenders::*;

pub mod store;
pub use store::*;
