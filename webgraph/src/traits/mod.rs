/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Traits used throughout the crate.

/**

A support trait that makes it possible to treat a pair (2-tuple) as a trait.

This approach (“traitification”) was suggested by
[David Henry Mantilla](https://github.com/danielhenrymantilla/lending-iterator.rs/issues/13#issuecomment-1735475634)
as a solution to the problem of specifying that a [`Lender`](lender::Lender)
should return pairs of nodes and successors, and to impose conditions on the two components
of the pairs. This is not possible directly, as a pair is a type, not a trait.

For example, [when implementing projections](crate::labels::proj) one need
to specify that the label of a labeling is a pair, and in the case a
component is `usize`, the associated projection can be seen as a graph.
To specify these constraints we have to resort to traitification using
the [`Pair`] trait.

The user should rarely, if ever, interact with this trait. Iterating over an iterator whose output
has been traitified using [`Pair`] is a bit cumbersome, as the output of the iterator is a [`Pair`]
and must be turned into a pair using the [`into_pair`](Pair::into_pair) method.

*/
pub trait Pair {
    /// The type of the first component of the [`Pair`].
    type Left;
    /// The type of the second component of the [`Pair`].
    type Right;
    /// Turn this [`Pair`] into an actual pair (i.e., a Rust 2-tuple).
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

mod serde;
pub use serde::*;

pub mod split;
pub use split::*;

pub mod par_map_fold;
pub use par_map_fold::*;

pub mod lenders;
pub use lenders::*;
