/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#![doc = include_str!("../README.md")]
// No warnings
//#![deny(warnings)]
// for now we don't need any new feature but we might remove this in the future
#![deny(unstable_features)]
// no dead code
//#![deny(dead_code)]
#![deny(trivial_casts)]
#![deny(unconditional_recursion)]
#![deny(clippy::empty_loop)]
#![deny(unreachable_code)]
#![deny(unreachable_pub)]
#![deny(unreachable_patterns)]
#![deny(unused_macro_rules)]
//#![deny(unused_results)]

// the code must be documented and everything should have a debug print implementation
#![deny(unused_doc_comments)]
//#![deny(missing_docs)]
//#![deny(clippy::missing_docs_in_private_items)]
//#![deny(clippy::missing_errors_doc)]
//#![deny(clippy::missing_panics_doc)]
//#![deny(clippy::missing_safety_doc)]
//#![deny(clippy::missing_doc_code_examples)]
//#![deny(clippy::missing_crate_level_docs)]
//#![deny(missing_debug_implementations)]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

/**

A support trait that makes it possible to treat a pair (2-tuple) as a trait.

This approach ("traitification") was suggested by
[David Henry Mantilla](https://github.com/danielhenrymantilla/lending-iterator.rs/issues/13#issuecomment-1735475634)
as a solution to the problem of specifying that a [`Lender`](hrbt_lending_iterator::Lender)
should return pairs of nodes and successors, and to impose conditions on the two components
of the pairs. This is not possible directly, as a pair is a type, not a trait.

For example, [`VecGraph::from_node_iter`](crate::graph::vec_graph::VecGraph::from_node_iter) accepts
an [`IntoLender`](hrbt_lending_iterator::IntoLender), but only if it returns
pairs whose first component is a `usize` and the second component is an [`IntoIterator`](std::iter::IntoIterator).
To specify these constraints we have to resort to traitification using the [`Tuple2`] trait. Note in particular
that the first constraint is an equality constraint, whereas the second constraint is a trait bound.

The user should rarely, if ever, interact with this trait. Iterating over an iterator whose output
has been traitified using [`Tuple2`] is a bit cumbersome, as the output of the iterator is a [`Tuple2`]
and must be turned into a pair using the [`into_tuple`](Tuple2::into_tuple) method, but the
[`for_iter!`] macro takes care of all these details for you.

*/
pub trait Tuple2 {
    /// The type of the first component of the [`Tuple2`].
    type _0;
    /// The type of the second component of the [`Tuple2`].
    type _1;
    /// Turn this [`Tuple2`] into a pair.
    fn into_tuple(self) -> (Self::_0, Self::_1);
}

impl<T, U> Tuple2 for (T, U) {
    type _0 = T;
    type _1 = U;

    fn into_tuple(self) -> (Self::_0, Self::_1) {
        self
    }
}

pub mod algorithms;
#[cfg(feature = "fuzz")]
pub mod fuzz;
pub mod graph;
pub mod traits;
pub mod utils;

/// The default version of EliasFano we use for the CLI
pub type EF<Memory> = sux::dict::EliasFano<
    sux::rank_sel::SelectFixed1<sux::bits::CountBitVec<Memory>, Memory, 8>,
    sux::bits::BitFieldVec<usize, Memory>,
>;

/// Prelude module to import everything from this crate
pub mod prelude {
    pub use crate::algorithms::*;
    pub use crate::graph::prelude::*;
    pub use crate::traits::graph::*;
    pub use crate::traits::*;
    pub use crate::utils::*;
    pub use crate::Tuple2;
}

/**

A macro to iterate easily over lending iterators returning pairs of nodes
and associated successors.

Iterating over a graph is fairly easy using the `while let` syntax. If however
you have a method receiving a generic [`Lender`](hrtb_lending_iterator::Lender) or
[`IntoLender`](hrtb_lending_iterator::IntoLender) returning pairs of nodes and successors,
such as, for example, [`VecGraph::add_node_iter`](crate::graph::vec_graph::VecGraph::add_node_iter), due
to traitification of 2-tuples using the [`Tuple2`] trait the syntax
is rather cumbersome.

This macro takes care of extracting the iterator and iterating over
it using the `while let` syntax, turning items into pairs.
The syntax makes it possible to write loops such as
```ignore
for_iter!{(x, succ) in iter =>
    println!("{}", x);
    for s in succ {
       println!("{}", s);
    }
}
```
*/
#[macro_export]
macro_rules! for_iter {
    (($node:ident, $succ:ident) in $iter:expr => $($tt:tt)*) => {
        let mut iter = $iter.into_lender();
        while let Some(($node, $succ)) = iter.next().map(|it| crate::Tuple2::into_tuple(it)) {
            $($tt)*
        }
    };
    ((_, $succ:ident) in $iter:expr => $($tt:tt)*) => {
        let mut iter = $iter.into_lender();
        while let Some((_, $succ)) = iter.next().map(|it| crate::Tuple2::into_tuple(it)) {
            $($tt)*
        }
    };
    (($node:ident, _) in $iter:expr => $($tt:tt)*) => {
        let mut iter = $iter.into_lender();
        while let Some(($node, _)) = iter.next().map(|it| crate::Tuple2::into_tuple(it)) {
            $($tt)*
        }
    };
    ((_, _) in $iter:expr => $($tt:tt)*) => {
        let mut iter = $iter.into_lender();
        while let Some((_, _)) = iter.next().map(|it| crate::Tuple2::into_tuple(it)) {
            $($tt)*
        }
    };
}

/// Invert the given permutation in place.
pub fn invert_in_place(perm: &mut [usize]) {
    for n in 0..perm.len() {
        let mut i = perm[n];
        if (i as isize) < 0 {
            perm[n] = !i;
        } else if i != n {
            let mut k = n;
            loop {
                let j = perm[i];
                perm[i] = !k;
                if j == n {
                    perm[n] = i;
                    break;
                }
                k = i;
                i = j;
            }
        }
    }
}

#[cfg(test)]
#[test]
fn test_invert_in_place() {
    use rand::prelude::SliceRandom;
    let mut v = (0..1000).collect::<Vec<_>>();
    v.shuffle(&mut rand::thread_rng());
    let mut w = v.clone();
    invert_in_place(&mut w);
    for i in 0..v.len() {
        assert_eq!(w[v[i]], i);
    }
}
