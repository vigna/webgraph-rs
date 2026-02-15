/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::vec::IntoIter;

use lender::{Lend, Lender, Lending, check_covariance};
use rand::{RngExt, SeedableRng, rngs::SmallRng};

use crate::{
    prelude::{NodeLabelsLender, SequentialGraph, SequentialLabeling},
    traits::{SortedIterator, SortedLender},
};

/// Provides a sequential implementation of Erdös-Rényi random graphs.
///
/// The Erdös-Rényi random graph model is a simple model for generating random
/// graphs. It is parameterized by the number of nodes `n` and the probability
/// `p` of an arc between any two nodes. In this implementation, loops are never
/// included.
///
/// Note that the time required to iterate over the graph is quadratic in `n`,
/// so if you plan to reuse it you should store the result in a more efficient
/// structure, such as a [`VecGraph`](crate::graphs::prelude::VecGraph). The
/// same applies if you need random access.
#[derive(Debug, Clone)]
pub struct ErdosRenyi {
    n: usize,
    p: f64,
    seed: u64,
}

impl ErdosRenyi {
    /// Creates a new Erdös-Rényi random graph, given the number of
    /// nodes, the probability of an edge between any two nodes, and a
    /// seed for the [pseudorandom number generator](SmallRng).
    pub fn new(n: usize, p: f64, seed: u64) -> Self {
        assert!((0.0..=1.0).contains(&p), "p must be in [0..1]");
        Self { n, p, seed }
    }
}

impl SequentialLabeling for ErdosRenyi {
    type Label = usize;
    type Lender<'a> = Iter;
    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.n
    }

    fn iter_from(&self, from: usize) -> Iter {
        let mut rng = SmallRng::seed_from_u64(self.seed);
        if self.n > 0 {
            for _ in 0..from * (self.n - 1) {
                rng.random_bool(self.p);
            }
        }
        Iter {
            n: self.n,
            p: self.p,
            x: from,
            rng,
        }
    }
}

unsafe impl SortedLender for Iter {}
unsafe impl SortedIterator for SuccIntoIter {}

#[derive(Debug, Clone)]
pub struct Iter {
    n: usize,
    p: f64,
    x: usize,
    rng: SmallRng,
}

impl NodeLabelsLender<'_> for Iter {
    type Label = usize;
    type IntoIterator = Succ;
}

impl<'succ> Lending<'succ> for Iter {
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

pub struct Succ(Vec<usize>);
pub struct SuccIntoIter(IntoIter<usize>);

impl Iterator for SuccIntoIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl IntoIterator for Succ {
    type Item = usize;
    type IntoIter = SuccIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let iter = self.0.into_iter();
        SuccIntoIter(iter)
    }
}

impl Lender for Iter {
    check_covariance!();

    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        if self.x >= self.n {
            return None;
        }

        let result = Some((
            self.x,
            Succ(
                (0..self.n)
                    .filter(|&y| y != self.x && self.rng.random_bool(self.p))
                    .collect::<Vec<_>>(),
            ),
        ));
        self.x += 1;
        result
    }
}

impl SequentialGraph for ErdosRenyi {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{transform, utils::MemoryUsage};

    #[test]
    fn test_er() {
        let g = ErdosRenyi::new(10, 0.3, 0);
        for from in 0..10 {
            let mut it0 = g.iter_from(from);
            let mut it1 = g.iter();
            for _ in 0..from {
                it1.next();
            }
            while let (Some((x, s)), Some((y, t))) = (it0.next(), it1.next()) {
                assert_eq!(x, y);
                assert_eq!(s.0, t.0);
            }
            assert!(it0.next().is_none());
            assert!(it1.next().is_none());
        }
    }

    #[test]
    fn test_sorted() {
        // This is just to test that we implemented correctly
        // the SortedIterator and SortedLender traits.
        let er = ErdosRenyi::new(100, 0.1, 0);
        transform::simplify_sorted(er, MemoryUsage::BatchSize(100)).unwrap();
    }
}
