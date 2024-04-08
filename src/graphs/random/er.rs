/*
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::{Lend, Lender, Lending};
use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::prelude::{NodeLabelsLender, SequentialGraph, SequentialLabeling};

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
    /// Create a new Erdös-Rényi random graph, given the number of
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
    fn num_nodes(&self) -> usize {
        self.n
    }

    fn iter_from(&self, from: usize) -> Iter {
        let mut rng = SmallRng::seed_from_u64(self.seed);
        for _ in 0..from * (self.n - 1) {
            rng.gen_bool(self.p);
        }
        Iter {
            n: self.n,
            p: self.p,
            x: from,
            rng,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Iter {
    n: usize,
    p: f64,
    x: usize,
    rng: SmallRng,
}

impl<'succ> NodeLabelsLender<'succ> for Iter {
    type Label = usize;
    type IntoIterator = Vec<usize>;
}

impl<'succ> Lending<'succ> for Iter {
    type Lend = (usize, <Self as NodeLabelsLender<'succ>>::IntoIterator);
}

impl Lender for Iter {
    #[inline(always)]
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        if self.x >= self.n {
            return None;
        }

        let result = Some((
            self.x,
            (0..self.n)
                .filter(|&y| y != self.x && self.rng.gen_bool(self.p))
                .collect::<Vec<_>>(),
        ));
        self.x += 1;
        result
    }
}

impl SequentialGraph for ErdosRenyi {}

#[cfg(test)]
mod tests {
    use super::*;

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
                assert_eq!(s, t);
            }
            assert!(it0.next().is_none());
            assert!(it1.next().is_none());
        }
    }
}
