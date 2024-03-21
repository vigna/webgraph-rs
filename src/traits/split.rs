/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::{prelude::*, Take};

use super::labels::{RandomAccessLabeling, SequentialLabeling};

pub trait SplitLabeling {
    type Lender<'a>: Lender
    where
        Self: 'a;
    type IntoIterator<'a>: IntoIterator<Item = Self::Lender<'a>>
    where
        Self: 'a;
    fn split_iter(&self, n: usize) -> Self::IntoIterator<'_>;
}

pub mod seq {
    use super::*;

    pub struct Iter<L: lender::Lender> {
        lender: L,
        nodes_per_iter: usize,
        how_many: usize,
        remaining: usize,
    }

    impl<L: lender::Lender + lender::ExactSizeLender> Iter<L> {
        pub fn new(lender: L, num_nodes: usize, how_many: usize) -> Self {
            let nodes_per_iter = num_nodes / how_many;
            Self {
                lender,
                nodes_per_iter,
                how_many,
                remaining: how_many,
            }
        }
    }

    impl<L: lender::Lender + Clone> Iterator for Iter<L> {
        type Item = Take<L>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.remaining == 0 {
                return None;
            }
            if self.remaining != self.how_many {
                self.lender.advance_by(self.nodes_per_iter).ok()?;
            }
            self.remaining -= 1;
            Some(self.lender.clone().take(self.nodes_per_iter))
        }
    }

    pub type Lender<'a, S: SequentialLabeling> = Take<<S as SequentialLabeling>::Iterator<'a>>;
    pub type IntoIterator<'a, S: SequentialLabeling> =
        Iter<<S as SequentialLabeling>::Iterator<'a>>;
}

pub mod ra {
    use super::*;
    pub struct SplitIter<L: lender::Lender> {
        lender: L,
        nodes_per_iter: usize,
        how_many: usize,
        remaining: usize,
    }

    pub struct Iter<L: lender::Lender> {
        lender: L,
        nodes_per_iter: usize,
        how_many: usize,
        remaining: usize,
    }

    impl<L: lender::Lender + lender::ExactSizeLender> Iter<L> {
        pub fn new(lender: L, num_nodes: usize, how_many: usize) -> Self {
            let nodes_per_iter = num_nodes / how_many;
            Self {
                lender,
                nodes_per_iter,
                how_many,
                remaining: how_many,
            }
        }
    }

    impl<L: lender::Lender + Clone> Iterator for Iter<L> {
        type Item = Take<L>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.remaining == 0 {
                return None;
            }
            if self.remaining != self.how_many {
                self.lender.advance_by(self.nodes_per_iter).ok()?;
            }
            self.remaining -= 1;
            Some(self.lender.clone().take(self.nodes_per_iter))
        }
    }

    pub type Lender<'a, R: RandomAccessLabeling> = Take<<R as SequentialLabeling>::Iterator<'a>>;
    pub type IntoIterator<'a, R: RandomAccessLabeling> =
        Iter<<R as SequentialLabeling>::Iterator<'a>>;
}
