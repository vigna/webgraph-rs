/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::lenders::NodeLabelsLender;

pub trait SplitLabeling {
    type Lender<'a>: NodeLabelsLender<'a>
    where
        Self: 'a;
    type IntoIterator<'a>: IntoIterator<Item = Self::Lender<'a>>
    where
        Self: 'a;
    fn split_iter(&self, n: usize) -> Self::IntoIterator<'_>;
}

pub mod seq {
    use crate::prelude::{NodeLabelsLender, SequentialLabeling};

    pub struct Iter<L: lender::Lender>
    where
        for<'a> L: NodeLabelsLender<'a>,
    {
        lender: L,
        nodes_per_iter: usize,
        how_many: usize,
        remaining: usize,
    }

    impl<L: lender::Lender + lender::ExactSizeLender> Iter<L>
    where
        for<'a> L: NodeLabelsLender<'a>,
    {
        pub fn new(lender: L, how_many: usize) -> Self {
            let nodes_per_iter = lender.len() / how_many;
            Self {
                lender,
                nodes_per_iter,
                how_many,
                remaining: how_many,
            }
        }
    }

    impl<L: lender::Lender + Clone> Iterator for Iter<L>
    where
        for<'a> L: NodeLabelsLender<'a>,
    {
        type Item = lender::Take<L>;

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

    impl<L: lender::Lender + Clone> ExactSizeIterator for Iter<L>
    where
        for<'a> L: NodeLabelsLender<'a>,
    {
        fn len(&self) -> usize {
            self.remaining
        }
    }

    pub type Lender<'a, S> = lender::Take<<S as SequentialLabeling>::Iterator<'a>>;
    pub type IntoIterator<'a, S> = Iter<<S as SequentialLabeling>::Iterator<'a>>;
}

pub mod ra {
    use crate::prelude::{RandomAccessLabeling, SequentialLabeling};

    pub struct Iter<'a, R: RandomAccessLabeling> {
        labeling: &'a R,
        nodes_per_iter: usize,
        how_many: usize,
        i: usize,
    }

    impl<'a, R: RandomAccessLabeling> Iter<'a, R> {
        pub fn new(labeling: &'a R, how_many: usize) -> Self {
            let nodes_per_iter = labeling.num_nodes() / how_many;
            Self {
                labeling,
                nodes_per_iter,
                how_many,
                i: 0,
            }
        }
    }

    impl<'a, R: RandomAccessLabeling> Iterator for Iter<'a, R> {
        type Item = Lender<'a, R>;

        fn next(&mut self) -> Option<Self::Item> {
            use lender::Lender;

            if self.i == self.how_many {
                return None;
            }
            self.i += 1;
            Some(
                self.labeling
                    .iter_from((self.i - 1) * self.nodes_per_iter)
                    .take(self.nodes_per_iter),
            )
        }
    }

    impl<'a, R: RandomAccessLabeling> ExactSizeIterator for Iter<'a, R> {
        fn len(&self) -> usize {
            self.how_many - self.i
        }
    }

    pub type Lender<'a, R> = lender::Take<<R as SequentialLabeling>::Iterator<'a>>;
    pub type IntoIterator<'a, R> = Iter<'a, R>;
}
