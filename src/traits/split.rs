/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::{prelude::*, Take};

use super::labels::SequentialLabeling;

pub trait SplitLabeling {
    type InnerLender<'a>: Lender
    where
        Self: 'a;
    type SplitIter<'a>: IntoIterator<Item = Self::InnerLender<'a>>
    where
        Self: 'a;
    fn split_iter(&self, n: usize) -> Self::SplitIter<'_>;
}

pub trait SeqSplitMarker: SequentialLabeling {}

pub struct SplitIter<L: Lender> {
    lender: L,
    nodes_per_iter: usize,
    how_many: usize,
    remaining: usize,
}

impl<L: Lender + Clone> Iterator for SplitIter<L> {
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

impl<S: SequentialLabeling + SeqSplitMarker> SplitLabeling for S
where
    for<'a> S::Iterator<'a>: Clone,
{
    type InnerLender<'a> = Take<<S as SequentialLabeling>::Iterator<'a>> where Self: 'a;
    type SplitIter<'a> = SplitIter<<S as SequentialLabeling>::Iterator<'a>> where Self: 'a;
    fn split_iter(&self, how_many: usize) -> Self::SplitIter<'_> {
        SplitIter {
            lender: self.iter(),
            nodes_per_iter: self.num_nodes() / how_many,
            how_many,
            remaining: how_many,
        }
    }
}
