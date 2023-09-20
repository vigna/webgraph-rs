/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// A simple iterator that deduplicates sorted iterators.
#[derive(Clone)]
pub struct DedupSortedIter<I: Iterator> {
    iter: I,
    last: Option<I::Item>,
}

impl<I: Iterator> DedupSortedIter<I>
where
    I::Item: PartialEq + Clone,
{
    #[inline]
    pub fn new(iter: I) -> Self {
        Self { iter, last: None }
    }
}

impl<I: Iterator> Iterator for DedupSortedIter<I>
where
    I::Item: PartialEq + Clone,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.iter.next();
        while let Some(n) = next {
            if self.last.as_ref() != Some(&n) {
                self.last = Some(n);
                return self.last.clone();
            }
            next = self.iter.next();
        }
        None
    }
}

impl<I: Iterator + ExactSizeIterator> ExactSizeIterator for DedupSortedIter<I>
where
    I::Item: PartialEq + Clone,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.iter.len()
    }
}
