/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// An iterator returning elements of an underlying iterator filtered through
/// an inclusion-exclusion block list.
///
/// A *mask* is a sequence of integers specifying inclusion-exclusion blocks.
/// The first value specifies how many elements to include, the second how many
/// to skip, the third how many to include, and so on. If there are elements
/// remaining after the blocks are exhausted, they are included if the number
/// of blocks is even, and excluded otherwise. All integers in the mask
/// must be positive, except possibly for the first one, which may be zero.
#[derive(Debug, Clone)]
pub struct MaskedIter<I> {
    /// The underlying iterator.
    parent: Box<I>,
    /// The inclusion-exclusion blocks.
    blocks: Vec<usize>,
    /// Index into blocks; always points to the next exclusion block.
    curr_mask: usize,
    /// Elements left in the current inclusion block. If negative, all remaining
    /// elements from the parent must be kept. If zero, no more elements must
    /// be returned.
    left: isize,
}

impl<I: Iterator<Item = usize>> MaskedIter<I> {
    /// Creates a new iterator that filters out blocks of values.
    ///
    /// The blocks of even index are copy blocks, the blocks of odd index are
    /// skip blocks. The tail of elements is considered a copy block if the
    /// number of blocks is even, a skip block otherwise.
    pub fn new(parent: I, blocks: Vec<usize>) -> Self {
        let mut result = Self {
            parent: Box::new(parent),
            blocks,
            curr_mask: 0,
            left: -1,
        };

        if !result.blocks.is_empty() {
            result.left = result.blocks[0] as isize;
            result.curr_mask = 1;
            result.advance();
        }

        result
    }

    /// If the current inclusion block is exhausted and there are more blocks,
    /// skips the next exclusion block and advances to the following inclusion
    /// block. If blocks are exhausted, sets `left` to -1 (keep all remaining).
    #[inline(always)]
    fn advance(&mut self) {
        debug_assert!(self.left >= 0);
        if self.left == 0 && self.curr_mask < self.blocks.len() {
            let node = self.parent.nth(self.blocks[self.curr_mask] - 1);
            debug_assert!(node.is_some());
            self.curr_mask += 1;
            self.left = if self.curr_mask < self.blocks.len() {
                let l = self.blocks[self.curr_mask] as isize;
                self.curr_mask += 1;
                l
            } else {
                -1
            };
        }
    }
}

impl<I: Iterator<Item = usize>> Iterator for MaskedIter<I> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        let next = self.parent.next()?;
        if self.left > 0 {
            self.left -= 1;
            self.advance();
        }
        Some(next)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut remaining = n;
        loop {
            if self.left == 0 {
                return None;
            }
            if self.left < 0 {
                // Pass-through mode: delegate to parent.
                return self.parent.nth(remaining);
            }
            // We are in an inclusion block.
            let left = self.left as usize;
            if remaining < left {
                // Can be satisfied within the current inclusion block.
                let result = self.parent.nth(remaining);
                self.left -= remaining as isize + 1;
                self.advance();
                return result;
            }
            // Skip the rest of this inclusion block and advance.
            self.parent.nth(left - 1);
            remaining -= left;
            self.left = 0;
            self.advance();
        }
    }
}
