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
    fn advance(&mut self) {
        debug_assert!(self.left >= 0);
        if self.left == 0 && self.curr_mask < self.blocks.len() {
            for _ in 0..self.blocks[self.curr_mask] {
                let node = self.parent.next();
                debug_assert!(node.is_some());
            }
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
}
