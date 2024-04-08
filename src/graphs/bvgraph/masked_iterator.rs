/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// An iterator that filters out blocks of values.
#[derive(Debug, Clone)]
pub struct MaskedIterator<I> {
    /// The resolved reference node, if present
    parent: Box<I>,
    /// The copy blocks from the ref node
    blocks: Vec<usize>,
    /// The id of block to parse
    block_idx: usize,
    /// Caching of the number of values returned, if needed
    size: usize,
}

impl<I: Iterator<Item = usize>> ExactSizeIterator for MaskedIterator<I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }
}

impl<I: Iterator<Item = usize> + ExactSizeIterator> MaskedIterator<I> {
    /// Create a new iterator that filters out blocks of values.
    /// The blocks of even index are copy blocks, the blocks of odd index are
    /// skip blocks.
    /// If the number of blocks is odd, a last copy block to the end is added.
    pub fn new(parent: I, mut blocks: Vec<usize>) -> Self {
        // the number of copied nodes
        let mut size: usize = 0;
        // the cumulative sum of the blocks
        let mut cumsum_blocks: usize = 0;
        // compute them
        for (i, x) in blocks.iter().enumerate() {
            // branchless add
            size += if i % 2 == 0 { *x } else { 0 };
            cumsum_blocks += x;
        }

        // an empty blocks means that we should take all the neighbours
        let remainder = parent.len() - cumsum_blocks;

        // check if the last block is a copy or skip block
        // avoid pushing it so we end faster
        if remainder != 0 && blocks.len() % 2 == 0 {
            size += remainder;
            blocks.push(remainder);
        }

        Self {
            parent: Box::new(parent),
            blocks,
            block_idx: 0,
            size,
        }
    }
}

impl<I: Iterator<Item = usize>> Iterator for MaskedIterator<I> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        debug_assert!(self.block_idx <= self.blocks.len());
        let mut current_block = self.blocks[self.block_idx];
        // we finished this block so we must skip the next one, if present
        if current_block == 0 {
            // skip the next block
            self.block_idx += 1;

            // no more copy blocks so we can stop the parsing
            if self.block_idx >= self.blocks.len() {
                return None;
            }

            debug_assert!(self.blocks[self.block_idx] > 0);
            for _ in 0..self.blocks[self.block_idx] {
                // should we add `?` and do an early return?
                // I don't think it improves speed because it add an
                // unpredictable branch and the blocks should be done so that
                // they are always right.
                let node = self.parent.next();
                debug_assert!(node.is_some());
            }
            self.block_idx += 1;
            current_block = self.blocks[self.block_idx];
            debug_assert_ne!(current_block, 0);
        }

        let result = self.parent.next();
        self.blocks[self.block_idx] -= 1;
        result
    }
}
