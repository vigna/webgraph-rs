/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;

#[repr(transparent)]
/// A debug wrapper on a code read that prints the codes it reads
/// to stderr
#[derive(Debug, Clone)]
pub struct DebugDecoder<CR: Decode> {
    pub cr: CR,
}

impl<CR: Decode> DebugDecoder<CR> {
    pub fn new(cr: CR) -> Self {
        Self { cr }
    }
}

impl<CR: Decode> Decode for DebugDecoder<CR> {
    fn read_outdegree(&mut self) -> u64 {
        let outdegree = self.cr.read_outdegree();
        eprintln!("outdegree: {}", outdegree);
        outdegree
    }

    fn read_reference_offset(&mut self) -> u64 {
        let reference_offset = self.cr.read_reference_offset();
        eprintln!("reference_offset: {}", reference_offset);
        reference_offset
    }

    fn read_block_count(&mut self) -> u64 {
        let block_count = self.cr.read_block_count();
        eprintln!("block_count: {}", block_count);
        block_count
    }

    fn read_block(&mut self) -> u64 {
        let blocks = self.cr.read_block();
        eprintln!("blocks: {}", blocks);
        blocks
    }

    fn read_interval_count(&mut self) -> u64 {
        let interval_count = self.cr.read_interval_count();
        eprintln!("interval_count: {}", interval_count);
        interval_count
    }

    fn read_interval_start(&mut self) -> u64 {
        let interval_start = self.cr.read_interval_start();
        eprintln!("interval_start: {}", interval_start);
        interval_start
    }

    fn read_interval_len(&mut self) -> u64 {
        let interval_len = self.cr.read_interval_len();
        eprintln!("interval_len: {}", interval_len);
        interval_len
    }

    fn read_first_residual(&mut self) -> u64 {
        let first_residual = self.cr.read_first_residual();
        eprintln!("first_residual: {}", first_residual);
        first_residual
    }

    fn read_residual(&mut self) -> u64 {
        let residual = self.cr.read_residual();
        eprintln!("residual: {}", residual);
        residual
    }
}
