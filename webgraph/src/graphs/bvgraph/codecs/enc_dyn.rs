/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{Encode, EncodeAndEstimate};
use crate::prelude::CompFlags;
use anyhow::Result;
use dsi_bitstream::dispatch::FuncCodeLen;
use dsi_bitstream::prelude::*;
use std::convert::Infallible;

type WriteResult<E, CW> = Result<usize, <CW as BitWrite<E>>::Error>;

/// An implementation of [`EncodeAndEstimate`] with runtime defined codes.
#[derive(Debug, Clone)]
pub struct DynCodesEncoder<E: Endianness, CW: CodesWrite<E>> {
    /// The code writer used to output the compressed data.
    code_writer: CW,
    /// The estimator for this encoder.
    estimator: DynCodesEstimator,
    write_outdegree: FuncCodeWriter<E, CW>,
    write_reference_offset: FuncCodeWriter<E, CW>,
    write_block_count: FuncCodeWriter<E, CW>,
    write_block: FuncCodeWriter<E, CW>,
    write_interval_count: FuncCodeWriter<E, CW>,
    write_interval_start: FuncCodeWriter<E, CW>,
    write_interval_len: FuncCodeWriter<E, CW>,
    write_first_residual: FuncCodeWriter<E, CW>,
    write_residual: FuncCodeWriter<E, CW>,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CW: CodesWrite<E>> DynCodesEncoder<E, CW> {
    pub fn new(code_writer: CW, cf: &CompFlags) -> Result<Self> {
        Ok(Self {
            code_writer,
            write_outdegree: FuncCodeWriter::new(cf.outdegrees)?,
            write_reference_offset: FuncCodeWriter::new(cf.references)?,
            write_block_count: FuncCodeWriter::new(cf.blocks)?,
            write_block: FuncCodeWriter::new(cf.blocks)?,
            write_interval_count: FuncCodeWriter::new(cf.intervals)?,
            write_interval_start: FuncCodeWriter::new(cf.intervals)?,
            write_interval_len: FuncCodeWriter::new(cf.intervals)?,
            write_first_residual: FuncCodeWriter::new(cf.residuals)?,
            write_residual: FuncCodeWriter::new(cf.residuals)?,
            estimator: DynCodesEstimator::new(cf)?,
            _marker: core::marker::PhantomData,
        })
    }
}

impl<E: Endianness, CW: CodesWrite<E> + BitSeek> BitSeek for DynCodesEncoder<E, CW> {
    type Error = <CW as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_writer.set_bit_pos(bit_index)
    }

    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_writer.bit_pos()
    }
}

impl<E: Endianness, CW: CodesWrite<E>> Encode for DynCodesEncoder<E, CW>
where
    <CW as BitWrite<E>>::Error: Send + Sync,
{
    type Error = <CW as BitWrite<E>>::Error;

    #[inline(always)]
    fn start_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }

    #[inline(always)]
    fn end_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_outdegree.write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_reference_offset
            .write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_block_count.write(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_block(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_block.write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_interval_count
            .write(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_interval_start
            .write(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_interval_len.write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_first_residual
            .write(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> WriteResult<E, CW> {
        self.write_residual.write(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn flush(&mut self) -> Result<usize, Self::Error> {
        self.code_writer.flush()
    }
}

impl<E: Endianness, CW: CodesWrite<E>> EncodeAndEstimate for DynCodesEncoder<E, CW>
where
    <CW as BitWrite<E>>::Error: Send + Sync,
{
    type Estimator<'a>
        = &'a mut DynCodesEstimator
    where
        Self: 'a;

    fn estimator(&mut self) -> Self::Estimator<'_> {
        &mut self.estimator
    }
}

#[derive(Debug, Clone)]
pub struct DynCodesEstimator {
    len_outdegree: FuncCodeLen,
    len_reference_offset: FuncCodeLen,
    len_block_count: FuncCodeLen,
    len_block: FuncCodeLen,
    len_interval_count: FuncCodeLen,
    len_interval_start: FuncCodeLen,
    len_interval_len: FuncCodeLen,
    len_first_residual: FuncCodeLen,
    len_residual: FuncCodeLen,
}

impl DynCodesEstimator {
    pub fn new(cf: &CompFlags) -> Result<Self> {
        Ok(Self {
            len_outdegree: FuncCodeLen::new(cf.outdegrees)?,
            len_reference_offset: FuncCodeLen::new(cf.references)?,
            len_block_count: FuncCodeLen::new(cf.blocks)?,
            len_block: FuncCodeLen::new(cf.blocks)?,
            len_interval_count: FuncCodeLen::new(cf.intervals)?,
            len_interval_start: FuncCodeLen::new(cf.intervals)?,
            len_interval_len: FuncCodeLen::new(cf.intervals)?,
            len_first_residual: FuncCodeLen::new(cf.residuals)?,
            len_residual: FuncCodeLen::new(cf.residuals)?,
        })
    }
}

impl Encode for DynCodesEstimator {
    type Error = Infallible;

    #[inline(always)]
    fn start_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }

    #[inline(always)]
    fn end_node(&mut self, _node: usize) -> Result<usize, Self::Error> {
        Ok(0)
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_outdegree.len(value))
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_reference_offset.len(value))
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_block_count.len(value))
    }
    #[inline(always)]
    fn write_block(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_block.len(value))
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_interval_count.len(value))
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_interval_start.len(value))
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_interval_len.len(value))
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_first_residual.len(value))
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok(self.len_residual.len(value))
    }

    #[inline(always)]
    fn flush(&mut self) -> Result<usize, Self::Error> {
        Ok(0)
    }
}
