/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{BVCodeWrite, Encode, EncodeAndEstimate};
use crate::{graphs::bvgraph::Code, prelude::CompFlags};
use dsi_bitstream::prelude::*;
use mem_dbg::{MemDbg, MemDbgImpl, MemSize, SizeFlags};
use std::convert::Infallible;

type WriteResult<E, CW> = Result<usize, <CW as BitWrite<E>>::Error>;

#[derive(Debug, Clone)]
pub struct DynCodesEncoder<E: Endianness, CW: BVCodeWrite<E>> {
    /// The code writer used by to output the compressed data.
    code_writer: CW,
    /// The estimator for this encoder.
    estimator: DynCodesEstimator,
    write_outdegree: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_reference_offset: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_block_count: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_blocks: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_interval_count: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_interval_start: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_interval_len: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_first_residual: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_residual: fn(&mut CW, u64) -> WriteResult<E, CW>,
    _marker: core::marker::PhantomData<E>,
}

/// Manual impl because of generic lifetime function pointers are not supported
/// yet by the derive macro
impl<E: Endianness, CW: BVCodeWrite<E>> MemSize for DynCodesEncoder<E, CW>
where
    CW: MemSize,
{
    fn mem_size(&self, flags: SizeFlags) -> usize {
        self.code_writer.mem_size(flags)
            + self.estimator.mem_size(flags)
            + core::mem::size_of::<usize>() * 9
    }
}

impl<E: Endianness, CW: BVCodeWrite<E>> MemDbgImpl for DynCodesEncoder<E, CW>
where
    CW: MemDbg,
{
    fn _mem_dbg_rec_on(
        &self,
        writer: &mut impl core::fmt::Write,
        total_size: usize,
        max_depth: usize,
        prefix: &mut String,
        _is_last: bool,
        flags: mem_dbg::DbgFlags,
    ) -> core::fmt::Result {
        let mut id_sizes: Vec<(usize, usize)> = vec![];
        id_sizes.push((0, core::mem::offset_of!(Self, code_writer)));
        id_sizes.push((1, core::mem::offset_of!(Self, estimator)));
        id_sizes.push((2, core::mem::offset_of!(Self, write_outdegree)));
        id_sizes.push((3, core::mem::offset_of!(Self, write_reference_offset)));
        id_sizes.push((4, core::mem::offset_of!(Self, write_block_count)));
        id_sizes.push((5, core::mem::offset_of!(Self, write_blocks)));
        id_sizes.push((6, core::mem::offset_of!(Self, write_interval_count)));
        id_sizes.push((7, core::mem::offset_of!(Self, write_interval_start)));
        id_sizes.push((8, core::mem::offset_of!(Self, write_interval_len)));
        id_sizes.push((9, core::mem::offset_of!(Self, write_first_residual)));
        id_sizes.push((10, core::mem::offset_of!(Self, write_residual)));

        let n = id_sizes.len();
        id_sizes.push((n, core::mem::size_of::<Self>()));
        // Sort by offset
        id_sizes.sort_by_key(|x| x.1);
        // Compute padded sizes
        for i in 0..n {
            id_sizes[i].1 = id_sizes[i + 1].1 - id_sizes[i].1;
        }
        // Put the candle back unless the user requested otherwise
        if !flags.contains(mem_dbg::DbgFlags::RUST_LAYOUT) {
            id_sizes.sort_by_key(|x| x.0);
        }

        for (i, (field_idx, padded_size)) in id_sizes.into_iter().enumerate().take(n) {
            let is_last = i == n - 1;
            match field_idx {
                0 => self.code_writer._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("code_writer"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                1 => self.estimator._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("estimator"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                // replace the fn pointers with usizes
                2 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_outdegree"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                3 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_reference_offset"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                4 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_block_count"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                5 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_blocks"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                6 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_interval_count"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                7 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_interval_start"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                8 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_interval_len"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                9 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_first_residual"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                10 => 0_usize._mem_dbg_depth_on(
                    writer,
                    total_size,
                    max_depth,
                    prefix,
                    Some("write_residual"),
                    is_last,
                    padded_size,
                    flags,
                )?,
                _ => unreachable!(),
            }
        }
        Ok(())
    }
}

fn write_zeta2<E: Endianness, CW: BVCodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 2)
}

fn write_zeta4<E: Endianness, CW: BVCodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 4)
}

fn write_zeta5<E: Endianness, CW: BVCodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 5)
}

fn write_zeta6<E: Endianness, CW: BVCodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 6)
}

fn write_zeta7<E: Endianness, CW: BVCodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 7)
}

impl<E: Endianness, CW: BVCodeWrite<E>> DynCodesEncoder<E, CW> {
    #[allow(clippy::type_complexity)]
    fn select_code(code: Code) -> fn(&mut CW, u64) -> WriteResult<E, CW> {
        match code {
            Code::Unary => CW::write_unary,
            Code::Gamma => CW::write_gamma,
            Code::Delta => CW::write_delta,
            Code::Zeta { k: 1 } => CW::write_gamma,
            Code::Zeta { k: 2 } => write_zeta2,
            Code::Zeta { k: 3 } => CW::write_zeta3,
            Code::Zeta { k: 4 } => write_zeta4,
            Code::Zeta { k: 5 } => write_zeta5,
            Code::Zeta { k: 6 } => write_zeta6,
            Code::Zeta { k: 7 } => write_zeta7,
            code => {
                panic!(
                    "Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed, {:?} is not supported",
                    code
                )
            }
        }
    }

    pub fn new(code_writer: CW, cf: &CompFlags) -> Self {
        Self {
            code_writer,
            write_outdegree: Self::select_code(cf.outdegrees),
            write_reference_offset: Self::select_code(cf.references),
            write_block_count: Self::select_code(cf.blocks),
            write_blocks: Self::select_code(cf.blocks),
            write_interval_count: Self::select_code(cf.intervals),
            write_interval_start: Self::select_code(cf.intervals),
            write_interval_len: Self::select_code(cf.intervals),
            write_first_residual: Self::select_code(cf.residuals),
            write_residual: Self::select_code(cf.residuals),
            estimator: DynCodesEstimator::new(cf),
            _marker: core::marker::PhantomData,
        }
    }
}

impl<E: Endianness, CW: BVCodeWrite<E> + BitSeek + Clone> BitSeek for DynCodesEncoder<E, CW> {
    type Error = <CW as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_writer.set_bit_pos(bit_index)
    }

    fn bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_writer.bit_pos()
    }
}

fn len_unary(value: u64) -> usize {
    value as usize + 1
}

impl<E: Endianness, CW: BVCodeWrite<E>> Encode for DynCodesEncoder<E, CW>
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
        (self.write_outdegree)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> WriteResult<E, CW> {
        (self.write_reference_offset)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> WriteResult<E, CW> {
        (self.write_block_count)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_block(&mut self, value: u64) -> WriteResult<E, CW> {
        (self.write_blocks)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> WriteResult<E, CW> {
        (self.write_interval_count)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> WriteResult<E, CW> {
        (self.write_interval_start)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> WriteResult<E, CW> {
        (self.write_interval_len)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> WriteResult<E, CW> {
        (self.write_first_residual)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> WriteResult<E, CW> {
        (self.write_residual)(&mut self.code_writer, value)
    }

    fn flush(&mut self) -> Result<usize, Self::Error> {
        self.code_writer.flush()
    }
}

impl<E: Endianness, CW: BVCodeWrite<E>> EncodeAndEstimate for DynCodesEncoder<E, CW>
where
    <CW as BitWrite<E>>::Error: Send + Sync,
{
    type Estimator<'a> = &'a mut DynCodesEstimator
        where Self: 'a;

    fn estimator(&mut self) -> Self::Estimator<'_> {
        &mut self.estimator
    }
}

#[derive(Debug, Clone, MemSize, MemDbg)]
pub struct DynCodesEstimator {
    len_outdegree: fn(u64) -> usize,
    len_reference_offset: fn(u64) -> usize,
    len_block_count: fn(u64) -> usize,
    len_block: fn(u64) -> usize,
    len_interval_count: fn(u64) -> usize,
    len_interval_start: fn(u64) -> usize,
    len_interval_len: fn(u64) -> usize,
    len_first_residual: fn(u64) -> usize,
    len_residual: fn(u64) -> usize,
}

impl DynCodesEstimator {
    /// Selects the length function for the given [`Code`].
    fn select_code(code: Code) -> fn(u64) -> usize {
        match code {
            Code::Unary => len_unary,
            Code::Gamma => len_gamma,
            Code::Delta => len_delta,
            Code::Zeta { k: 1 } => len_gamma,
            Code::Zeta { k: 2 } => |x| len_zeta(x, 2),
            Code::Zeta { k: 3 } => |x| len_zeta(x, 3),
            Code::Zeta { k: 4 } => |x| len_zeta(x, 4),
            Code::Zeta { k: 5 } => |x| len_zeta(x, 5),
            Code::Zeta { k: 6 } => |x| len_zeta(x, 6),
            Code::Zeta { k: 7 } => |x| len_zeta(x, 7),
            code => panic!(
                "Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed, {:?} is not supported",
                code
            ),
        }
    }

    pub fn new(cf: &CompFlags) -> Self {
        Self {
            len_outdegree: Self::select_code(cf.outdegrees),
            len_reference_offset: Self::select_code(cf.references),
            len_block_count: Self::select_code(cf.blocks),
            len_block: Self::select_code(cf.blocks),
            len_interval_count: Self::select_code(cf.intervals),
            len_interval_start: Self::select_code(cf.intervals),
            len_interval_len: Self::select_code(cf.intervals),
            len_first_residual: Self::select_code(cf.residuals),
            len_residual: Self::select_code(cf.residuals),
        }
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
        Ok((self.len_outdegree)(value))
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_reference_offset)(value))
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_block_count)(value))
    }
    #[inline(always)]
    fn write_block(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_block)(value))
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_interval_count)(value))
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_interval_start)(value))
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_interval_len)(value))
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_first_residual)(value))
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        Ok((self.len_residual)(value))
    }

    fn flush(&mut self) -> Result<usize, Self::Error> {
        Ok(0)
    }
}
