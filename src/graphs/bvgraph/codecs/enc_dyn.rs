/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{CodeWrite, Encode, MeasurableEncoder};
use crate::{graphs::Code, prelude::CompFlags};
use dsi_bitstream::prelude::*;
use std::convert::Infallible;

type WriteResult<E, CW> = Result<usize, <CW as BitWrite<E>>::Error>;

#[derive(Debug, Clone)]
pub struct DynCodesEncoder<E: Endianness, CW: CodeWrite<E>> {
    /// The code writer used by to output the compressed data.
    code_writer: CW,
    /// The estimator for this encoder.
    estimator: DynCodesEstimator,
    write_outdegree: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_reference_offset: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_block_count: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_block: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_interval_count: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_interval_start: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_interval_len: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_first_residual: fn(&mut CW, u64) -> WriteResult<E, CW>,
    write_residual: fn(&mut CW, u64) -> WriteResult<E, CW>,
    _marker: core::marker::PhantomData<E>,
}

fn write_zeta2<E: Endianness, CW: CodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 2)
}

fn write_zeta4<E: Endianness, CW: CodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 4)
}

fn write_zeta5<E: Endianness, CW: CodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 5)
}

fn write_zeta6<E: Endianness, CW: CodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 6)
}

fn write_zeta7<E: Endianness, CW: CodeWrite<E>>(cw: &mut CW, x: u64) -> WriteResult<E, CW> {
    CW::write_zeta(cw, x, 7)
}

impl<E: Endianness, CW: CodeWrite<E>> DynCodesEncoder<E, CW> {
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
            write_block: Self::select_code(cf.blocks),
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

impl<E: Endianness, CW: CodeWrite<E> + BitSeek + Clone> BitSeek for DynCodesEncoder<E, CW> {
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

impl<E: Endianness, CW: CodeWrite<E>> Encode for DynCodesEncoder<E, CW>
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
        (self.write_block)(&mut self.code_writer, value)
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

impl<E: Endianness, CW: CodeWrite<E>> MeasurableEncoder for DynCodesEncoder<E, CW>
where
    <CW as BitWrite<E>>::Error: Send + Sync,
{
    type Estimator<'a> = &'a mut DynCodesEstimator
        where Self: 'a;

    fn estimator(&mut self) -> Self::Estimator<'_> {
        &mut self.estimator
    }
}

#[derive(Debug, Clone)]
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
