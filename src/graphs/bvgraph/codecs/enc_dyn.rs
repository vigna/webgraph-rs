/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::{CodeWrite, Encoder, MeasurableEncoder};
use crate::{graphs::Code, prelude::CompFlags};
use dsi_bitstream::prelude::*;
use std::convert::Infallible;

pub struct DynCodesEncoder<E: Endianness, CW: CodeWrite<E>> {
    code_writer: CW,
    write_outdegree: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_reference_offset: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_block_count: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_blocks: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_interval_count: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_interval_start: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_interval_len: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_first_residual: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    write_residual: fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error>,
    _marker: core::marker::PhantomData<E>,
}

fn write_zeta2<E: Endianness, CW: CodeWrite<E>>(
    cw: &mut CW,
    x: u64,
) -> Result<usize, <CW as BitWrite<E>>::Error> {
    CW::write_zeta(cw, x, 2)
}

fn write_zeta4<E: Endianness, CW: CodeWrite<E>>(
    cw: &mut CW,
    x: u64,
) -> Result<usize, <CW as BitWrite<E>>::Error> {
    CW::write_zeta(cw, x, 4)
}

fn write_zeta5<E: Endianness, CW: CodeWrite<E>>(
    cw: &mut CW,
    x: u64,
) -> Result<usize, <CW as BitWrite<E>>::Error> {
    CW::write_zeta(cw, x, 5)
}

fn write_zeta6<E: Endianness, CW: CodeWrite<E>>(
    cw: &mut CW,
    x: u64,
) -> Result<usize, <CW as BitWrite<E>>::Error> {
    CW::write_zeta(cw, x, 6)
}

fn write_zeta7<E: Endianness, CW: CodeWrite<E>>(
    cw: &mut CW,
    x: u64,
) -> Result<usize, <CW as BitWrite<E>>::Error> {
    CW::write_zeta(cw, x, 7)
}

impl<E: Endianness, CW: CodeWrite<E>> DynCodesEncoder<E, CW> {
    fn select_code(code: Code) -> fn(&mut CW, u64) -> Result<usize, <CW as BitWrite<E>>::Error> {
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
            _marker: core::marker::PhantomData,
        }
    }
}

impl<E: Endianness, CW: CodeWrite<E> + BitSeek + Clone> BitSeek for DynCodesEncoder<E, CW> {
    type Error = <CW as BitSeek>::Error;

    fn set_bit_pos(&mut self, bit_index: u64) -> Result<(), Self::Error> {
        self.code_writer.set_bit_pos(bit_index)
    }

    fn get_bit_pos(&mut self) -> Result<u64, Self::Error> {
        self.code_writer.get_bit_pos()
    }
}

fn len_unary(value: u64) -> usize {
    value as usize + 1
}

impl<E: Endianness, CW: CodeWrite<E>> Encoder for DynCodesEncoder<E, CW>
where
    <CW as BitWrite<E>>::Error: Send + Sync,
{
    type Error = <CW as BitWrite<E>>::Error;

    #[inline(always)]
    fn start_node(_node: usize) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn end_node(_node: usize) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_outdegree)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_reference_offset)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_block_count)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_block(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_blocks)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_interval_count)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_interval_start)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_interval_len)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_first_residual)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize, Self::Error> {
        (self.write_residual)(&mut self.code_writer, value)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.code_writer.flush()
    }
}

impl<E: Endianness, CW: CodeWrite<E>> MeasurableEncoder for DynCodesEncoder<E, CW>
where
    <CW as BitWrite<E>>::Error: Send + Sync,
{
    type Estimator = DynCodesEstimator;

    fn estimator(&self) -> Self::Estimator {
        macro_rules! reconstruct_code {
            ($code:expr) => {{
                let code = $code as usize;
                if code == CW::write_unary as usize {
                    len_unary
                } else if code == CW::write_gamma as usize {
                    len_gamma
                } else if code == CW::write_delta as usize {
                    len_delta
                } else if code == write_zeta2::<E, CW> as usize {
                    |x| len_zeta(x, 2)
                } else if code == CW::write_zeta3 as usize {
                    |x| len_zeta(x, 3)
                } else if code == write_zeta4::<E, CW> as usize {
                    |x| len_zeta(x, 4)
                } else if code == write_zeta5::<E, CW> as usize {
                    |x| len_zeta(x, 5)
                } else if code == write_zeta6::<E, CW> as usize {
                    |x| len_zeta(x, 6)
                } else if code == write_zeta7::<E, CW> as usize {
                    |x| len_zeta(x, 7)
                } else {
                    unreachable!()
                }
            }};
        }
        DynCodesEstimator {
            len_outdegree: reconstruct_code!(self.write_outdegree),
            len_reference_offset: reconstruct_code!(self.write_reference_offset),
            len_block_count: reconstruct_code!(self.write_block_count),
            len_blocks: reconstruct_code!(self.write_blocks),
            len_interval_count: reconstruct_code!(self.write_interval_count),
            len_interval_start: reconstruct_code!(self.write_interval_start),
            len_interval_len: reconstruct_code!(self.write_interval_len),
            len_first_residual: reconstruct_code!(self.write_first_residual),
            len_residual: reconstruct_code!(self.write_residual),
        }
    }
}

#[derive(Clone)]
pub struct DynCodesEstimator {
    len_outdegree: fn(u64) -> usize,
    len_reference_offset: fn(u64) -> usize,
    len_block_count: fn(u64) -> usize,
    len_blocks: fn(u64) -> usize,
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
            Code::Zeta { k: 1 } => |x| len_gamma(x),
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
            len_blocks: Self::select_code(cf.blocks),
            len_interval_count: Self::select_code(cf.intervals),
            len_interval_start: Self::select_code(cf.intervals),
            len_interval_len: Self::select_code(cf.intervals),
            len_first_residual: Self::select_code(cf.residuals),
            len_residual: Self::select_code(cf.residuals),
        }
    }
}

impl Encoder for DynCodesEstimator {
    type Error = Infallible;

    #[inline(always)]
    fn start_node(_node: usize) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn end_node(_node: usize) -> Result<(), Self::Error> {
        Ok(())
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
        Ok((self.len_blocks)(value))
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

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
