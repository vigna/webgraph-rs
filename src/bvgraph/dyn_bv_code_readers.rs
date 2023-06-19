use super::*;
use anyhow::{bail, Result};
use dsi_bitstream::prelude::*;

/// An implementation of [`WebGraphCodesReader`] with the most commonly used codes
#[derive(Clone)]
pub struct DynamicCodesReader<E: Endianness, CR: ReadCodes<E> + BitSeek> {
    pub(crate) code_reader: CR,
    pub(crate) read_outdegree: fn(&mut CR) -> u64,
    pub(crate) read_reference_offset: fn(&mut CR) -> u64,
    pub(crate) read_block_count: fn(&mut CR) -> u64,
    pub(crate) read_blocks: fn(&mut CR) -> u64,
    pub(crate) read_interval_count: fn(&mut CR) -> u64,
    pub(crate) read_interval_start: fn(&mut CR) -> u64,
    pub(crate) read_interval_len: fn(&mut CR) -> u64,
    pub(crate) read_first_residual: fn(&mut CR) -> u64,
    pub(crate) read_residual: fn(&mut CR) -> u64,
    pub(crate) _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek> DynamicCodesReader<E, CR> {
    fn select_code(code: &Code) -> Result<fn(&mut CR) -> u64> {
        Ok(match code {
            Code::Unary => |x| CR::read_unary(x).unwrap(),
            Code::Gamma => |x| CR::read_gamma(x).unwrap(),
            Code::Delta => |x| CR::read_delta(x).unwrap(),
            Code::Zeta { k: 1 } => |x| CR::read_gamma(x).unwrap(),
            Code::Zeta { k: 2 } => |x| CR::read_zeta(x, 2).unwrap(),
            Code::Zeta { k: 3 } => |x| CR::read_zeta3(x).unwrap(),
            Code::Zeta { k: 4 } => |x| CR::read_zeta(x, 4).unwrap(),
            Code::Zeta { k: 5 } => |x| CR::read_zeta(x, 5).unwrap(),
            Code::Zeta { k: 6 } => |x| CR::read_zeta(x, 6).unwrap(),
            Code::Zeta { k: 7 } => |x| CR::read_zeta(x, 7).unwrap(),
            _ => bail!("Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed"),
        })
    }

    pub fn new(code_reader: CR, cf: &CompFlags) -> Result<Self> {
        Ok(Self {
            code_reader,
            read_outdegree: Self::select_code(&cf.outdegrees)?,
            read_reference_offset: Self::select_code(&cf.references)?,
            read_block_count: Self::select_code(&cf.blocks)?,
            read_blocks: Self::select_code(&cf.blocks)?,
            read_interval_count: Self::select_code(&cf.intervals)?,
            read_interval_start: Self::select_code(&cf.intervals)?,
            read_interval_len: Self::select_code(&cf.intervals)?,
            read_first_residual: Self::select_code(&cf.residuals)?,
            read_residual: Self::select_code(&cf.residuals)?,
            _marker: core::marker::PhantomData::default(),
        })
    }
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek> BitSeek for DynamicCodesReader<E, CR> {
    fn set_pos(&mut self, bit_index: usize) -> Result<()> {
        self.code_reader.set_pos(bit_index)
    }

    fn get_pos(&self) -> usize {
        self.code_reader.get_pos()
    }
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek> WebGraphCodesReader for DynamicCodesReader<E, CR> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        (self.read_outdegree)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        (self.read_reference_offset)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        (self.read_block_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> u64 {
        (self.read_blocks)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        (self.read_interval_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        (self.read_interval_start)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        (self.read_interval_len)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        (self.read_first_residual)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        (self.read_residual)(&mut self.code_reader)
    }
}

/// An implementation of [`WebGraphCodesReader`] with the most commonly used codes
#[derive(Clone)]
pub struct DynamicCodesReaderSkipper<E: Endianness, CR: ReadCodes<E> + BitSeek> {
    pub(crate) code_reader: CR,
    pub(crate) read_outdegree: fn(&mut CR) -> u64,
    pub(crate) skip_outdegrees: fn(&mut CR, usize) -> usize,
    pub(crate) read_reference_offset: fn(&mut CR) -> u64,
    pub(crate) skip_reference_offsets: fn(&mut CR, usize) -> usize,
    pub(crate) read_block_count: fn(&mut CR) -> u64,
    pub(crate) skip_block_counts: fn(&mut CR, usize) -> usize,
    pub(crate) read_blocks: fn(&mut CR) -> u64,
    pub(crate) skip_blocks: fn(&mut CR, usize) -> usize,
    pub(crate) read_interval_count: fn(&mut CR) -> u64,
    pub(crate) skip_interval_counts: fn(&mut CR, usize) -> usize,
    pub(crate) read_interval_start: fn(&mut CR) -> u64,
    pub(crate) skip_interval_starts: fn(&mut CR, usize) -> usize,
    pub(crate) read_interval_len: fn(&mut CR) -> u64,
    pub(crate) skip_interval_lens: fn(&mut CR, usize) -> usize,
    pub(crate) read_first_residual: fn(&mut CR) -> u64,
    pub(crate) skip_first_residuals: fn(&mut CR, usize) -> usize,
    pub(crate) read_residual: fn(&mut CR) -> u64,
    pub(crate) skip_residuals: fn(&mut CR, usize) -> usize,
    pub(crate) _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek> DynamicCodesReaderSkipper<E, CR> {
    fn select_code(code: &Code) -> Result<fn(&mut CR) -> u64> {
        Ok(match code {
            Code::Unary => |x| CR::read_unary(x).unwrap(),
            Code::Gamma => |x| CR::read_gamma(x).unwrap(),
            Code::Delta => |x| CR::read_delta(x).unwrap(),
            Code::Zeta { k: 1 } => |x| CR::read_gamma(x).unwrap(),
            Code::Zeta { k: 2 } => |x| CR::read_zeta(x, 2).unwrap(),
            Code::Zeta { k: 3 } => |x| CR::read_zeta3(x).unwrap(),
            Code::Zeta { k: 4 } => |x| CR::read_zeta(x, 4).unwrap(),
            Code::Zeta { k: 5 } => |x| CR::read_zeta(x, 5).unwrap(),
            Code::Zeta { k: 6 } => |x| CR::read_zeta(x, 6).unwrap(),
            Code::Zeta { k: 7 } => |x| CR::read_zeta(x, 7).unwrap(),
            _ => bail!("Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed"),
        })
    }

    fn select_skip_code(code: &Code) -> Result<fn(&mut CR, usize) -> usize> {
        Ok(match code {
            Code::Unary => |x, n| CR::skip_unary(x, n).unwrap(),
            Code::Gamma => |x, n| CR::skip_gamma(x, n).unwrap(),
            Code::Delta => |x, n| CR::skip_delta(x, n).unwrap(),
            Code::Zeta { k: 1 } => |x, n| CR::skip_gamma(x, n).unwrap(),
            Code::Zeta { k: 2 } => |x, n| CR::skip_zeta(x, 2, n).unwrap(),
            Code::Zeta { k: 3 } => |x, n| CR::skip_zeta3(x, n).unwrap(),
            Code::Zeta { k: 4 } => |x, n| CR::skip_zeta(x, 4, n).unwrap(),
            Code::Zeta { k: 5 } => |x, n| CR::skip_zeta(x, 5, n).unwrap(),
            Code::Zeta { k: 6 } => |x, n| CR::skip_zeta(x, 6, n).unwrap(),
            Code::Zeta { k: 7 } => |x, n| CR::skip_zeta(x, 7, n).unwrap(),
            _ => bail!("Only unary, ɣ, δ, and ζ₁-ζ₇ codes are allowed"),
        })
    }

    pub fn new(code_reader: CR, cf: &CompFlags) -> Result<Self> {
        Ok(Self {
            code_reader,
            read_outdegree: Self::select_code(&cf.outdegrees)?,
            skip_outdegrees: Self::select_skip_code(&cf.outdegrees)?,
            read_reference_offset: Self::select_code(&cf.references)?,
            skip_reference_offsets: Self::select_skip_code(&cf.references)?,
            read_block_count: Self::select_code(&cf.blocks)?,
            skip_block_counts: Self::select_skip_code(&cf.blocks)?,
            read_blocks: Self::select_code(&cf.blocks)?,
            skip_blocks: Self::select_skip_code(&cf.blocks)?,
            read_interval_count: Self::select_code(&cf.intervals)?,
            skip_interval_counts: Self::select_skip_code(&cf.intervals)?,
            read_interval_start: Self::select_code(&cf.intervals)?,
            skip_interval_starts: Self::select_skip_code(&cf.intervals)?,
            read_interval_len: Self::select_code(&cf.intervals)?,
            skip_interval_lens: Self::select_skip_code(&cf.intervals)?,
            read_first_residual: Self::select_code(&cf.residuals)?,
            skip_first_residuals: Self::select_skip_code(&cf.residuals)?,
            read_residual: Self::select_code(&cf.residuals)?,
            skip_residuals: Self::select_skip_code(&cf.residuals)?,
            _marker: core::marker::PhantomData::default(),
        })
    }
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek> BitSeek for DynamicCodesReaderSkipper<E, CR> {
    fn set_pos(&mut self, bit_index: usize) -> Result<()> {
        self.code_reader.set_pos(bit_index)
    }

    fn get_pos(&self) -> usize {
        self.code_reader.get_pos()
    }
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek> WebGraphCodesReader
    for DynamicCodesReaderSkipper<E, CR>
{
    #[inline(always)]
    fn read_outdegree(&mut self) -> u64 {
        (self.read_outdegree)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> u64 {
        (self.read_reference_offset)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> u64 {
        (self.read_block_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> u64 {
        (self.read_blocks)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> u64 {
        (self.read_interval_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> u64 {
        (self.read_interval_start)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> u64 {
        (self.read_interval_len)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> u64 {
        (self.read_first_residual)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> u64 {
        (self.read_residual)(&mut self.code_reader)
    }
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek> WebGraphCodesSkipper
    for DynamicCodesReaderSkipper<E, CR>
{
    #[inline(always)]
    fn skip_outdegrees(&mut self, n: usize) -> usize {
        (self.skip_outdegrees)(&mut self.code_reader, n)
    }

    #[inline(always)]
    fn skip_reference_offsets(&mut self, n: usize) -> usize {
        (self.skip_reference_offsets)(&mut self.code_reader, n)
    }

    #[inline(always)]
    fn skip_block_counts(&mut self, n: usize) -> usize {
        (self.skip_block_counts)(&mut self.code_reader, n)
    }
    #[inline(always)]
    fn skip_blocks(&mut self, n: usize) -> usize {
        (self.skip_blocks)(&mut self.code_reader, n)
    }

    #[inline(always)]
    fn skip_interval_counts(&mut self, n: usize) -> usize {
        (self.skip_interval_counts)(&mut self.code_reader, n)
    }
    #[inline(always)]
    fn skip_interval_starts(&mut self, n: usize) -> usize {
        (self.skip_interval_starts)(&mut self.code_reader, n)
    }
    #[inline(always)]
    fn skip_interval_lens(&mut self, n: usize) -> usize {
        (self.skip_interval_lens)(&mut self.code_reader, n)
    }

    #[inline(always)]
    fn skip_first_residuals(&mut self, n: usize) -> usize {
        (self.skip_first_residuals)(&mut self.code_reader, n)
    }
    #[inline(always)]
    fn skip_residuals(&mut self, n: usize) -> usize {
        (self.skip_residuals)(&mut self.code_reader, n)
    }
}

/// An implementation of [`WebGraphCodesWriter`] with the most commonly used codes
pub struct DynamicCodesWriter<E: Endianness, CW: WriteCodes<E>> {
    code_writer: CW,
    write_outdegree: fn(&mut CW, u64) -> Result<usize>,
    write_reference_offset: fn(&mut CW, u64) -> Result<usize>,
    write_block_count: fn(&mut CW, u64) -> Result<usize>,
    write_blocks: fn(&mut CW, u64) -> Result<usize>,
    write_interval_count: fn(&mut CW, u64) -> Result<usize>,
    write_interval_start: fn(&mut CW, u64) -> Result<usize>,
    write_interval_len: fn(&mut CW, u64) -> Result<usize>,
    write_first_residual: fn(&mut CW, u64) -> Result<usize>,
    write_residual: fn(&mut CW, u64) -> Result<usize>,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CW: WriteCodes<E>> DynamicCodesWriter<E, CW> {
    fn select_code(code: &Code) -> fn(&mut CW, u64) -> Result<usize> {
        match code {
            Code::Unary => CW::write_unary,
            Code::Gamma => CW::write_gamma,
            Code::Delta => CW::write_delta,
            Code::Zeta { k: 3 } => CW::write_zeta3,
            _ => panic!("Only unary, ɣ, δ, and ζ₃ codes are allowed"),
        }
    }

    pub fn new(code_writer: CW, cf: &CompFlags) -> Self {
        Self {
            code_writer,
            write_outdegree: Self::select_code(&cf.outdegrees),
            write_reference_offset: Self::select_code(&cf.references),
            write_block_count: Self::select_code(&cf.blocks),
            write_blocks: Self::select_code(&cf.blocks),
            write_interval_count: Self::select_code(&cf.intervals),
            write_interval_start: Self::select_code(&cf.intervals),
            write_interval_len: Self::select_code(&cf.intervals),
            write_first_residual: Self::select_code(&cf.residuals),
            write_residual: Self::select_code(&cf.residuals),
            _marker: core::marker::PhantomData::default(),
        }
    }
}

impl<E: Endianness, CW: WriteCodes<E> + BitSeek + Clone> BitSeek for DynamicCodesWriter<E, CW> {
    fn set_pos(&mut self, bit_index: usize) -> Result<()> {
        self.code_writer.set_pos(bit_index)
    }

    fn get_pos(&self) -> usize {
        self.code_writer.get_pos()
    }
}

impl<E: Endianness, CW: WriteCodes<E>> WebGraphCodesWriter for DynamicCodesWriter<E, CW> {
    type MockWriter = DynamicCodesMockWriter;
    fn mock(&self) -> Self::MockWriter {
        macro_rules! reconstruct_code {
            ($code:expr) => {{
                let code = $code as usize;
                if code == CW::write_unary as usize {
                    len_unary
                } else if code == CW::write_gamma as usize {
                    len_gamma
                } else if code == CW::write_delta as usize {
                    len_delta
                } else if code == CW::write_zeta3 as usize {
                    |x| len_zeta(x, 3)
                } else {
                    unreachable!()
                }
            }};
        }
        DynamicCodesMockWriter {
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

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize> {
        (self.write_outdegree)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize> {
        (self.write_reference_offset)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize> {
        (self.write_block_count)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_blocks(&mut self, value: u64) -> Result<usize> {
        (self.write_blocks)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize> {
        (self.write_interval_count)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize> {
        (self.write_interval_start)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize> {
        (self.write_interval_len)(&mut self.code_writer, value)
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize> {
        (self.write_first_residual)(&mut self.code_writer, value)
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize> {
        (self.write_residual)(&mut self.code_writer, value)
    }

    fn flush(self) -> Result<()> {
        self.code_writer.flush()
    }
}

/// An implementation of [`WebGraphCodesWriter`] that doesn't write anything
/// but just returns the length of the bytes that would have been written.
#[derive(Clone)]
pub struct DynamicCodesMockWriter {
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

impl DynamicCodesMockWriter {
    fn select_code(code: &Code) -> fn(u64) -> usize {
        match code {
            Code::Unary => len_unary,
            Code::Gamma => len_gamma,
            Code::Delta => len_delta,
            Code::Zeta { k: 3 } => |x| len_zeta(x, 3),
            _ => panic!("Only unary, ɣ, δ, and ζ₃ codes are allowed"),
        }
    }

    pub fn new(cf: &CompFlags) -> Self {
        Self {
            len_outdegree: Self::select_code(&cf.outdegrees),
            len_reference_offset: Self::select_code(&cf.references),
            len_block_count: Self::select_code(&cf.blocks),
            len_blocks: Self::select_code(&cf.blocks),
            len_interval_count: Self::select_code(&cf.intervals),
            len_interval_start: Self::select_code(&cf.intervals),
            len_interval_len: Self::select_code(&cf.intervals),
            len_first_residual: Self::select_code(&cf.residuals),
            len_residual: Self::select_code(&cf.residuals),
        }
    }
}

impl WebGraphCodesWriter for DynamicCodesMockWriter {
    type MockWriter = Self;
    fn mock(&self) -> Self::MockWriter {
        self.clone()
    }

    #[inline(always)]
    fn write_outdegree(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_outdegree)(value))
    }

    #[inline(always)]
    fn write_reference_offset(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_reference_offset)(value))
    }

    #[inline(always)]
    fn write_block_count(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_block_count)(value))
    }
    #[inline(always)]
    fn write_blocks(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_blocks)(value))
    }

    #[inline(always)]
    fn write_interval_count(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_interval_count)(value))
    }
    #[inline(always)]
    fn write_interval_start(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_interval_start)(value))
    }
    #[inline(always)]
    fn write_interval_len(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_interval_len)(value))
    }

    #[inline(always)]
    fn write_first_residual(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_first_residual)(value))
    }
    #[inline(always)]
    fn write_residual(&mut self, value: u64) -> Result<usize> {
        Ok((self.len_residual)(value))
    }

    fn flush(self) -> Result<()> {
        Ok(())
    }
}
