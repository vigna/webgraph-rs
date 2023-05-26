use super::*;
use anyhow::bail;
use anyhow::Result;
use dsi_bitstream::codes::Code::*;
use dsi_bitstream::prelude::*;

/// An implementation of [`WebGraphCodesReader`] with the most commonly used codes
pub struct DynamicCodesReader<E: Endianness, CR: ReadCodes<E> + BitSeek + Clone> {
    code_reader: CR,
    read_outdegree: fn(&mut CR) -> Result<u64>,
    read_reference_offset: fn(&mut CR) -> Result<u64>,
    read_block_count: fn(&mut CR) -> Result<u64>,
    read_blocks: fn(&mut CR) -> Result<u64>,
    read_interval_count: fn(&mut CR) -> Result<u64>,
    read_interval_start: fn(&mut CR) -> Result<u64>,
    read_interval_len: fn(&mut CR) -> Result<u64>,
    read_first_residual: fn(&mut CR) -> Result<u64>,
    read_residual: fn(&mut CR) -> Result<u64>,
    _marker: core::marker::PhantomData<E>,
}
impl<E: Endianness, CR: ReadCodes<E> + BitSeek + Clone> DynamicCodesReader<E, CR> {
    fn select_code(code: &Code) -> Result<fn(&mut CR) -> Result<u64>> {
        Ok(match code {
            Unary => CR::read_unary,
            Gamma => CR::read_gamma,
            Delta => CR::read_delta,
            Zeta { k: 1 } => CR::read_gamma,
            Zeta { k: 2 } => |x| CR::read_zeta(x, 2),
            Zeta { k: 3 } => CR::read_zeta3,
            Zeta { k: 4 } => |x| CR::read_zeta(x, 4),
            Zeta { k: 5 } => |x| CR::read_zeta(x, 5),
            Zeta { k: 6 } => |x| CR::read_zeta(x, 6),
            Zeta { k: 7 } => |x| CR::read_zeta(x, 7),
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

impl<E: Endianness, CR: ReadCodes<E> + BitSeek + Clone> Clone for DynamicCodesReader<E, CR> {
    fn clone(&self) -> Self {
        Self {
            code_reader: self.code_reader.clone(),
            read_outdegree: self.read_outdegree,
            read_reference_offset: self.read_reference_offset,
            read_block_count: self.read_block_count,
            read_blocks: self.read_blocks,
            read_interval_count: self.read_interval_count,
            read_interval_start: self.read_interval_start,
            read_interval_len: self.read_interval_len,
            read_first_residual: self.read_first_residual,
            read_residual: self.read_residual,
            _marker: self._marker,
        }
    }
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek + Clone> BitSeek for DynamicCodesReader<E, CR> {
    fn set_pos(&mut self, bit_index: usize) -> Result<()> {
        self.code_reader.set_pos(bit_index)
    }

    fn get_pos(&self) -> usize {
        self.code_reader.get_pos()
    }
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek + Clone> WebGraphCodesReader
    for DynamicCodesReader<E, CR>
{
    #[inline(always)]
    fn read_outdegree(&mut self) -> Result<u64> {
        (self.read_outdegree)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> Result<u64> {
        (self.read_reference_offset)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> Result<u64> {
        (self.read_block_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> Result<u64> {
        (self.read_blocks)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> Result<u64> {
        (self.read_interval_count)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> Result<u64> {
        (self.read_interval_start)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> Result<u64> {
        (self.read_interval_len)(&mut self.code_reader)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> Result<u64> {
        (self.read_first_residual)(&mut self.code_reader)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> Result<u64> {
        (self.read_residual)(&mut self.code_reader)
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
            Unary => CW::write_unary,
            Gamma => CW::write_gamma,
            Delta => CW::write_delta,
            Zeta { k: 3 } => CW::write_zeta3,
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
            Unary => len_unary,
            Gamma => len_gamma,
            Delta => len_delta,
            Zeta { k: 3 } => |x| len_zeta(3, x),
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
}
