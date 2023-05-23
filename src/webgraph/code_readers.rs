use super::*;
use dsi_bitstream::prelude::*;

#[repr(transparent)]
/// An implementation of WebGraphCodesReader with the most commonly used codes
pub struct DefaultCodesReader<
    E: Endianness,
    CR: ReadCodes<E>,
    // ɣ
    const OUTDEGREES: usize = 1,
    // unary
    const REFERENCES: usize = 0,
    // ɣ
    const BLOCKS: usize = 2,
    // ɣ
    const INTERVALS: usize = 2,
    // ζ
    const RESIDUALS: usize = 3,
    const K: u64 = 3,
> {
    code_reader: CR,
    _marker: core::marker::PhantomData<E>,
}

impl<
        E: Endianness,
        CR: ReadCodes<E> + Clone,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > Clone for DefaultCodesReader<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    fn clone(&self) -> Self {
        Self {
            code_reader: self.code_reader.clone(),
            _marker: self._marker.clone(),
        }
    }
}

impl<
        E: Endianness,
        CR: ReadCodes<E> + BitSeek,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > BitSeek
    for DefaultCodesReader<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    fn set_pos(&mut self, bit_index: usize) -> Result<()> {
        self.code_reader.set_pos(bit_index)
    }

    fn get_pos(&self) -> usize {
        self.code_reader.get_pos()
    }
}

impl<
        E: Endianness,
        CR: ReadCodes<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > DefaultCodesReader<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    pub fn new(code_reader: CR) -> Self {
        Self {
            code_reader,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

macro_rules! select_code {
    ($self:ident, $code:expr) => {
        match $code {
            0 => $self.code_reader.read_unary(),
            1 => $self.code_reader.read_gamma(),
            2 => $self.code_reader.read_delta(),
            3 => $self.code_reader.read_zeta3(),
            4 => $self.code_reader.read_zeta(K),
            _ => panic!("Only values in the range [0..5) are allowed to represent codes"),
        }
    };
}

impl<
        E: Endianness,
        CR: ReadCodes<E>,
        const OUTDEGREES: usize,
        const REFERENCES: usize,
        const BLOCKS: usize,
        const INTERVALS: usize,
        const RESIDUALS: usize,
        const K: u64,
    > WebGraphCodesReader
    for DefaultCodesReader<E, CR, OUTDEGREES, REFERENCES, BLOCKS, INTERVALS, RESIDUALS, K>
{
    #[inline(always)]
    fn read_outdegree(&mut self) -> Result<u64> {
        select_code!(self, OUTDEGREES)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> Result<u64> {
        select_code!(self, REFERENCES)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> Result<u64> {
        select_code!(self, BLOCKS)
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> Result<u64> {
        select_code!(self, BLOCKS)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> Result<u64> {
        select_code!(self, INTERVALS)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> Result<u64> {
        select_code!(self, INTERVALS)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> Result<u64> {
        select_code!(self, INTERVALS)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> Result<u64> {
        select_code!(self, RESIDUALS)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> Result<u64> {
        select_code!(self, RESIDUALS)
    }
}
