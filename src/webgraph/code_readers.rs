use super::*;
use dsi_bitstream::prelude::*;

pub mod const_codes {
    pub const UNARY: usize = 0;
    pub const GAMMA: usize = 1;
    pub const DELTA: usize = 2;
    pub const ZETA: usize = 3;
}

#[repr(transparent)]
/// An implementation of WebGraphCodesReader with the most commonly used codes
pub struct DefaultCodesReader<
    E: Endianness,
    CR: ReadCodes<E>,
    const OUTDEGREES: usize = { const_codes::GAMMA },
    const REFERENCES: usize = { const_codes::UNARY },
    const BLOCKS: usize = { const_codes::GAMMA },
    const INTERVALS: usize = { const_codes::GAMMA },
    const RESIDUALS: usize = { const_codes::ZETA },
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
            _marker: self._marker,
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
    ($self:ident, $code:expr, $k: expr) => {
        match $code {
            const_codes::UNARY => $self.code_reader.read_unary(),
            const_codes::GAMMA => $self.code_reader.read_gamma(),
            const_codes::DELTA => $self.code_reader.read_delta(),
            const_codes::ZETA if $k == 3 => $self.code_reader.read_zeta3(),
            const_codes::ZETA => $self.code_reader.read_zeta(K),
            _ => panic!("Only values in the range [0..4) are allowed to represent codes"),
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
        select_code!(self, OUTDEGREES, K)
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> Result<u64> {
        select_code!(self, REFERENCES, K)
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> Result<u64> {
        select_code!(self, BLOCKS, K)
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> Result<u64> {
        select_code!(self, BLOCKS, K)
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> Result<u64> {
        select_code!(self, INTERVALS, K)
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> Result<u64> {
        select_code!(self, INTERVALS, K)
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> Result<u64> {
        select_code!(self, INTERVALS, K)
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> Result<u64> {
        select_code!(self, RESIDUALS, K)
    }
    #[inline(always)]
    fn read_residual(&mut self) -> Result<u64> {
        select_code!(self, RESIDUALS, K)
    }
}
