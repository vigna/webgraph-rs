use super::*;
use dsi_bitstream::prelude::*;

/// An implementation of WebGraphCodesReader with the most commonly used codes
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
    pub fn new(code_reader: CR) -> Self {
        Self {
            code_reader: code_reader,
            read_outdegree: CR::read_gamma,
            read_reference_offset: CR::read_unary,
            read_block_count: CR::read_gamma,
            read_blocks: CR::read_gamma,
            read_interval_count: CR::read_gamma,
            read_interval_start: CR::read_gamma,
            read_interval_len: CR::read_gamma,
            read_first_residual: CR::read_zeta3,
            read_residual: CR::read_zeta3,
            _marker: core::marker::PhantomData::default(),
        }
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
            _marker: self._marker.clone(),
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
