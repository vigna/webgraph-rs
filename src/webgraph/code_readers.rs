use super::*;
use dsi_bitstream::prelude::*;

#[repr(transparent)]
/// An implementation of WebGraphCodesReader with the most commonly used codes
pub struct DefaultCodesReader<E: Endianness, CR: ReadCodes<E>> {
    code_reader: CR,
    _marker: core::marker::PhantomData<E>,
}

impl<E: Endianness, CR: ReadCodes<E> + Clone> Clone for DefaultCodesReader<E, CR> {
    fn clone(&self) -> Self {
        Self {
            code_reader: self.code_reader.clone(),
            _marker: self._marker.clone(),
        }
    }
}

impl<E: Endianness, CR: ReadCodes<E> + BitSeek> BitSeek for DefaultCodesReader<E, CR> {
    fn set_pos(&mut self, bit_index: usize) -> Result<()> {
        self.code_reader.set_pos(bit_index)
    }

    fn get_pos(&self) -> usize {
        self.code_reader.get_pos()
    }
}

impl<E: Endianness, CR: ReadCodes<E>> DefaultCodesReader<E, CR> {
    pub fn new(code_reader: CR) -> Self {
        Self {
            code_reader,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

impl<E: Endianness, CR: ReadCodes<E>> WebGraphCodesReader for DefaultCodesReader<E, CR> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> Result<u64> {
        self.code_reader.read_gamma()
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> Result<u64> {
        self.code_reader.read_unary()
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> Result<u64> {
        self.code_reader.read_gamma()
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> Result<u64> {
        self.code_reader.read_gamma()
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> Result<u64> {
        self.code_reader.read_gamma()
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> Result<u64> {
        self.code_reader.read_gamma()
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> Result<u64> {
        self.code_reader.read_gamma()
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> Result<u64> {
        self.code_reader.read_zeta3()
    }
    #[inline(always)]
    fn read_residual(&mut self) -> Result<u64> {
        self.code_reader.read_zeta3()
    }
}
