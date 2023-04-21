use super::*;
use crate::codes::*;

#[repr(transparent)]
/// An implementation of WebGraphCodesReader with the most commonly used codes
pub struct DefaultCodesReader<BO: BitOrder, CR: 
    BitRead<BO> + GammaRead<BO> + ZetaRead<BO>
>{
    code_reader: CR,
    _marker: core::marker::PhantomData<BO>,
}

impl<BO: BitOrder, CR: BitRead<BO> + GammaRead<BO> + ZetaRead<BO> + Clone> Clone 
    for DefaultCodesReader<BO, CR> {
    fn clone(&self) -> Self {
        Self {
            code_reader: self.code_reader.clone(),
            _marker: self._marker.clone(),
        }
    }
}

impl<BO: BitOrder, CR: BitRead<BO> + GammaRead<BO> + ZetaRead<BO> + BitSeek> BitSeek 
    for DefaultCodesReader<BO, CR> {
        fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
            self.code_reader.seek_bit(bit_index)
        }

        fn get_position(&self) -> usize {
            self.code_reader.get_position()
        }
}

impl<BO: BitOrder, CR: BitRead<BO> + GammaRead<BO> + ZetaRead<BO>> 
    DefaultCodesReader<BO, CR> {
    pub fn new(code_reader: CR) -> Self {
        Self {
            code_reader,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

impl<BO: BitOrder, CR: BitRead<BO> + GammaRead<BO> + ZetaRead<BO>> 
    WebGraphCodesReader for DefaultCodesReader<BO, CR> {
    #[inline(always)]
    fn read_outdegree(&mut self) -> Result<u64> {
        self.code_reader.read_gamma::<true>()
    }

    #[inline(always)]
    fn read_reference_offset(&mut self) -> Result<u64> {
        self.code_reader.read_unary::<false>()
    }

    #[inline(always)]
    fn read_block_count(&mut self) -> Result<u64> {
        self.code_reader.read_gamma::<true>()
    }
    #[inline(always)]
    fn read_blocks(&mut self) -> Result<u64>{
        self.code_reader.read_gamma::<true>()
    }

    #[inline(always)]
    fn read_interval_count(&mut self) -> Result<u64>{
        self.code_reader.read_gamma::<true>()
    }
    #[inline(always)]
    fn read_interval_start(&mut self) -> Result<u64>{
        self.code_reader.read_gamma::<true>()
    }
    #[inline(always)]
    fn read_interval_len(&mut self) -> Result<u64>{
        self.code_reader.read_gamma::<true>()
    }

    #[inline(always)]
    fn read_first_residual(&mut self) -> Result<u64>{
        self.code_reader.read_zeta3::<true>()
    }
    #[inline(always)]
    fn read_residual(&mut self) -> Result<u64>{
        self.code_reader.read_zeta3::<true>()
    }
}