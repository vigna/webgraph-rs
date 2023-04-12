use super::{
    BitOrder, M2L, L2M,
    BitRead,
    WordRead, WordStream,
};
use anyhow::{Result, bail};

// I'm not really happy about implementing it over a seekable stream instead of 
// a slice but this way is more general and I checked that the compiler generate
// decent code.

/// An impementation of [`BitRead`] on a Seekable word stream [`WordRead`] 
/// + [`WordStream`]
pub struct BitStreamRead<BO: BitOrder, WR: WordRead + WordStream> {
    /// The stream which we will read words from
    data: WR,
    /// The index of the current bit we are ate
    bit_idx: usize,
    /// Make the compiler happy
    _marker: core::marker::PhantomData<BO>,
}

impl<WR: WordRead + WordStream> BitRead<M2L> for BitStreamRead<M2L, WR> {
    #[inline]
    fn skip_bits(&mut self, n_bits: u8) -> Result<()> {
        self.bit_idx += n_bits as usize;
        Ok(())
    }

    #[inline]
    fn read_bits(&mut self, n_bits: u8) -> Result<u64> {
        let res = self.peek_bits(n_bits)?;
        self.skip_bits(n_bits)?;
        Ok(res)
    }

    #[inline]
    fn peek_bits(&mut self, n_bits: u8) -> Result<u64> {
        if n_bits > 64 {
            bail!("The n of bits to read has to be in [0, 64] and {} is not.", n_bits);
        }
        if n_bits == 0 {
            return Ok(0);
        }
        self.data.set_position(self.bit_idx / 64)?;
        let in_word_offset = self.bit_idx % 64;

        let res = if(in_word_offset + n_bits as usize) < 64 {
            // single word access
            let word = self.data.read_next_word()?;
            word >> in_word_offset
        } else {
            // double word access
            let low_word  = self.data.read_next_word()?;
            let high_word = self.data.read_next_word()?;
            todo!();
        };
        self.bit_idx += n_bits as usize;
        Ok(res)
    }

    #[inline]
    fn read_unary<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        todo!();
    }
}