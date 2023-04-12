use super::{
    BitOrder, M2L, L2M,
    BitRead,
    WordRead, WordStream, BitSeek,
};
use crate::utils::get_lowest_bits;
use anyhow::{Result, bail};

// I'm not really happy about implementing it over a seekable stream instead of 
// a slice but this way is more general and I checked that the compiler generate
// decent code.

/// An impementation of [`BitRead`] on a Seekable word stream [`WordRead`] 
/// + [`WordStream`]
pub struct UnbufferedBitStreamRead<BO: BitOrder, WR: WordRead + WordStream> {
    /// The stream which we will read words from
    data: WR,
    /// The index of the current bit we are ate
    bit_idx: usize,
    /// Make the compiler happy
    _marker: core::marker::PhantomData<BO>,
}

impl<BO: BitOrder, WR: WordRead + WordStream> UnbufferedBitStreamRead<BO, WR> {
    /// Create a new BitStreamRead on a generig WordRead
    pub fn new(data: WR) -> Self {
        Self{
            data,
            bit_idx: 0,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

impl<WR: WordRead + WordStream> BitRead<M2L> for UnbufferedBitStreamRead<M2L, WR> {
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

        let res = if (in_word_offset + n_bits as usize) < 64 {
            // single word access
            let word = self.data.read_next_word()?;
            (word << in_word_offset) >> (64 - n_bits)
        } else {
            // double word access
            let high_word = self.data.read_next_word()? as u128;
            let low_word  = self.data.read_next_word()? as u128;
            let composed = (high_word << 64) | low_word;
            ((composed << in_word_offset) >> (128 - n_bits)) as u64
        };
        self.bit_idx += n_bits as usize;
        Ok(res)
    }

    #[inline]
    fn read_unary<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        self.data.set_position(self.bit_idx / 64)?;
        let in_word_offset = self.bit_idx % 64;
        let mut bits_in_word = 64 - in_word_offset;
        let mut total = 0;

        let mut word = self.data.read_next_word()?;
        word <<= in_word_offset;
        loop {
            let zeros = word.leading_zeros() as usize;
            // the unary code fits in the word
            if zeros < bits_in_word {
                self.bit_idx += total + zeros + 1;
                return Ok(total as u64 + zeros as u64);
            }
            total += bits_in_word;
            bits_in_word = 64;
            word = self.data.read_next_word()?;
        }
    }
}

impl<WR: WordRead + WordStream> BitSeek for UnbufferedBitStreamRead<L2M, WR> {
    fn get_position(&self) -> usize {
        self.bit_idx
    }

    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        self.bit_idx = bit_index;
        Ok(())
    }
}

impl<WR: WordRead + WordStream> BitSeek for UnbufferedBitStreamRead<M2L, WR> {
    fn get_position(&self) -> usize {
        self.bit_idx
    }

    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        self.bit_idx = bit_index;
        Ok(())
    }
}

impl<WR: WordRead + WordStream> BitRead<L2M> for UnbufferedBitStreamRead<L2M, WR> {
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

        let res = if (in_word_offset + n_bits as usize) < 64 {
            // single word access
            let word = self.data.read_next_word()?;
            get_lowest_bits(
                word >> in_word_offset,
                n_bits,
            )
        } else {
            // double word access
            let high_word = self.data.read_next_word()? as u128;
            let low_word  = self.data.read_next_word()? as u128;
            let composed = (high_word << 64) | low_word;
            get_lowest_bits(
                (composed >> in_word_offset) as u64,
                n_bits,
            )
        };
        self.bit_idx += n_bits as usize;
        Ok(res)
    }

    #[inline]
    fn read_unary<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        self.data.set_position(self.bit_idx / 64)?;
        let in_word_offset = self.bit_idx % 64;
        let mut bits_in_word = 64 - in_word_offset;
        let mut total = 0;

        let mut word = self.data.read_next_word()?;
        word >>= in_word_offset;
        loop {
            let zeros = word.trailing_zeros() as usize;
            // the unary code fits in the word
            if zeros < bits_in_word {
                self.bit_idx += total + zeros + 1;
                return Ok(total as u64 + zeros as u64);
            }
            total += bits_in_word;
            bits_in_word = 64;
            word = self.data.read_next_word()?;
        }
    }
}