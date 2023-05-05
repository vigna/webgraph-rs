use crate::codes::unary_tables;
use crate::traits::*;
use anyhow::{bail, Context, Result};

/// A BitStream built uppon a generic [`WordRead`] that caches the read words
/// in a buffer
#[derive(Debug)]
pub struct BufferedBitStreamRead<E: BitOrder, BW: Word, WR: WordRead> {
    /// The backend that is used to read the words to fill the buffer.
    backend: WR,
    /// The bit buffer (at most 2 words) that is used to read the codes. It is never full.
    buffer: BW,
    /// Number of bits valid left in the buffer. It is always smaller than `BW::BITS`.
    valid_bits: usize,
    /// Just needed to specify the BitOrder.
    _marker: core::marker::PhantomData<E>,
}

impl<E: BitOrder, BW: Word, WR: WordRead + Clone> core::clone::Clone
    for BufferedBitStreamRead<E, BW, WR>
{
    // No need to copy the buffer
    // TODO!: think about how to make a lightweight clone
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            buffer: BW::ZERO,
            valid_bits: 0,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

impl<E: BitOrder, BW: Word, WR: WordRead> BufferedBitStreamRead<E, BW, WR> {
    /// Create a new [`BufferedBitStreamRead`] on a generic backend
    ///
    /// ### Example
    /// ```
    /// use webgraph::prelude::*;
    /// let words: [u64; 1] = [0x0043b59fccf16077];
    /// let word_reader = MemWordRead::new(&words);
    /// let mut bitstream = <BufferedBitStreamRead<M2L, u128, _>>::new(word_reader);
    /// ```
    #[must_use]
    pub fn new(backend: WR) -> Self {
        Self {
            backend,
            buffer: BW::ZERO,
            valid_bits: 0,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

impl<BW: Word, WR: WordRead> BufferedBitStreamRead<M2L, BW, WR>
where
    WR::Word: UpcastableInto<BW>,
{
    /// Ensure that in the buffer there are at least `WR::Word::BITS` bits to read
    /// The user has the responsability of guaranteeing that there are at least
    /// `WR::Word::BITS` free bits in the buffer.
    #[inline(always)]
    fn refill(&mut self) -> Result<()> {
        // if we have 64 valid bits, we don't have space for a new word
        // and by definition we can only read
        let free_bits = BW::BITS - self.valid_bits;
        debug_assert!(free_bits >= WR::Word::BITS);

        let new_word: BW = self
            .backend
            .read_next_word()
            .with_context(|| "Error while reflling BufferedBitStreamRead")?
            .to_be()
            .upcast();
        self.valid_bits += WR::Word::BITS;
        self.buffer |= new_word << (BW::BITS - self.valid_bits);
        Ok(())
    }
}

impl<BW: Word, WR: WordRead + WordStream> BitSeek for BufferedBitStreamRead<M2L, BW, WR>
where
    WR::Word: UpcastableInto<BW>,
{
    #[inline]
    fn get_position(&self) -> usize {
        self.backend.get_position() * WR::Word::BITS - self.valid_bits
    }

    #[inline]
    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        self.backend
            .set_position(bit_index / WR::Word::BITS)
            .with_context(|| "BufferedBitStreamRead was seeking_bit")?;
        let bit_offset = bit_index % WR::Word::BITS;
        self.buffer = BW::ZERO;
        self.valid_bits = 0;
        if bit_offset != 0 {
            let new_word: BW = self.backend.read_next_word()?.to_be().upcast();
            self.valid_bits = WR::Word::BITS - bit_offset;
            self.buffer = new_word << (BW::BITS - self.valid_bits);
        }
        Ok(())
    }
}

impl<BW: Word, WR: WordRead> BitRead<M2L> for BufferedBitStreamRead<M2L, BW, WR>
where
    BW: DowncastableInto<WR::Word> + CastableInto<u64>,
    WR::Word: UpcastableInto<BW> + UpcastableInto<u64>,
{
    type PeekType = WR::Word;

    #[inline]
    fn peek_bits(&mut self, n_bits: usize) -> Result<Self::PeekType> {
        if n_bits > WR::Word::BITS {
            bail!(
                "The n of bits to peek has to be in [0, {}] and {} is not.",
                WR::Word::BITS,
                n_bits
            );
        }
        if n_bits == 0 {
            return Ok(WR::Word::ZERO);
        }
        // a peek can do at most one refill, otherwise we might loose data
        if n_bits > self.valid_bits {
            self.refill()?;
        }

        // read the `n_bits` highest bits of the buffer and shift them to
        // be the lowest
        Ok((self.buffer >> (BW::BITS - n_bits)).downcast())
    }

    #[inline]
    fn skip_bits(&mut self, mut n_bits: usize) -> Result<()> {
        // happy case, just shift the buffer
        if n_bits <= self.valid_bits {
            self.valid_bits -= n_bits;
            self.buffer <<= n_bits;
            return Ok(());
        }

        // clean the buffer data
        n_bits -= self.valid_bits;
        self.valid_bits = 0;
        // skip words as needed
        while n_bits > WR::Word::BITS {
            let _ = self.backend.read_next_word()?;
            n_bits -= WR::Word::BITS;
        }
        // read the new word and clear the final bits
        self.refill()?;
        self.valid_bits -= n_bits;
        self.buffer <<= n_bits;

        Ok(())
    }

    #[inline(always)]
    fn skip_bits_after_table_lookup(&mut self, n_bits: usize) -> Result<()> {
        self.valid_bits -= n_bits;
        self.buffer <<= n_bits;
        Ok(())
    }

    #[inline]
    fn read_bits(&mut self, mut n_bits: usize) -> Result<u64> {
        debug_assert!(self.valid_bits < BW::BITS);

        // most common path, we just read the buffer
        if n_bits <= self.valid_bits {
            // Valid right shift of BW::BITS - n_bits, even when n_bits is zero
            let result: u64 = (self.buffer >> (BW::BITS - n_bits - 1) >> 1).cast();
            self.valid_bits -= n_bits;
            self.buffer <<= n_bits;
            return Ok(result);
        }

        if n_bits > 64 {
            bail!(
                "The n of bits to peek has to be in [0, 64] and {} is not.",
                n_bits
            );
        }

        let mut result: u64 = if self.valid_bits != 0 {
            self.buffer >> (BW::BITS - self.valid_bits)
        } else {
            BW::ZERO
        }
        .cast();
        n_bits -= self.valid_bits;

        // Directly read to the result without updating the buffer
        while n_bits > WR::Word::BITS {
            let new_word: u64 = self.backend.read_next_word()?.to_be().upcast();
            result = (result << WR::Word::BITS) | new_word;
            n_bits -= WR::Word::BITS;
        }
        // get the final word
        let new_word = self.backend.read_next_word()?.to_be();
        self.valid_bits = WR::Word::BITS - n_bits;
        // compose the remaining bits
        let upcasted: u64 = new_word.upcast();
        let final_bits: u64 = (upcasted >> self.valid_bits).downcast();
        result = (result << n_bits) | final_bits;
        // and put the rest in the buffer
        self.buffer = new_word.upcast();
        self.buffer = (self.buffer << (BW::BITS - self.valid_bits - 1)) << 1;

        Ok(result)
    }

    #[inline]
    fn read_unary<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        if USE_TABLE {
            if let Some(res) = unary_tables::read_table_m2l(self)? {
                return Ok(res);
            }
        }
        let mut result: u64 = 0;
        loop {
            // count the zeros from the left
            let zeros: usize = self.buffer.leading_zeros();

            // if we encountered an 1 in the valid_bits we can return
            if zeros < self.valid_bits {
                result += zeros as u64;
                self.buffer <<= zeros + 1;
                self.valid_bits -= zeros + 1;
                return Ok(result);
            }

            result += self.valid_bits as u64;

            // otherwise we didn't encounter the ending 1 yet so we need to
            // refill and iter again
            let new_word: BW = self.backend.read_next_word()?.to_be().upcast();
            self.valid_bits = WR::Word::BITS;
            self.buffer = new_word << (BW::BITS - WR::Word::BITS);
        }
    }
}

impl<BW: Word, WR: WordRead> BufferedBitStreamRead<L2M, BW, WR>
where
    WR::Word: UpcastableInto<BW>,
{
    /// Ensure that in the buffer there are at least `WR::Word::BITS` bits to read
    /// The user has the responsability of guaranteeing that there are at least
    /// `WR::Word::BITS` free bits in the buffer.
    #[inline(always)]
    fn refill(&mut self) -> Result<()> {
        // if we have 64 valid bits, we don't have space for a new word
        // and by definition we can only read
        let free_bits = BW::BITS - self.valid_bits;
        debug_assert!(free_bits >= WR::Word::BITS);

        let new_word: BW = self
            .backend
            .read_next_word()
            .with_context(|| "Error while reflling BufferedBitStreamRead")?
            .to_le()
            .upcast();
        self.buffer |= new_word << self.valid_bits;
        self.valid_bits += WR::Word::BITS;
        Ok(())
    }
}

impl<BW: Word, WR: WordRead + WordStream> BitSeek for BufferedBitStreamRead<L2M, BW, WR>
where
    WR::Word: UpcastableInto<BW>,
{
    #[inline]
    fn get_position(&self) -> usize {
        self.backend.get_position() * WR::Word::BITS - self.valid_bits
    }

    #[inline]
    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        self.backend
            .set_position(bit_index / WR::Word::BITS)
            .with_context(|| "BufferedBitStreamRead was seeking_bit")?;
        let bit_offset = bit_index % WR::Word::BITS;
        self.buffer = BW::ZERO;
        self.valid_bits = 0;
        if bit_offset != 0 {
            let new_word: BW = self.backend.read_next_word()?.to_le().upcast();
            self.valid_bits = WR::Word::BITS - bit_offset;
            self.buffer = new_word >> self.valid_bits;
        }
        Ok(())
    }
}

impl<BW: Word, WR: WordRead> BitRead<L2M> for BufferedBitStreamRead<L2M, BW, WR>
where
    BW: DowncastableInto<WR::Word> + CastableInto<u64>,
    WR::Word: UpcastableInto<BW> + UpcastableInto<u64>,
{
    type PeekType = WR::Word;

    #[inline]
    fn skip_bits(&mut self, mut n_bits: usize) -> Result<()> {
        // happy case, just shift the buffer
        if n_bits <= self.valid_bits {
            self.valid_bits -= n_bits;
            self.buffer >>= n_bits;
            return Ok(());
        }

        // clean the buffer data
        n_bits -= self.valid_bits;
        self.valid_bits = 0;
        // skip words as needed
        while n_bits > WR::Word::BITS {
            let _ = self.backend.read_next_word()?;
            n_bits -= WR::Word::BITS;
        }
        // read the new word and clear the final bits
        self.refill()?;
        self.valid_bits -= n_bits;
        self.buffer >>= n_bits;

        Ok(())
    }

    #[inline(always)]
    fn skip_bits_after_table_lookup(&mut self, n_bits: usize) -> Result<()> {
        self.valid_bits -= n_bits;
        self.buffer >>= n_bits;
        Ok(())
    }

    #[inline]
    fn read_bits(&mut self, mut n_bits: usize) -> Result<u64> {
        debug_assert!(self.valid_bits < BW::BITS);

        // most common path, we just read the buffer
        if n_bits <= self.valid_bits {
            let result: u64 = (self.buffer & ((BW::ONE << n_bits) - BW::ONE)).cast();
            self.valid_bits -= n_bits;
            self.buffer >>= n_bits;
            return Ok(result);
        }

        if n_bits > 64 {
            bail!(
                "The n of bits to peek has to be in [0, 64] and {} is not.",
                n_bits
            );
        }

        let mut result: u64 = self.buffer.cast();
        n_bits -= self.valid_bits;
        let mut bits_in_res = self.valid_bits;

        // Directly read to the result without updating the buffer
        while n_bits > WR::Word::BITS {
            let new_word: u64 = self.backend.read_next_word()?.to_le().upcast();
            result |= new_word << bits_in_res;
            n_bits -= WR::Word::BITS;
            bits_in_res += WR::Word::BITS;
        }

        // get the final word
        let new_word = self.backend.read_next_word()?.to_le();
        self.valid_bits = WR::Word::BITS - n_bits;
        // compose the remaining bits
        let shamt = 64 - n_bits;
        let upcasted: u64 = new_word.upcast();
        let final_bits: u64 = ((upcasted << shamt) >> shamt).downcast();
        result |= final_bits << bits_in_res;
        // and put the rest in the buffer
        self.buffer = new_word.upcast();
        self.buffer >>= n_bits;

        Ok(result)
    }

    #[inline]
    fn peek_bits(&mut self, n_bits: usize) -> Result<Self::PeekType> {
        if n_bits > WR::Word::BITS {
            bail!(
                "The n of bits to peek has to be in [0, {}] and {} is not.",
                WR::Word::BITS,
                n_bits
            );
        }
        if n_bits == 0 {
            return Ok(WR::Word::ZERO);
        }
        // a peek can do at most one refill, otherwise we might loose data
        if n_bits > self.valid_bits {
            self.refill()?;
        }

        // read the `n_bits` highest bits of the buffer and shift them to
        // be the lowest
        let shamt = BW::BITS - n_bits;
        Ok(((self.buffer << shamt) >> shamt).downcast())
    }

    #[inline]
    fn read_unary<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        if USE_TABLE {
            if let Some(res) = unary_tables::read_table_l2m(self)? {
                return Ok(res);
            }
        }
        let mut result: u64 = 0;
        loop {
            // count the zeros from the left
            let zeros: usize = self.buffer.trailing_zeros();

            // if we encountered an 1 in the valid_bits we can return
            if zeros < self.valid_bits {
                result += zeros as u64;
                self.buffer >>= zeros + 1;
                self.valid_bits -= zeros + 1;
                return Ok(result);
            }

            result += self.valid_bits as u64;

            // otherwise we didn't encounter the ending 1 yet so we need to
            // refill and iter again
            let new_word: BW = self.backend.read_next_word()?.to_le().upcast();
            self.valid_bits = WR::Word::BITS;
            self.buffer = new_word;
        }
    }
}
