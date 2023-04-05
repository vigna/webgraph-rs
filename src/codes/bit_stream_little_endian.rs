use super::{WordReader, ReadBits};
use anyhow::{Result, bail, Context};

/// A BitStream built uppon a generic [`WordReader`] that caches the read words 
/// in a buffer
pub struct BufferedBitStreamReaderLittle<WR: WordReader> {
    /// The backend that's used to read the words to fill the buffer
    backend: WR,
    /// The current cache of bits (at most 2 words) that's used to read the 
    /// codes. The bits are read FROM LSB TO MSB
    buffer: u128,
    /// Number of bits valid left in the buffer
    valid_bits: u8,
}

impl<WR: WordReader> BufferedBitStreamReaderLittle<WR> {

    /// Create a new [`BufferedBitStreamReaderLittle`] on a generic backend
    /// 
    /// ### Example
    /// ```
    /// use webgraph::codes::*;
    /// let words = [0x0043b59fccf16077];
    /// let word_reader = MemWordReader::new(&words);
    /// let mut bitstream = BufferedBitStreamReaderLittle::new(word_reader);
    /// ```
    pub fn new(backend: WR) -> Self {

        Self {
            backend,
            buffer: 0,
            valid_bits: 0,
        }
    }

    /// Ensure that in the buffer there are at least 64 bits to read
    #[inline]
    fn refill(&mut self) -> Result<()> {
        // if we have 64 valid bits, we don't have space for a new word
        // and by definition we can only read
        if self.valid_bits > 64 {
            return Ok(());
        }

        // Read a new 64-bit word and put it in the buffer
        let new_word = self.backend.read_next_word()
            .with_context(|| "Error while reflling BufferedBitStreamReaderLittle")?.to_le();
        self.buffer |= (new_word as u128) << self.valid_bits;
        self.valid_bits += 64;
        
        Ok(())
    }
}

impl<WR: WordReader> ReadBits for BufferedBitStreamReaderLittle<WR> {
    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        self.backend.set_position(bit_index / 64)
            .with_context(|| format!("BufferedBitStreamReaderLittle was seeking_bit {}", bit_index))?;
        let bit_offset = bit_index % 64;
        self.buffer = 0;
        self.valid_bits = 0;
        if bit_offset != 0 {
            self.read_bits(bit_offset as u8)
                .with_context(|| format!("BufferedBitStreamReaderLittle was seeking_bit {}", bit_index))?;
        }
        Ok(())
    }

    fn read_bits(&mut self, n_bits: u8) -> Result<u64> {
        if n_bits == 0 || n_bits > 64 {
            bail!("The n of bits to read has to be in [1, 64] and {} is not.", n_bits);
        }

        if n_bits > self.valid_bits {
            self.refill()?;
        }

        // read the `n_bits` highest bits of the buffer and shift them to
        // be the lowest
        let result = crate::utils::get_lowest_bits(self.buffer as u64, n_bits);

        // remove the read bits from the buffer
        self.valid_bits -= n_bits;
        self.buffer >>= n_bits;
        
        Ok(result as u64)
    }

    fn read_unary(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        loop {
            // count the zeros from the left
            let zeros = self.buffer.trailing_zeros() as u8;

            // if we encountered an 1 in the valid_bits we can return            
            if zeros < self.valid_bits {
                result += zeros as u64;
                self.buffer >>= zeros + 1;
                self.valid_bits -= zeros + 1;
                return Ok(result);
            }

            result += self.valid_bits as u64;
            self.valid_bits = 0;
            
            // otherwise we didn't encounter the ending 1 yet so we need to 
            // refill and iter again
            self.refill()?;
        }
    }
}
