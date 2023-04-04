use super::WordReader;
use anyhow::{Result, bail, Context};

/// Common traits for objects that can read a fixed number of bits and unary 
/// codes from a stream of bits
pub trait ReadBits {
    /// Move the stream cursor so that if we call `read_bits(1)` we will read 
    /// the `bit_index`-th bit in the stream
    fn seek_bit(&mut self, bit_index: usize) -> Result<()>;

    /// Read `n_bits` bits from the buffer and return them in the lowest bits
    fn read_bits(&mut self, n_bits: u8) -> Result<u64>;

    /// Read an unary code
    fn read_unary(&mut self) -> Result<u64> {
        let mut count = 0;
        loop {
            let bit = self.read_bits(1)?;
            if bit != 0 {
                return Ok(count);
            }
            count += 1;
        }
    }
}

/// A BitStream built uppon a generic [`WordReader`] that caches the read words 
/// in a buffer
pub struct BufferedBitStreamReader<WR: WordReader> {
    /// The backend that's used to read the words to fill the buffer
    backend: WR,
    /// The current cache of bits (at most 2 words) that's used to read the 
    /// codes. The bits are read FROM MSB TO LSB
    buffer: u128,
    /// Number of bits valid left in the buffer
    valid_bits: u8,
}

impl<WR: WordReader> BufferedBitStreamReader<WR> {

    /// Create a new [`BufferedBitStreamReader`] on a generic backend
    /// 
    /// ### Example
    /// ```
    /// use webgraph::codes::*;
    /// let words = [0x0043b59fccf16077];
    /// let word_reader = MemWordReader::new(&words);
    /// let mut bitstream = BufferedBitStreamReader::new(word_reader);
    /// ```
    pub fn new(backend: WR) -> Self {

        // TODO!: Should we do early filling? 
        // This would fail if the backend has only 64 bits which, while 
        // unlikely, it should be possible.
        // 
        // ```
        // let low_word = backend.read_next_word()? as u128;
        // let high_word = backend.read_next_word()? as u128;
        // let buffer = (high_word << 64) | low_word;
        // ```

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
            .with_context(|| "Error while reflling BufferedBitStreamReader")?.to_be();
        self.valid_bits += 64;
        self.buffer |= (new_word as u128) << (128 - self.valid_bits);
        
        Ok(())
    }
}

impl<WR: WordReader> ReadBits for BufferedBitStreamReader<WR> {
    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        self.backend.set_position(bit_index / 64)
            .with_context(|| format!("BufferedBitStreamReader was seeking_bit {}", bit_index))?;
        let bit_offset = bit_index % 64;
        self.buffer = 0;
        self.valid_bits = 0;
        if bit_offset != 0 {
            self.read_bits(bit_offset as u8)
                .with_context(|| format!("BufferedBitStreamReader was seeking_bit {}", bit_index))?;
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
        let result = self.buffer >> (128 - n_bits);

        // remove the read bits from the buffer
        self.valid_bits -= n_bits;
        self.buffer <<= n_bits;
        
        Ok(result as u64)
    }

    fn read_unary(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        loop {
            // count the zeros from the left
            let zeros = self.buffer.leading_zeros() as u8;

            // if we encountered an 1 in the valid_bits we can return            
            if zeros < self.valid_bits {
                result += zeros as u64;
                self.buffer <<= zeros + 1;
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
