use super::{
    WordRead, 
    BitSeek, BitRead,
    BitOrder, M2L, L2M,
    unary_tables, 
    GammaRead, gamma_tables,
};
use crate::utils::get_lowest_bits;
use anyhow::{Result, bail, Context};

/// A BitStream built uppon a generic [`WordRead`] that caches the read words 
/// in a buffer
pub struct BufferedBitStreamRead<E: BitOrder, WR: WordRead> {
    /// The backend that's used to read the words to fill the buffer
    backend: WR,
    /// The current cache of bits (at most 2 words) that's used to read the 
    /// codes. The bits are read FROM MSB TO LSB
    buffer: u128,
    /// Number of bits valid left in the buffer
    valid_bits: u8,
    /// Just needed to specify the BitOrder
    _marker: core::marker::PhantomData<E>,
}

impl<E: BitOrder, WR: WordRead> BufferedBitStreamRead<E, WR> {
    /// Create a new [`BufferedBitStreamRead`] on a generic backend
    /// 
    /// ### Example
    /// ```
    /// use webgraph::codes::*;
    /// use webgraph::utils::*;
    /// let words = [0x0043b59fccf16077];
    /// let word_reader = MemWordRead::new(&words);
    /// let mut bitstream = <BufferedBitStreamRead<M2L, _>>::new(word_reader);
    /// ```
    #[must_use]
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
            _marker: core::marker::PhantomData::default(),
        }
    }
}

impl<WR: WordRead> BufferedBitStreamRead<M2L, WR> {
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
            .with_context(|| "Error while reflling BufferedBitStreamRead")?.to_be();
        self.valid_bits += 64;
        self.buffer |= (new_word as u128) << (128 - self.valid_bits);
        
        Ok(())
    }
}

impl<WR: WordRead> BufferedBitStreamRead<L2M, WR> {
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
            .with_context(|| "Error while reflling BufferedBitStreamRead")?.to_le();
        self.buffer |= (new_word as u128) << self.valid_bits;
        self.valid_bits += 64;
        
        Ok(())
    }
}

impl<WR: WordRead> BitSeek for BufferedBitStreamRead<L2M, WR> {
    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        self.backend.set_position(bit_index / 64)
            .with_context(|| format!("BufferedBitStreamRead was seeking_bit {}", bit_index))?;
        let bit_offset = bit_index % 64;
        self.buffer = 0;
        self.valid_bits = 0;
        if bit_offset != 0 {
            self.refill()?;
            self.valid_bits -= bit_offset as u8;
            self.buffer >>= bit_offset;
        }
        Ok(())
    }
}

impl<WR: WordRead> BitSeek for BufferedBitStreamRead<M2L, WR> {
    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        self.backend.set_position(bit_index / 64)
            .with_context(|| format!("BufferedBitStreamRead was seeking_bit {}", bit_index))?;
        let bit_offset = bit_index % 64;
        self.buffer = 0;
        self.valid_bits = 0;
        if bit_offset != 0 {
            self.refill()?;
            self.valid_bits -= bit_offset as u8;
            self.buffer <<= bit_offset;
        }
        Ok(())
    }
}

macro_rules! impl_table_call_m2l {
    ($self:expr, $USE_TABLE:expr, $tabs:ident) => {
if $USE_TABLE {
    if let Ok(idx) = $self.peek_bits($tabs::READ_BITS) {
        let (value, len) = $tabs::READ_M2L[idx as usize];
        if len != $tabs::MISSING_VALUE_LEN {
            $self.buffer <<= len;
            $self.valid_bits -= len as u8;
            return Ok(value as u64);
        }
    }
}
    };
}

macro_rules! impl_table_call_l2m {
    ($self:expr, $USE_TABLE:ident, $tabs:ident) => {
if $USE_TABLE {
    if let Ok(idx) = $self.peek_bits($tabs::READ_BITS) {
        let (value, len) = $tabs::READ_L2M[idx as usize];
        if len != $tabs::MISSING_VALUE_LEN {
            $self.buffer >>= len;
            $self.valid_bits -= len as u8;
            return Ok(value as u64);
        }
    }
}
    };
}

impl<WR: WordRead> BitRead for BufferedBitStreamRead<M2L, WR> {
    #[must_use]
    fn read_bits(&mut self, n_bits: u8) -> Result<u64> {
        if n_bits > 64 {
            bail!("The n of bits to read has to be in [1, 64] and {} is not.", n_bits);
        }
        if n_bits == 0 {
            return Ok(0);
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

    #[must_use]
    fn peek_bits(&mut self, n_bits: u8) -> Result<u64> {
        if n_bits > 64 {
            bail!("The n of bits to read has to be in [1, 64] and {} is not.", n_bits);
        }
        if n_bits == 0 {
            return Ok(0);
        }

        if n_bits > self.valid_bits {
            self.refill()?;
        }

        // read the `n_bits` highest bits of the buffer and shift them to
        // be the lowest
        let result = self.buffer >> (128 - n_bits);
        
        Ok(result as u64)
    }
    #[must_use]
    fn read_unary<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        impl_table_call_m2l!(self, USE_TABLE, unary_tables);
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

impl<WR: WordRead> BitRead for BufferedBitStreamRead<L2M, WR> {
    #[must_use]
    fn read_bits(&mut self, n_bits: u8) -> Result<u64> {
        if n_bits > 64 {
            bail!("The n of bits to read has to be in [1, 64] and {} is not.", n_bits);
        }
        if n_bits == 0 {
            return Ok(0);
        }

        if n_bits > self.valid_bits {
            self.refill()?;
        }

        // read the `n_bits` highest bits of the buffer and shift them to
        // be the lowest
        let result = get_lowest_bits(self.buffer as u64, n_bits);

        // remove the read bits from the buffer
        self.valid_bits -= n_bits;
        self.buffer >>= n_bits;
        
        Ok(result as u64)
    }

    #[must_use]
    fn peek_bits(&mut self, n_bits: u8) -> Result<u64> {
        if n_bits > 64 {
            bail!("The n of bits to read has to be in [1, 64] and {} is not.", n_bits);
        }
        if n_bits == 0 {
            return Ok(0);
        }

        if n_bits > self.valid_bits {
            self.refill()?;
        }

        // read the `n_bits` highest bits of the buffer and shift them to
        // be the lowest
        let result = get_lowest_bits(self.buffer as u64, n_bits);
        
        Ok(result as u64)
    }

    fn read_unary<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        impl_table_call_l2m!(self, USE_TABLE, unary_tables);
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

impl<WR: WordRead> GammaRead for BufferedBitStreamRead<M2L, WR> {
    fn read_gamma<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        impl_table_call_m2l!(self, USE_TABLE, gamma_tables);
        self._default_read_gamma()
    }
}
impl<WR: WordRead> GammaRead for BufferedBitStreamRead<L2M, WR> {
    fn read_gamma<const USE_TABLE: bool>(&mut self) -> Result<u64> {
        impl_table_call_l2m!(self, USE_TABLE, gamma_tables);
        self._default_read_gamma()
    }
}