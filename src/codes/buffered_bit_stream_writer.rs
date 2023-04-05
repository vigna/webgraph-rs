use super::{
    WordWrite, 
    BitStream, BitWrite,
    BitOrder, M2L, L2M,
};
use anyhow::{Result, bail};

/// A BitStream built uppon a generic [`WordRead`] that caches the read words 
/// in a buffer
pub struct BufferedBitStreamWriter<E: BitOrder, WR: WordWrite> {
    ///
    backend: WR,
    ///
    buffer: u128,
    ///
    bits_in_buffer: u8,
    ///
    _marker: core::marker::PhantomData<E>,
}

impl<E: BitOrder, WR: WordWrite> BufferedBitStreamWriter<E, WR> {
    ///
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
            bits_in_buffer: 0,
            _marker: core::marker::PhantomData::default(),
        }
    }

    #[inline(always)]
    #[must_use]
    fn space_left_in_buffer(&self) -> u8 {
        128 - self.bits_in_buffer
    }
}

impl<E: BitOrder, WR: WordWrite> core::ops::Drop for BufferedBitStreamWriter<E, WR> {
    fn drop(&mut self) {
        // During a drop we can't save anything if it goes bad :/
        #[allow(clippy::unwrap_used)]
        self.flush().unwrap();
    }
}

impl<WR: WordWrite> BufferedBitStreamWriter<L2M, WR> {
    /// Flush only if there are at least 64 bits
    fn partial_flush(&mut self) -> Result<()> {
        if self.bits_in_buffer < 64 {
            return Ok(());
        }

        self.backend.write_word(self.buffer as u64)?;
        self.buffer >>= 64;
        self.bits_in_buffer -= 64;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.partial_flush()?;
        self.backend.write_word(self.buffer as u64)?;
        self.buffer = 0;
        self.bits_in_buffer = 0;
        Ok(())
    }
}


impl<WR: WordWrite> BitStream for BufferedBitStreamWriter<L2M, WR> {
    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        todo!();
    }
}

impl<WR: WordWrite> BitWrite for BufferedBitStreamWriter<L2M, WR> {
    fn write_bits(&mut self, value: u64, n_bits: u8) -> Result<()> {
        if n_bits == 0 || n_bits > 64 {
            bail!("The n of bits to read has to be in [1, 64] and {} is not.", n_bits);
        }

        if n_bits > self.space_left_in_buffer() {
            self.partial_flush()?;
        }

        self.buffer |= (value as u128) << self.bits_in_buffer;
        self.bits_in_buffer += n_bits;

        Ok(())
    }

    fn write_unary<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()> {
        debug_assert_ne!(value, u64::MAX);
        let mut code_length = value + 1;

        loop {
            let space_left = self.space_left_in_buffer() as u64;
            if code_length <= space_left {
                break;
            }
            //TODO!: CHECK ORDER
            self.backend.write_word(self.buffer as u64)?;
            self.backend.write_word((self.buffer >> 64) as u64)?;
            self.bits_in_buffer = 0;
            code_length -= space_left;
        }

        self.bits_in_buffer += code_length as u8;
        self.buffer |= 1_u128 << (self.bits_in_buffer as u64 - 1);

        Ok(())
    }
}

impl<WR: WordWrite> BitStream for BufferedBitStreamWriter<M2L, WR> {
    fn seek_bit(&mut self, bit_index: usize) -> Result<()> {
        todo!();
    }
}

impl<WR: WordWrite> BitWrite for BufferedBitStreamWriter<M2L, WR> {
    fn write_bits(&mut self, value: u64, n_bits: u8) -> Result<()> {
        if n_bits == 0 || n_bits > 64 {
            bail!("The n of bits to read has to be in [1, 64] and {} is not.", n_bits);
        }

        if n_bits > self.space_left_in_buffer() {
            self.partial_flush()?;
        }

        self.buffer >>= n_bits;
        self.buffer |= (value as u128) << (128 - n_bits);
        self.bits_in_buffer += n_bits;

        Ok(())
    }

    fn write_unary<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()> {
        debug_assert_ne!(value, u64::MAX);
        let mut code_length = value + 1;

        loop {
            let space_left = self.space_left_in_buffer() as u64;
            if code_length <= space_left {
                break;
            }
            // TODO!: check order
            self.backend.write_word((self.buffer >> 64) as u64)?;
            self.backend.write_word(self.buffer as u64)?;
            self.bits_in_buffer = 0;
            code_length -= space_left;
        }

        self.bits_in_buffer += code_length as u8;
        self.buffer |= 1_u128 << (128 - self.bits_in_buffer + 1);

        Ok(())
    }
}