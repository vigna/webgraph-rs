use super::{
    WordWrite, 
    BitWrite, BitWriteBuffered,
    BitOrder, M2L, L2M,
};
use anyhow::{Result, bail};

/// A BitStream built uppon a generic [`WordRead`] that caches the read words 
/// in a buffer
pub struct BufferedBitStreamWrite<E: BitOrder + BBSWDrop<WR>, WR: WordWrite> {
    ///
    backend: WR,
    ///
    buffer: u128,
    ///
    bits_in_buffer: u8,
    ///
    _marker: core::marker::PhantomData<E>,
}

impl<E: BitOrder + BBSWDrop<WR>, WR: WordWrite> BufferedBitStreamWrite<E, WR> {
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

impl<E: BitOrder + BBSWDrop<WR>, WR: WordWrite> core::ops::Drop for BufferedBitStreamWrite<E, WR> {
    fn drop(&mut self) {
        // During a drop we can't save anything if it goes bad :/
        #[allow(clippy::unwrap_used)]
        E::drop(self).unwrap();
    }
}

/// Ignore. Inner trait needed for dispatching of drop logic based on endianess 
/// of a [`BufferedBitStreamWrite`]. This is public to avoid the leak of 
/// private traits in public defs, an user should never need to implement this.
/// TODO!: should we make a wrapper trait to make this trait private?
pub trait BBSWDrop<WR: WordWrite>: Sized + BitOrder {
    /// handle the drop
    fn drop(data: &mut  BufferedBitStreamWrite<Self, WR>) -> Result<()>;
}

impl<WR: WordWrite> BBSWDrop<WR> for M2L {
    #[inline]
    fn drop(data: &mut  BufferedBitStreamWrite<Self, WR>) -> Result<()> {
        data.partial_flush()?;
        if data.bits_in_buffer > 0 {
            // TODO!: should we clean the lower bits? we are leaking data
            data.backend.write_word((data.buffer >> 64) as u64)?;
        }
        Ok(())
    }
}

impl<WR: WordWrite> BBSWDrop<WR> for L2M {
    #[inline]
    fn drop(data: &mut  BufferedBitStreamWrite<Self, WR>) -> Result<()> {
        data.partial_flush()?;
        if data.bits_in_buffer > 0 {
            // TODO!: should we clean the lower bits? we are leaking data
            data.backend.write_word(data.buffer as u64)?;
        }
        Ok(())
    }
}

impl<WR: WordWrite> BitWriteBuffered for BufferedBitStreamWrite<M2L, WR> {
    #[inline]
    fn partial_flush(&mut self) -> Result<()> {
        if self.bits_in_buffer < 64 {
            return Ok(());
        }
        self.backend.write_word((self.buffer >> (128 - self.bits_in_buffer)) as u64)?;
        self.bits_in_buffer -= 64;
        Ok(())
    }
}

impl<WR: WordWrite> BitWriteBuffered for BufferedBitStreamWrite<L2M, WR> {
    #[inline]
    fn partial_flush(&mut self) -> Result<()> {
        if self.bits_in_buffer < 64 {
            return Ok(());
        }
        self.bits_in_buffer -= 64;
        self.backend.write_word((self.buffer >> self.bits_in_buffer) as u64)?;
        Ok(())
    }
}

impl<WR: WordWrite> BitWrite for BufferedBitStreamWrite<M2L, WR> {
    #[inline]
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

    #[inline]
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

impl<WR: WordWrite> BitWrite for BufferedBitStreamWrite<L2M, WR> {
    #[inline]
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

    #[inline]
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