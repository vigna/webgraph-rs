use anyhow::Result;

/// Trait to convert a Stream to a Seekable Stream
pub trait BitSeek {
    /// Move the stream cursor so that if we call `read_bits(1)` we will read 
    /// the `bit_index`-th bit in the stream
    fn seek_bit(&mut self, bit_index: usize) -> Result<()>;
}

/// Objects that can read a fixed number of bits and unary codes from a stream 
/// of bits. The endianess of the returned bytes HAS TO BE THE NATIVE ONE.
pub trait BitRead {
    /// Read `n_bits` bits from the stream and return them in the lowest bits
    #[must_use]
    fn read_bits(&mut self, n_bits: u8) -> Result<u64>;

    /// Read an unary code
    #[must_use]
    fn read_unary<const USE_TABLE: bool>(&mut self) -> Result<u64> {
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

/// Objects that can read a fixed number of bits and unary codes from a stream 
/// of bits. The endianess of the returned bytes HAS TO BE THE NATIVE ONE.
/// [`BitWrite`] does not depends on [`BitStream`] because on most implementation
/// we will have to write on bytes or words. Thus to be able to write the bits 
/// we would have to be able to read them back, thus impling implementing
/// [`BitRead`]. Nothing stops someone to implement both [`BitStream`] and
/// [`BitWrite`] for the same structure
pub trait BitWrite {
    /// Write the lowest `n_bits` of value to the steam
    fn write_bits(&mut self, value: u64, n_bits: u8) -> Result<()>;

    /// Write `value` as an unary code to the stream
    fn write_unary<const USE_TABLE: bool>(&mut self, value: u64) -> Result<()> {
        for _ in 0..value {
            self.write_bits(0, 1)?;
        }
        self.write_bits(1, 1)?;
        Ok(())
    }
}

/// [`BitWrite`] objects that use buffering also need to control the flushing
/// of said buffer. Since this is a subtrait of [`BitWrite`], objects 
/// implementing this trait **HAVE TO** call flush on drop.
pub trait BitWriteBuffered: BitWrite {
    /// Try to flush part of the buffer, this does not guarantee that **all**
    /// data will be flushed.
    fn partial_flush(&mut self) -> Result<()>;
}