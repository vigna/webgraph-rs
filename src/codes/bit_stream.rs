use anyhow::Result;

/// A BitStream, The endianess and bit order of all values read and written
/// **has to** match the machine's native ones
pub trait BitStream {
    /// Move the stream cursor so that if we call `read_bits(1)` we will read 
    /// the `bit_index`-th bit in the stream
    fn seek_bit(&mut self, bit_index: usize) -> Result<()>;
}

/// Objects that can read a fixed number of bits and unary codes from a stream 
/// of bits
pub trait BitRead: BitStream {
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
/// of bits
pub trait BitWrite: BitStream {
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