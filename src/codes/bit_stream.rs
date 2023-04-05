use anyhow::Result;

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