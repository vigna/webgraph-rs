use anyhow::Result;
use super::BitOrder;
use crate::*;

/// Trait to convert a Stream to a Seekable Stream
pub trait BitSeek {
    /// Move the stream cursor so that if we call `read_bits(1)` we will read 
    /// the `bit_index`-th bit in the stream
    /// 
    /// # Errors
    /// This function return an error if the bit_index is not within the available
    /// span of bits.
    fn seek_bit(&mut self, bit_index: usize) -> Result<()>;

    #[must_use]
    /// Return the current bit index
    fn get_position(&self) -> usize;
}

/// Objects that can read a fixed number of bits and unary codes from a stream 
/// of bits. The endianess of the returned bytes HAS TO BE THE NATIVE ONE.
pub trait BitRead<BO: BitOrder> {
    /// The type we can read form the stream without advancing.
    /// On buffered readers this is usually half the buffer size. 
    type PeekType: UpcastableInto<u64>;
    /// Read `n_bits` bits from the stream and return them in the lowest bits
    /// 
    /// # Errors
    /// This function return an error if we cannot read `n_bits`, this usually
    /// happens if we finished the stream.
    fn read_bits(&mut self, n_bits: usize) -> Result<u64>;

    /// Like read_bits but it doesn't seek forward 
    /// 
    /// # Errors
    /// This function return an error if we cannot read `n_bits`, this usually
    /// happens if we finished the stream.
    fn peek_bits(&mut self, n_bits: usize) -> Result<Self::PeekType>;

    /// Skip n_bits from the stream
    /// 
    /// # Errors
    /// Thi function errors if skipping n_bits the underlying streams ends.
    fn skip_bits(&mut self, n_bits: usize) -> Result<()>;

    /// Read an unary code
    /// 
    /// # Errors
    /// This function return an error if we cannot read the unary code, this 
    /// usually happens if we finished the stream.
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
/// [`BitWrite`] does not depends on [`BitRead`] because on most implementation
/// we will have to write on bytes or words. Thus to be able to write the bits 
/// we would have to be able to read them back, thus impling implementing
/// [`BitRead`]. Nothing stops someone to implement both [`BitRead`] and
/// [`BitWrite`] for the same structure
pub trait BitWrite<BO: BitOrder> {
    /// Write the lowest `n_bits` of value to the steam
    /// 
    /// # Errors
    /// This function return an error if we cannot write `n_bits`, this usually
    /// happens if we finished the stream.
    fn write_bits(&mut self, value: u64, n_bits: usize) -> Result<()>;

    /// Write `value` as an unary code to the stream
    /// 
    /// # Errors
    /// This function return an error if we cannot write the unary code, this 
    /// usually happens if we finished the stream.
    fn write_unary<const USE_TABLE: bool>(&mut self, mut value: u64) -> Result<()> {
        while value > 0 {
            self.write_bits(0, 1)?;
            value -= 1;
        }
        self.write_bits(1, 1)?;
        Ok(())
    }
}

/// [`BitWrite`] objects that use buffering also need to control the flushing
/// of said buffer. Since this is a subtrait of [`BitWrite`], objects 
/// implementing this trait **HAVE TO** call flush on drop.
pub trait BitWriteBuffered<BO: BitOrder>: BitWrite<BO> {
    /// Try to flush part of the buffer, this does not guarantee that **all**
    /// data will be flushed.
    /// 
    /// # Errors
    /// This function might fail if we have bits in the buffer, but we finished
    /// the writable stream. TODO!: figure out how to handle this situation.
    fn partial_flush(&mut self) -> Result<()>;
}