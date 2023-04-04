use std::error::Error;

/// A trait that allows reading [`u64`] words from a stream of bytes.
/// This trait is used to abstract the logic and allow homogeneous use of 
/// files, memory-mapped files, memory, sockets, and other sources.
/// 
/// ### Word size
/// While it shares many similarities with [`std::io::Read`], this works on 
/// [`u64`] instead of [`u8`]. 
/// 
/// The stream has to be a multiple of 8.
///
/// If needed, the last word has to be 0-padded, as it is the default `mmap` 
/// behaviour on `linux`:
/// > The system shall always zero-fill any partial page at the end of an object.
/// [Source](https://manned.org/mmap.3p)
pub trait WordReader {
    /// Generic error type raised by the backend. This is usually 
    /// [`std::io::Error`] but we don't enforce it and allow for custom error 
    /// types.
    type Error: Error;

    /// Return the number of [`u64`] words readable from the start of the stream.
    /// Any index in  `[0, self.len())` is valid.
    fn len(&self) -> usize;

    /// Return the index of the **next** word that  will be
    /// read on the next [`read_next_word`] call.
    fn get_position(&self) -> usize;
    
    /// Set the position in the stream so that the `word_index`-th word will be
    /// read on the next [`read_next_word`] call.
    fn set_position(&mut self, word_index: usize) -> Result<(), Self::Error>;

    /// Read a [`u64`] word from the stream and advance the position by 8 bytes.
    fn read_next_word(&mut self) -> Result<u64, Self::Error>;
}
