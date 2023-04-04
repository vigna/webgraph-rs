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

/// An Implementation of [`WordReader`] for a slice of memory
/// 
/// # Example
/// ```
/// use webgraph::codes::*;
/// // data converted to little-endian u64 words
/// let words: [u64; 2] = [
///     0x0043b59fcdf16077,
///     0x702863e6f9739b86,
/// ];
/// 
/// let mut word_reader = MemWordReader::new(&words);
/// 
/// // the stream is read sequentially
/// assert_eq!(word_reader.len(), 2);
/// assert_eq!(word_reader.get_position(), 0);
/// assert_eq!(word_reader.read_next_word().unwrap(), 0x0043b59fcdf16077);
/// assert_eq!(word_reader.get_position(), 1);
/// assert_eq!(word_reader.read_next_word().unwrap(), 0x702863e6f9739b86);
/// assert_eq!(word_reader.get_position(), 2);
/// assert!(word_reader.read_next_word().is_err());
/// 
/// // you can change position
/// assert!(word_reader.set_position(1).is_ok());
/// assert_eq!(word_reader.get_position(), 1);
/// assert_eq!(word_reader.read_next_word().unwrap(), 0x702863e6f9739b86);
/// 
/// // errored set position doesn't change the current position
/// assert_eq!(word_reader.get_position(), 2);
/// assert!(word_reader.set_position(100).is_err());
/// assert_eq!(word_reader.get_position(), 2);
/// ```
pub struct MemWordReader<'a> {
    data: &'a [u64],
    word_index: usize,
}

impl<'a> MemWordReader<'a> {
    /// Create a new [`MemWordReader`] from a slice of data
    pub fn new(data: &'a [u64]) -> Self {
        Self { 
            data, 
            word_index: 0 
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
/// Enumeration of errors raisable by [`MemWordReader`]
pub enum MemWordReaderError {
    /// An 
    OutOfBound{
        /// The index that was tried to be accessed
        index: usize,
        /// The lenght of the memory slice
        len: usize,
    }
}

impl std::fmt::Display for MemWordReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO!: add meaningful error strings
        // for now fallback to debug
        match self {
            Self::OutOfBound { index, len } => {
                f.write_fmt(format_args!(
                    "Index {} is out of bound on a MemWordReader of length {}",
                    index, len
                ))
            }
        }
    }
}

// Marker trait, no need for concrete implementation
impl std::error::Error for MemWordReaderError {}


impl<'a> WordReader for MemWordReader<'a> {
    type Error = MemWordReaderError;

    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn get_position(&self) -> usize {
        self.word_index
    }

    #[inline]
    fn set_position(&mut self, word_index: usize) -> Result<(), Self::Error> {
        if word_index >= self.len() {
            return Err(MemWordReaderError::OutOfBound { 
                index: word_index, 
                len: self.len() 
            });
        }
        self.word_index = word_index;
        Ok(())
    }

    #[inline]
    fn read_next_word(&mut self) -> Result<u64, Self::Error> {
        match self.data.get(self.word_index) {
            Some(word) => {
                self.word_index += 1;
                Ok(*word)
            }
            None => {
                Err(MemWordReaderError::OutOfBound { 
                    index: self.word_index, 
                    len: self.len() 
                })
            }
        }
    }
}