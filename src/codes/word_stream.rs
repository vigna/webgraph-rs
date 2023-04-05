use anyhow::{Result, bail};

/// Trait of the common aspects of [`WordRead`] and [`WordWrite`].
/// 
/// While usually a Writer implies the possibility to read, this is not always
/// true, so we treat them as invariant traits.
/// 
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
pub trait WordStream {
    /// Return the number of [`u64`] words readable from the start of the stream.
    /// Any index in  `[0, self.len())` is valid.
    #[must_use]
    fn len(&self) -> usize;

    /// Return the index of the **next** word that  will be
    /// read on the next [`WordRead::read_next_word`] call.
    #[must_use]
    fn get_position(&self) -> usize;
    
    /// Set the position in the stream so that the `word_index`-th word will be
    /// read on the next [`WordRead::read_next_word`] call.
    fn set_position(&mut self, word_index: usize) -> Result<()>;
}

/// A [`WordStream`] that can be read from!
pub trait WordRead: WordStream {
    /// Read a [`u64`] word from the stream and advance the position by 8 bytes.
    #[must_use]
    fn read_next_word(&mut self) -> Result<u64>;
}

/// A [`WordStream`] that can be written to!
pub trait WordWrite: WordStream {
    /// Write a [`u64`] word from the stream and advance the position by 8 bytes.
    #[must_use]
    fn write_word(&mut self, word: u64) -> Result<()>;
}

/// An Implementation of [`WordRead`] for a slice of memory `&[u64]`
/// 
/// # Example
/// ```
/// use webgraph::codes::*;
/// 
/// let words: [u64; 2] = [
///     0x0043b59fcdf16077,
///     0x702863e6f9739b86,
/// ];
/// 
/// let mut word_reader = MemWordRead::new(&words);
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
pub struct MemWordRead<'a> {
    data: &'a [u64],
    word_index: usize,
}

impl<'a> MemWordRead<'a> {
    /// Create a new [`MemWordRead`] from a slice of data
    #[must_use]
    pub fn new(data: &'a [u64]) -> Self {
        Self { 
            data, 
            word_index: 0 
        }
    }
}


/// An Implementation of [`WordStream`], [`WordRead`], [`WordWrite`] for a mutable slice of memory `&mut [u64]`
/// 
/// # Example
/// ```
/// use webgraph::codes::*;
/// 
/// let mut words: [u64; 2] = [
///     0x0043b59fcdf16077,
///     0x702863e6f9739b86,
/// ];
/// 
/// let mut word_writer = MemWordWrite::new(&mut words);
/// 
/// // the stream is read sequentially
/// assert_eq!(word_writer.len(), 2);
/// assert_eq!(word_writer.get_position(), 0);
/// assert_eq!(word_writer.read_next_word().unwrap(), 0x0043b59fcdf16077);
/// assert_eq!(word_writer.get_position(), 1);
/// assert_eq!(word_writer.read_next_word().unwrap(), 0x702863e6f9739b86);
/// assert_eq!(word_writer.get_position(), 2);
/// assert!(word_writer.read_next_word().is_err());
/// 
/// // you can change position
/// assert!(word_writer.set_position(1).is_ok());
/// assert_eq!(word_writer.get_position(), 1);
/// assert_eq!(word_writer.read_next_word().unwrap(), 0x702863e6f9739b86);
/// 
/// // errored set position doesn't change the current position
/// assert_eq!(word_writer.get_position(), 2);
/// assert!(word_writer.set_position(100).is_err());
/// assert_eq!(word_writer.get_position(), 2);
/// 
/// // we can write and read back!
/// assert!(word_writer.set_position(0).is_ok());
/// assert!(word_writer.write_word(0x0b801b2bf696e8d2).is_ok());
/// assert_eq!(word_writer.get_position(), 1);
/// assert!(word_writer.set_position(0).is_ok());
/// assert_eq!(word_writer.read_next_word().unwrap(), 0x0b801b2bf696e8d2);
/// assert_eq!(word_writer.get_position(), 1);
/// ```
pub struct MemWordWrite<'a> {
    data: &'a mut [u64],
    word_index: usize,
}

impl<'a> MemWordWrite<'a> {
    /// Create a new [`MemWordWrite`] from a slice of data
    #[must_use]
    pub fn new(data: &'a mut [u64]) -> Self {
        Self { 
            data, 
            word_index: 0 
        }
    }
}

// Use a macro for "duplicated" logic so we have to fix it just once :)
macro_rules! impl_memword {
    ($ty:ident) => {

// A WordWrite can easily also read words, so we can implement this trait for 
// both
impl<'a> WordRead for $ty<'a> {
    #[inline]
    #[must_use]
    fn read_next_word(&mut self) -> Result<u64> {
        match self.data.get(self.word_index) {
            Some(word) => {
                self.word_index += 1;
                Ok(*word)
            }
            None => {
                bail!(
                    "Cannot read next word as the underlying memory ended",
                );
            }
        }
    }
}

impl<'a> WordStream for $ty<'a> {
    #[inline]
    #[must_use]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    #[must_use]
    fn get_position(&self) -> usize {
        self.word_index
    }

    #[inline]
    fn set_position(&mut self, word_index: usize) -> Result<()> {
        if word_index >= self.len() {
            bail!(
                "Index {} is out of bound on a MemWordRead of length {}",
                word_index, self.len()
            );
        }
        self.word_index = word_index;
        Ok(())
    }
}   
    };
}

impl_memword!(MemWordRead);
impl_memword!(MemWordWrite);

impl<'a> WordWrite for MemWordWrite<'a> {
    fn write_word(&mut self, word: u64) -> Result<()> {
        match self.data.get_mut(self.word_index) {
            Some(word_ref) => {
                self.word_index += 1;
                *word_ref = word;
                Ok(())
            }
            None => {
                bail!(
                    "Cannot write next word as the underlying memory ended",
                );
            }
        }
    }
}