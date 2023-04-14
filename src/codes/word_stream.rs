use anyhow::{Result, bail};

/// A Seekable word stream
pub trait WordStream {
    /// Return the number of [`u64`] words readable from the start of the stream.
    /// Any index in  `[0, self.len())` is valid.
    #[must_use]
    fn len(&self) -> usize;

    #[must_use]
    /// Return if the stream has any words or it's empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the index of the **next** word that  will be
    /// read on the next [`WordRead::read_next_word`] call.
    #[must_use]
    fn get_position(&self) -> usize;
    
    /// Set the position in the stream so that the `word_index`-th word will be
    /// read on the next [`WordRead::read_next_word`] call.
    /// 
    /// # Errors
    /// This function fails if the given `word_index` is out of bound of the 
    /// underneath backend memory.
    fn set_position(&mut self, word_index: usize) -> Result<()>;
}

/// An object we can read words from sequentially
pub trait WordRead {
    /// Read a [`u64`] word from the stream and advance the position by 8 bytes.
    /// 
    /// # Errors
    /// This function fails if we cannot read the next word in the stream,
    /// usually this happens when the stream ended.
    fn read_next_word(&mut self) -> Result<u64>;
}

/// An object that we can write words to sequentially
pub trait WordWrite {
    /// Write a [`u64`] word from the stream and advance the position by 8 bytes.
    /// 
    /// # Errors
    /// This function fails if we cannot write a word to the stream,
    /// usually this happens when the stream ended.
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


/// An Implementation of [`WordStream`], [`WordRead`], [`WordWrite`] for a 
/// mutable slice of memory `&mut [u64]`
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
    /// Create a new [`MemWordWrite`] from a slice of **ZERO INITIALIZED** data
    #[must_use]
    pub fn new(data: &'a mut [u64]) -> Self {
        Self { 
            data, 
            word_index: 0 
        }
    }
}

/// An Implementation of [`WordStream`], [`WordRead`], [`WordWrite`] 
/// for a mutable [`Vec<u64>`]. The core difference between [`MemWordWrite`]
/// and [`MemWordWriteVec`] is that the former allocates new memory
/// if the stream writes out of bound by 1.
/// 
/// # Example
/// ```
/// 
/// use webgraph::codes::*;
/// 
/// let mut words: [u64; 2] = [
///     0x0043b59fcdf16077,
/// ];
/// 
/// let mut word_writer = MemWordWriteVec::new(&mut words);
/// 
/// // the stream is read sequentially
/// assert_eq!(word_writer.len(), 1);
/// assert_eq!(word_writer.get_position(), 0);
/// assert!(word_writer.write_word(0).is_err());
/// assert_eq!(word_writer.len(), 1);
/// assert_eq!(word_writer.get_position(), 1);
/// assert!(word_writer.write_word(1).is_err());
/// assert_eq!(word_writer.len(), 2);
/// assert_eq!(word_writer.get_position(), 2);
/// ```
#[cfg(feature="alloc")]
pub struct MemWordWriteVec<'a> {
    data: &'a mut alloc::vec::Vec<u64>,
    word_index: usize,
}

#[cfg(feature="alloc")]
impl<'a> MemWordWriteVec<'a> {
    /// Create a new [`MemWordWrite`] from a slice of **ZERO INITIALIZED** data
    #[must_use]
    pub fn new(data: &'a mut alloc::vec::Vec<u64>) -> Self {
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
#[cfg(feature="alloc")]
impl_memword!(MemWordWriteVec);

impl<'a> WordWrite for MemWordWrite<'a> {
    #[inline]
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

#[cfg(feature="alloc")]
impl<'a> WordWrite for MemWordWriteVec<'a> {
    #[inline]
    fn write_word(&mut self, word: u64) -> Result<()> {
        if self.word_index >= self.data.len() {
            self.data.resize(self.word_index + 1, 0);
        }
        self.data[self.word_index] = word;
        self.word_index += 1;
        Ok(())
    }
}