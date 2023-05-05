use crate::traits::*;
use anyhow::{bail, Result};

/// An Implementation of [`WordRead`] for a slice of memory `&[u64]`
///
/// # Example
/// ```
/// use webgraph::prelude::*;
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
#[derive(Debug, Clone)]
pub struct MemWordRead<'a, W: Word> {
    data: &'a [W],
    word_index: usize,
}

impl<'a, W: Word> MemWordRead<'a, W> {
    /// Create a new [`MemWordRead`] from a slice of data
    #[must_use]
    pub fn new(data: &'a [W]) -> Self {
        Self {
            data,
            word_index: 0,
        }
    }
}

///
#[derive(Debug, Clone)]
pub struct MemWordReadInfinite<'a, W: Word> {
    data: &'a [W],
    word_index: usize,
}

impl<'a, W: Word> MemWordReadInfinite<'a, W> {
    /// Create a new [`MemWordReadInfinite`] from a slice of data
    #[must_use]
    pub fn new(data: &'a [W]) -> Self {
        Self {
            data,
            word_index: 0,
        }
    }
}
impl<'a, W: Word> WordRead for MemWordReadInfinite<'a, W> {
    type Word = W;

    #[inline(always)]
    fn read_next_word(&mut self) -> Result<W> {
        let res = self.data.get(self.word_index).copied().unwrap_or(W::ZERO);
        self.word_index += 1;
        Ok(res)
    }
}

impl<'a, W: Word> WordStream for MemWordReadInfinite<'a, W> {
    #[inline(always)]
    #[must_use]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    #[must_use]
    fn get_position(&self) -> usize {
        self.word_index
    }

    #[inline(always)]
    fn set_position(&mut self, word_index: usize) -> Result<()> {
        self.word_index = word_index;
        Ok(())
    }
}

/// An Implementation of [`WordStream`], [`WordRead`], [`WordWrite`] for a
/// mutable slice of memory `&mut [u64]`
///
/// # Example
/// ```
/// use webgraph::prelude::*;
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
#[derive(Debug)]
pub struct MemWordWrite<'a, W: Word> {
    data: &'a mut [W],
    word_index: usize,
}

impl<'a, W: Word> MemWordWrite<'a, W> {
    /// Create a new [`MemWordWrite`] from a slice of **ZERO INITIALIZED** data
    #[must_use]
    pub fn new(data: &'a mut [W]) -> Self {
        Self {
            data,
            word_index: 0,
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
/// use webgraph::prelude::*;
///
/// let mut words: Vec<u64> = vec![
///     0x0043b59fcdf16077,
/// ];
///
/// let mut word_writer = MemWordWriteVec::new(&mut words);
///
/// // the stream is read sequentially
/// assert_eq!(word_writer.len(), 1);
/// assert_eq!(word_writer.get_position(), 0);
/// assert!(word_writer.write_word(0).is_ok());
/// assert_eq!(word_writer.len(), 1);
/// assert_eq!(word_writer.get_position(), 1);
/// assert!(word_writer.write_word(1).is_ok());
/// assert_eq!(word_writer.len(), 2);
/// assert_eq!(word_writer.get_position(), 2);
/// ```
#[derive(Debug)]
#[cfg(feature = "alloc")]
pub struct MemWordWriteVec<'a, W: Word> {
    data: &'a mut alloc::vec::Vec<W>,
    word_index: usize,
}

#[cfg(feature = "alloc")]
impl<'a, W: Word> MemWordWriteVec<'a, W> {
    /// Create a new [`MemWordWrite`] from a slice of **ZERO INITIALIZED** data
    #[must_use]
    pub fn new(data: &'a mut alloc::vec::Vec<W>) -> Self {
        Self {
            data,
            word_index: 0,
        }
    }
}

// Use a macro for "duplicated" logic so we have to fix it just once :)
macro_rules! impl_memword {
    ($ty:ident) => {
        // A WordWrite can easily also read words, so we can implement this trait for
        // both
        impl<'a, W: Word> WordRead for $ty<'a, W> {
            type Word = W;

            #[inline]
            fn read_next_word(&mut self) -> Result<W> {
                match self.data.get(self.word_index) {
                    Some(word) => {
                        self.word_index += 1;
                        Ok(*word)
                    }
                    None => {
                        bail!("Cannot read next word as the underlying memory ended",);
                    }
                }
            }
        }

        impl<'a, W: Word> WordStream for $ty<'a, W> {
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
                        word_index,
                        self.len()
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
#[cfg(feature = "alloc")]
impl_memword!(MemWordWriteVec);

impl<'a, W: Word> WordWrite for MemWordWrite<'a, W> {
    type Word = W;

    #[inline]
    fn write_word(&mut self, word: W) -> Result<()> {
        match self.data.get_mut(self.word_index) {
            Some(word_ref) => {
                self.word_index += 1;
                *word_ref = word;
                Ok(())
            }
            None => {
                bail!("Cannot write next word as the underlying memory ended",);
            }
        }
    }
}

#[cfg(feature = "alloc")]
impl<'a, W: Word> WordWrite for MemWordWriteVec<'a, W> {
    type Word = W;

    #[inline]
    fn write_word(&mut self, word: W) -> Result<()> {
        if self.word_index >= self.data.len() {
            self.data.resize(self.word_index + 1, W::ZERO);
        }
        self.data[self.word_index] = word;
        self.word_index += 1;
        Ok(())
    }
}
