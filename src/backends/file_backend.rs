use crate::traits::*;
use anyhow::Result;

/// A word backend implementation of [`WordStream`], [`WordRead`], [`WordWrite`]
/// for a generic file, this could transparently handle [`std::fs::File`],
/// [`std::io::BufReader`], [`std::io::BufWriter`], and sockets.
/// 
/// # Implementation details and decisions
/// While we could write blanket implementations for any generic type that 
/// implements [`std::io::Read`], [`std::io::Write`], or [`std::io::Seek`],
/// doing so would force us to set an unique word `W`, while this wrapper allows
/// to choose the read and wite words at the cost of a more complicated type. 
/// The alternative is to modify the WordSteam to have a generic type instead of
/// an associated one, but that would require the memory slices we read to 
/// always be aligned to 16 bytes (u128). For memory mapped regions it's ok,
/// but we can't enforce it by types.
/// 
/// TODO!: maybe FileBackend is not the best name, as it's more generic than 
/// that
#[repr(transparent)]
pub struct FileBackend<W: Word, B> {
    file: B,
    _marker: core::marker::PhantomData<W>,
}

impl<W: Word, B> FileBackend<W, B> {
    /// Create a new FileBackend
    pub fn new(file: B) -> Self {
        Self {
            file,
            _marker: core::marker::PhantomData::default(),
        }
    }
}

/// forward [`Clone`] if the backend supports it
impl<W: Word, B: Clone> Clone for FileBackend<W, B> {
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            _marker: core::marker::PhantomData::default(),
        }
    }
}

/// forward [`core::fmt::Debug`] if the backend supports it
impl<W: Word, B: core::fmt::Debug> core::fmt::Debug for FileBackend<W, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.file.fmt(f)
    }
}

/// Convert [`std::io::Seek`] to [`WordStream`]
impl<W: Word, B: std::io::Seek> WordStream for FileBackend<W, B> {
    #[inline]
    #[must_use]
    fn len(&self) -> usize {
        self.file.stream_len()
    }

    #[inline]
    #[must_use]
    fn get_position(&self) -> usize {
        self.file.stream_position()
    }

    #[inline]
    fn set_position(&mut self, word_index: usize) -> Result<()> {
        self.file.seek(std::io::SeekFrom::Start(word_index))
    }
}

/// Convert [`std::io::Read`] to [`WordRead`]
impl<W: Word, B: std::io::Read> WordRead for FileBackend<W, B> {
    type Word = W;

    #[inline]
    fn read_next_word(&mut self) -> Result<W> {
        let mut res = [0; 8];
        self.file.read(&mut res)?;
        Ok(W::from_ne_bytes(res))
    }
}

/// Convert [`std::io::Write`] to [`WordWrite`]
impl<W: Word, B: std::io::Write> WordWrite for FileBackend<W, B> {
    type Word = W;

    #[inline]
    fn write_word(&mut self, word: W) -> Result<()> {
        self.file.write(&word.to_ne_bytes())?;
        Ok(())
    }
}
