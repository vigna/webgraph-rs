/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Facilities to sort externally pairs of nodes with an associated payload.

use super::{ArcMmapHelper, MmapHelper};
use crate::traits::{BitDeserializer, BitSerializer};
use anyhow::{anyhow, Context};
use dary_heap::PeekMut;
use dsi_bitstream::prelude::*;
use log::debug;
use mmap_rs::MmapFlags;
use rdst::*;
use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
    sync::Arc,
};

pub type BitWriter = BufBitWriter<NE, WordAdapter<usize, BufWriter<File>>>;
pub type BitReader = BufBitReader<NE, MemWordReader<u32, ArcMmapHelper<u32>>>;

/// An arc expressed as a pair of nodes and the associated payload.
///
/// Equality and order are defined only (lexicographically) on the pair of
/// nodes.
#[derive(Clone, Debug, Copy)]
pub struct Triple<L: Copy> {
    pair: [usize; 2],
    label: L,
}

impl<T: Copy> RadixKey for Triple<T> {
    const LEVELS: usize = 16;

    fn get_level(&self, level: usize) -> u8 {
        (self.pair[1 - level / 8] >> ((level % 8) * 8)) as u8
    }
}

impl<T: Copy> PartialEq for Triple<T> {
    fn eq(&self, other: &Self) -> bool {
        self.pair == other.pair
    }
}

impl<T: Copy> Eq for Triple<T> {}

impl<T: Copy> PartialOrd for Triple<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.pair.cmp(&other.pair))
    }
}

impl<T: Copy> Ord for Triple<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pair.cmp(&other.pair)
    }
}

/// A struct that provides external sorting for pairs of nodes with an
/// associated payload.
///
/// An instance of this structure ingests pairs of nodes with an associated
/// payload, sort them in chunks of `batch_size` triples, and dumps them to
/// disk. Then, a call to [`iter`](SortPairs::iter) returns an iterator that
/// merges the batches on disk on the fly, returning the triples sorted by
/// lexicographical order of the pairs of nodes.
///
/// The structure accept as type parameter a [`BitSerializer`] and a
/// [`BitDeserializer`] that are used to serialize and deserialize the payload.
/// In case they are both `()`, the structure behaves as if there is no payload.
/// In the first case, you add triples with
/// [`push_labeled`](SortPairs::push_labeled), in the second case you add
/// triples with [`push`](SortPairs::push).
///
/// The bit deserializer must be [`Clone`] because we need one for each
/// [`BatchIterator`], and there are possible scenarios in which the
/// deserializer might not be stateless.
///
/// You create a new instance using [`SortPairs::new_labeled`], and
/// add labelled pairs using [`SortPairs::push_labeled`]. Then you can
/// iterate over the pairs using [`SortPairs::iter`].
///
/// `SortPars<(), ()>` has commodity `new` and `push` methods without
/// payload. Note however that the [resulting iterator](SortPairs::iter)
/// is labelled, and returns pairs labeled with `()`.

pub struct SortPairs<
    S: BitSerializer<NE, BitWriter> = (),
    D: BitDeserializer<NE, BitReader> + Clone = (),
> where
    S::SerType: Send + Sync + Copy,
{
    /// A stateful serializer we will pass to batch iterators to serialize
    /// the labels to a bitstream.
    serializer: S,
    /// A stateful deserializer we will pass to batch iterators to deserialize
    /// the labels from a bitstream.
    deserializer: D,
    /// The batch size.
    batch_size: usize,
    /// The length of the last batch, which might be smaller than [`SortPairs::batch_size`].
    last_batch_len: usize,
    /// The batch of triples we are currently building.
    batch: Vec<Triple<S::SerType>>,
    /// Where we are going to store the batches.
    dir: PathBuf,
    /// Keeps track of how many batches we created.
    num_batches: usize,
}

impl SortPairs<(), ()> {
    /// Create a new `SortPairs` with a given batch size
    ///
    /// The `dir` must be empty, and in particular it **must not** be shared
    /// with other `SortPairs` instances.
    pub fn new<P: AsRef<Path>>(batch_size: usize, dir: P) -> anyhow::Result<Self> {
        Self::new_labeled(batch_size, dir, (), ())
    }
    /// Add a triple to the graph.
    pub fn push(&mut self, x: usize, y: usize) -> anyhow::Result<()> {
        self.push_labeled(x, y, ())
    }
}

impl<S: BitSerializer<NE, BitWriter>, D: BitDeserializer<NE, BitReader> + Clone> SortPairs<S, D>
where
    S::SerType: Send + Sync + Copy,
{
    /// Create a new `SortPairs` with a given batch size
    ///
    /// The `dir` must be empty, and in particular it **must not** be shared
    /// with other `SortPairs` instances.
    pub fn new_labeled<P: AsRef<Path>>(
        batch_size: usize,
        dir: P,
        serializer: S,
        deserializer: D,
    ) -> anyhow::Result<Self> {
        let dir = dir.as_ref();
        let mut dir_entries =
            std::fs::read_dir(dir).with_context(|| format!("Could not list {}", dir.display()))?;
        if dir_entries.next().is_some() {
            Err(anyhow!("{} is not empty", dir.display()))
        } else {
            Ok(SortPairs {
                batch_size,
                last_batch_len: 0,
                batch: Vec::with_capacity(batch_size),
                dir: dir.to_owned(),
                num_batches: 0,
                serializer,
                deserializer,
            })
        }
    }

    /// Add a triple to the graph.
    pub fn push_labeled(&mut self, x: usize, y: usize, t: S::SerType) -> anyhow::Result<()> {
        self.batch.push(Triple {
            pair: [x, y],
            label: t,
        });
        if self.batch.len() >= self.batch_size {
            self.dump()?;
        }
        Ok(())
    }

    /// Dump the current batch to disk
    fn dump(&mut self) -> anyhow::Result<()> {
        // This method must be idempotent as it is called by `iter`
        if self.batch.is_empty() {
            return Ok(());
        }

        // create a batch file where to dump
        let batch_name = self.dir.join(format!("{:06x}", self.num_batches));
        BatchIterator::new_from_vec_labeled(
            batch_name,
            &mut self.batch,
            &self.serializer,
            self.deserializer.clone(),
        )?;
        self.last_batch_len = self.batch.len();
        self.batch.clear();
        self.num_batches += 1;
        Ok(())
    }

    /// Cancel all the files that were created
    pub fn delete_batches(&mut self) -> anyhow::Result<()> {
        for i in 0..self.num_batches {
            let batch_name = self.dir.join(format!("{:06x}", i));
            // It's OK if something is not OK here
            std::fs::remove_file(&batch_name)
                .with_context(|| format!("Could not remove file {}", batch_name.display()))?;
        }
        self.num_batches = 0;
        self.last_batch_len = 0;
        self.batch.clear();
        Ok(())
    }

    pub fn iter(&mut self) -> anyhow::Result<KMergeIters<BatchIterator<D>, D::DeserType>> {
        self.dump()?;
        Ok(KMergeIters::new((0..self.num_batches).map(|batch_idx| {
            BatchIterator::new_labeled(
                self.dir.join(format!("{:06x}", batch_idx)),
                if batch_idx == self.num_batches - 1 {
                    self.last_batch_len
                } else {
                    self.batch_size
                },
                self.deserializer.clone(),
            )
            .unwrap()
        })))
    }
}

/// An iterator that can read the batch files generated by [`SortPairs`] and
/// iterate over the triples
pub struct BatchIterator<D: BitDeserializer<NE, BitReader> = ()> {
    stream: BitReader,
    len: usize,
    current: usize,
    prev_src: usize,
    prev_dst: usize,
    deserializer: D,
}

impl BatchIterator<()> {
    /// Sort the given triples in memory, dump them in `file_path` and return an iterator
    /// over them
    #[inline]
    pub fn new_from_vec<P: AsRef<Path>>(
        file_path: P,
        batch: &mut [(usize, usize)],
    ) -> anyhow::Result<Self> {
        Self::new_from_vec_labeled(file_path, unsafe { core::mem::transmute(batch) }, &(), ())
    }
    /// Dump the given triples in `file_path` and return an iterator
    /// over them, assuming they are already sorted
    pub fn new_from_vec_sorted<P: AsRef<Path>>(
        file_path: P,
        batch: &[(usize, usize)],
    ) -> anyhow::Result<Self> {
        Self::new_from_vec_sorted_labeled(
            file_path,
            unsafe { core::mem::transmute(batch) },
            &(),
            (),
        )
    }

    /// Create a new iterator over the triples previously serialized in `file_path`
    pub fn new<P: AsRef<std::path::Path>>(file_path: P, len: usize) -> anyhow::Result<Self> {
        Self::new_labeled(file_path, len, ())
    }
}

impl<D: BitDeserializer<NE, BitReader>> BatchIterator<D> {
    /// Sort the given triples in memory, dump them in `file_path` and return an iterator
    /// over them
    #[inline]
    pub fn new_from_vec_labeled<S: BitSerializer<NE, BitWriter>>(
        file_path: impl AsRef<Path>,
        batch: &mut [Triple<S::SerType>],
        serializer: &S,
        deserializer: D,
    ) -> anyhow::Result<Self>
    where
        S::SerType: Send + Sync + Copy,
    {
        let start = std::time::Instant::now();
        batch.radix_sort_unstable();
        debug!("Sorted {} arcs in {:?}", batch.len(), start.elapsed());
        Self::new_from_vec_sorted_labeled(file_path, batch, serializer, deserializer)
    }

    /// Dump the given triples in `file_path` and return an iterator
    /// over them, assuming they are already sorted
    pub fn new_from_vec_sorted_labeled<S: BitSerializer<NE, BitWriter>>(
        file_path: impl AsRef<Path>,
        batch: &[Triple<S::SerType>],
        serializer: &S,
        deserializer: D,
    ) -> anyhow::Result<Self>
    where
        S::SerType: Send + Sync + Copy,
    {
        // create a batch file where to dump
        let file_path = file_path.as_ref();
        let file = std::io::BufWriter::with_capacity(
            1 << 16,
            std::fs::File::create(file_path).with_context(|| {
                format!(
                    "Could not create BatchIterator temporary file {}",
                    file_path.display()
                )
            })?,
        );
        // createa bitstream to write to the file
        let mut stream = <BufBitWriter<NE, _>>::new(<WordAdapter<usize, _>>::new(file));
        // Dump the triples to the bitstream
        let (mut prev_src, mut prev_dst) = (0, 0);
        for Triple {
            pair: [src, dst],
            label: payload,
        } in batch.iter()
        {
            // write the src gap as gamma
            stream
                .write_gamma((src - prev_src) as _)
                .with_context(|| format!("Could not write {} after {}", src, prev_src))?;
            if *src != prev_src {
                // Reset prev_y
                prev_dst = 0;
            }
            // write the dst gap as gamma
            stream
                .write_gamma((dst - prev_dst) as _)
                .with_context(|| format!("Could not write {} after {}", dst, prev_dst))?;
            // write the payload
            serializer
                .serialize(payload, &mut stream)
                .context("Could not serialize payload")?;
            (prev_src, prev_dst) = (*src, *dst);
        }
        // flush the stream and reset the buffer
        stream.flush().context("Could not flush stream")?;

        Self::new_labeled(file_path, batch.len(), deserializer)
    }

    /// Create a new iterator over the triples previously serialized in `file_path`
    pub fn new_labeled<P: AsRef<std::path::Path>>(
        file_path: P,
        len: usize,
        deserializer: D,
    ) -> anyhow::Result<Self> {
        let stream = <BufBitReader<NE, _>>::new(MemWordReader::new(ArcMmapHelper(Arc::new(
            MmapHelper::mmap(
                file_path.as_ref(),
                MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::SEQUENTIAL,
            )
            .with_context(|| format!("Could not mmap {}", file_path.as_ref().display()))?,
        ))));
        Ok(BatchIterator {
            stream,
            len,
            current: 0,
            prev_src: 0,
            prev_dst: 0,
            deserializer,
        })
    }
}

impl<D: BitDeserializer<NE, BitReader> + Clone> Clone for BatchIterator<D> {
    fn clone(&self) -> Self {
        BatchIterator {
            stream: self.stream.clone(),
            len: self.len,
            current: self.current,
            prev_src: self.prev_src,
            prev_dst: self.prev_dst,
            deserializer: self.deserializer.clone(),
        }
    }
}

//unsafe impl<D: BitDeserializer> SortedIterator for BatchIterator<D> {}

impl<D: BitDeserializer<NE, BitReader>> Iterator for BatchIterator<D> {
    type Item = (usize, usize, D::DeserType);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.len {
            return None;
        }
        let src = self.prev_src + self.stream.read_gamma().unwrap() as usize;
        if src != self.prev_src {
            // Reset prev_y
            self.prev_dst = 0;
        }
        let dst = self.prev_dst + self.stream.read_gamma().unwrap() as usize;
        let payload = self.deserializer.deserialize(&mut self.stream).unwrap();
        self.prev_src = src;
        self.prev_dst = dst;
        self.current += 1;
        Some((src, dst, payload))
    }
}

#[derive(Clone, Debug)]
/// Private struct that can be used to sort triples based only on the nodes and
/// ignoring the payload
struct HeadTail<T, I: Iterator<Item = (usize, usize, T)>> {
    head: (usize, usize, T),
    tail: I,
}

impl<T, I: Iterator<Item = (usize, usize, T)>> PartialEq for HeadTail<T, I> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        (self.head.0, self.head.1) == (other.head.0, other.head.1)
    }
}

impl<T, I: Iterator<Item = (usize, usize, T)>> Eq for HeadTail<T, I> {}

impl<T, I: Iterator<Item = (usize, usize, T)>> PartialOrd for HeadTail<T, I> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some((other.head.0, other.head.1).cmp(&(self.head.0, self.head.1)))
    }
}

impl<T, I: Iterator<Item = (usize, usize, T)>> Ord for HeadTail<T, I> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (other.head.0, other.head.1).cmp(&(self.head.0, self.head.1))
    }
}

#[derive(Clone, Debug)]
/// Merge K different sorted iterators
pub struct KMergeIters<I: Iterator<Item = (usize, usize, T)>, T = ()> {
    heap: dary_heap::QuaternaryHeap<HeadTail<T, I>>,
}

impl<T, I: Iterator<Item = (usize, usize, T)>> KMergeIters<I, T> {
    pub fn new(iters: impl Iterator<Item = I>) -> Self {
        let mut heap = dary_heap::QuaternaryHeap::with_capacity(iters.size_hint().1.unwrap_or(10));
        for mut iter in iters {
            if let Some((src, dst, payload)) = iter.next() {
                heap.push(HeadTail {
                    head: (src, dst, payload),
                    tail: iter,
                });
            }
        }
        KMergeIters { heap }
    }
}

#[allow(clippy::uninit_assumed_init)]
impl<T, I: Iterator<Item = (usize, usize, T)>> Iterator for KMergeIters<I, T> {
    type Item = (usize, usize, T);

    fn next(&mut self) -> Option<Self::Item> {
        let mut head_tail = self.heap.peek_mut()?;

        match head_tail.tail.next() {
            None => Some(PeekMut::pop(head_tail).head),
            Some((src, dst, payload)) => {
                Some(std::mem::replace(&mut head_tail.head, (src, dst, payload)))
            }
        }
    }
}

// unsafe impl<T, I> SortedIterator for KMergeIters<I, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_sort_pairs() -> anyhow::Result<()> {
        use tempfile::Builder;

        #[derive(Clone, Debug)]
        struct MyDessert;

        impl BitDeserializer<NE, BitReader> for MyDessert {
            type DeserType = usize;
            fn deserialize(
                &self,
                bitstream: &mut BitReader,
            ) -> Result<Self::DeserType, <BitReader as BitRead<NE>>::Error> {
                bitstream.read_delta().map(|x| x as usize)
            }
        }

        impl BitSerializer<NE, BitWriter> for MyDessert {
            type SerType = usize;
            fn serialize(
                &self,
                value: &Self::SerType,
                bitstream: &mut BitWriter,
            ) -> Result<usize, <BitWriter as BitWrite<NE>>::Error> {
                bitstream.write_delta(*value as u64)
            }
        }
        let dir = Builder::new().prefix("test_sort_pairs-").tempdir()?;
        let mut sp = SortPairs::new_labeled(10, dir.path(), MyDessert, MyDessert)?;
        let n = 25;
        for i in 0..n {
            sp.push_labeled(i, i + 1, i + 2)?;
        }
        let mut iter = sp.iter()?;
        let mut cloned = iter.clone();

        for _ in 0..n {
            let (x, y, p) = iter.next().unwrap();
            println!("{} {} {}", x, y, p);
            assert_eq!(x + 1, y);
            assert_eq!(x + 2, p);
        }

        for _ in 0..n {
            let (x, y, p) = cloned.next().unwrap();
            println!("{} {} {}", x, y, p);
            assert_eq!(x + 1, y);
            assert_eq!(x + 2, p);
        }
        Ok(())
    }
}
