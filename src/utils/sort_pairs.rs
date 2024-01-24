/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//use crate::traits::SortedIterator;
use crate::traits::{BitDeserializer, BitSerializer};
use crate::utils::MmapBackend;
use anyhow::{anyhow, Context};
use core::mem::MaybeUninit;
use core::ptr::addr_of_mut;
use dary_heap;
use dsi_bitstream::prelude::*;
use mmap_rs::MmapFlags;
use rayon::prelude::*;
use std::path::{Path, PathBuf};

/// A struct that ingests paris of nodes and a generic payload and sort them
/// in chunks of `batch_size` triples, then dumps them to disk.
///
/// We require that the bit deserializer is `Clone` because we need
/// to be able to do the parallel compression of BVGraphs. Thus, it's suggested
/// that if you have big structures, you wrap them in an [`Arc`](`std::sync::Arc`) or use references.

pub struct SortPairs<S: BitSerializer = (), D: BitDeserializer + Clone = ()> {
    /// The batch size
    batch_size: usize,
    /// The length of the last batch might be smaller than `batch_size`
    last_batch_len: usize,
    /// The batch of triples we are currently building
    batch: Vec<(usize, usize, S::SerType)>,
    /// were we are going to store the tmp files
    dir: PathBuf,
    /// keep track of how many batches we created
    num_batches: usize,
    /// A stateufl serializer we will pass to batch iterators to serialize
    /// the labels to a bitstream
    serializer: S,
    /// A stateufl deserializer we will pass to batch iterators to deserialize
    /// the labels from a bitstream
    deserializer: D,
}

impl<S: BitSerializer, D: BitDeserializer + Clone> core::ops::Drop for SortPairs<S, D> {
    fn drop(&mut self) {
        let _ = self.dump();
    }
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

impl<S: BitSerializer, D: BitDeserializer + Clone> SortPairs<S, D> {
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
        self.batch.push((x, y, t));
        if self.batch.len() >= self.batch_size {
            self.dump()?;
        }
        Ok(())
    }

    /// Dump the current batch to disk
    fn dump(&mut self) -> anyhow::Result<()> {
        // early exit
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
    pub fn cancel_batches(&mut self) -> anyhow::Result<()> {
        for i in 0..self.num_batches {
            let batch_name = self.dir.join(format!("{:06x}", i));
            // It's OK if something is not OK here
            std::fs::remove_file(batch_name)?;
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
#[derive(Debug)]
pub struct BatchIterator<D: BitDeserializer = ()> {
    stream: BufBitReader<NE, MemWordReader<u32, MmapBackend<u32>>>,
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
    #[inline]
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
    #[inline]
    pub fn new<P: AsRef<std::path::Path>>(file_path: P, len: usize) -> anyhow::Result<Self> {
        Self::new_labeled(file_path, len, ())
    }
}

impl<D: BitDeserializer> BatchIterator<D> {
    /// Sort the given triples in memory, dump them in `file_path` and return an iterator
    /// over them
    #[inline]
    pub fn new_from_vec_labeled<P: AsRef<Path>, S: BitSerializer>(
        file_path: P,
        batch: &mut [(usize, usize, S::SerType)],
        serializer: &S,
        deserializer: D,
    ) -> anyhow::Result<Self> {
        batch.par_sort_unstable_by_key(|(src, dst, _)| (*src, *dst));
        Self::new_from_vec_sorted_labeled(file_path, batch, serializer, deserializer)
    }

    /// Dump the given triples in `file_path` and return an iterator
    /// over them, assuming they are already sorted
    pub fn new_from_vec_sorted_labeled<P: AsRef<Path>, S: BitSerializer>(
        file_path: P,
        batch: &[(usize, usize, S::SerType)],
        serializer: &S,
        deserializer: D,
    ) -> anyhow::Result<Self> {
        // create a batch file where to dump
        let file =
            std::io::BufWriter::with_capacity(1 << 22, std::fs::File::create(file_path.as_ref())?);
        // createa bitstream to write to the file
        let mut stream = <BufBitWriter<NE, _>>::new(<WordAdapter<usize, _>>::new(file));
        // Dump the triples to the bitstream
        let (mut prev_src, mut prev_dst) = (0, 0);
        for (src, dst, payload) in batch.iter() {
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

        Self::new_labeled(file_path.as_ref(), batch.len(), deserializer)
    }

    /// Create a new iterator over the triples previously serialized in `file_path`
    pub fn new_labeled<P: AsRef<std::path::Path>>(
        file_path: P,
        len: usize,
        deserializer: D,
    ) -> anyhow::Result<Self> {
        let stream = <BufBitReader<NE, _>>::new(MemWordReader::new(
            MmapBackend::load(file_path.as_ref(), MmapFlags::TRANSPARENT_HUGE_PAGES)
                .with_context(|| format!("Could not mmap {}", file_path.as_ref().display()))?,
        ));
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

impl<D: BitDeserializer + Clone> Clone for BatchIterator<D> {
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

impl<D: BitDeserializer> Iterator for BatchIterator<D> {
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
    fn eq(&self, other: &Self) -> bool {
        (self.head.0, self.head.1) == (other.head.0, other.head.1)
    }
}

impl<T, I: Iterator<Item = (usize, usize, T)>> Eq for HeadTail<T, I> {}

impl<T, I: Iterator<Item = (usize, usize, T)>> PartialOrd for HeadTail<T, I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some((other.head.0, other.head.1).cmp(&(self.head.0, self.head.1)))
    }
}

impl<T, I: Iterator<Item = (usize, usize, T)>> Ord for HeadTail<T, I> {
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
            match iter.next() {
                None => {}
                Some((src, dst, payload)) => {
                    heap.push(HeadTail {
                        head: (src, dst, payload),
                        tail: iter,
                    });
                }
            }
        }
        KMergeIters { heap }
    }
}

#[allow(clippy::uninit_assumed_init)]
impl<T, I: Iterator<Item = (usize, usize, T)>> Iterator for KMergeIters<I, T> {
    type Item = (usize, usize, T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }
        let mut head_tail = self.heap.peek_mut().unwrap();

        match head_tail.tail.next() {
            None => {
                drop(head_tail);
                Some(self.heap.pop().unwrap().head)
            }
            Some((src, dst, payload)) => {
                Some(std::mem::replace(&mut head_tail.head, (src, dst, payload)))
            }
        }
    }
}

// unsafe impl<T, I> SortedIterator for KMergeIters<I, T> {}

#[cfg(test)]
#[test]
pub fn test_push() -> anyhow::Result<()> {
    use crate::prelude::{CodeRead, CodeWrite};

    #[derive(Clone, Debug)]
    struct MyDessert;

    impl BitDeserializer for MyDessert {
        type DeserType = usize;
        fn deserialize<E: Endianness, B: CodeRead<E>>(
            &self,
            bitstream: &mut B,
        ) -> Result<Self::DeserType, B::Error> {
            Ok(bitstream.read_delta().map(|x| x as usize)?)
        }
    }

    impl BitSerializer for MyDessert {
        type SerType = usize;
        fn serialize<E: Endianness, B: CodeWrite<E>>(
            &self,
            value: &Self::SerType,
            bitstream: &mut B,
        ) -> Result<usize, B::Error> {
            Ok(bitstream.write_delta(*value as u64)?)
        }
    }
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new_labeled(10, dir.into_path(), MyDessert, MyDessert)?;
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
