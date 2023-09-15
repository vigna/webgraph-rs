<<<<<<< HEAD
/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

=======
use crate::utils::MmapBackend;
>>>>>>> 9471e70 (ser des)
use crate::{
    traits::{BitDeserializer, BitSerializer, DummyBitSerDes, SortedIterator},
    utils::KAryHeap,
};
use anyhow::{anyhow, Context, Result};
use core::mem::MaybeUninit;
use core::ptr::addr_of_mut;
use dsi_bitstream::prelude::*;
use mmap_rs::MmapFlags;
use rayon::prelude::*;
use std::path::{Path, PathBuf};

/// A struct that ingests paris of nodes and a generic payload and sort them
/// in chunks of `batch_size` triples, then dumps them to disk.
pub struct SortPairs<S: BitSerializer = DummyBitSerDes, D: BitDeserializer = DummyBitSerDes> {
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

impl<S: BitSerializer, D: BitDeserializer> core::ops::Drop for SortPairs<S, D> {
    fn drop(&mut self) {
        let _ = self.dump();
    }
}

impl SortPairs<DummyBitSerDes, DummyBitSerDes> {
    /// Create a new `SortPairs` with a given batch size
    ///
    /// The `dir` must be empty, and in particular it **must not** be shared
    /// with other `SortPairs` instances.
    pub fn new<P: AsRef<Path>>(batch_size: usize, dir: P) -> Result<Self> {
        Self::new_labelled(batch_size, dir, DummyBitSerDes, DummyBitSerDes)
    }
    /// Add a triple to the graph.
    pub fn push(&mut self, x: usize, y: usize) -> Result<()> {
        self.push_labelled(x, y, ())
    }
}

impl<S: BitSerializer, D: BitDeserializer> SortPairs<S, D> {
    /// Create a new `SortPairs` with a given batch size
    ///
    /// The `dir` must be empty, and in particular it **must not** be shared
    /// with other `SortPairs` instances.
    pub fn new_labelled<P: AsRef<Path>>(
        batch_size: usize,
        dir: P,
        serializer: S,
        deserializer: D,
    ) -> Result<Self> {
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
    pub fn push_labelled(&mut self, x: usize, y: usize, t: S::SerType) -> Result<()> {
        self.batch.push((x, y, t));
        if self.batch.len() >= self.batch_size {
            self.dump()?;
        }
        Ok(())
    }

    /// Dump the current batch to disk
    fn dump(&mut self) -> Result<()> {
        // early exit
        if self.batch.is_empty() {
            return Ok(());
        }
        // create a batch file where to dump
        let batch_name = self.dir.join(format!("{:06x}", self.num_batches));
        BatchIterator::new_from_vec_labelled(
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
    pub fn cancel_batches(&mut self) -> Result<()> {
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

    pub fn iter(&mut self) -> Result<KMergeIters<BatchIterator<D>, D::DeserType>> {
        self.dump()?;
        Ok(KMergeIters::new((0..self.num_batches).map(|batch_idx| {
            BatchIterator::new_labelled(
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
pub struct BatchIterator<D: BitDeserializer = DummyBitSerDes> {
    stream: BufferedBitStreamRead<LE, u64, MemWordReadInfinite<u32, MmapBackend<u32>>>,
    len: usize,
    current: usize,
    prev_src: usize,
    prev_dst: usize,
    deserializer: D,
}

impl BatchIterator<DummyBitSerDes> {
    /// Sort the given triples in memory, dump them in `file_path` and return an iterator
    /// over them
    #[inline]
    pub fn new_from_vec<P: AsRef<Path>>(
        file_path: P,
        batch: &mut [(usize, usize)],
    ) -> Result<Self> {
        Self::new_from_vec_labelled(
            file_path,
            unsafe { core::mem::transmute(batch) },
            &DummyBitSerDes,
            DummyBitSerDes,
        )
    }
    /// Dump the given triples in `file_path` and return an iterator
    /// over them, assuming they are already sorted
    #[inline]
    pub fn new_from_vec_sorted<P: AsRef<Path>>(
        file_path: P,
        batch: &[(usize, usize)],
    ) -> Result<Self> {
        Self::new_from_vec_sorted_labelled(
            file_path,
            unsafe { core::mem::transmute(batch) },
            &DummyBitSerDes,
            DummyBitSerDes,
        )
    }

    /// Create a new iterator over the triples previously serialized in `file_path`
    #[inline]
    pub fn new<P: AsRef<std::path::Path>>(file_path: P, len: usize) -> Result<Self> {
        Self::new_labelled(file_path, len, DummyBitSerDes)
    }
}

impl<D: BitDeserializer> BatchIterator<D> {
    /// Sort the given triples in memory, dump them in `file_path` and return an iterator
    /// over them
    #[inline]
    pub fn new_from_vec_labelled<P: AsRef<Path>, S: BitSerializer>(
        file_path: P,
        batch: &mut [(usize, usize, S::SerType)],
        serializer: &S,
        deserializer: D,
    ) -> Result<Self> {
        batch.par_sort_unstable_by_key(|(src, dst, _)| (*src, *dst));
        Self::new_from_vec_sorted_labelled(file_path, batch, serializer, deserializer)
    }

    /// Dump the given triples in `file_path` and return an iterator
    /// over them, assuming they are already sorted
    pub fn new_from_vec_sorted_labelled<P: AsRef<Path>, S: BitSerializer>(
        file_path: P,
        batch: &[(usize, usize, S::SerType)],
        serializer: &S,
        deserializer: D,
    ) -> Result<Self> {
        // create a batch file where to dump
        let file =
            std::io::BufWriter::with_capacity(1 << 22, std::fs::File::create(file_path.as_ref())?);
        // createa bitstream to write to the file
        let mut stream = <BufferedBitStreamWrite<LE, _>>::new(FileBackend::new(file));
        // Dump the triples to the bitstream
        let (mut prev_src, mut prev_dst) = (0, 0);
        for (src, dst, payload) in batch.iter() {
            // write the src gap as gamma
            stream.write_gamma((src - prev_src) as _)?;
            if *src != prev_src {
                // Reset prev_y
                prev_dst = 0;
            }
            // write the dst gap as gamma
            stream.write_gamma((dst - prev_dst) as _)?;
            // write the payload
            serializer.serialize(&payload, &mut stream)?;
            (prev_src, prev_dst) = (*src, *dst);
        }
        // flush the stream and reset the buffer
        stream.flush()?;

        Self::new_labelled(file_path.as_ref(), batch.len(), deserializer)
    }

    /// Create a new iterator over the triples previously serialized in `file_path`
    pub fn new_labelled<P: AsRef<std::path::Path>>(
        file_path: P,
        len: usize,
        deserializer: D,
    ) -> Result<Self> {
        let stream = <BufferedBitStreamRead<LE, u64, _>>::new(MemWordReadInfinite::new(
            MmapBackend::load(file_path, MmapFlags::TRANSPARENT_HUGE_PAGES)?,
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

impl<D: BitDeserializer> Clone for BatchIterator<D> {
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

unsafe impl<D: BitDeserializer> SortedIterator for BatchIterator<D> {}

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
struct HeadTail<T, I: Iterator<Item = (usize, usize, T)> + SortedIterator> {
    head: (usize, usize),
    payload: T,
    tail: I,
}

impl<T, I: Iterator<Item = (usize, usize, T)> + SortedIterator> PartialEq for HeadTail<T, I> {
    fn eq(&self, other: &Self) -> bool {
        self.head == other.head
    }
}
impl<T, I: Iterator<Item = (usize, usize, T)> + SortedIterator> PartialOrd for HeadTail<T, I> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.head.cmp(&other.head))
    }
}

#[derive(Clone, Debug)]
/// Merge K different sorted iterators
pub struct KMergeIters<I: Iterator<Item = (usize, usize, T)> + SortedIterator, T = ()> {
    heap: KAryHeap<HeadTail<T, I>>,
}

impl<T, I: Iterator<Item = (usize, usize, T)> + SortedIterator> KMergeIters<I, T> {
    pub fn new(iters: impl Iterator<Item = I>) -> Self {
        let mut heap = KAryHeap::with_capacity(iters.size_hint().1.unwrap_or(10));
        for mut iter in iters {
            match iter.next() {
                None => {}
                Some((src, dst, payload)) => {
                    heap.push(HeadTail {
                        head: (src, dst),
                        payload,
                        tail: iter,
                    });
                }
            }
        }
        KMergeIters { heap }
    }
}

impl<T, I: Iterator<Item = (usize, usize, T)> + SortedIterator> Iterator for KMergeIters<I, T> {
    type Item = (usize, usize, T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }
        // Read the head of the heap
        let head_tail = self.heap.peek_mut();
        let (src, dst) = head_tail.head;
        // get the payload without requiring clone or copy
        // this leaves head_tail.payload uninitalized
        // but we will wither replace it or drop it
        let res = Some((src, dst, unsafe {
            core::ptr::replace(
                addr_of_mut!(head_tail.payload),
                MaybeUninit::uninit().assume_init(),
            )
        }));

        match head_tail.tail.next() {
            None => {
                // Remove the head of the heap if the iterator ended
                let HeadTail { payload, .. } = self.heap.pop().unwrap();
                core::mem::forget(payload); // so we don't drop the maybe uninit
            }
            Some((src, dst, payload)) => {
                // set the new values
                head_tail.head = (src, dst);
                head_tail.payload = payload;
                // fix the heap
                self.heap.bubble_down(0);
            }
        }
        res
    }
}

unsafe impl<T, I: Iterator<Item = (usize, usize, T)> + SortedIterator> SortedIterator
    for KMergeIters<I, T>
{
}

#[cfg(test)]
#[test]
pub fn test_push() -> Result<()> {
    #[derive(Clone, Debug)]
    struct MyDessert;

    impl BitDeserializer for MyDessert {
        type DeserType = usize;
        fn deserialize<E: Endianness, B: ReadCodes<E>>(
            &self,
            bitstream: &mut B,
        ) -> Result<Self::DeserType> {
            bitstream.read_delta().map(|x| x as usize)
        }
    }

    impl BitSerializer for MyDessert {
        type SerType = usize;
        fn serialize<E: Endianness, B: WriteCodes<E>>(
            &self,
            value: &Self::SerType,
            bitstream: &mut B,
        ) -> Result<usize> {
            bitstream.write_delta(*value as u64)
        }
    }
    let dir = tempfile::tempdir()?;
    let mut sp = SortPairs::new_labelled(10, dir.into_path(), MyDessert, MyDessert)?;
    let n = 25;
    for i in 0..n {
        sp.push_labelled(i, i + 1, i + 2)?;
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
