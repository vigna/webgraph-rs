use dsi_bitstream::prelude::*;
use itertools;
use itertools::KMerge;
use rayon::prelude::*;
use std::path::PathBuf;
use tempfile::tempdir;

pub struct SortPairs<T: Send + Copy> {
    max_len: usize,
    pairs: Vec<(usize, usize, T)>,
    dir: PathBuf,
    num_batches: usize,
}

impl<T: Send + Copy> SortPairs<T> {
    pub fn new(batch_size: usize) -> anyhow::Result<Self> {
        Ok(SortPairs {
            max_len: batch_size,
            pairs: Vec::with_capacity(batch_size),
            dir: tempdir()?.into_path(),
            num_batches: 0,
        })
    }

    fn dump(&mut self) {
        self.pairs.par_sort_unstable_by_key(|(x, y, _)| (*x, *y));
        let batch_name = self.dir.join(format!("{:06x}", self.num_batches));
        let file = std::io::BufWriter::new(std::fs::File::create(&batch_name).unwrap());
        let mut stream = <BufferedBitStreamWrite<LE, _>>::new(FileBackend::new(file));
        // TODO!: here the labels t are not considered
        let (mut prev_x, mut prev_y, _prev_t) = (0, 0, 0);
        for &(x, y, _t) in &self.pairs {
            stream.write_gamma((x - prev_x) as _).unwrap();
            if x != prev_x {
                // Reset prev_y
                prev_y = 0;
            }
            stream.write_gamma((y - prev_y) as _).unwrap();
            (prev_x, prev_y) = (x, y);
        }
        println!("Dumping");
        drop(stream);

        let file = std::io::BufReader::new(std::fs::File::open(&batch_name).unwrap());
        let mut stream = <BufferedBitStreamRead<LE, u64, _>>::new(<FileBackend<u32, _>>::new(file));
        for _ in 0..self.pairs.len() {
            let _x = stream.read_gamma().unwrap();
            let _y = stream.read_gamma().unwrap();
        }

        self.pairs.clear();
        self.num_batches += 1;
        println!("End");
    }

    pub fn push(&mut self, x: usize, y: usize, t: T) {
        self.pairs.push((x, y, t));
        if self.pairs.len() >= self.max_len {
            self.dump();
        }
    }

    pub fn build(mut self) -> KMerge<BatchIterator> {
        println!("Building");
        let last_batch_len = self.pairs.len();
        self.dump();
        let mut iterators = Vec::with_capacity(self.num_batches);
        for i in 0..self.num_batches {
            let batch_name = self.dir.join(format!("{:06x}", i));
            let file = std::io::BufReader::new(std::fs::File::open(&batch_name).unwrap());
            let stream = <BufferedBitStreamRead<LE, u64, _>>::new(<FileBackend<u32, _>>::new(file));
            iterators.push(BatchIterator {
                len: if i == self.num_batches - 1 {
                    last_batch_len
                } else {
                    self.max_len
                },
                stream: stream,
                current: 0,
                prev_x: 0,
                prev_y: 0,
            });
        }

        itertools::kmerge(iterators)
    }
}

pub struct BatchIterator {
    stream: BufferedBitStreamRead<LE, u64, FileBackend<u32, std::io::BufReader<std::fs::File>>>,
    len: usize,
    current: usize,
    prev_x: usize,
    prev_y: usize,
}

impl Iterator for BatchIterator {
    type Item = (usize, usize);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.len {
            return None;
        }
        let x = self.prev_x + self.stream.read_gamma().unwrap() as usize;
        if x != self.prev_x {
            // Reset prev_y
            self.prev_y = 0;
        }
        let y = self.prev_y + self.stream.read_gamma().unwrap() as usize;
        self.prev_x = x;
        self.prev_y = y;
        self.current += 1;
        Some((x, y))
    }
}

#[cfg(test)]
#[test]
pub fn test_push() {
    let mut sp = SortPairs::new(10).unwrap();
    for i in 0..25 {
        sp.push(i, i, i);
    }

    let iter = sp.build();
    for (x, y) in iter {
        println!("{} {}", x, y)
    }
}
