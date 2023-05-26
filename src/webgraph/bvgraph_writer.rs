use super::CircularBuffer;
use crate::traits::*;
use crate::utils::int2nat;
use anyhow::Result;

pub struct BVComp<WGCW: WebGraphCodesWriter> {
    backrefs: CircularBuffer,
    bit_write: WGCW,
    #[allow(dead_code)]
    min_interval_length: usize,
    #[allow(dead_code)]
    compression_window: usize,
    curr_node: usize,
    extra_nodes: Vec<usize>,
    left_interval: Vec<usize>,
    len_interval: Vec<usize>,
    residuals: Vec<usize>,
}

impl<WGCW: WebGraphCodesWriter> BVComp<WGCW> {
    /// This value for `min_interval_length` implies that no intervalization will be performed.
    pub const NO_INTERVALS: usize = 0;

    pub fn new(bit_write: WGCW, compression_window: usize, min_interval_length: usize) -> Self {
        BVComp {
            backrefs: CircularBuffer::new(compression_window + 1),
            bit_write,
            min_interval_length,
            compression_window,
            curr_node: 0,
            extra_nodes: Vec::with_capacity(1024),
            left_interval: Vec::with_capacity(1024),
            len_interval: Vec::with_capacity(1024),
            residuals: Vec::with_capacity(1024),
        }
    }

    fn intervalize(&mut self) {
        let vl = self.extra_nodes.len();
        let mut i = 0;
        self.left_interval.clear();
        self.len_interval.clear();
        self.residuals.clear();

        while i < vl {
            let mut j = 0;
            if i < vl - 1 && self.extra_nodes[i] + 1 == self.extra_nodes[i + 1] {
                j += 1;
                while i + j < vl - 1 && self.extra_nodes[i + j] + 1 == self.extra_nodes[i + j + 1] {
                    j += 1;
                }
                j += 1;

                // Now j is the number of integers in the interval.
                if j >= self.min_interval_length {
                    self.left_interval.push(self.extra_nodes[i]);
                    self.len_interval.push(j);
                    i += j - 1;
                }
            }
            if j < self.min_interval_length {
                self.residuals.push(self.extra_nodes[i]);
            }

            i += 1;
        }
    }

    pub fn push<I: Iterator<Item = usize>>(&mut self, succ_iter: I) -> Result<usize> {
        let mut succ_vec = self.backrefs.take();
        let mut written_bits = 0;
        succ_vec.extend(succ_iter);
        let d = succ_vec.len();
        written_bits += self.bit_write.write_outdegree(d as u64)?;

        self.extra_nodes.clear();
        self.extra_nodes.extend_from_slice(&succ_vec);

        if d != 0 {
            if self.min_interval_length != Self::NO_INTERVALS {
                self.intervalize();
                self.bit_write
                    .write_interval_count(self.left_interval.len() as u64)?;

                if !self.left_interval.is_empty() {
                    self.bit_write.write_interval_start(dbg!(int2nat(dbg!(
                        self.left_interval[0] as i64 - self.curr_node as i64
                    ),)))?;
                    self.bit_write.write_interval_len(
                        (self.len_interval[0] - self.min_interval_length) as u64,
                    )?;
                    let mut prev = self.left_interval[0] + self.len_interval[0];

                    for i in 1..self.left_interval.len() {
                        self.bit_write
                            .write_interval_start((self.left_interval[i] - prev - 1) as u64)?;
                        self.bit_write.write_interval_len(
                            (self.len_interval[i] - self.min_interval_length) as u64,
                        )?;
                        prev = self.left_interval[i] + self.len_interval[i];
                    }
                }
            }

            if !self.residuals.is_empty() {
                written_bits += self.bit_write.write_first_residual(int2nat(
                    self.residuals[0] as i64 - self.curr_node as i64,
                ))?;

                for i in 1..self.residuals.len() {
                    written_bits += self
                        .bit_write
                        .write_residual((self.residuals[i] - self.residuals[i - 1] - 1) as u64)?;
                }
            }
        }
        self.backrefs.push(succ_vec);
        self.curr_node += 1;
        Ok(written_bits)
    }

    pub fn extend<I: Iterator<Item = (usize, J)>, J: Iterator<Item = usize>>(
        &mut self,
        iter_nodes: I,
    ) -> Result<usize> {
        iter_nodes.map(|(_, succ)| self.push(succ)).sum()
    }

    pub fn flush(self) -> Result<()> {
        self.bit_write.flush()
    }
}

#[cfg(test)]
#[test]
fn test_writer() -> Result<()> {
    use crate::{prelude::*, webgraph::VecGraph};
    use dsi_bitstream::prelude::*;
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
    let mut buffer: Vec<u64> = Vec::new();
    let bit_write = <BufferedBitStreamWrite<LE, _>>::new(MemWordWriteVec::new(&mut buffer));

    let codes_writer = DynamicCodesWriter::new(
        bit_write,
        &CompFlags {
            ..Default::default()
        },
    );
    //let codes_writer = ConstCodesWriter::new(bit_write);
    let mut bvcomp = BVComp::new(codes_writer, 0, 1);
    bvcomp.extend(g.iter_nodes()).unwrap();
    bvcomp.flush()?;

    let buffer_32: &[u32] = unsafe { buffer.align_to().1 };
    let bit_read = <BufferedBitStreamRead<LE, u64, _>>::new(MemWordReadInfinite::new(buffer_32));
    let codes_reader = <DynamicCodesReader<LE, _>>::new(bit_read, &CompFlags::default())?;
    let seq_iter = WebgraphSequentialIter::new(codes_reader, 0, 1, g.num_nodes());
    for (node, succ) in seq_iter {
        dbg!(node, succ);
    }

    //let h = VecGraph::from_node_iter(seq_iter);
    //assert_eq!(g, h);
    Ok(())
}
