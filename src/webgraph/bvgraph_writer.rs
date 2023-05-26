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
    blocks: Vec<usize>,
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
            blocks: Vec::with_capacity(1024),
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

    fn diff_comp(&mut self, ref_list: &[usize], curr_list: &[usize]) {
        // j is the index of the next successor of the current node we must examine
        let mut j = 0;
        // k is the index of the next successor of the reference node we must examine
        let mut k = 0;
        // copying is true iff we are producing a copy block (instead of an ignore block)
        let mut copying = true;
        // currBlockLen is the number of entries (in the reference list) we have already copied/ignored (in the current block)
        let mut curr_block_len = 0;

        self.blocks.clear();
        self.extra_nodes.clear();

        while j < curr_list.len() && k < ref_list.len() {
            // First case: we are currectly copying entries from the reference list
            if copying {
                if curr_list[j] > ref_list[k] {
                    /* If while copying we trespass the current element of the reference list,
                    we must stop copying. */
                    self.blocks.push(curr_block_len);
                    copying = false;
                    curr_block_len = 0;
                } else if curr_list[j] < ref_list[k] {
                    /* If while copying we find a non-matching element of the reference list which
                    is larger than us, we can just add the current element to the extra list
                    and move on. j gets increased. */
                    self.extra_nodes.push(curr_list[j]);
                    j += 1;
                } else {
                    // currList[j] == refList[k]
                    /* If the current elements of the two lists are equal, we just increase the block length.
                    both j and k get increased. */
                    j += 1;
                    k += 1;
                    curr_block_len += 1;
                    // if (forReal) copiedArcs++;
                }
            } else if curr_list[j] < curr_list[k] {
                /* If we did not trespass the current element of the reference list, we just
                add the current element to the extra list and move on. j gets increased. */
                self.extra_nodes.push(curr_list[j]);
                j += 1;
            } else if curr_list[j] > ref_list[k] {
                /* If we trespassed the currented element of the reference list, we
                increase the block length. k gets increased. */
                k += 1;
                curr_block_len += 1;
            } else {
                // currList[j] == refList[k]
                /* If we found a match we flush the current block and start a new copying phase. */
                self.blocks.push(curr_block_len);
                copying = true;
                curr_block_len = 0;
            }
        }
        /* We do not record the last block. The only case when we have to enqueue the last block's length
         * is when we were copying and we did not copy up to the end of the reference list.
         */
        if copying && k < ref_list.len() {
            self.blocks.push(curr_block_len);
        }

        // If there are still missing elements, we add them to the extra list.
        while j < curr_list.len() {
            self.extra_nodes.push(curr_list[j]);
            j += 1;
        }
    }

    pub fn push<I: Iterator<Item = usize>>(&mut self, succ_iter: I) -> Result<usize> {
        let mut succ_vec = self.backrefs.take();
        let mut written_bits = 0;
        succ_vec.extend(succ_iter);
        let d = succ_vec.len();
        written_bits += self.bit_write.write_outdegree(d as u64)?;

        if d != 0 {
            if self.compression_window != 0 {
                self.diff_comp(&self.backrefs[self.curr_node as isize - 1], &succ_vec);
            } else {
                self.extra_nodes.clear();
                self.extra_nodes.extend(&succ_vec)
            }

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
