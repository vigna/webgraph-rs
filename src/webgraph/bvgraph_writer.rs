use super::CircularBuffer;
use crate::traits::*;
use crate::utils::int2nat;
use anyhow::Result;

pub struct BVComp<WGCW: WebGraphCodesWriter> {
    backrefs: CircularBuffer,
    bit_write: WGCW,
    mock_writer: WGCW::MockWriter,
    compressors: Vec<Compressor>,
    #[allow(dead_code)]
    min_interval_length: usize,
    #[allow(dead_code)]
    compression_window: usize,
    curr_node: usize,
}

#[derive(Debug, Clone)]
struct Compressor {
    outdegree: usize,
    blocks: Vec<usize>,
    extra_nodes: Vec<usize>,
    left_interval: Vec<usize>,
    len_interval: Vec<usize>,
    residuals: Vec<usize>,
}

impl Compressor {
    const NO_INTERVALS: usize = 0;

    fn new() -> Self {
        Compressor {
            outdegree: 0,
            blocks: Vec::with_capacity(1024),
            extra_nodes: Vec::with_capacity(1024),
            left_interval: Vec::with_capacity(1024),
            len_interval: Vec::with_capacity(1024),
            residuals: Vec::with_capacity(1024),
        }
    }

    /// Writes the current node to the bitstream, this has to be called after
    /// compress.
    fn write<WGCW: WebGraphCodesWriter>(
        &self,
        writer: &mut WGCW,
        curr_node: usize,
        reference_offset: Option<usize>,
        min_interval_length: usize,
    ) -> Result<usize> {
        let mut written_bits = 0;
        written_bits += writer.write_outdegree(self.outdegree as u64)?;
        if self.outdegree != 0 {
            if let Some(reference_offset) = reference_offset {
                writer.write_reference_offset(reference_offset as u64)?;
                if reference_offset != 0 {
                    writer.write_block_count(self.blocks.len() as _)?;
                    if !self.blocks.is_empty() {
                        for i in 0..self.blocks.len() {
                            writer.write_blocks((self.blocks[i] - 1) as u64)?;
                        }
                    }
                }
            }
        }

        if !self.extra_nodes.is_empty() && min_interval_length != Self::NO_INTERVALS {
            writer.write_interval_count(self.left_interval.len() as _)?;

            if !self.left_interval.is_empty() {
                writer.write_interval_start(int2nat(
                    self.left_interval[0] as i64 - curr_node as i64,
                ))?;
                writer.write_interval_len((self.len_interval[0] - min_interval_length) as u64)?;
                let mut prev = self.left_interval[0] + self.len_interval[0];

                for i in 1..self.left_interval.len() {
                    writer.write_interval_start((self.left_interval[i] - prev - 1) as u64)?;
                    writer
                        .write_interval_len((self.len_interval[i] - min_interval_length) as u64)?;
                    prev = self.left_interval[i] + self.len_interval[i];
                }
            }
        }

        if !self.residuals.is_empty() {
            written_bits += writer
                .write_first_residual(int2nat(self.residuals[0] as i64 - curr_node as i64))?;

            for i in 1..self.residuals.len() {
                written_bits += writer
                    .write_residual((self.residuals[i] - self.residuals[i - 1] - 1) as u64)?;
            }
        }

        Ok(written_bits)
    }

    /// setup the internal buffers for the compression of the given values
    fn compress(
        &mut self,
        curr_list: &[usize],
        ref_list: Option<&[usize]>,
        curr_node: usize,
        min_interval_length: usize,
    ) -> Result<()> {
        self.outdegree = curr_list.len();
        // reset all vectors
        self.blocks.clear();
        self.extra_nodes.clear();
        self.left_interval.clear();
        self.len_interval.clear();
        self.residuals.clear();

        if self.outdegree != 0 {
            if let Some(ref_list) = ref_list {
                if curr_node > 0 {
                    self.diff_comp(curr_list, ref_list);
                } else {
                    self.extra_nodes.extend(curr_list)
                }
            } else {
                self.extra_nodes.extend(curr_list)
            }

            if !self.extra_nodes.is_empty() {
                if min_interval_length != Self::NO_INTERVALS {
                    self.intervalize(min_interval_length);
                } else {
                    self.residuals.extend(&self.extra_nodes);
                }
            }
        }
        debug_assert_eq!(self.left_interval.len(), self.len_interval.len());
        Ok(())
    }

    fn intervalize(&mut self, min_interval_length: usize) {
        let vl = self.extra_nodes.len();
        let mut i = 0;

        while i < vl {
            let mut j = 0;
            if i < vl - 1 && self.extra_nodes[i] + 1 == self.extra_nodes[i + 1] {
                j += 1;
                while i + j < vl - 1 && self.extra_nodes[i + j] + 1 == self.extra_nodes[i + j + 1] {
                    j += 1;
                }
                j += 1;

                // Now j is the number of integers in the interval.
                if j >= min_interval_length {
                    self.left_interval.push(self.extra_nodes[i]);
                    self.len_interval.push(j);
                    i += j - 1;
                }
            }
            if j < min_interval_length {
                self.residuals.push(self.extra_nodes[i]);
            }

            i += 1;
        }
    }

    fn diff_comp(&mut self, curr_list: &[usize], ref_list: &[usize]) {
        // j is the index of the next successor of the current node we must examine
        let mut j = 0;
        // k is the index of the next successor of the reference node we must examine
        let mut k = 0;
        // currBlockLen is the number of entries (in the reference list) we have already copied/ignored (in the current block)
        let mut curr_block_len = 0;
        // copying is true iff we are producing a copy block (instead of an ignore block)
        let mut copying = true;

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
            } else if curr_list[j] < ref_list[k] {
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
        // add a 1 to the first block so we can uniformly write them later
        if !self.blocks.is_empty() {
            self.blocks[0] += 1;
        }
    }
}

impl<WGCW: WebGraphCodesWriter> BVComp<WGCW> {
    /// This value for `min_interval_length` implies that no intervalization will be performed.
    pub const NO_INTERVALS: usize = Compressor::NO_INTERVALS;

    pub fn new(bit_write: WGCW, compression_window: usize, min_interval_length: usize) -> Self {
        BVComp {
            backrefs: CircularBuffer::new(compression_window + 1),
            mock_writer: bit_write.mock(),
            bit_write,
            min_interval_length,
            compression_window,
            curr_node: 0,
            compressors: (0..compression_window + 1)
                .map(|_| Compressor::new())
                .collect(),
        }
    }

    pub fn push<I: Iterator<Item = usize>>(&mut self, succ_iter: I) -> Result<usize> {
        // collect the iterator inside the backrefs, to reuse the capacity already
        // allocated
        {
            let mut succ_vec = self.backrefs.take();
            succ_vec.extend(succ_iter);
            self.backrefs.push(succ_vec);
        }
        // get the ref
        let curr_list = &self.backrefs[self.curr_node];
        // first try to compress the current node without references
        let compressor = &mut self.compressors[0];
        // Compute how we would compress this
        compressor.compress(curr_list, None, self.curr_node, self.min_interval_length)?;
        // avoid the mock writing
        if self.compression_window == 0 {
            let written_bits = compressor.write(
                &mut self.bit_write,
                self.curr_node,
                None,
                self.min_interval_length,
            )?;
            // update the current node
            self.curr_node += 1;
            return Ok(written_bits);
        }

        // The delta of the best reference, by default 0 which is no compression
        let mut ref_delta = 0;
        // Write the compressed data
        let mut min_bits = compressor.write(
            &mut self.mock_writer,
            self.curr_node,
            Some(0),
            self.min_interval_length,
        )?;

        let deltas = 1 + self.compression_window.min(self.curr_node);
        // compression windows is not zero, so compress the current node
        for delta in 1..deltas {
            // Get the neighbours of this previous node
            let ref_list = &self.backrefs[self.curr_node - delta];
            // Get its compressor
            let compressor = &mut self.compressors[delta];
            // Compute how we would compress this
            compressor.compress(
                curr_list,
                Some(ref_list),
                self.curr_node,
                self.min_interval_length,
            )?;
            // Compute how many bits it would use, using the mock writer
            let bits = compressor.write(
                &mut self.mock_writer,
                self.curr_node,
                Some(delta),
                self.min_interval_length,
            )?;
            // keep track of the best, it's strictly less so we keep the
            // nearest one in the case of multiple equal ones
            if bits < min_bits {
                min_bits = bits;
                ref_delta = delta;
            }
        }
        // write the best result reusing the precomputed compression
        let compressor = &mut self.compressors[ref_delta];
        let written_bits = compressor.write(
            &mut self.bit_write,
            self.curr_node,
            Some(ref_delta),
            self.min_interval_length,
        )?;
        // consistency check
        debug_assert_eq!(written_bits, min_bits);
        // update the current node
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
mod test {
    use super::*;
    use crate::prelude::*;
    use dsi_bitstream::prelude::*;

    #[test]
    fn test_writer_window_zero() -> Result<()> {
        test_compression(0, 0)?;
        test_compression(0, 1)?;
        test_compression(0, 2)?;
        Ok(())
    }

    #[test]
    fn test_writer_window_one() -> Result<()> {
        test_compression(1, 0)?;
        test_compression(1, 1)?;
        test_compression(1, 2)?;
        Ok(())
    }

    #[test]
    fn test_writer_window_two() -> Result<()> {
        test_compression(2, 0)?;
        test_compression(2, 1)?;
        test_compression(2, 2)?;
        Ok(())
    }

    fn test_compression(compression_window: usize, min_interval_length: usize) -> Result<()> {
        let mut true_iter = WebgraphSequentialIter::load_mapped("tests/data/cnr-2000")?;

        // Compress the graph
        let mut buffer: Vec<u64> = Vec::new();
        let bit_write = <BufferedBitStreamWrite<LE, _>>::new(MemWordWriteVec::new(&mut buffer));

        let comp_flags = CompFlags {
            ..Default::default()
        };

        //let codes_writer = DynamicCodesWriter::new(
        //    bit_write,
        //    &comp_flags,
        //);
        let codes_writer = <ConstCodesWriter<LE, _>>::new(bit_write);

        let mut bvcomp = BVComp::new(codes_writer, compression_window, min_interval_length);

        bvcomp
            .extend(WebgraphSequentialIter::load_mapped("tests/data/cnr-2000")?)
            .unwrap();
        bvcomp.flush()?;

        // Read it back

        let buffer_32: &[u32] = unsafe { buffer.align_to().1 };
        let bit_read =
            <BufferedBitStreamRead<LE, u64, _>>::new(MemWordReadInfinite::new(buffer_32));

        //let codes_reader = <DynamicCodesReader<LE, _>>::new(bit_read, &comp_flags)?;
        let codes_reader = <ConstCodesReader<LE, _>>::new(bit_read, &comp_flags)?;

        let mut seq_iter = WebgraphSequentialIter::new(
            codes_reader,
            compression_window,
            min_interval_length,
            true_iter.num_nodes(),
        );

        // Check that the graph is the same
        for i in 0..true_iter.num_nodes() {
            let (true_node_id, true_succ) = true_iter.next().unwrap();
            let (seq_node_id, seq_succ) = seq_iter.next().unwrap();

            assert_eq!(true_node_id, i);
            assert_eq!(true_node_id, seq_node_id);
            let true_succ = true_succ.collect::<Vec<_>>();
            let seq_succ = seq_succ.collect::<Vec<_>>();
            dbg!(true_node_id, &true_succ);
            assert_eq!(true_succ, seq_succ, "node_id: {}", i);
        }

        Ok(())
    }
}
