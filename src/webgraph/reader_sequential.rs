use super::*;
use crate::utils::nat2int;
use anyhow::Result;
use dsi_bitstream::prelude::*;

/// A fast sequential iterator over the nodes of the graph and their successors.
/// This iterator does not require to know the offsets of each node in the graph.
pub struct WebgraphSequentialIter<CR: WebGraphCodesReader> {
    codes_reader: CR,
    backrefs: CircularBuffer,
    compression_window: usize,
    min_interval_length: usize,
    number_of_nodes: usize,
}
impl<CR: WebGraphCodesReader + BitSeek> WebgraphSequentialIter<CR> {
    pub fn get_pos(&self) -> usize {
        self.codes_reader.get_pos()
    }
}

impl<CR: WebGraphCodesReader> NumNodes for WebgraphSequentialIter<CR> {
    fn num_nodes(&self) -> usize {
        self.number_of_nodes
    }
}

impl<CR: WebGraphCodesReader> WebgraphSequentialIter<CR> {
    pub fn new(
        codes_reader: CR,
        min_interval_length: usize,
        compression_window: usize,
        number_of_nodes: usize,
    ) -> Self {
        Self {
            codes_reader,
            backrefs: CircularBuffer::new(compression_window + 1),
            compression_window,
            min_interval_length,
            number_of_nodes,
        }
    }

    /// Get the successors of the next node in the stream
    pub fn next_successors(&mut self) -> Result<&[usize]> {
        let node_id = self.backrefs.get_end_node_id();
        let mut res = self.backrefs.take();
        self.get_successors_iter_priv(node_id, &mut res)?;
        Ok(self.backrefs.push(res))
    }

    #[inline(always)]
    fn get_successors_iter_priv(&mut self, node_id: usize, results: &mut Vec<usize>) -> Result<()> {
        let degree = self.codes_reader.read_outdegree()? as usize;
        // no edges, we are done!
        if degree == 0 {
            return Ok(());
        }

        // ensure that we have enough capacity in the vector for not reallocating
        results.reserve(degree.saturating_sub(results.capacity()));

        // read the reference offset
        let ref_delta = if self.compression_window != 0 {
            self.codes_reader.read_reference_offset()? as usize
        } else {
            0
        };
        // if we copy nodes from a previous one
        if ref_delta != 0 {
            // compute the node id of the reference
            let reference_node_id = node_id - ref_delta;
            // retrieve the data
            let neighbours = self.backrefs.get(reference_node_id);
            debug_assert!(!neighbours.is_empty());
            // get the info on which destinations to copy
            let number_of_blocks = self.codes_reader.read_block_count()? as usize;

            // no blocks, we copy everything
            if number_of_blocks == 0 {
                results.extend_from_slice(neighbours);
            } else {
                // otherwise we copy only the blocks of even index

                // the first block could be zero
                let mut idx = self.codes_reader.read_blocks()? as usize;
                results.extend_from_slice(&neighbours[..idx]);

                // while the other can't
                for block_id in 1..number_of_blocks {
                    let block = self.codes_reader.read_blocks()? as usize;
                    let end = idx + block + 1;
                    if block_id % 2 == 0 {
                        results.extend_from_slice(&neighbours[idx..end]);
                    }
                    idx = end;
                }
                if number_of_blocks & 1 == 0 {
                    results.extend_from_slice(&neighbours[idx..]);
                }
            }
        };

        // if we still have to read nodes
        let nodes_left_to_decode = degree - results.len();
        if nodes_left_to_decode != 0 && self.min_interval_length != 0 {
            // read the number of intervals
            let number_of_intervals = self.codes_reader.read_interval_count()? as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                let node_id_offset = nat2int(self.codes_reader.read_interval_start()?);
                debug_assert!((node_id as i64 + node_id_offset) >= 0);
                let mut start = (node_id as i64 + node_id_offset) as usize;
                let mut delta = self.codes_reader.read_interval_len()? as usize;
                delta += self.min_interval_length;
                // save the first interval
                results.extend(start..(start + delta));
                start += delta;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    start += 1 + self.codes_reader.read_interval_start()? as usize;
                    delta = self.codes_reader.read_interval_len()? as usize;
                    delta += self.min_interval_length;

                    results.extend(start..(start + delta));

                    start += delta;
                }
            }
        }

        // decode the extra nodes if needed
        let nodes_left_to_decode = degree - results.len();
        if nodes_left_to_decode != 0 {
            // pre-allocate with capacity for efficency
            let node_id_offset = nat2int(self.codes_reader.read_first_residual()?);
            let mut extra = (node_id as i64 + node_id_offset) as usize;
            results.push(extra);
            // decode the successive extra nodes
            for _ in 1..nodes_left_to_decode {
                extra += 1 + self.codes_reader.read_residual()? as usize;
                results.push(extra);
            }
        }

        results.sort();
        Ok(())
    }
}

impl<CR: WebGraphCodesReader> Iterator for WebgraphSequentialIter<CR> {
    type Item = (usize, std::vec::IntoIter<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let node_id = self.backrefs.get_end_node_id();
        if node_id >= self.number_of_nodes as _ {
            return None;
        }
        let mut res = self.backrefs.take();
        self.get_successors_iter_priv(node_id, &mut res).unwrap();

        // this clippy suggestion is wrong, we cannot return a reference to a
        // local variable
        #[allow(clippy::unnecessary_to_owned)]
        Some((node_id, self.backrefs.push(res).to_vec().into_iter()))
    }
}

#[cfg(feature = "std")]
/// `std` dependent implementations for [`WebgraphSequentialIter`]
mod p {
    use super::*;
    use crate::utils::MmapBackend;
    use anyhow::Result;
    use java_properties;
    use mmap_rs::*;
    use std::fs::*;
    use std::io::*;

    type ReadType = u32;
    type BufferType = u64;

    impl
        WebgraphSequentialIter<
            ConstCodesReader<
                BE,
                BufferedBitStreamRead<
                    BE,
                    BufferType,
                    MemWordReadInfinite<ReadType, MmapBackend<ReadType>>,
                >,
            >,
        >
    {
        pub fn load_mapped(basename: &str) -> Result<Self> {
            let f = File::open(format!("{}.properties", basename))?;
            let map = java_properties::read(BufReader::new(f))?;

            let mut file = std::fs::File::open(format!("{}.graph", basename)).unwrap();
            let mut file_len = file.seek(std::io::SeekFrom::End(0)).unwrap();

            // align the len to readtypes, TODO!: arithmize
            while file_len % std::mem::size_of::<ReadType>() as u64 != 0 {
                file_len += 1;
            }

            let data = unsafe {
                MmapOptions::new(file_len as _)
                    .unwrap()
                    .with_file(file, 0)
                    .map()
                    .unwrap()
            };

            let code_reader = ConstCodesReader::new(
                BufferedBitStreamRead::<BE, BufferType, _>::new(MemWordReadInfinite::new(
                    MmapBackend::new(data),
                )),
                &CompFlags::default(),
            )?;
            let seq_reader = WebgraphSequentialIter::new(
                code_reader,
                map.get("minintervallength").unwrap().parse::<usize>()?,
                map.get("windowsize").unwrap().parse::<usize>()?,
                map.get("nodes").unwrap().parse::<usize>()?,
            );

            Ok(seq_reader)
        }
    }
}
