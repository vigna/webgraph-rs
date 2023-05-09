use super::*;
use crate::utils::nat2int;
use anyhow::Result;

pub struct WebgraphReaderSequential<CR: WebGraphCodesReader> {
    codes_reader: CR,
    backrefs: CircularBuffer,
    min_interval_length: usize,
}

impl<CR: WebGraphCodesReader> WebgraphReaderSequential<CR> {
    pub fn new(
        codes_reader: CR,
        min_interval_length: usize,
        compression_window: usize,
    ) -> Self {
        Self {
            codes_reader,
            backrefs: CircularBuffer::new(compression_window + 1),
            min_interval_length,
        }
    }
    /// Get the successors of the next node in the stream
    pub fn next_successors(&mut self) -> Result<&[u64]> {
        let node_id = self.backrefs.get_end_node_id();
        let mut res = self.backrefs.take();
        self.get_successors_iter_priv(node_id, &mut res)?;
        Ok(self.backrefs.push(res))
    }

    #[inline(always)]
    fn get_successors_iter_priv(&mut self, node_id: u64, results: &mut Vec<u64>) -> Result<()> {
        let degree = self.codes_reader.read_outdegree()? as usize;
        // no edges, we are done!
        if degree == 0 {
            return Ok(());
        }

        // ensure that we have enough capacity in the vector for not reallocating
        results.reserve(degree.saturating_sub(results.capacity()));

        // read the reference offset
        let ref_delta = self.codes_reader.read_reference_offset()?;
        // if we copy nodes from a previous one
        if ref_delta != 0 {
            // compute the node id of the reference
            let reference_node_id = node_id - ref_delta;
            // retrieve the data
            let neighbours = self.backrefs.get(reference_node_id);
            debug_assert!(neighbours.len() != 0);
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
        if nodes_left_to_decode != 0 {
            // read the number of intervals
            let number_of_intervals = self.codes_reader.read_interval_count()? as usize;
            if number_of_intervals != 0 {
                // pre-allocate with capacity for efficency
                let node_id_offset = nat2int(self.codes_reader.read_interval_start()?);
                debug_assert!((node_id as i64 + node_id_offset) >= 0);
                let mut start = (node_id as i64 + node_id_offset) as u64;
                let mut delta = self.codes_reader.read_interval_len()? as usize;
                delta += self.min_interval_length;
                // save the first interval
                results.extend(start..(start + delta as u64));
                start += delta as u64;
                // decode the intervals
                for _ in 1..number_of_intervals {
                    start += 1 + self.codes_reader.read_interval_start()?;
                    delta = self.codes_reader.read_interval_len()? as usize;
                    delta += self.min_interval_length;

                    results.extend(start..(start + delta as u64));
                    
                    start += delta as u64;
                }
            }
        }

        // decode the extra nodes if needed
        let nodes_left_to_decode = degree - results.len();
        if nodes_left_to_decode != 0 {
            // pre-allocate with capacity for efficency
            let node_id_offset = nat2int(self.codes_reader.read_first_residual()?);
            let mut extra = (node_id as i64 + node_id_offset) as u64;
            results.push(extra);
            // decode the successive extra nodes
            for _ in 1..nodes_left_to_decode {
                extra += 1 + self.codes_reader.read_residual()?;
                results.push(extra);
            }
        }

        results.sort();
        Ok(())
    }
}

/*
#[cfg(feature="std")]
/// `std` dependent implementations for [`WebgraphReaderSequential`]
mod p {
    use super::*;
    use java_properties;
    use mmap_rs::*;
    use std::fs::*;
    use std::io::*;
    use crate::prelude::{BufferedBitStreamRead, MemWordReadInfinite};

    fn mmap_file<T>(path: String) -> Result<T> {
        let mut file = std::fs::File::open(path).unwrap();
        let file_len = file.seek(std::io::SeekFrom::End(0)).unwrap();
        unsafe {
            MmapOptions::new(file_len as _)
                .unwrap()
                .with_file(file, 0)
                .map()
                .unwrap()
        }
    
    
    }

    impl<CR: WebGraphCodesReader> WebgraphReaderSequential<CR> {
        pub fn from_basename<'a>(basename: &'a str) -> Result<
            WebgraphReaderSequential<DefaultCodesReader<M2L, 
                BufferedBitStreamRead<M2L, u64, MemWordReadInfinite<'a, u32>>
            >>
        > {
            let f = File::open(format!("{}.properties", basename))?;
            let map = java_properties::read(BufReader::new(f))?;

            let num_nodes = map.get("nodes").unwrap().parse::<u64>()?;

            // Read the offsets
            let data_offsets = mmap_file(&format!("{}.offsets", basename));
            let data_graph = mmap_file(&format!("{}.graph", basename));

            panic!();
        }
    }
}

*/