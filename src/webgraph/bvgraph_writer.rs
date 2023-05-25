use super::CircularBuffer;
use crate::traits::*;
use crate::utils::int2nat;
use anyhow::Result;
use dsi_bitstream::prelude::*;

pub struct BVComp<WGCW: WebGraphCodesWriter> {
    backrefs: CircularBuffer,
    bit_write: WGCW,
    #[allow(dead_code)]
    max_interval_length: usize,
    #[allow(dead_code)]
    compression_window: usize,
    curr_node: usize,
}

impl<WGCW: WebGraphCodesWriter> BVComp<WGCW> {
    pub fn new(bit_write: WGCW, compression_window: usize, max_interval_length: usize) -> Self {
        BVComp {
            backrefs: CircularBuffer::new(compression_window + 1),
            bit_write,
            max_interval_length,
            compression_window,
            curr_node: 0,
        }
    }

    pub fn push<I: Iterator<Item = usize>>(&mut self, succ_iter: I) -> Result<usize> {
        let mut succ_vec = self.backrefs.take();
        let mut written_bits = 0;
        succ_vec.extend(succ_iter);
        let d = succ_vec.len();
        written_bits += self.bit_write.write_outdegree(d as u64)?;

        if d != 0 {
            written_bits += self
                .bit_write
                .write_first_residual(int2nat(succ_vec[0] as i64 - self.curr_node as i64))?;

            for i in 1..d {
                written_bits += self
                    .bit_write
                    .write_residual((succ_vec[i] - succ_vec[i - 1] - 1) as u64)?;
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
fn test() -> Result<()> {
    use crate::{prelude::*, webgraph::VecGraph};
    let g = VecGraph::from_arc_list(&[(0, 1), (1, 2), (2, 0), (2, 1)]);
    let mut buffer: Vec<u64> = Vec::new();
    let mut bit_write = <BufferedBitStreamWrite<LE, _>>::new(MemWordWriteVec::new(&mut buffer));

    let mut codes_writer = DynamicCodesWriter::new(
        bit_write,
        &CompFlags {
            ..Default::default()
        },
    );
    //let codes_writer = ConstCodesWriter::new(bit_write);
    let mut bvcomp = BVComp::new(codes_writer, 0, 0);
    bvcomp.extend(g.iter_nodes()).unwrap();
    bvcomp.flush()?;

    let mut buffer_32: &[u32] = unsafe { buffer.align_to().1 };
    let mut bit_read =
        <BufferedBitStreamRead<LE, u64, _>>::new(MemWordReadInfinite::new(buffer_32));
    let mut codes_reader = <DynamicCodesReader<LE, _>>::new(bit_read, &CompFlags::default());
    let seq_iter = WebgraphSequentialIter::new(codes_reader, 0, 0, g.num_nodes());
    let h = VecGraph::from_node_iter(seq_iter);
    assert_eq!(g, h);
    Ok(())
}
