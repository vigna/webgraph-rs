use webgraph::prelude::*;

type ReadType = u32;
type BufferType = u64;

const NODES: usize = 325557;

#[test]
fn test_sequential_reading() {
    // Read the offsets
    let mut data = std::fs::read("tests/data/cnr-2000.offsets").unwrap();
    // pad with zeros so we can read with ReadType words
    while data.len() % core::mem::size_of::<ReadType>() != 0 {
        data.push(0);
    }
    // we must do this becasue Vec<u8> is not guaranteed to be properly aligned
    let data = data
        .chunks(core::mem::size_of::<ReadType>())
        .map(|chunk| ReadType::from_ne_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();

    let mut offsets = Vec::with_capacity(NODES);
    let mut reader = BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&data));
    let mut offset = 0;
    for _ in 0..NODES {
        offset += reader.read_gamma::<true>().unwrap() as usize;
        offsets.push(offset);
    }

    let mut data = std::fs::read("tests/data/cnr-2000.graph").unwrap();
    // pad with zeros so we can read with ReadType words
    while data.len() % core::mem::size_of::<ReadType>() != 0 {
        data.push(0);
    }
    // we must do this becasue Vec<u8> is not guaranteed to be properly aligned
    let data = data
        .chunks(core::mem::size_of::<ReadType>())
        .map(|chunk| ReadType::from_ne_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();

    let mut code_reader = DefaultCodesReader::new(
        BufferedBitStreamRead::<M2L, BufferType, _>::new(MemWordReadInfinite::new(&data)),
    );
    let mut reader = WebgraphReaderRandomAccess::new(code_reader, offsets, 4);

    for node_id in 0..(NODES as u64) {
        println!("{:?}", reader.get_successors_iter(node_id).unwrap().collect::<Vec<_>>());
    }
}
