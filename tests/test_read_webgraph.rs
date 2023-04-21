use webgraph::prelude::*;

type ReadType = u32;
type BufferType = u64;

#[test]
fn test_sequential_reading() {
    let mut data = std::fs::read("tests/data/cnr-2000-hc.graph").unwrap();
    // pad with zeros so we can read with ReadType words
    while data.len() % core::mem::size_of::<ReadType>() != 0 {
        data.push(0);
    }
    // we must do this becasue Vec<u8> is not guaranteed to be properly aligned
    let data = data.chunks(core::mem::size_of::<ReadType>())
        .map(|chunk| {
            ReadType::from_ne_bytes(chunk.try_into().unwrap())
        })
        .collect::<Vec<_>>();

    let mut code_reader = DefaultCodesReader::new(
        BufferedBitStreamRead::<L2M, BufferType, _>::new(MemWordReadInfinite::new(&data))
    );
    let mut reader = WebgraphReaderSequential::new(&mut code_reader, 4, 16 + 1);

    for node_id in 0..325557 {
        println!("{:?}", reader.get_neighbours(node_id));
    }
}