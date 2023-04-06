#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use webgraph::codes::*;

#[derive(Arbitrary, Debug)]
struct FuzzCase {
    init: Vec<u64>,
    commands: Vec<RandomCommand>
}

#[derive(Arbitrary, Debug)]
enum RandomCommand {
    SeekBit(usize),
    ReadBits(u8),
    ReadUnary,
}

fuzz_target!(|data: FuzzCase| {
    let mut big = BufferedBitStreamRead::<M2L, _>::new(MemWordRead::new(&data.init));
    let mut little = BufferedBitStreamRead::<L2M, _>::new(MemWordRead::new(&data.init));
    for command in data.commands {
        match command {
            RandomCommand::SeekBit(idx) => {
                let b = big.seek_bit(idx);
                let l = little.seek_bit(idx);
                assert_eq!(b.is_ok(), l.is_ok(), "{:?} {:?}", b, l);
            },
            RandomCommand::ReadBits(n_bits) => {
                let _ = big.read_bits(n_bits);
                let _ = little.read_bits(n_bits);
            },
            RandomCommand::ReadUnary => {
                let _ = big.read_unary::<true>();
                let _ = little.read_unary::<true>();
            },
        };
    }
});
