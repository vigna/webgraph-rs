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
    let mut reader = BufferedBitStreamReader::new(MemWordReader::new(&data.init));
    for command in data.commands {
        match command {
            RandomCommand::SeekBit(idx) => {let _ = reader.seek_bit(idx);},
            RandomCommand::ReadBits(n_bits) => {let _ = reader.read_bits(n_bits);},
            RandomCommand::ReadUnary => {let _ = reader.read_unary();},
        };
    }
});
