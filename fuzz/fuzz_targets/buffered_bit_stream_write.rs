#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use webgraph::codes::*;
use webgraph::utils::get_lowest_bits;

#[derive(Arbitrary, Debug)]
struct FuzzCase {
    buffer_len: u8,
    commands: Vec<RandomCommand>
}

#[derive(Arbitrary, Debug)]
enum RandomCommand {
    WriteBits(u64, u8),
    WriteUnary(u64),
}

fuzz_target!(|data: FuzzCase| {
    let mut buffer_m2l = vec![0; data.buffer_len as usize];
    let mut buffer_l2m = vec![0; data.buffer_len as usize];
    let mut writes = vec![];
    // write
    {
        let mut big = BufferedBitStreamWrite::<M2L, _>::new(MemWordWrite::new(&mut buffer_m2l));
        let mut little = BufferedBitStreamWrite::<L2M, _>::new(MemWordWrite::new(&mut buffer_l2m));

        for command in data.commands.iter() {
            match command {
                RandomCommand::WriteBits(value, n_bits) => {
                    let value = get_lowest_bits(*value, (*n_bits).min(64).max(1));
                    let big_success = big.write_bits(value, *n_bits).is_ok();
                    let little_success = little.write_bits(value, *n_bits).is_ok();
                    assert_eq!(big_success, little_success);
                    writes.push(big_success);
                },
                RandomCommand::WriteUnary(value) => {
                    let big_success = big.write_unary::<true>(*value).is_ok();
                    let little_success = little.write_unary::<true>(*value).is_ok();
                    assert_eq!(big_success, little_success);
                    writes.push(big_success);
                },
            };
        }
    }
    // read back
    {
        let mut big = BufferedBitStreamRead::<M2L, _>::new(MemWordRead::new(&buffer_m2l));
        let mut little = BufferedBitStreamRead::<L2M, _>::new(MemWordRead::new(&buffer_l2m));

        for (succ, command) in writes.iter().zip(data.commands.iter()) {
            match command {
                RandomCommand::WriteBits(value, n_bits) => {
                    let b = big.read_bits(*n_bits);
                    let l = little.read_bits(*n_bits);
                    if *succ {
                        let value = get_lowest_bits(*value, (*n_bits).min(64).max(1));
                        assert_eq!(b.unwrap(), value);
                        assert_eq!(l.unwrap(), value);
                    } else {
                        assert!(b.is_err());
                        assert!(l.is_err());
                    }
                },
                RandomCommand::WriteUnary(value) => {
                    let b = big.read_unary::<true>();
                    let l = little.read_unary::<true>();
                    if *succ {
                        assert_eq!(b.unwrap(), *value);
                        assert_eq!(l.unwrap(), *value);
                    } else {
                        assert!(b.is_err());
                        assert!(l.is_err());
                    }
                },
            };
        }
    }
});
