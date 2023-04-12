#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use webgraph::codes::*;
use webgraph::utils::get_lowest_bits;

#[derive(Arbitrary, Debug)]
struct FuzzCase {
    commands: Vec<RandomCommand>
}

#[derive(Arbitrary, Debug)]
enum RandomCommand {
    WriteBits(u64, u8),
    WriteUnary(u8, bool, bool),
    Gamma(u64, bool, bool),
    Delta(u64, bool, bool),
}

fuzz_target!(|data: FuzzCase| {
    //println!("{:#4?}", data);
    let mut buffer_m2l = vec![];
    let mut buffer_l2m = vec![];
    let mut writes = vec![];
    // write
    {
        let mut big = BufferedBitStreamWrite::<M2L, _>::new(MemWordWriteVec::new(&mut buffer_m2l));
        let mut little = BufferedBitStreamWrite::<L2M, _>::new(MemWordWriteVec::new(&mut buffer_l2m));

        for command in data.commands.iter() {
            match command {
                RandomCommand::WriteBits(value, n_bits) => {
                    let n_bits = (*n_bits).min(64).max(1);
                    let value = get_lowest_bits(*value, n_bits);
                    let big_success = big.write_bits(value, n_bits).is_ok();
                    let little_success = little.write_bits(value, n_bits).is_ok();
                    assert_eq!(big_success, little_success);
                    writes.push(big_success);
                },
                RandomCommand::WriteUnary(value, _read_tab, write_tab) => {
                    let (big_success, little_success) = if *write_tab {
                        (
                            big.write_unary::<true>(*value as u64).is_ok(),
                            little.write_unary::<true>(*value as u64).is_ok(),
                        )
                    } else {
                        (
                            big.write_unary::<false>(*value as u64).is_ok(),
                            little.write_unary::<false>(*value as u64).is_ok(),
                        )
                    };
                    assert_eq!(big_success, little_success);
                    writes.push(big_success);
                },
                RandomCommand::Gamma(value, _, write_tab) => {
                    let value = (*value).min(u64::MAX - 1);
                    let (big_success, little_success) = if *write_tab {
                        (
                            big.write_gamma::<true>(value).is_ok(),
                            little.write_gamma::<true>(value).is_ok(),
                        )
                    } else {
                        (
                            big.write_gamma::<false>(value).is_ok(),
                            little.write_gamma::<false>(value).is_ok(),
                        )
                    };
                    assert_eq!(big_success, little_success);
                    writes.push(big_success);
                },
                RandomCommand::Delta(value, _, write_tab) => {
                    let value = (*value).min(u64::MAX - 1);
                    let (big_success, little_success) = if *write_tab {
                        (
                            big.write_delta::<true>(value).is_ok(),
                            little.write_delta::<true>(value).is_ok(),
                        )
                    } else {
                        (
                            big.write_delta::<false>(value).is_ok(),
                            little.write_delta::<false>(value).is_ok(),
                        )
                    };
                    assert_eq!(big_success, little_success);
                    writes.push(big_success);
                },
            };
        }
    }
    // read back
    //println!("{:?}", buffer_m2l);
    //println!("{:?}", buffer_l2m);
    {
        let mut big = BufferedBitStreamRead::<M2L, _>::new(MemWordRead::new(&buffer_m2l));
        let mut little = BufferedBitStreamRead::<L2M, _>::new(MemWordRead::new(&buffer_l2m));

        for (succ, command) in writes.iter().zip(data.commands.iter()) {
            let pos = big.get_position();
            assert_eq!(pos, little.get_position());

            match command {
                RandomCommand::WriteBits(value, n_bits) => {
                    let n_bits = (*n_bits).min(64).max(1);
                    let b = big.read_bits(n_bits);
                    let l = little.read_bits(n_bits);
                    if *succ {
                        let value = get_lowest_bits(*value, n_bits);
                        let b = b.unwrap();
                        let l = l.unwrap();
                        assert_eq!(b, value, "\nread : {:0n$b}\ntruth: {:0n$b}", b, value, n=n_bits as _);
                        assert_eq!(l, value, "\nread : {:0n$b}\ntruth: {:0n$b}", l, value, n=n_bits as _);
                        assert_eq!(pos + n_bits as usize, big.get_position());
                        assert_eq!(pos + n_bits as usize, little.get_position());
                    } else {
                        assert!(b.is_err());
                        assert!(l.is_err());
                        assert_eq!(pos, big.get_position());
                        assert_eq!(pos, little.get_position());
                    }
                },
                RandomCommand::WriteUnary(value, read_tab, _write_tab) => {
                    let (b, l) = if *read_tab {
                        (
                            big.read_unary::<true>(),
                            little.read_unary::<true>(),
                        )
                    } else {
                        (
                            big.read_unary::<false>(),
                            little.read_unary::<false>(),
                        )
                    };
                    if *succ {
                        assert_eq!(b.unwrap(), *value as u64);
                        assert_eq!(l.unwrap(), *value as u64);
                        assert_eq!(pos + len_unary::<true>(*value as u64), big.get_position());
                        assert_eq!(pos + len_unary::<true>(*value as u64), little.get_position());
                        assert_eq!(pos + len_unary::<false>(*value as u64), big.get_position());
                        assert_eq!(pos + len_unary::<false>(*value as u64), little.get_position());
                    } else {
                        assert!(b.is_err());
                        assert!(l.is_err());
                        assert_eq!(pos, big.get_position());
                        assert_eq!(pos, little.get_position());
                    }
                },
                RandomCommand::Gamma(value, read_tab, _) => {
                    let value = (*value).min(u64::MAX - 1);
                    let (b, l) = if *read_tab {
                        (
                            big.read_gamma::<true>(),
                            little.read_gamma::<true>(),
                        )
                    } else {
                        (
                            big.read_gamma::<false>(),
                            little.read_gamma::<false>(),
                        )
                    };
                    if *succ {
                        assert_eq!(b.unwrap(), value);
                        assert_eq!(l.unwrap(), value);
                        assert_eq!(pos + len_gamma::<false>(value), big.get_position());
                        assert_eq!(pos + len_gamma::<false>(value), little.get_position());
                        assert_eq!(pos + len_gamma::<true>(value), big.get_position());
                        assert_eq!(pos + len_gamma::<true>(value), little.get_position());
                    } else {
                        assert!(b.is_err());
                        assert!(l.is_err());
                        assert_eq!(pos, big.get_position());
                        assert_eq!(pos, little.get_position());
                    }
                },
                RandomCommand::Delta(value, read_tab, _) => {
                    let value = (*value).min(u64::MAX - 1);
                    let (b, l) = if *read_tab {
                        (
                            big.read_delta::<true>(),
                            little.read_delta::<true>(),
                        )
                    } else {
                        (
                            big.read_delta::<false>(),
                            little.read_delta::<false>(),
                        )
                    };
                    if *succ {
                        assert_eq!(b.unwrap(), value);
                        assert_eq!(l.unwrap(), value);
                        assert_eq!(pos + len_delta::<true>(value), big.get_position());
                        assert_eq!(pos + len_delta::<true>(value), little.get_position());
                        assert_eq!(pos + len_delta::<false>(value), big.get_position());
                        assert_eq!(pos + len_delta::<false>(value), little.get_position());
                    } else {
                        assert!(b.is_err());
                        assert!(l.is_err());
                        assert_eq!(pos, big.get_position());
                        assert_eq!(pos, little.get_position());
                    }
                },
            };
        }
    }
});
