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
    Gamma(u64, bool, bool),
}

fuzz_target!(|data: FuzzCase| {
    let mut buffer_m2l = vec![];
    let mut buffer_l2m = vec![];
    let mut writes = vec![];
    // write
    {
        // TODO!: also fuzz len
        let mut big = BufferedBitStreamWrite::<M2L, _>::new(MemWordWriteVec::new(&mut buffer_m2l));
        let mut little = BufferedBitStreamWrite::<L2M, _>::new(MemWordWriteVec::new(&mut buffer_l2m));

        for command in data.commands.iter() {
            match command {
                RandomCommand::Gamma(value, _, write_tab) => {
                    let value = (*value).min(u64::MAX - 1);
                    let (big_success, little_success) = if write_tab {
                        (
                            big.write_gamma::<true>(value as u64).is_ok(),
                            little.write_gamma::<true>(value as u64).is_ok(),
                        )
                    } else {
                        (
                            big.write_gamma::<false>(value as u64).is_ok(),
                            little.write_gamma::<false>(value as u64).is_ok(),
                        )
                    };
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
                RandomCommand::Gamma(value, read_tab, _) => {
                    let value = (*value).min(u64::MAX - 1);
                    let (b, l) = if read_tab {
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
                        assert_eq!(b.unwrap(), value as u64);
                        assert_eq!(l.unwrap(), value as u64);
                    } else {
                        assert!(b.is_err());
                        assert!(l.is_err());
                    }
                },
            };
        }
    }
});
