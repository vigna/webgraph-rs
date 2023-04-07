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
    WriteUnary(u8),
}

fuzz_target!(|data: FuzzCase| {
    //println!("{:#4?}", data);
    let mut buffer_m2l = vec![];
    let mut buffer_m2lt = vec![];
    let mut buffer_l2m = vec![];
    let mut buffer_l2mt = vec![];
    let mut writes = vec![];
    // write
    {
        let mut big = BufferedBitStreamWrite::<M2L, _>::new(MemWordWriteVec::new(&mut buffer_m2l));
        let mut bigt = BufferedBitStreamWrite::<M2L, _>::new(MemWordWriteVec::new(&mut buffer_m2lt));
        let mut little = BufferedBitStreamWrite::<L2M, _>::new(MemWordWriteVec::new(&mut buffer_l2m));
        let mut littlet = BufferedBitStreamWrite::<L2M, _>::new(MemWordWriteVec::new(&mut buffer_l2mt));

        for command in data.commands.iter() {
            match command {
                RandomCommand::WriteBits(value, n_bits) => {
                    let n_bits = (*n_bits).min(64).max(1);
                    let value = get_lowest_bits(*value, n_bits);
                    let big_success = big.write_bits(value, n_bits).is_ok();
                    let big_successt = bigt.write_bits(value, n_bits).is_ok();
                    let little_success = little.write_bits(value, n_bits).is_ok();
                    let little_successt = littlet.write_bits(value, n_bits).is_ok();
                    assert_eq!(big_success, big_successt);
                    assert_eq!(big_success, little_success);
                    assert_eq!(little_success, little_successt);
                    writes.push(big_success);
                },
                RandomCommand::WriteUnary(value) => {
                    let big_success = big.write_unary::<false>(*value as u64).is_ok();
                    let big_successt = bigt.write_unary::<true>(*value as u64).is_ok();
                    let little_success = little.write_unary::<false>(*value as u64).is_ok();
                    let little_successt = littlet.write_unary::<true>(*value as u64).is_ok();
                    assert_eq!(big_success, big_successt);
                    assert_eq!(little_success, little_successt);
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
        let mut bigt = BufferedBitStreamRead::<M2L, _>::new(MemWordRead::new(&buffer_m2lt));
        let mut little = BufferedBitStreamRead::<L2M, _>::new(MemWordRead::new(&buffer_l2m));
        let mut littlet = BufferedBitStreamRead::<L2M, _>::new(MemWordRead::new(&buffer_l2mt));

        for (succ, command) in writes.iter().zip(data.commands.iter()) {
            match command {
                RandomCommand::WriteBits(value, n_bits) => {
                    let n_bits = (*n_bits).min(64).max(1);
                    let b = big.read_bits(n_bits);
                    let bt = bigt.read_bits(n_bits);
                    let l = little.read_bits(n_bits);
                    let lt = littlet.read_bits(n_bits);
                    if *succ {
                        let value = get_lowest_bits(*value, n_bits);
                        let b = b.unwrap();
                        let bt = bt.unwrap();
                        let l = l.unwrap();
                        let lt = lt.unwrap();
                        assert_eq!(b, value, "\nread : {:0n$b}\ntruth: {:0n$b}", b, value, n=n_bits as _);
                        assert_eq!(bt, value, "\nread : {:0n$b}\ntruth: {:0n$b}", bt, value, n=n_bits as _);
                        assert_eq!(l, value, "\nread : {:0n$b}\ntruth: {:0n$b}", l, value, n=n_bits as _);
                        assert_eq!(lt, value, "\nread : {:0n$b}\ntruth: {:0n$b}", lt, value, n=n_bits as _);
                    } else {
                        assert!(b.is_err());
                        assert!(bt.is_err());
                        assert!(l.is_err());
                        assert!(lt.is_err());
                    }
                },
                RandomCommand::WriteUnary(value) => {
                    let b = big.read_unary::<false>();
                    let bt = bigt.read_unary::<true>();
                    let l = little.read_unary::<false>();
                    let lt = littlet.read_unary::<true>();
                    if *succ {
                        assert_eq!(b.unwrap(), *value as u64);
                        assert_eq!(bt.unwrap(), *value as u64);
                        assert_eq!(l.unwrap(), *value as u64);
                        assert_eq!(lt.unwrap(), *value as u64);
                    } else {
                        assert!(b.is_err());
                        assert!(bt.is_err());
                        assert!(l.is_err());
                        assert!(lt.is_err());
                    }
                },
            };
        }
    }
});
