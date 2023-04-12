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
    SkipBits(u8),
    ReadBits(u8),
    ReadUnary,
}

fuzz_target!(|data: FuzzCase| {
    let mut big = BufferedBitStreamRead::<M2L, _>::new(MemWordRead::new(&data.init));
    let mut little = BufferedBitStreamRead::<L2M, _>::new(MemWordRead::new(&data.init));
    for command in data.commands {
        let bpos = big.get_position();
        let lpos = little.get_position();
        match command {
            RandomCommand::SeekBit(idx) => {
                let b = big.seek_bit(idx);
                let l = little.seek_bit(idx);
                assert_eq!(b.is_ok(), l.is_ok(), "{:?} {:?}", b, l);
                if b.is_ok() {
                    assert_eq!(big.get_position(), idx);
                    assert_eq!(little.get_position(), idx);
                } else {
                    assert_eq!(big.get_position(), bpos);
                    assert_eq!(little.get_position(), lpos);
                }
            },
            RandomCommand::SkipBits(n_bits) => {
                let bs = big.skip_bits(n_bits as _).is_ok();
                let ls = little.skip_bits(n_bits as _).is_ok();
                if bs {
                    assert_eq!(big.get_position(), bpos + n_bits as usize);
                }
                if ls {
                    assert_eq!(little.get_position(), lpos + n_bits as usize);
                }
                // TODO!: discussion, should a failed skip_bits leave the position
                // unaltered?
                // else {
                //    assert_eq!(big.get_position(), pos);
                //    assert_eq!(little.get_position(), pos);
                //}
            },
            RandomCommand::ReadBits(n_bits) => {
                let b = big.peek_bits(n_bits);
                assert_eq!(big.get_position(), bpos);
                if b.is_ok() {
                    assert_eq!(
                        b.unwrap(),
                        big.read_bits(n_bits).unwrap(),
                    );
                    assert_eq!(big.get_position(), bpos + n_bits as usize);
                } else {
                    assert!(big.read_bits(n_bits).is_err());
                    assert_eq!(big.get_position(), bpos);
                }

                let l = little.peek_bits(n_bits);
                assert_eq!(little.get_position(), lpos);
                if l.is_ok() {
                    assert_eq!(
                        l.unwrap(),
                        little.read_bits(n_bits).unwrap(),
                    );
                    assert_eq!(little.get_position(), lpos + n_bits as usize);
                } else {
                    assert!(little.read_bits(n_bits).is_err());
                    assert_eq!(little.get_position(), lpos);
                }
            },
            RandomCommand::ReadUnary => {
                let _ = big.read_unary::<true>();
                let _ = little.read_unary::<true>();
            },
        };
    }
});
