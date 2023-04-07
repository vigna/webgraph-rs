#![feature(test)]
#![allow(non_snake_case)]
extern crate test;
use test::{black_box, Bencher};

extern crate webgraph;
pub use webgraph::codes::*;

pub const VALUES: usize = 10_000;
pub const SEED: u64 = 0x8c2b_781f_2866_90fd;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x << 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

macro_rules! impl_code_bitorder {
    ($code_name:ident, $read:ident, $write:ident, $reader:ident, $writer:ident, $BO:ty, $table:expr) => {
#[bench]
fn write(b: &mut Bencher) {
    let mut rng = Rng(SEED);
    let random_vals = (0..VALUES)
        .map(|_| {
            rng.next() % 256
        })
        .collect::<Vec<_>>();
    let mut buffer = Vec::with_capacity(VALUES);
    b.iter(|| {
        buffer.clear();
        let mut big = $writer::<$BO, _>::new(
            MemWordWriteVec::new(black_box(&mut buffer))
        );
        for n_bits in &random_vals {
            big.$write::<$table>(*n_bits).unwrap();
        }
    });
    b.bytes =  8 * buffer.len() as u64;
}

#[bench]
fn read(b: &mut Bencher) {
    let mut rng = Rng(SEED);
    let random_vals = (0..VALUES)
        .map(|_| {
            rng.next() % 256
        })
        .collect::<Vec<_>>();
    let mut buffer = Vec::with_capacity(VALUES);
    {
        let mut big = $writer::<$BO, _>::new(
            MemWordWriteVec::new(black_box(&mut buffer))
        );
        for n_bits in &random_vals {
            big.$write::<true>(*n_bits as u64).unwrap();
        }
    }
    b.bytes = 8 * buffer.len() as u64;
    b.iter(|| {
        let mut big = $reader::<$BO, _>::new(
            MemWordRead::new(black_box(&buffer))
        );
        for _ in &random_vals {
            big.$read::<$table>().unwrap();
        }
    });
}
    };
}

macro_rules! impl_code {
    ($code_name:ident, $read:ident, $write:ident, $reader:ident, $writer:ident) => {
    
pub mod $code_name {
    pub use super::*;
    pub mod M2L {
        pub use super::*;
        pub use webgraph::codes::M2L;
        impl_code_bitorder!($code_name, $read, $write, $reader, $writer, M2L, false);
        pub mod Table {
            pub use super::*;
            pub use webgraph::codes::M2L;
            impl_code_bitorder!($code_name, $read, $write, $reader, $writer, M2L, true);
        }
    }
    pub mod L2M {
        pub use super::*;
        pub use webgraph::codes::L2M;
        impl_code_bitorder!($code_name, $read, $write, $reader, $writer, L2M, false);
        pub mod Table {
            pub use super::*;
            pub use webgraph::codes::M2L;
            impl_code_bitorder!($code_name, $read, $write, $reader, $writer, L2M, true);
        }
    }
}
    };
}

macro_rules! impl_fixed {
    ($reader:ident, $writer:ident, $BO:ident) => {
pub mod $BO {
    pub use super::*;
    pub use webgraph::codes::$BO;
    #[bench]
    fn write(b: &mut Bencher) {
        let mut rng = Rng(SEED);
        let random_vals = (0..VALUES)
            .map(|_| {
                (
                    rng.next(),
                    rng.next() as u8 % 65,
                )
            })
            .collect::<Vec<_>>();
        let mut buffer = Vec::with_capacity(VALUES);
        b.iter(|| {
            buffer.clear();
            let mut big = $writer::<$BO, _>::new(
                MemWordWriteVec::new(black_box(&mut buffer))
            );
            for (value, n_bits) in &random_vals {
                big.write_bits(*value, *n_bits).unwrap();
            }
        });
        b.bytes = 8*buffer.len() as u64;
    }

    #[bench]
    fn read(b: &mut Bencher) {
        let mut rng = Rng(SEED);
        let random_vals = (0..VALUES)
            .map(|_| {
                (
                    rng.next(),
                    rng.next() as u8 % 65,
                )
            })
            .collect::<Vec<_>>();
        let mut buffer = Vec::with_capacity(VALUES);
        {
            let mut big = $writer::<$BO, _>::new(
                MemWordWriteVec::new(black_box(&mut buffer))
            );
            for (value, n_bits) in &random_vals {
                big.write_bits(*value, *n_bits).unwrap();
            }
        }
        b.iter(|| {
            let mut big = $reader::<$BO, _>::new(
                MemWordRead::new(black_box(&buffer))
            );
            for (_, n_bits) in &random_vals {
                big.read_bits(*n_bits).unwrap();
            }
        });
        b.bytes = 8*buffer.len() as u64;
    }
}
    };
}

macro_rules! impl_main {
    ($mod_name:ident, $reader:ident, $writer:ident) => {

pub mod $mod_name {
    pub use super::*;
    //impl_code!(gamma, read_gamma, write_gamma, $reader, $writer);
    impl_code!(unary, read_unary, write_unary, $reader, $writer);

    //pub mod fixed_len {
    //    pub use super::*;
    //    impl_fixed!($reader, $writer, M2L);
    //    impl_fixed!($reader, $writer, L2M);
    //}
}

    };
}

impl_main!(buffered, BufferedBitStreamRead, BufferedBitStreamWrite);