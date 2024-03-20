use rand::rngs::SmallRng;
use rand::RngCore;
use rand::SeedableRng;
use rayon::prelude::*;
use std::hint::black_box;

fn main() {
    let mut rng = SmallRng::seed_from_u64(0xbad5eed);

    let mut values = Vec::with_capacity(std::env::var("CAPACITY").unwrap().parse().unwrap());

    for _ in 0..values.capacity() {
        values.push(rng.next_u64());
    }

    values.par_sort_unstable();

    let _ = black_box(values);
}
