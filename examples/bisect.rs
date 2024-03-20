use rand::rngs::SmallRng;
use rand::RngCore;
use rand::SeedableRng;
use rayon::prelude::*;
use std::hint::black_box;

fn main() {
    let mut rng = SmallRng::seed_from_u64(
        std::env::var("SEED")
            .unwrap_or("0".to_string())
            .parse()
            .unwrap(),
    );

    let mut values = vec![1; std::env::var("CAPACITY").unwrap().parse().unwrap()];
    println!("done");

    for _ in 0..values.capacity() {
        values.push(black_box(rng.next_u64()));
    }
    println!("filled");

    values.par_sort_unstable();
    println!("sorted");

    println!(
        "{} {} {}",
        values[0],
        values[rng.next_u64() as usize % values.len()],
        values[values.len() - 1]
    );
}
