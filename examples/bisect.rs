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
    println!("start");
    let cap = std::env::var("CAPACITY").unwrap().parse().unwrap();
    let mut values = vec![1; cap];
    println!("done :0x{:x}", values.as_ptr() as usize);

    for i in 0..cap {
        values[i] = black_box(rng.next_u64());
    }
    println!("filled :0x{:x}", values.as_ptr() as usize);

    values.par_sort_unstable();
    println!("sorted");

    println!(
        "{} {} {}",
        values[0],
        values[rng.next_u64() as usize % values.len()],
        values[values.len() - 1]
    );
}
