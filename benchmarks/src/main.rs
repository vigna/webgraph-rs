use webgraph::codes::*;
use rand::Rng;

/// How many random codes we will write and read in the benchmark
const VALUES: usize = 1_000_000;
/// How many iterations to do before starting measuring, this is done to warmup
/// the caches and the branch predictor
const WARMUP_ITERS: usize = 100;
/// How many iterations of measurement we will execute
const BENCH_ITERS: usize = 11;
/// For how many times we will measure the measurement overhead
const CALIBRATION_ITERS: usize = 100_000;
/// To proprly test delta we compute a discrete version of the indended 
/// distribution. The original distribution is infinite but we need to cut it
/// down to a finite set. This value represent the maximum value we are going to
/// extract
const DELTA_DISTR_SIZE: usize = 1_000_000;

#[cfg(feature = "rtdsc")]
mod rdtsc;
#[cfg(feature = "rtdsc")]
use rdtsc::*;

#[cfg(not(feature = "rtdsc"))]
use std::time::Instant;

mod metrics;
use metrics::*;

mod utils;
use utils::*;

mod data;
use data::*;

macro_rules! bench {
    ($cal:expr, $code:literal, $read:ident, $write:ident, $gen_data:ident, $bo:ident, $table:expr) => {{
// the memory where we will write values
let mut buffer = Vec::with_capacity(VALUES);
// counters for the total read time and total write time
let mut read_buff = MetricsStream::with_capacity(VALUES);
let mut read_unbuff = MetricsStream::with_capacity(VALUES);
let mut write = MetricsStream::with_capacity(VALUES);

// measure
let (ratio, data) = $gen_data();
for iter in 0..(WARMUP_ITERS + BENCH_ITERS) {
    buffer.clear();
    // write the codes
    {   
        // init the writer
        let mut r = BufferedBitStreamWrite::<$bo, _>::new(
            MemWordWriteVec::new(&mut buffer)
        );
        // measure
        let w_start = Instant::now();
        for value in &data {
            r.$write::<$table>(*value).unwrap();
        }
        let nanos = w_start.elapsed().as_nanos();
        // add the measurement if we are not in the warmup
        if iter >= WARMUP_ITERS {
            write.update((nanos - $cal) as f64);
        }
    }
    // read the codes
    {
        // init the reader
        let mut r = BufferedBitStreamRead::<$bo, _>::new(
            MemWordRead::new(&mut buffer)
        );
        // measure
        let r_start = Instant::now();
        for _ in &data {
            r.$read::<$table>().unwrap();
        }
        let nanos =  r_start.elapsed().as_nanos();
        // add the measurement if we are not in the warmup
        if iter >= WARMUP_ITERS {
            read_buff.update((nanos - $cal) as f64);
        }
    }
    {
        // init the reader
        let mut r = UnbufferedBitStreamRead::<$bo, _>::new(
            MemWordRead::new(&mut buffer)
        );
        // measure
        let r_start = Instant::now();
        for _ in &data {
            r.$read::<$table>().unwrap();
        }
        let nanos =  r_start.elapsed().as_nanos();
        // add the measurement if we are not in the warmup
        if iter >= WARMUP_ITERS {
            read_unbuff.update((nanos - $cal) as f64);
        }
    }
}
// convert from cycles to nano seconds
let read_buff = read_buff.finalize();
let read_unbuff = read_unbuff.finalize();
let write = write.finalize();

let table = if $table {
    "Table"
} else {
    "NoTable"
};
// print the results
println!("{}::{}::{},{},{},{},{},{},{},{}",
    $code, stringify!($bo), table, // the informations about what we are benchmarking
    "write",
    ratio,
    write.avg / VALUES as f64, 
    write.std / VALUES as f64,
    write.percentile_25 / VALUES as f64, 
    write.median / VALUES as f64,
    write.percentile_75 / VALUES as f64,
);
println!("{}::{}::{},{},{},{},{},{},{},{}",
    $code, stringify!($bo), table, // the informations about what we are benchmarking
    "read_buff",
    ratio,
    read_buff.avg / VALUES as f64, 
    read_buff.std / VALUES as f64,
    read_buff.percentile_25 / VALUES as f64, 
    read_buff.median / VALUES as f64,
    read_buff.percentile_75 / VALUES as f64,
);
println!("{}::{}::{},{},{},{},{},{},{},{}",
    $code, stringify!($bo), table, // the informations about what we are benchmarking
    "read_unbuff",
    ratio,
    read_unbuff.avg / VALUES as f64, 
    read_unbuff.std / VALUES as f64,
    read_unbuff.percentile_25 / VALUES as f64, 
    read_unbuff.median / VALUES as f64,
    read_unbuff.percentile_75 / VALUES as f64,
);

}};
}

/// macro to implement all combinations of bit order and table use
macro_rules! impl_code {
    ($cal:expr, $code:literal, $read:ident, $write:ident, $gen_data:ident) => {
        bench!(
            $cal, $code, $read, $write, $gen_data, M2L, false
        );
        bench!(
            $cal, $code, $read, $write, $gen_data, M2L, true
        );
        bench!(
            $cal, $code, $read, $write, $gen_data, L2M, false
        );
        bench!(
            $cal, $code, $read, $write, $gen_data, L2M, true
        );
    };
}

pub fn main() {
    // tricks to reduce the noise
    #[cfg(target_os="linux")]
    pin_to_core(5);
    //unsafe{assert_ne!(libc::nice(-20-libc::nice(0)), -1);}
    
    // figure out how much overhead we add by measuring
    let calibration = calibrate_overhead();
    // print the header of the csv
    println!("pat,type,ratio,ns_avg,ns_std,ns_perc25,ns_median,ns_perc75");

    // benchmark the buffered impl

    impl_code!(
        calibration, "unary", read_unary, write_unary, gen_unary_data
    );
    impl_code!(
        calibration, "gamma", read_gamma, write_gamma, gen_gamma_data
    );
    impl_code!(
        calibration, "delta", read_delta, write_delta, gen_delta_data
    );
    impl_code!(
        calibration, "zeta3", read_zeta3, write_zeta3, gen_zeta3_data
    );
}