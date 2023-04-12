use webgraph::codes::*;
use rand::Rng;
use core::arch::x86_64::{__rdtscp, __cpuid, _mm_lfence, _mm_mfence, _mm_sfence};

/// How many random codes we will write and read in the benchmark
const VALUES: usize = 10_000;
/// How many iterations to do before starting measuring, this is done to warmup
/// the caches and the branch predictor
const WARMUP_ITERS: usize = 100;
/// How many iterations of measurement we will execute
const BENCH_ITERS: usize = 1_000;
/// For how many times we will measure the measurement overhead
const CALIBRATION_ITERS: usize = 1_000_000;
/// The TimeStampCounter frequency in Hertz. 
/// find tsc freq with `dmesg | grep tsc` or `journalctl | grep tsc` 
/// and convert it to hertz
const TSC_FREQ: u64 = 3_609_600_000;

/// This is our 
fn rdtsc() -> u64 {
    unsafe{
        let mut aux: u32 = 0;
        let _ = __cpuid(0);
        let _ = _mm_lfence();
        let _ = _mm_mfence();
        let _ = _mm_sfence();
        __rdtscp(&mut aux as *mut u32)
    }
}

/// Routine for measuring the measurement overhead. This is usually around
/// 147 cycles.
fn calibrate_rdtsc() -> u64 {
    let mut vals = Vec::with_capacity(CALIBRATION_ITERS);
    // For many times, measure an empty block 
    for _ in 0..CALIBRATION_ITERS {
        let start = rdtsc();
        let end = rdtsc();
        vals.push(end - start);
    }
    // compute the mean
    let mut res = 0.0;
    for val in vals {
        res += val as f64 / CALIBRATION_ITERS as f64;
    }
    eprintln!("RDTSC calibration is: {}", res);
    res as u64
}

/// Pin the process to one core to avoid context switching and caches flushes
/// which would result in noise in the measurement.
fn pin_to_core(core_id: usize) {
    unsafe{
        let mut cpu_set = core::mem::MaybeUninit::zeroed().assume_init();
        libc::CPU_ZERO(&mut cpu_set);
        libc::CPU_SET(core_id, &mut cpu_set);
        let res = libc::sched_setaffinity(
            libc::getpid(), 
            core::mem::size_of::<libc::cpu_set_t>(), 
            &cpu_set as *const libc::cpu_set_t,
        );
        assert_ne!(res, -1);
    }    
}

macro_rules! bench {
    ($cal:expr, $mod_name:literal, $reader:ident, $writer:ident, $code:literal, $read:ident, $write:ident, $data:expr, $bo:ident, $table:expr) => {{
// the memory where we will write values
let mut buffer = Vec::with_capacity(VALUES);
// counters for the total read time and total write time
let mut read_cycles_max: f64 = 0.0;
let mut read_cycles_avg: f64 = 0.0;
let mut read_cycles_min: f64 = f64::INFINITY;
let mut read_cycles_squares: f64 = 0.0;

let mut write_cycles_max: f64 = 0.0;
let mut write_cycles_avg: f64 = 0.0;
let mut write_cycles_min: f64 = f64::INFINITY;
let mut write_cycles_squares: f64 = 0.0;
// measure
for iter in 0..(WARMUP_ITERS + BENCH_ITERS) {
    buffer.clear();
    // write the codes
    {   
        // init the writer
        let mut r = $writer::<$bo, _>::new(
            MemWordWriteVec::new(&mut buffer)
        );
        // measure
        let w_start = rdtsc();
        for value in &$data {
            r.$write::<$table>(*value).unwrap();
        }
        let w_end = rdtsc();
        // add the measurement if we are not in the warmup
        if iter >= WARMUP_ITERS {
            let cycles = ((w_end - w_start) - $cal) as f64;
            write_cycles_avg += cycles /  BENCH_ITERS as f64;
            write_cycles_squares += cycles*cycles / BENCH_ITERS as f64;
            write_cycles_max = write_cycles_max.max(cycles);
            write_cycles_min = write_cycles_min.min(cycles);
        }
    }
    // read the codes
    {
        // init the reader
        let mut r = $reader::<$bo, _>::new(
            MemWordRead::new(&mut buffer)
        );
        // measure
        let r_start = rdtsc();
        for _ in &$data {
            r.$read::<$table>().unwrap();
        }
        let r_end = rdtsc();
        // add the measurement if we are not in the warmup
        if iter >= WARMUP_ITERS {
            let cycles = ((r_end - r_start) - $cal) as f64;
            read_cycles_avg += cycles /  BENCH_ITERS as f64;
            read_cycles_squares += cycles*cycles / BENCH_ITERS as f64;
            read_cycles_max = read_cycles_max.max(cycles);
            read_cycles_min = read_cycles_min.min(cycles);
        }
    }
}
// convert from cycles to nano seconds
let read_time_avg  = read_cycles_avg * 1e9 / TSC_FREQ as f64;
let write_time_avg = write_cycles_avg * 1e9 / TSC_FREQ as f64;
let read_time_min  = read_cycles_min * 1e9 / TSC_FREQ as f64;
let write_time_min = write_cycles_min * 1e9 / TSC_FREQ as f64;
let read_time_max  = read_cycles_max * 1e9 / TSC_FREQ as f64;
let write_time_max = write_cycles_max * 1e9 / TSC_FREQ as f64;
let read_time_squares  = read_cycles_squares  * 1e9 / TSC_FREQ as f64;
let write_time_squares = write_cycles_squares * 1e9 / TSC_FREQ as f64;

// compute the stds
let read_time_std = (read_time_squares - read_time_avg*read_time_avg).sqrt();
let write_time_std = (write_time_squares - write_time_avg*write_time_avg).sqrt();

let bytes = buffer.len() * 8;
let table = if $table {
    "Table"
} else {
    "NoTable"
};
// print the results
println!("{}::{}::{}::{},{},{},{},{},{},{},{},{},{}",
    $mod_name, $code, stringify!($bo), table, // the informations about what we are benchmarking
    bytes,
    read_time_avg / VALUES as f64, 
    read_time_std / VALUES as f64, 
    read_time_max / VALUES as f64, 
    read_time_min / VALUES as f64,
    write_time_avg / VALUES as f64, 
    write_time_std / VALUES as f64,
    write_time_max / VALUES as f64, 
    write_time_min / VALUES as f64,
);


}};
}

/// macro to implement all combinations of bit order and table use
macro_rules! impl_code {
    ($cal:expr, $mod_name:literal, $reader:ident, $writer:ident, $code:literal, $read:ident, $write:ident, $data:expr) => {
        bench!(
            $cal, $mod_name, $reader, $writer, $code, $read, $write, $data, M2L, false
        );
        bench!(
            $cal, $mod_name, $reader, $writer, $code, $read, $write, $data, M2L, true
        );
        bench!(
            $cal, $mod_name, $reader, $writer, $code, $read, $write, $data, L2M, false
        );
        bench!(
            $cal, $mod_name, $reader, $writer, $code, $read, $write, $data, L2M, true
        );
    };
}

/// macro to implement the benchmarking of all the codes for the current backend
macro_rules! impl_bench {
    ($cal:expr, $mod_name:literal, $reader:ident, $writer:ident) => {
        let mut rng = rand::thread_rng();
        
        let unary_data = (0..VALUES)
            .map(|_| {
                let v: u64 = rng.gen();
                v.trailing_zeros() as u64
            })
            .collect::<Vec<_>>();

        impl_code!(
            $cal, $mod_name, $reader, $writer, "unary", read_unary, write_unary, unary_data
        );

        let gamma_data = (0..VALUES)
            .map(|_| {
                rng.sample(rand_distr::Zeta::new(2.0).unwrap()) as u64
            })
            .collect::<Vec<_>>();
        impl_code!(
            $cal, $mod_name, $reader, $writer, "gamma", read_gamma, write_gamma, gamma_data
        );

        let delta_data = (0..VALUES)
            .map(|_| {
                rng.sample(rand_distr::Zeta::new(1.01).unwrap()) as u64
            })
            .collect::<Vec<_>>();
        impl_code!(
            $cal, $mod_name, $reader, $writer, "delta", read_delta, write_delta, delta_data
        );
    };
}

pub fn main() {
    // tricks to reduce the noise
    pin_to_core(5);
    //unsafe{assert_ne!(libc::nice(-20-libc::nice(0)), -1);}
    
    // figure out how much overhead we add by measuring
    let calibration = calibrate_rdtsc();
    // print the header of the csv
    print!("pat,bytes,");
    print!("read_ns_avg,read_ns_std,read_ns_max,read_ns_min,");
    print!("write_ns_avg,write_ns_std,write_ns_max,write_ns_min");
    print!("\n");

    // benchmark the buffered impl
    impl_bench!(calibration, "buffered", BufferedBitStreamRead, BufferedBitStreamWrite);
}