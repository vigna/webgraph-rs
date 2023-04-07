use webgraph::codes::*;
use rand::prelude::*;
use rand::distributions::Distribution;
use core::arch::x86_64::{_rdtsc, __rdtscp, __cpuid, _mm_lfence, _mm_mfence, _mm_sfence};

const VALUES: usize = 10_000;
const WARMUP_ITERS: usize = 100;
const BENCH_ITERS: usize = 10_000;
const CALIBRATION_ITERS: usize = 1_000_000;
const SEED: u64 = 0x8c2b_781f_2866_90fd;
const TSC_FREQ: u64 = 4_000_000_000;  

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

fn calibrate_rdtsc() -> u64 {
    let mut vals = Vec::with_capacity(CALIBRATION_ITERS);
    for _ in 0..CALIBRATION_ITERS {
        let start = rdtsc();
        let end = rdtsc();
        vals.push(end - start);
    }
    let mut res = 0.0;
    for val in vals {
        res += val as f64 / CALIBRATION_ITERS as f64;
    }
    eprintln!("RDTSC calibration is: {}", res);
    res as u64
}

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
let mut buffer = Vec::with_capacity(VALUES);
let mut read_time: u64 = 0;
let mut write_time: u64 = 0;
for iter in 0..(WARMUP_ITERS + BENCH_ITERS) {
    buffer.clear();
    {
        let mut r = $writer::<$bo, _>::new(
            MemWordWriteVec::new(&mut buffer)
        );
        let w_start = rdtsc();
        for value in &$data {
            r.$write::<$table>(*value).unwrap();
        }
        let w_end = rdtsc();
        if iter >= WARMUP_ITERS {
            write_time += (w_end - w_start) - $cal;
        }
    }
    {
        let mut r = $reader::<$bo, _>::new(
            MemWordRead::new(&mut buffer)
        );
        let r_start = rdtsc();
        for _ in &$data {
            r.$read::<$table>().unwrap();
        }
        let r_end = rdtsc();
        if iter >= WARMUP_ITERS {
            read_time += (r_end - r_start) - $cal;
        }
    }
}

let read_time = read_time as f64 / BENCH_ITERS as f64;
let write_time = write_time as f64 / BENCH_ITERS as f64;

let bytes = buffer.len() * 8;
let table = if $table {
    "Table"
} else {
    "NoTable"
};
println!("{}::{}::{}::{},{},{},{},{},{},{}",
    $mod_name, $code, stringify!($bo), table,
    read_time, write_time,
    read_time / TSC_FREQ as f64, 
    write_time / TSC_FREQ as f64,
    bytes as f64 / (read_time / TSC_FREQ as f64), 
    bytes as f64 / (write_time / TSC_FREQ as f64), 
);


}};
}

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

        let mut zipf = zipf::ZipfDistribution::new(1000, 2.0).unwrap();
        let gamma_data = (0..VALUES)
            .map(|_| {
                zipf.sample(&mut rng) as u64
            })
            .collect::<Vec<_>>();
        impl_code!(
            $cal, $mod_name, $reader, $writer, "gamma", read_gamma, write_gamma, gamma_data
        );
    };
}

pub fn main() {
    pin_to_core(5);
    //unsafe{assert_ne!(libc::nice(-20-libc::nice(0)), -1);}
    let calibration = calibrate_rdtsc();
    println!("pat,read_cycles,write_cycles,read_seconds,write_seconds,read_bs,write_bs");
    impl_bench!(calibration, "buffered", BufferedBitStreamRead, BufferedBitStreamWrite);
}