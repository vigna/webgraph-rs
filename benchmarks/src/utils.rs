use super::*;

/// Routine for measuring the measurement overhead.
pub fn calibrate_overhead() -> u128 {
    let mut nanos = MetricsStream::with_capacity(CALIBRATION_ITERS);
    // For many times, measure an empty block 
    for _ in 0..CALIBRATION_ITERS {
        let start = Instant::now();
        let delta = start.elapsed().as_nanos();
        nanos.update(delta as f64);
    }
    let measures = nanos.finalize();
    eprintln!("Timesource calibration is: {:#4?}", measures);
    measures.avg as u128
}

/// Pin the process to one core to avoid context switching and caches flushes
/// which would result in noise in the measurement.
#[cfg(target_os="linux")]
pub fn pin_to_core(core_id: usize) {
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