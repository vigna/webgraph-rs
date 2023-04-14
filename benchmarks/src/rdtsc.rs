//! An implementation of Instant that exploits the `rtdscp` instruction to get
//! more precise measurements that are not effected by CPU frequency swings
pub struct Instant(u64);
    
impl Instant {
    #[inline(always)]
    fn now() -> Self {
        Self(rdtsc())
    }

    fn elapsed(&self) -> Duration {
        Duration(rdtsc() - self.0)
    }
}

pub struct Duration(u64);

impl Duration {
    fn as_nanos(&self) -> u128 {
        /// The TimeStampCounter frequency in Hertz. 
        /// find tsc freq with `dmesg | grep tsc` or `journalctl | grep tsc` 
        /// and convert it to hertz
        const TSC_FREQ: u128 = 3_609_600_000;
        const TO_NS: u128 = 1_000_000_000;
        self.0 as u128 * TO_NS / TSC_FREQ
    }
}

#[inline(always)]
fn rdtsc() -> u64 {
    
    use core::arch::x86_64::{
        __rdtscp, __cpuid, 
        _mm_lfence, _mm_mfence, _mm_sfence
    };
    
    unsafe{
        let mut aux: u32 = 0;
        let _ = __cpuid(0);
        let _ = _mm_lfence();
        let _ = _mm_mfence();
        let _ = _mm_sfence();
        __rdtscp(&mut aux as *mut u32)
    }
}