use log::info;
use std::fmt::{Display, Formatter, Result};
use std::time::{Duration, Instant};

/// A tunable progress logger to log progress information about long-lasting activities.
/// It is a port of the Java class `it.unimi.dsi.util.ProgressLogger` from the [DSI Utilities](https://dsiutils.di.unimi.it/).
///
/// Once you create a progress logger, you can set the name of the counted items, the log interval and
/// optionally the expected number of items, which will be used to estimate the completion time.
///
/// To log the progress of an activity, you call [`ProgressLogger::start`]. Then, each time you want to mark progress,
/// you call usually [`xProgressLogger::update`], which increases the item counter, and will log progress information
/// if enough time has passed since the last log. The time check happens only on multiples of
/// [`ProgressLogger::LIGHT_UPDATE_MASK`] + 1 in the case of [`ProgressLogger::light_update`], which should be used when the activity
///
/// At any time, displaying the progress logger will give you time information up to the present.
/// When the activity is over, you call [`ProgressLogger::stop`], which fixes the final time, and
/// possibly display the logger. [`ProgressLogger::done`] will stop the logger and log the final data.
///
/// After you finished a run of the progress logger, you can change its attributes and call [`ProgressLogger::start`]
/// again to measure another activity.
///
/// A typical call sequence to a progress logger is as follows:
/// ```
/// use webgraph::utils::ProgressLogger;
/// let mut pl = ProgressLogger::default();
/// pl.name = "pumpkins".to_string();
/// pl.start("Smashing pumpkins...");
/// for _ in 0..100 {
/// 	// do something on each pumlkin
/// 	pl.update();
/// }
/// pl.done();
/// ```
/// A progress logger can also be used as a handy timer:
/// ```
/// use webgraph::utils::ProgressLogger;
/// let mut pl = ProgressLogger::default();
/// pl.name = "pumpkins".to_string();
/// pl.start("Smashing pumpkins...");
/// for _ in 0..100 {
/// 	// do something on each pumlkin
/// }
/// pl.done_with_count(100);
/// ```
///

pub struct ProgressLogger {
    pub name: String,
    pub log_interval: Duration,
    pub expected_updates: Option<usize>,
    start: Option<Instant>,
    next_log_time: Instant,
    stop_time: Option<Instant>,
    count: usize,
}

impl Default for ProgressLogger {
    fn default() -> Self {
        Self {
            name: "items".to_string(),
            log_interval: Duration::from_secs(10),
            expected_updates: None,
            start: None,
            next_log_time: Instant::now(),
            stop_time: None,
            count: 0,
        }
    }
}

impl ProgressLogger {
    const LIGHT_UPDATE_MASK: usize = (1 << 10) - 1;

    pub fn start<T: AsRef<str>>(&mut self, msg: T) {
		let now = Instant::now();
        self.start = Some(now);
        self.stop_time = None;
        self.next_log_time = now + self.log_interval;
        info!("{}", msg.as_ref());
    }

    fn update_if(&mut self) {
        let now = Instant::now();
        if self.next_log_time <= now {
            info!("{}", self);

            self.next_log_time = now + self.log_interval;
        }
    }

    pub fn light_update(&mut self) {
        self.count += 1;
        if (self.count & Self::LIGHT_UPDATE_MASK) == 0 {
            self.update_if();
        }
    }

    pub fn update(&mut self) {
        self.count += 1;
        self.update_if();
    }

    pub fn update_and_display(&mut self) {
        self.count += 1;
        info!("{}", self);
        self.next_log_time = Instant::now() + self.log_interval;
    }

    pub fn done_with_count(&mut self, count: usize) {
        self.count = count;
        self.done();
    }

    pub fn stop(&mut self) {
        self.stop_time = Some(Instant::now());
        self.expected_updates = None;
    }

    pub fn done(&mut self) {
        self.stop();
        info!("Completed.");
        // just to avoid wrong reuses
        info!("{}", self);
    }
}

impl Display for ProgressLogger {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let now = self.stop_time.unwrap_or_else(|| Instant::now());
        if let Some(start) = self.start {
            let elapsed = now - start;
            let rate = self.count as f64 / elapsed.as_secs_f64();
            let speed_in_ns = 1.0E9 / rate;
            write!(
                f,
                "{count} {name}, {speed_in_ns} ns/{name} {rate} {name}/s",
                count = self.count,
                name = self.name
            )
        } else {
            write!(f, "ProgressLogger not started")
        }
    }
}
