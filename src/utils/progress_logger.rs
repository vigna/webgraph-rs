use log::info;
use num_format::{Locale, ToFormattedString};
use pluralizer::pluralize;
use std::fmt::{Display, Formatter, Result};
use std::time::{Duration, Instant};

#[derive(Debug, Copy, Clone)]

pub enum TimeUnit {
    NanoSeconds,
    MicroSeconds,
    MilliSeconds,
    Seconds,
    Minutes,
    Hours,
    Days,
}

pub const TIME_UNITS: [TimeUnit; 7] = [
    TimeUnit::NanoSeconds,
    TimeUnit::MicroSeconds,
    TimeUnit::MilliSeconds,
    TimeUnit::Seconds,
    TimeUnit::Minutes,
    TimeUnit::Hours,
    TimeUnit::Days,
];

impl TimeUnit {
    pub fn label(&self) -> &'static str {
        match self {
            TimeUnit::NanoSeconds => "ns",
            TimeUnit::MicroSeconds => "μs",
            TimeUnit::MilliSeconds => "ms",
            TimeUnit::Seconds => "s",
            TimeUnit::Minutes => "m",
            TimeUnit::Hours => "h",
            TimeUnit::Days => "d",
        }
    }

    pub fn to_seconds(&self) -> f64 {
        match self {
            TimeUnit::NanoSeconds => 1.0e-9,
            TimeUnit::MicroSeconds => 1.0e-6,
            TimeUnit::MilliSeconds => 1.0e-3,
            TimeUnit::Seconds => 1.0,
            TimeUnit::Minutes => 60.0,
            TimeUnit::Hours => 3600.0,
            TimeUnit::Days => 86400.0,
        }
    }

    pub fn nice_time_unit(seconds: f64) -> Self {
        for unit in TIME_UNITS.iter().rev() {
            if seconds >= unit.to_seconds() {
                return *unit;
            }
        }
        TimeUnit::NanoSeconds
    }

    pub fn nice_speed_unit(seconds: f64) -> Self {
        for unit in TIME_UNITS[3..].iter() {
            if seconds <= unit.to_seconds() {
                return *unit;
            }
        }
        TimeUnit::NanoSeconds
    }

    pub fn pretty_print(milliseconds: u128) -> String {
        let mut result = String::new();

        if milliseconds < 1000 {
            return format!("{}ms", milliseconds);
        }

        let mut seconds = milliseconds / 1000;

        for unit in [TimeUnit::Days, TimeUnit::Hours, TimeUnit::Minutes] {
            let to_seconds = unit.to_seconds() as u128;
            if seconds >= to_seconds {
                result.push_str(&format!("{}{} ", seconds / to_seconds, unit.label(),));
                seconds %= to_seconds;
            }
        }

        result.push_str(&format!("{}s", seconds));

        result
    }
}

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
    pub time_unit: Option<TimeUnit>,
    pub local_speed: bool,
    start: Option<Instant>,
    last_log_time: Instant,
    next_log_time: Instant,
    stop_time: Option<Instant>,
    count: usize,
    last_count: usize,
}

impl Default for ProgressLogger {
    fn default() -> Self {
        Self {
            name: "items".to_string(),
            log_interval: Duration::from_secs(10),
            expected_updates: None,
            time_unit: None,
            local_speed: false,
            start: None,
            last_log_time: Instant::now(),
            next_log_time: Instant::now(),
            stop_time: None,
            count: 0,
            last_count: 0,
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
            self.last_count = self.count;
            self.last_log_time = now;
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

    fn fmt_timing_speed(&self, f: &mut Formatter<'_>, seconds_per_item: f64) -> Result {
        let items_per_second = 1.0 / seconds_per_item;

        let time_unit_timing = self
            .time_unit
            .unwrap_or_else(|| TimeUnit::nice_time_unit(seconds_per_item));

        let time_unit_speed = self
            .time_unit
            .unwrap_or_else(|| TimeUnit::nice_speed_unit(seconds_per_item));

        f.write_fmt(format_args!(
            "{:.2} {}/{}, {:.2} {}/{}",
            seconds_per_item / time_unit_timing.to_seconds(),
            time_unit_timing.label(),
            self.name,
            items_per_second * time_unit_speed.to_seconds(),
            pluralize(&self.name, 2, false),
            time_unit_speed.label()
        ))?;

        Ok(())
    }
}

impl Display for ProgressLogger {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let now = self.stop_time.unwrap_or_else(|| Instant::now());
        if let Some(start) = self.start {
            let elapsed = now - start;

            f.write_fmt(format_args!(
                "{} {}, {}, ",
                self.count.to_formatted_string(&Locale::en),
                pluralize(&self.name, self.count as isize, false),
                TimeUnit::pretty_print(elapsed.as_millis()),
            ))?;

            let seconds_per_item = elapsed.as_secs_f64() / self.count as f64;
            self.fmt_timing_speed(f, seconds_per_item)?;

            if let Some(expected_updates) = self.expected_updates {
                let millis_to_end: u128 = ((expected_updates - self.count) as u128
                    * elapsed.as_millis())
                    / (self.count as u128 + 1);
                f.write_fmt(format_args!(
                    "; {:.2}% done, {} to end",
                    100.0 * self.count as f64 / expected_updates as f64,
                    TimeUnit::pretty_print(millis_to_end)
                ))?;
            }

            if self.local_speed && self.stop_time.is_none() {
                f.write_fmt(format_args!(" ["))?;

                let elapsed = now - self.last_log_time;
                let seconds_per_item =
                    elapsed.as_secs_f64() / (self.count - self.last_count) as f64;
                self.fmt_timing_speed(f, seconds_per_item)?;

                f.write_fmt(format_args!("]"))?;
            }

            Ok(())
        } else {
            write!(f, "ProgressLogger not started")
        }
    }
}
