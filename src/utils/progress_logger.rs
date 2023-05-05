//! ProgressLogger is a simple utility to log progress of a long running task.

use log::info;
use std::time::{Instant,Duration};
use std::fmt::{Display,Formatter,Result};

pub struct ProgressLogger { 
	start: Instant,
	next_log_time: Instant,
	log_interval: Duration,
	count: usize,
	pub expected_updates: usize,
	name: String,	
}

impl ProgressLogger {
	const LIGHT_UPDATE_MASK: usize = (1 << 10) - 1;

	pub fn new<S: ToString>(name: S) -> Self {
		ProgressLogger {
			start: Instant::now(),
			next_log_time: Instant::now(),
			log_interval: Duration::from_secs(10),
			count: 0,
			expected_updates: usize::MAX,
			name: name.to_string()
		}
	}

	pub fn start<T: AsRef<str>>(&mut self, msg: T) {
		self.start = Instant::now();
		self.next_log_time = self.start + self.log_interval;
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

	pub fn done<T: AsRef<str>>(&mut self, opt_msg: Option<T>) {
		if let Some(msg) = opt_msg {
			info!("{}", msg.as_ref());
		}
		// just to avoid wrong reuses
		self.expected_updates = usize::MAX;
		info!("{}", self);
	}
}	

impl Display for ProgressLogger {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result {
		let now = Instant::now();
		let elapsed = now - self.start;
		let rate = self.count as f64 / elapsed.as_secs_f64();
		let speed_in_ns = 1.0E9 / rate;
		write!(f, "{count} {name}, {speed_in_ns} ns/{name} {rate} {name}/s", count = self.count, name = self.name)
	}
}