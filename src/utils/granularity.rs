/*
 * SPDX-FileCopyrightText: 2025 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// Relative or absolute specification of the granularity of parallel tasks.
///
/// This enum provides a simple (and possibly size-independent) way of
/// specifying the granularity of parallel tasks. It is used by
/// [`SequentialLabeling::par_apply`](crate::traits::SequentialLabeling::par_apply)
/// and
/// [`SequentialLabeling::par_node_apply`](crate::traits::SequentialLabeling::par_node_apply).
/// The method [`Granularity::granularity`] will return an
/// appropriate value depending on the variant, on the number of elements to
/// process and on the number of threads.
#[derive(Debug, Clone, Copy)]
pub enum Granularity {
    /// Absolute granularity.
    Absolute(usize),
    /// Relative granularity.
    ///
    /// Granularity will be first determined by the overall number of element to
    /// process, divided by the number of threads multiplied by the slack; then,
    /// the resulting granularity will be clamped between a minimum and a
    /// maximum value.
    Relative {
        slack: f64,
        min_len: usize,
        max_len: usize,
    },
}

impl core::default::Default for Granularity {
    /// Return a default relative granularity with slack factor 4,
    /// minimum length 1000, and maximum length 1000000.
    fn default() -> Self {
        Self::Relative {
            slack: 4.0,
            min_len: 1000,
            max_len: 1000000,
        }
    }
}

impl Granularity {
    /// Return a granularity for a given number of elements and threads.
    ///
    /// * [`Granularity::Absolute`]: granularity is just the fixed value.
    ///   variant, this method will just return its fixed
    /// * [`Granularity::Relative`]: granularity will be first given by the
    /// overall number of element to process, divided by the number of threads
    /// multiplied by the slack; then, the resulting granularity will be clamped
    /// between a minimum and a maximum value.
    pub fn granularity(&self, num_elements: usize, num_threads: usize) -> usize {
        match *self {
            Granularity::Absolute(fixed) => fixed,
            Granularity::Relative {
                slack,
                min_len,
                max_len,
            } => {
                let tasks = ((num_threads as f64 * slack) as usize).max(1);
                (num_elements / tasks).max(min_len).min(max_len)
            }
        }
    }
}
