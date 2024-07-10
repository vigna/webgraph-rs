/*
 * SPDX-FileCopyrightText: 2023 Inria
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::*;
use core::sync::atomic::{AtomicUsize, Ordering};

fn len_golomb(value: u64, b: u64) -> usize {
    (value / b) as usize + 1 + len_minimal_binary(value % b, b)
}

#[derive(Default, Clone, Debug)]
/// Keeps track of the space needed to store a stream of integers using different codes.
///
/// This structure can be used to determine empirically which code
/// provides the best compression for a given stream.
pub struct CodesStats {
    pub unary: AtomicUsize,
    pub gamma: AtomicUsize,
    pub delta: AtomicUsize,
    pub zeta: [AtomicUsize; 10],
    pub golomb: [AtomicUsize; 20],
}

impl CodesStats {
    /// Update the stats with the lengths of the codes for `n` and return back
    /// `n` for convenience.
    pub fn update(&self, n: u64) -> u64 {
        self.unary.fetch_add(n as usize + 1, Ordering::Relaxed);
        self.gamma.fetch_add(len_gamma(n), Ordering::Relaxed);
        self.delta.fetch_add(len_delta(n), Ordering::Relaxed);

        for (k, val) in self.zeta.iter().enumerate() {
            val.fetch_add(len_zeta(n, (k + 1) as _), Ordering::Relaxed);
        }
        for (b, val) in self.golomb.iter().enumerate() {
            val.fetch_add(len_golomb(n, (b + 1) as _), Ordering::Relaxed);
        }
        n
    }

    /// Return the best code for the stream and its space usage.
    pub fn get_best_code(&self) -> (Code, usize) {
        // TODO!: make cleaner
        let mut best = self.unary.load(Ordering::Relaxed);
        let mut best_code = Code::Unary;

        macro_rules! check {
            ($code:expr, $len:expr) => {
                let len = $len.load(Ordering::Relaxed);
                if len < best {
                    best = len;
                    best_code = $code;
                }
            };
        }

        check!(Code::Gamma, self.gamma);
        check!(Code::Delta, self.delta);

        for (k, val) in self.zeta.iter().enumerate() {
            check!(Code::Zeta { k: (k + 1) as _ }, *val);
        }
        for (b, val) in self.golomb.iter().enumerate() {
            check!(Code::Golomb { b: (b + 1) as _ }, *val);
        }

        (best_code, best)
    }
}
