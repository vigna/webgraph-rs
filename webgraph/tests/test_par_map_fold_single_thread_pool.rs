/*
 * SPDX-FileCopyrightText: 2026 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Isolated regression test for `par_map_fold` in a one-thread Rayon pool.
//!
//! This test builds a one-thread Rayon pool, which perturbs Miri's cooperative
//! scheduler enough to trip experimental Tree Borrows in the ordered
//! `par_map_fold` tests when they are co-scheduled in the same binary (a
//! protector conflict in rayon-core's `in_place_scope` teardown). Running it in
//! its own binary keeps the deadlock regression under both normal CI and Miri
//! without that cross-test interaction.

use webgraph::traits::par_map_fold::ParMapFold;

#[test]
fn test_par_map_fold_single_thread_pool() {
    // Regression: with a one-thread Rayon pool the caller occupied the only
    // pool thread, the scoped workers never ran, and the bounded input
    // channel deadlocked. Run in a helper thread so a regression fails
    // instead of hanging the suite.
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let sum: usize = pool.install(|| (0..100usize).par_map_fold(|x| x, |a, b| a + b));
        tx.send(sum).unwrap();
    });
    let sum = rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("par_map_fold deadlocked in a single-thread pool");
    // Once the worker has sent its result, join it so its Rayon pool is torn
    // down before we assert the sum (on a deadlock the recv_timeout above
    // already failed the test).
    handle.join().expect("single-thread pool worker panicked");
    assert_eq!(sum, 4950);
}
