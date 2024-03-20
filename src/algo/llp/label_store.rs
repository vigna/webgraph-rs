/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct LabelStore {
    pub(crate) labels: Box<[AtomicUsize]>,
    volumes: Box<[AtomicUsize]>,
}

impl LabelStore {
    pub(crate) fn new(n: usize) -> Self {
        let mut labels = Vec::with_capacity(n);
        labels.extend((0..n).map(|_| AtomicUsize::new(0)));
        let mut volumes = Vec::with_capacity(n);
        volumes.extend((0..n).map(|_| AtomicUsize::new(0)));

        Self {
            labels: labels.into_boxed_slice(),
            volumes: volumes.into_boxed_slice(),
        }
    }

    pub(crate) fn init(&self) {
        for l in 0..self.labels.len() {
            self.labels[l].store(l, Ordering::Relaxed);
            self.volumes[l].store(1, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    pub(crate) fn set(&self, node: usize, new_label: usize) {
        let old_label = self.labels[node].swap(new_label, Ordering::Relaxed);
        self.volumes[old_label].fetch_sub(1, Ordering::Relaxed);
        self.volumes[new_label].fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn label(&self, node: usize) -> usize {
        self.labels[node].load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub(crate) fn volume(&self, label: usize) -> usize {
        self.volumes[label].load(Ordering::Relaxed)
    }
}

unsafe impl Send for LabelStore {}
unsafe impl Sync for LabelStore {}
