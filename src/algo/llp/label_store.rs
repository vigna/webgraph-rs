/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicUsize, Ordering},
};

pub(crate) struct LabelStore {
    labels: Box<[UnsafeCell<usize>]>,
    volumes: Box<[AtomicUsize]>,
}

impl LabelStore {
    pub(crate) fn new(n: usize) -> Self {
        let mut labels = Vec::with_capacity(n);
        labels.extend((0..n).map(|_| UnsafeCell::new(0)));
        let mut volumes = Vec::with_capacity(n);
        volumes.extend((0..n).map(|_| AtomicUsize::new(0)));

        Self {
            labels: labels.into_boxed_slice(),
            volumes: volumes.into_boxed_slice(),
        }
    }

    pub(crate) fn init(&mut self) {
        for l in 0..self.labels.len() {
            *self.labels[l].get_mut() = l;
            self.volumes[l].store(1, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    pub(crate) fn volume_set(&self, node: usize, new_label: usize) {
        unsafe { *self.labels[node].get() = new_label };
        self.volumes[new_label].fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn label(&self, node: usize) -> usize {
        unsafe { *self.labels[node].get() }
    }

    #[inline(always)]
    pub(crate) fn volume_fetch_sub(&self, label: usize) -> usize {
        self.volumes[label].fetch_sub(1, Ordering::Relaxed)
    }

    pub(crate) fn labels(&self) -> &[usize] {
        unsafe { std::mem::transmute::<&[UnsafeCell<usize>], &[usize]>(&self.labels) }
    }
}

unsafe impl Send for LabelStore {}
unsafe impl Sync for LabelStore {}
