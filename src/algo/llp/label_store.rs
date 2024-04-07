/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use rayon::prelude::*;
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
        self.volumes
            .par_iter()
            .with_min_len(1024)
            .for_each(|v| v.store(1, Ordering::Relaxed));
        self.labels
            .par_iter_mut()
            .enumerate()
            .with_min_len(1024)
            .for_each(|(i, l)| *l.get_mut() = i);
    }

    #[inline(always)]
    pub(crate) fn label(&self, node: usize) -> usize {
        unsafe { *self.labels[node].get() }
    }

    #[inline(always)]
    pub(crate) fn volume(&self, node: usize) -> usize {
        self.volumes[node].load(Ordering::Relaxed)
    }

    /// Updates the label of a node.
    #[inline(always)]
    pub(crate) fn update(&self, node: usize, new_label: usize) {
        let old_label = unsafe { core::mem::replace(&mut *self.labels[node].get(), new_label) };
        self.volumes[old_label].fetch_sub(1, Ordering::Relaxed);
        self.volumes[new_label].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn labels(&mut self) -> &mut [usize] {
        unsafe { std::mem::transmute::<&mut [UnsafeCell<usize>], &mut [usize]>(&mut self.labels) }
    }
}

unsafe impl Send for LabelStore {}
unsafe impl Sync for LabelStore {}
