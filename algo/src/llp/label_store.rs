/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use rayon::prelude::*;
use std::{
    cell::Cell,
    sync::atomic::{AtomicUsize, Ordering},
};

use super::RAYON_MIN_LEN;

pub(crate) struct LabelStore {
    labels: Box<[Cell<usize>]>,
    volumes: Box<[AtomicUsize]>,
}

impl LabelStore {
    pub(crate) fn new(n: usize) -> Self {
        let mut labels = Vec::with_capacity(n);
        labels.extend((0..n).map(|_| Cell::new(0)));
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
            .with_min_len(RAYON_MIN_LEN)
            .for_each(|v| v.store(1, Ordering::Relaxed));
        self.labels
            .par_iter_mut()
            .with_min_len(RAYON_MIN_LEN)
            .enumerate()
            .for_each(|(i, l)| *l.get_mut() = i);
    }

    #[inline(always)]
    pub(crate) fn label(&self, node: usize) -> usize {
        self.labels[node].get()
    }

    #[inline(always)]
    pub(crate) fn volume(&self, node: usize) -> usize {
        self.volumes[node].load(Ordering::Relaxed)
    }

    /// Updates the label of a node.
    #[inline(always)]
    pub(crate) fn update(&self, node: usize, new_label: usize) {
        let old_label = self.labels[node].replace(new_label);
        self.volumes[old_label].fetch_sub(1, Ordering::Relaxed);
        self.volumes[new_label].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn labels_and_volumes(&mut self) -> (&mut [usize], &mut [usize]) {
        // SAFETY: Cell<usize> and usize have the same layout, so the transmutes are valid.
        unsafe {
            (
                // This is just a transparent wrapper
                std::mem::transmute::<&mut [Cell<usize>], &mut [usize]>(&mut self.labels),
                // Transmuting &mut from atomic to non-atomic is sound
                std::mem::transmute::<&mut [AtomicUsize], &mut [usize]>(&mut self.volumes),
            )
        }
    }
}

// SAFETY: LabelStore uses Cell for interior mutability but access is controlled externally.
unsafe impl Send for LabelStore {}
unsafe impl Sync for LabelStore {}
