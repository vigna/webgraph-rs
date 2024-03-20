/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use anyhow::ensure;
use predicates::{reflection::PredicateReflection, Predicate};
use std::fmt::{Display, Write};

pub struct PredParams {
    pub num_nodes: usize,
    pub num_arcs: u64,
    pub gain: f64,
    pub modified: usize,
    pub update: usize,
}

#[derive(Debug, Clone)]
pub struct MaxUpdates {
    pub max_updates: usize,
}

impl MaxUpdates {
    pub const DEFAULT_MAX_UPDATES: usize = usize::MAX;
}

impl From<Option<usize>> for MaxUpdates {
    fn from(max_updates: Option<usize>) -> Self {
        match max_updates {
            Some(max_updates) => MaxUpdates { max_updates },
            None => Self::default(),
        }
    }
}

impl From<usize> for MaxUpdates {
    fn from(max_updates: usize) -> Self {
        Some(max_updates).into()
    }
}

impl Default for MaxUpdates {
    fn default() -> Self {
        Self::from(Self::DEFAULT_MAX_UPDATES)
    }
}

impl Display for MaxUpdates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("(max updates: {})", self.max_updates))
    }
}

impl PredicateReflection for MaxUpdates {}
impl Predicate<PredParams> for MaxUpdates {
    fn eval(&self, pred_params: &PredParams) -> bool {
        pred_params.update >= self.max_updates
    }
}

#[derive(Debug, Clone)]
pub struct MinGain {
    pub threshold: f64,
}

impl MinGain {
    pub const DEFAULT_THRESHOLD: f64 = 0.001;
}

impl TryFrom<Option<f64>> for MinGain {
    type Error = anyhow::Error;
    fn try_from(threshold: Option<f64>) -> anyhow::Result<Self> {
        Ok(match threshold {
            Some(threshold) => {
                ensure!(!threshold.is_nan());
                ensure!(threshold >= 0.0, "The threshold must be nonnegative");
                MinGain { threshold }
            }
            None => Self::default(),
        })
    }
}

impl TryFrom<f64> for MinGain {
    type Error = anyhow::Error;
    fn try_from(threshold: f64) -> anyhow::Result<Self> {
        Some(threshold).try_into()
    }
}

impl Default for MinGain {
    fn default() -> Self {
        Self::try_from(Self::DEFAULT_THRESHOLD).unwrap()
    }
}

impl Display for MinGain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("(min gain: {})", self.threshold))
    }
}

impl PredicateReflection for MinGain {}
impl Predicate<PredParams> for MinGain {
    fn eval(&self, pred_params: &PredParams) -> bool {
        pred_params.gain < self.threshold
    }
}

#[derive(Debug, Clone, Default)]
pub struct MinModified {}

impl Display for MinModified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("(min modified: √n)")
    }
}

impl PredicateReflection for MinModified {}
impl Predicate<PredParams> for MinModified {
    fn eval(&self, pred_params: &PredParams) -> bool {
        (pred_params.modified as f64) <= (pred_params.num_nodes as f64).sqrt()
    }
}
