/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Predicates implementing stopping conditions.
//!
//! The implementation of [layered label propagation] requires a [predicate] to
//! stop the algorithm. This module provides a few such predicates: they
//! evaluate to true if the updates should be stopped.
//!
//! You can combine the predicates using the `and` and `or` methods provided by
//! the [`Predicate`] trait.
//!
//! # Examples
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use predicates::prelude::*;
//! use webgraph_algo::llp::preds::{MinGain, MaxUpdates};
//!
//! let mut predicate = MinGain::try_from(0.001)?.boxed();
//! predicate = predicate.or(MaxUpdates::from(100)).boxed();
//! #     Ok(())
//! # }
//! ```
//!
//! [layered label propagation]: super
//! [predicate]: Predicate

use anyhow::ensure;
use predicates::{Predicate, reflection::PredicateReflection};
use std::fmt::Display;

#[doc(hidden)]
/// This structure is passed to stopping predicates to provide the information
/// that is needed to evaluate them.
#[derive(Debug)]
pub struct PredParams {
    pub num_nodes: usize,
    pub num_arcs: u64,
    pub gain: f64,
    pub avg_gain_impr: f64,
    pub modified: usize,
    pub update: usize,
}

/// Stops after at most the provided number of updates for a given ɣ.
#[derive(Debug, Clone)]
pub struct MaxUpdates {
    max_updates: usize,
}

impl MaxUpdates {
    /// The default maximum number of updates.
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
        pred_params.update + 1 >= self.max_updates
    }
}

threshold_predicate! {
    /// Stops if the gain of the objective function is below the given threshold.
    ///
    /// The [default threshold] is the same as that of the Java implementation.
    ///
    /// [default threshold]: Self::DEFAULT_THRESHOLD
    MinGain,
    default: 0.001,
    label: "min gain",
    check: |threshold| { ensure!(threshold >= 0.0, "The threshold must be nonnegative"); },
    eval: impl<> Predicate<PredParams> |pred_params| pred_params.gain,
}

threshold_predicate! {
    /// Stops if the average improvement of the gain of the objective function on
    /// a window of ten updates is below the given threshold.
    ///
    /// This criterion is a second-order version of [`MinGain`]. It is very useful
    /// to avoid a large number of iterations which do not improve the objective
    /// function significantly.
    MinAvgImprov,
    default: 0.1,
    label: "min avg gain improvement",
    check: |threshold| {},
    eval: impl<> Predicate<PredParams> |pred_params| pred_params.avg_gain_impr,
}

/// Stops after the number of modified nodes falls below the square root of the
/// number of nodes.
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

/// Stops after the number of modified nodes falls below
/// a specified percentage of the number of nodes.
#[derive(Debug, Clone, Default)]
pub struct PercModified {
    threshold: f64,
}

impl Display for PercModified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("(min modified: {}%)", self.threshold * 100.0))
    }
}

impl TryFrom<f64> for PercModified {
    type Error = anyhow::Error;
    fn try_from(threshold: f64) -> anyhow::Result<Self> {
        ensure!(
            threshold >= 0.0,
            "The percent threshold must be nonnegative"
        );
        ensure!(
            threshold <= 100.0,
            "The percent threshold must be at most 100"
        );
        Ok(PercModified {
            threshold: threshold / 100.0,
        })
    }
}

impl PredicateReflection for PercModified {}
impl Predicate<PredParams> for PercModified {
    fn eval(&self, pred_params: &PredParams) -> bool {
        (pred_params.modified as f64) <= (pred_params.num_nodes as f64) * self.threshold
    }
}
