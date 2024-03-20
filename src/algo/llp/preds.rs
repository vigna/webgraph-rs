/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::RandomAccessGraph;
use anyhow::ensure;
use predicates::{reflection::PredicateReflection, Predicate};
use std::fmt::Display;

pub struct PredParams<'a, R: RandomAccessGraph + Sync + 'a> {
    pub graph: &'a R,
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
        <Self as core::fmt::Debug>::fmt(&self, f)
    }
}

impl PredicateReflection for MaxUpdates {}
impl<'a, R: RandomAccessGraph + Sync> Predicate<PredParams<'a, R>> for MaxUpdates {
    fn eval(&self, pred_params: &PredParams<'a, R>) -> bool {
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
        <Self as core::fmt::Debug>::fmt(&self, f)
    }
}

impl PredicateReflection for MinGain {}
impl<'a, R: RandomAccessGraph + Sync> Predicate<PredParams<'a, R>> for MinGain {
    fn eval(&self, pred_params: &PredParams<'a, R>) -> bool {
        pred_params.gain < self.threshold
    }
}

#[derive(Debug, Clone, Default)]
pub struct MinModified {}

impl Display for MinModified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as core::fmt::Debug>::fmt(&self, f)
    }
}

impl PredicateReflection for MinModified {}
impl<'a, R: RandomAccessGraph + Sync> Predicate<PredParams<'a, R>> for MinModified {
    fn eval(&self, pred_params: &PredParams<'a, R>) -> bool {
        (pred_params.modified as f64) <= (pred_params.graph.num_nodes() as f64).sqrt()
    }
}

/*
pub trait ObjFunc: Sized {
    fn compute(&mut self) -> (bool, f64);
}

pub struct Log2Gaps;
impl ObjFunc for Log2Gaps {
    fn compute(&mut self) -> (bool, f64) {
        todo!();
    }
}

pub struct NodesModifies;
impl ObjFunc for NodesModifies {
    fn compute(
        &mut self,
        graph: &impl SequentialGraph,
        perm: &[usize],
        labels: &[usize],
        modified: usize,
    ) -> (bool, f64) {
        (false, modified as f64 / graph.num_nodes() as f64)
    }
}

pub struct EarlyStopping<O: ObjFunc> {
    func: O,
    patience: usize,
    min_delta: f64,
    counter: usize,
}

impl<O: ObjFunc> ObjFunc for EarlyStopping<O> {
    fn compute(
        &mut self,
        graph: &impl SequentialGraph,
        perm: &[usize],
        labels: &[usize],
        modified: usize,
    ) -> (bool, f64) {
        let (exit, res) = self.func.compute(graph, perm, labels, modified);
        if exit {
            return (exit, res);
        }
        if res >= self.min_delta {
            self.counter = 0;
            return (exit, res);
        }
        self.counter += 1;
        if self.counter >= self.patience {
            return (true, res);
        }
        return (false, res);
    }
}

pub struct MaxIters<O: ObjFunc> {
    func: O,
    max_iters: usize,
    counter: usize,
}

impl<O: ObjFunc> ObjFunc for MaxIters<O> {
    fn compute(
        &mut self,
        graph: &impl SequentialGraph,
        perm: &[usize],
        labels: &[usize],
        modified: usize,
    ) -> (bool, f64) {
        let (exit, res) = self.func.compute(graph, perm, labels, modified);
        if exit {
            return (exit, res);
        }
        self.counter += 1;
        if self.counter >= self.max_iters {
            return (true, res);
        }
        return (false, res);
    }
}

pub struct AbsStop<O: ObjFunc> {
    func: O,
    min_delta: f64,
}

impl<O: ObjFunc> ObjFunc for AbsStop<O> {
    fn compute(
        &mut self,
        graph: &impl SequentialGraph,
        perm: &[usize],
        labels: &[usize],
        modified: usize,
    ) -> (bool, f64) {
        let (exit, res) = self.func.compute(graph, perm, labels, modified);
        if exit {
            return (exit, res);
        }
        if res >= self.min_delta {
            return (exit, res);
        }
        return (true, res);
    }
}
*/
