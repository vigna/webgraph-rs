/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::fmt::Display;

use dsi_progress_logger::ProgressLogger;
use predicates::{reflection::PredicateReflection, Predicate};

use super::{gap_cost::compute_log_gap_cost, RandomAccessGraph, DCF};

pub struct PredParams<'a, R: RandomAccessGraph + Sync> {
    graph: &'a R,
    thread_pool: &'a rayon::ThreadPool,
    deg_cumul: &'a DCF,
    pl: Option<&'a mut ProgressLogger>,
    perm: &'a [usize],
    labels: &'a [usize],
    modified: usize,
    update: usize,
}

#[derive(Debug, Clone)]
pub struct MaxUpdates {
    max_updates: usize,
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
pub struct MaxLogGapCost {
    threshold: f64,
}

impl Display for MaxLogGapCost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as core::fmt::Debug>::fmt(&self, f)
    }
}

impl PredicateReflection for MaxLogGapCost {}
impl<'a, R: RandomAccessGraph + Sync> Predicate<PredParams<'a, R>> for MaxLogGapCost {
    fn eval(&self, pred_params: &PredParams<'a, R>) -> bool {
        let gap_cost = compute_log_gap_cost(
            pred_params.thread_pool,
            pred_params.graph,
            pred_params.deg_cumul,
            None, // TODO
        );
        gap_cost < self.threshold
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
