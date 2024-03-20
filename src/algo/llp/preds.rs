/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 * SPDX-FileCopyrightText: 2024 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use super::RandomAccessGraph;

pub struct PredParams<'a, R: RandomAccessGraph> {
    graph: &'a R,
    perm: &'a [usize],
    labels: &'a [usize],
    modified: usize,
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
