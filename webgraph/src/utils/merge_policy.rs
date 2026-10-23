/*
 * SPDX-FileCopyrightText: 2026 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Policies for repeated pairs when sorting.
//!
//! When sorting labeled pairs, the same pair may appear several times. A
//! merge policy decides what happens to repeated pairs:
//!
//! - [`NoMerge`] keeps all of them, each with its label;
//! - [`Dedup`] keeps one of them, with one of their labels;
//! - [`WithMerge`] keeps one of them, whose label is obtained by merging
//!   their labels with a [`MergeLabels`].
//!
//! Sorting writes sorted batches to disk, and then merges them. Repeated
//! pairs are handled in both stages: [batch codecs] collapse the repeated
//! pairs of a batch using [`AddPolicy`], and [`KMergeIters`] collapses the
//! repeated pairs coming from different batches using [`MergePolicy`]. Batch
//! codecs own their policy and pass it to the [`KMergeIters`] merging their
//! batches, so the two stages always agree.
//!
//! Both traits are sealed: the three types above are their only
//! implementations. Usually you do not need to use these types directly:
//! see [`ParSortedLabeledGraphConf::dedup`] and
//! [`ParSortedLabeledGraphConf::merge_labels`].
//!
//! [batch codecs]: crate::utils::BatchCodec
//! [`KMergeIters`]: crate::utils::kmerge_iters::KMergeIters
//! [`ParSortedLabeledGraphConf::dedup`]: crate::graphs::par_sorted_graph::ParSortedLabeledGraphConf::dedup
//! [`ParSortedLabeledGraphConf::merge_labels`]: crate::graphs::par_sorted_graph::ParSortedLabeledGraphConf::merge_labels

use crate::traits::{AddLabel, MergeLabels};

/// A [merge policy] keeping all repeated pairs, each with its label.
///
/// [merge policy]: self
#[derive(Debug, Clone, Copy, Default)]
pub struct NoMerge;

/// A [merge policy] keeping one of the repeated pairs, with one of their
/// labels.
///
/// Which label is kept is unspecified.
///
/// [merge policy]: self
#[derive(Debug, Clone, Copy, Default)]
pub struct Dedup;

/// A [merge policy] keeping one of the repeated pairs, whose label is
/// obtained by merging their labels with a [`MergeLabels`].
///
/// [merge policy]: self
#[derive(Debug, Clone, Copy, Default)]
pub struct WithMerge<M>(pub M);

mod sealed {
    pub trait Sealed {
        /// Whether repeated pairs are collapsed into a single pair.
        const COLLAPSES: bool;
    }
}

impl sealed::Sealed for NoMerge {
    const COLLAPSES: bool = false;
}

impl sealed::Sealed for Dedup {
    const COLLAPSES: bool = true;
}

impl<M> sealed::Sealed for WithMerge<M> {
    const COLLAPSES: bool = true;
}

/// The way a [merge policy] handles repeated pairs coming from different
/// sorted batches, whose labels are states of type `S`.
///
/// This trait is sealed: see the [module documentation].
///
/// [merge policy]: self
/// [module documentation]: self
pub trait MergePolicy<S>: sealed::Sealed + Clone + Send + Sync {
    /// The type of the labels of the pairs after merging.
    type Output;

    /// Merges into `state` the state `other` of a repeated pair.
    #[doc(hidden)]
    fn merge(&self, state: &mut S, other: S);

    /// Turns a fully merged state into the label of the pair.
    #[doc(hidden)]
    fn finish(&self, state: S) -> Self::Output;
}

/// The way a [merge policy] handles repeated pairs labeled by `L` in a
/// sorted batch.
///
/// This trait is sealed: see the [module documentation].
///
/// [merge policy]: self
/// [module documentation]: self
pub trait AddPolicy<L>: sealed::Sealed + Clone + Send + Sync {
    /// The type of the states written to batches.
    type State;

    /// A buffer for states, reused across runs.
    #[doc(hidden)]
    type Buffer;

    /// Returns a new buffer.
    #[doc(hidden)]
    fn buffer(&self) -> Self::Buffer;

    /// Returns the state to write for `run`, a nonempty run of equal
    /// pairs, using `buffer` if necessary.
    ///
    /// Runs have length one if repeated pairs are not collapsed.
    #[doc(hidden)]
    fn state<'a>(
        &self,
        buffer: &'a mut Self::Buffer,
        run: &'a [((usize, usize), L)],
    ) -> &'a Self::State;
}

impl<S> MergePolicy<S> for NoMerge {
    type Output = S;

    #[inline(always)]
    fn merge(&self, _state: &mut S, _other: S) {
        unreachable!("NoMerge does not collapse repeated pairs");
    }

    #[inline(always)]
    fn finish(&self, state: S) -> S {
        state
    }
}

impl<S> MergePolicy<S> for Dedup {
    type Output = S;

    #[inline(always)]
    fn merge(&self, _state: &mut S, _other: S) {}

    #[inline(always)]
    fn finish(&self, state: S) -> S {
        state
    }
}

impl<M: MergeLabels + Clone + Send + Sync> MergePolicy<M::State> for WithMerge<M> {
    type Output = M::Output;

    #[inline(always)]
    fn merge(&self, state: &mut M::State, other: M::State) {
        self.0.merge(state, other);
    }

    #[inline(always)]
    fn finish(&self, state: M::State) -> M::Output {
        self.0.finish(state)
    }
}

impl<L: Copy> AddPolicy<L> for NoMerge {
    type State = L;
    type Buffer = ();

    #[inline(always)]
    fn buffer(&self) {}

    #[inline(always)]
    fn state<'a>(&self, _buffer: &'a mut (), run: &'a [((usize, usize), L)]) -> &'a L {
        debug_assert_eq!(run.len(), 1);
        &run[0].1
    }
}

impl<L: Copy> AddPolicy<L> for Dedup {
    type State = L;
    type Buffer = ();

    #[inline(always)]
    fn buffer(&self) {}

    #[inline(always)]
    fn state<'a>(&self, _buffer: &'a mut (), run: &'a [((usize, usize), L)]) -> &'a L {
        &run[0].1
    }
}

impl<L: Copy, M: AddLabel<L> + Clone + Send + Sync> AddPolicy<L> for WithMerge<M> {
    type State = M::State;
    type Buffer = M::State;

    #[inline(always)]
    fn buffer(&self) -> M::State {
        M::State::default()
    }

    #[inline(always)]
    fn state<'a>(&self, buffer: &'a mut M::State, run: &'a [((usize, usize), L)]) -> &'a M::State {
        self.0.init(buffer);
        for &(_, label) in run {
            self.0.add(buffer, label);
        }
        buffer
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::traits::CountRepeats;

    #[test]
    fn test_add_policies() {
        let run = [((0, 1), 'a'), ((0, 1), 'b'), ((0, 1), 'c')];
        assert_eq!(
            *Dedup.state(&mut AddPolicy::<char>::buffer(&Dedup), &run),
            'a'
        );
        assert_eq!(
            *NoMerge.state(&mut AddPolicy::<char>::buffer(&NoMerge), &run[1..2]),
            'b'
        );
        let policy = WithMerge(CountRepeats);
        let mut buffer = AddPolicy::<char>::buffer(&policy);
        assert_eq!(*policy.state(&mut buffer, &run), 3);
        // The buffer is reused, and initialized at each run
        assert_eq!(*policy.state(&mut buffer, &run[..1]), 1);
    }

    #[test]
    fn test_merge_policies() {
        let policy = WithMerge(CountRepeats);
        let mut state = 2;
        MergePolicy::<u64>::merge(&policy, &mut state, 3);
        assert_eq!(policy.finish(state), 5);
        let mut state = 'x';
        Dedup.merge(&mut state, 'y');
        assert_eq!(Dedup.finish(state), 'x');
        assert_eq!(NoMerge.finish('z'), 'z');
    }
}
