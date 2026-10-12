/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Ranking algorithms for graphs.

pub mod preds {
    //! Predicates implementing stopping conditions for iterative ranking
    //! algorithms.
    //!
    //! Each predicate is generic over any type that implements the
    //! corresponding extraction trait ([`HasIteration`] for [`MaxIter`],
    //! [`HasL1Norm`] for [`L1Norm`]). The convenience struct
    //! [`PredParams`] implements both traits and is used by the built-in
    //! algorithms.
    //!
    //! You can combine predicates using the `and` and `or` methods provided
    //! by the [`Predicate`] trait. Composed predicates require the evaluated
    //! type to implement all the traits needed by the individual predicates.
    //!
    //! Each algorithm defines its own `PredParams` type implementing only the
    //! traits it supports. For example, [`BiRank`] computes both ℓ₁ and ℓ_∞
    //! norms, so its [`PredParams`] implements [`HasL1Norm`] and
    //! [`HasLInfNorm`], whereas [`PageRank`] only provides an ℓ₁ bound.
    //! Attempting to use [`LInfNorm`] with PageRank is a compile-time error.
    //!
    //! # Examples
    //!
    //! ```
    //! # fn main() -> Result<(), Box<dyn std::error::Error>> {
    //! use predicates::prelude::*;
    //! use webgraph_algo::rank::birank::PredParams;
    //! use webgraph_algo::rank::preds::{L1Norm, LInfNorm, MaxIter};
    //!
    //! // When boxing, the target type must be specified explicitly because
    //! // predicates are generic over any type implementing the extraction
    //! // traits.
    //! let mut predicate: predicates::BoxPredicate<PredParams> =
    //!     LInfNorm::try_from(1E-6)?.boxed();
    //! predicate = predicate.or(MaxIter::from(100)).boxed();
    //! #     Ok(())
    //! # }
    //! ```
    //!
    //! [`BiRank`]: super::BiRank
    //! [`PredParams`]: super::birank::PredParams
    //! [`PageRank`]: super::PageRank

    use predicates::{Predicate, reflection::PredicateReflection};
    use std::fmt::Display;

    /// Provides the current iteration count to a stopping predicate.
    pub trait HasIteration {
        /// Returns the number of completed iterations.
        fn iteration(&self) -> usize;
    }

    /// Provides the ℓ₁-norm to a stopping predicate.
    ///
    /// The norm might be an estimate of the ℓ₁ norm of the difference with the
    /// target value, or the ℓ₁ norm of the difference between successive
    /// approximations,
    /// depending on the algorithm.
    pub trait HasL1Norm {
        /// Returns the ℓ₁ norm of the rank-vector change after the last
        /// iteration.
        fn l1_norm(&self) -> f64;
    }

    /// Provides the ℓ_∞-norm to a stopping predicate.
    ///
    /// The norm might be an estimate of the ℓ_∞ norm of difference with the target
    /// value, or the ℓ_∞ norm of the difference between successive approximations,
    /// depending on the algorithm.
    pub trait HasLInfNorm {
        /// Returns the ℓ_∞ norm of the rank-vector change after the last
        /// iteration.
        fn linf_norm(&self) -> f64;
    }

    /// Stops after at most the provided number of iterations.
    #[derive(Debug, Clone)]
    pub struct MaxIter {
        max_iter: usize,
    }

    impl MaxIter {
        /// The default maximum number of iterations.
        pub const DEFAULT_MAX_ITER: usize = usize::MAX;
    }

    impl From<usize> for MaxIter {
        fn from(max_iter: usize) -> Self {
            MaxIter { max_iter }
        }
    }

    impl Default for MaxIter {
        fn default() -> Self {
            Self::from(Self::DEFAULT_MAX_ITER)
        }
    }

    impl Display for MaxIter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("(max iter: {})", self.max_iter))
        }
    }

    impl PredicateReflection for MaxIter {}

    impl<T: HasIteration> Predicate<T> for MaxIter {
        fn eval(&self, params: &T) -> bool {
            params.iteration() >= self.max_iter
        }
    }

    threshold_predicate! {
        /// Stops when the norm of the difference between successive approximations
        /// falls below a given threshold.
        L1Norm,
        default: 1E-6,
        label: "norm",
        check: |threshold| { ensure!(threshold > 0.0, "The threshold must be positive"); },
        eval: impl<T: HasL1Norm> Predicate<T> |params| params.l1_norm(),
    }

    threshold_predicate! {
        /// Stops when the ℓ_∞ norm of the difference between successive
        /// approximations falls below a given threshold.
        LInfNorm,
        default: 1E-6,
        label: "linf norm",
        check: |threshold| { ensure!(threshold > 0.0, "The threshold must be positive"); },
        eval: impl<T: HasLInfNorm> Predicate<T> |params| params.linf_norm(),
    }
}

/// BiRank algorithm implementation.
pub mod birank;
/// PageRank algorithm implementation.
pub mod pagerank;
pub use birank::BiRank;
pub use pagerank::{Mode, PageRank};
