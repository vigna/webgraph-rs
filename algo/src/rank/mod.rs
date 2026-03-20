/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
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
    //! traits it supports. For example, [`BiRank`](super::BiRank) computes
    //! both ℓ₁ and ℓ_∞ norms, so its [`PredParams`](super::birank::PredParams)
    //! implements [`HasL1Norm`] and [`HasLInfNorm`], whereas
    //! [`PageRank`](super::PageRank) only provides an ℓ₁ bound. Attempting
    //! to use [`LInfNorm`] with PageRank is a compile-time error.
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

    use anyhow::ensure;
    use predicates::{Predicate, reflection::PredicateReflection};
    use std::fmt::Display;

    /// Provides the current iteration count to a stopping predicate.
    pub trait HasIteration {
        /// Returns the number of completed iterations.
        fn iteration(&self) -> usize;
    }

    /// Provides the ℓ₁-norm to a stopping predicate.
    ///
    /// The norm might be an estimate of the ℓ₁∞ norm of difference with the target
    /// value, or the ℓ₁∞ norm of the difference between successive approximations,
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

    /// Stops when the norm of the difference between successive approximations
    /// falls below a given threshold.
    #[derive(Debug, Clone)]
    pub struct L1Norm {
        threshold: f64,
    }

    impl L1Norm {
        pub const DEFAULT_THRESHOLD: f64 = 1E-6;
    }

    impl TryFrom<Option<f64>> for L1Norm {
        type Error = anyhow::Error;
        fn try_from(threshold: Option<f64>) -> anyhow::Result<Self> {
            Ok(match threshold {
                Some(threshold) => {
                    ensure!(!threshold.is_nan());
                    ensure!(threshold > 0.0, "The threshold must be positive");
                    L1Norm { threshold }
                }
                None => Self::default(),
            })
        }
    }

    impl TryFrom<f64> for L1Norm {
        type Error = anyhow::Error;
        fn try_from(threshold: f64) -> anyhow::Result<Self> {
            Some(threshold).try_into()
        }
    }

    impl Default for L1Norm {
        fn default() -> Self {
            Self::try_from(Self::DEFAULT_THRESHOLD).unwrap()
        }
    }

    impl Display for L1Norm {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("(norm: {})", self.threshold))
        }
    }

    impl PredicateReflection for L1Norm {}

    impl<T: HasL1Norm> Predicate<T> for L1Norm {
        fn eval(&self, params: &T) -> bool {
            params.l1_norm() <= self.threshold
        }
    }

    /// Stops when the ℓ_∞ norm of the difference between successive
    /// approximations falls below a given threshold.
    #[derive(Debug, Clone)]
    pub struct LInfNorm {
        threshold: f64,
    }

    impl LInfNorm {
        pub const DEFAULT_THRESHOLD: f64 = 1E-6;
    }

    impl TryFrom<Option<f64>> for LInfNorm {
        type Error = anyhow::Error;
        fn try_from(threshold: Option<f64>) -> anyhow::Result<Self> {
            Ok(match threshold {
                Some(threshold) => {
                    ensure!(!threshold.is_nan());
                    ensure!(threshold > 0.0, "The threshold must be positive");
                    LInfNorm { threshold }
                }
                None => Self::default(),
            })
        }
    }

    impl TryFrom<f64> for LInfNorm {
        type Error = anyhow::Error;
        fn try_from(threshold: f64) -> anyhow::Result<Self> {
            Some(threshold).try_into()
        }
    }

    impl Default for LInfNorm {
        fn default() -> Self {
            Self::try_from(Self::DEFAULT_THRESHOLD).unwrap()
        }
    }

    impl Display for LInfNorm {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("(linf norm: {})", self.threshold))
        }
    }

    impl PredicateReflection for LInfNorm {}

    impl<T: HasLInfNorm> Predicate<T> for LInfNorm {
        fn eval(&self, params: &T) -> bool {
            params.linf_norm() <= self.threshold
        }
    }
}

pub mod birank;
pub mod pagerank;
pub use birank::BiRank;
pub use pagerank::{Mode, PageRank};
