/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Utilities.

/// Defines a stopping predicate that is true when a value is at most a
/// threshold.
///
/// The predicate has a `DEFAULT_THRESHOLD` constant, can be built from an
/// `f64` or an `Option<f64>` (`None` meaning the default threshold), and
/// implements [`Display`] and [`Predicate`]. Besides being checked against
/// NaN, the threshold must satisfy the `ensure!` conditions given in `check`,
/// which refer to it using the given binding.
///
/// [`Display`]: std::fmt::Display
/// [`Predicate`]: predicates::Predicate
macro_rules! threshold_predicate {
    (
        $(#[$meta:meta])*
        $name:ident,
        default: $default:expr,
        label: $label:literal,
        check: |$threshold:ident| { $(ensure!($($cond:tt)*);)* },
        eval: impl<$($gen:ident: $bound:path)?> Predicate<$params:ty> |$p:ident| $value:expr $(,)?
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone)]
        pub struct $name {
            threshold: f64,
        }

        impl $name {
            /// The default threshold.
            pub const DEFAULT_THRESHOLD: f64 = $default;
        }

        impl TryFrom<Option<f64>> for $name {
            type Error = anyhow::Error;
            fn try_from(threshold: Option<f64>) -> anyhow::Result<Self> {
                Ok(match threshold {
                    Some($threshold) => {
                        anyhow::ensure!(!$threshold.is_nan());
                        $(anyhow::ensure!($($cond)*);)*
                        $name { threshold: $threshold }
                    }
                    None => Self::default(),
                })
            }
        }

        impl TryFrom<f64> for $name {
            type Error = anyhow::Error;
            fn try_from(threshold: f64) -> anyhow::Result<Self> {
                Some(threshold).try_into()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::try_from(Self::DEFAULT_THRESHOLD).unwrap()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_fmt(format_args!(concat!("(", $label, ": {})"), self.threshold))
            }
        }

        impl predicates::reflection::PredicateReflection for $name {}

        impl<$($gen: $bound)?> predicates::Predicate<$params> for $name {
            fn eval(&self, $p: &$params) -> bool {
                $value <= self.threshold
            }
        }
    };
}

mod argmax;
mod argmin;

use webgraph::traits::RandomAccessGraph;

/// Checks that `transpose` has the same number of nodes and arcs as `graph`,
/// a necessary condition for it to be the transpose of `graph`.
pub(crate) fn check_transpose(
    graph: &impl RandomAccessGraph,
    transpose: &impl RandomAccessGraph,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        transpose.num_nodes() == graph.num_nodes(),
        "the transpose should have the same number of nodes as the graph ({}), got {}",
        graph.num_nodes(),
        transpose.num_nodes()
    );
    anyhow::ensure!(
        transpose.num_arcs() == graph.num_arcs(),
        "the transpose should have the same number of arcs as the graph ({}), got {}",
        graph.num_arcs(),
        transpose.num_arcs()
    );
    Ok(())
}

/// Like [`check_transpose`], but panics instead of returning an error.
pub(crate) fn assert_transpose(graph: &impl RandomAccessGraph, transpose: &impl RandomAccessGraph) {
    if let Err(e) = check_transpose(graph, transpose) {
        panic!("{e}");
    }
}

/// Module containing mathematical utilities.
pub mod math {
    pub use super::argmax::*;
    pub use super::argmin::*;
}
