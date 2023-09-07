#![doc = include_str!("../README.md")]
// No warnings
//#![deny(warnings)]
// for now we don't need any new feature but we might remove this in the future
#![deny(unstable_features)]
// no dead code
//#![deny(dead_code)]
#![deny(trivial_casts)]
#![deny(unconditional_recursion)]
#![deny(clippy::empty_loop)]
#![deny(unreachable_code)]
#![deny(unreachable_pub)]
#![deny(unreachable_patterns)]
#![deny(unused_macro_rules)]
//#![deny(unused_results)]

// the code must be documented and everything should have a debug print implementation
#![deny(unused_doc_comments)]
//#![deny(missing_docs)]
//#![deny(clippy::missing_docs_in_private_items)]
//#![deny(clippy::missing_errors_doc)]
//#![deny(clippy::missing_panics_doc)]
//#![deny(clippy::missing_safety_doc)]
//#![deny(clippy::missing_doc_code_examples)]
//#![deny(clippy::missing_crate_level_docs)]
//#![deny(missing_debug_implementations)]
#![cfg_attr(not(feature = "std"), no_std)]

use sux::prelude::*;

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod algorithms;
#[cfg(feature = "fuzz")]
pub mod fuzz;
pub mod graph;
pub mod traits;
pub mod utils;

/// The default version of EliasFano we use for the CLI
pub type EF<Memory> =
    EliasFano<SparseIndex<CountingBitmap<Memory, usize>, Memory, 8>, CompactArray<Memory>>;

/// Prelude module to import everything from this crate
pub mod prelude {
    pub use crate::algorithms::*;
    pub use crate::graph::prelude::*;
    pub use crate::traits::*;
    pub use crate::utils::*;
}
