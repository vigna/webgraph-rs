use crate::traits::SequentialGraph;

pub mod bvgraph;
pub mod permuted_graph;
pub mod vec_graph;

pub mod prelude {
    pub use super::bvgraph::*;
    pub use super::permuted_graph::*;
    pub use super::vec_graph::*;
}
use core::ops::Range;
use core::sync::atomic::{AtomicUsize, Ordering};
use dsi_progress_logger::ProgressLogger;
use std::sync::Mutex;
