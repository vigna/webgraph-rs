//! # Traits
//! This modules contains the traits that are used throughout the crate.
//! They are collected into a module so you can do `use webgraph::traits::*;`
//! for ease of use.

mod bvgraph_codes;
pub use bvgraph_codes::*;

pub(crate) mod graph;
pub use graph::*;
