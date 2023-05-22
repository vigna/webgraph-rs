//! # Traits
//! This modules contains the traits that are used throughout the crate.
//! They are collected into a module so you can do `use webgraph::traits::*;`
//! for ease of use.

mod webgraph_codes;
pub use webgraph_codes::*;

pub(crate) mod graph;
pub use graph::*;
