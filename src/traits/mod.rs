//! # Traits
//! This modules contains the traits that are used throughout the crate.
//! They are collected into a module so you can do `use webgraph::traits::*;`
//! for ease of use.

mod castable;
pub use castable::*;

mod downcastable;
pub use downcastable::*;

mod upcastable;
pub use upcastable::*;

mod word;
pub use word::*;

mod bit_stream;
pub use bit_stream::*;

mod word_stream;
pub use word_stream::*;

mod bit_order;
pub use bit_order::*;

mod webgraph_codes;
pub use webgraph_codes::*;

mod graph;
pub use graph::*;
