/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Labeling implementations and combinators.
//!
//! A *labeling* associates with each node of a graph a list of labels. The
//! traits defining sequential and random-access labelings live in
//! [`crate::traits`]; this module provides concrete implementations and
//! tools for combining them.
//!
//! - [`bitstream`]: bitstream-based label storage; includes
//!   [`BitStreamLabeling`] / [`BitStreamLabelingSeq`] for reading, and
//!   [`BitStreamStoreLabelsConfig`] for writing (used by
//!   [`BvCompConfig::comp_labeled_graph`] and
//!   [`BvCompConfig::par_comp_labeled`]).
//! - [`zip`]: zips two labelings together, pairing their labels.
//! - [`proj`]: projects away one component of a paired labeling.
//!
//! [`BvCompConfig::comp_labeled_graph`]: crate::graphs::bvgraph::BvCompConfig::comp_labeled_graph
//! [`BvCompConfig::par_comp_labeled`]: crate::graphs::bvgraph::BvCompConfig::par_comp_labeled

pub mod bitstream;
pub use bitstream::{BitStreamLabeling, BitStreamLabelingSeq, BitStreamStoreLabelsConfig};

pub mod zip;
pub use zip::*;

pub mod proj;
pub use proj::*;
