/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Utility structures for labelings.

pub mod bitstream;
pub use bitstream::SeqLabels;

pub mod zip;
pub use zip::*;

pub mod proj;
pub use proj::*;
