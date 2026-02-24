/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use crate::prelude::*;
use dsi_bitstream::prelude::*;

#[derive(Clone, Debug, arbitrary::Arbitrary)]
pub enum CodeFuzz {
    Unary,
    Gamma,
    Delta,
    Zeta3,
}
impl From<CodeFuzz> for Codes {
    fn from(value: CodeFuzz) -> Self {
        match value {
            CodeFuzz::Unary => Codes::Unary,
            CodeFuzz::Gamma => Codes::Gamma,
            CodeFuzz::Delta => Codes::Delta,
            CodeFuzz::Zeta3 => Codes::Zeta(3),
        }
    }
}

#[derive(Clone, Debug, arbitrary::Arbitrary)]
pub struct CompFlagsFuzz {
    pub outdegrees: CodeFuzz,
    pub references: CodeFuzz,
    pub blocks: CodeFuzz,
    pub intervals: CodeFuzz,
    pub residuals: CodeFuzz,
    pub min_interval_length: u8,
    pub compression_window: u8,
    pub max_ref_count: u8,
}

impl From<CompFlagsFuzz> for CompFlags {
    fn from(value: CompFlagsFuzz) -> Self {
        CompFlags {
            outdegrees: value.outdegrees.into(),
            references: value.references.into(),
            blocks: value.blocks.into(),
            intervals: value.intervals.into(),
            residuals: value.residuals.into(),
            min_interval_length: value.min_interval_length as usize,
            compression_window: value.compression_window as usize,
            max_ref_count: value.max_ref_count as usize,
        }
    }
}
