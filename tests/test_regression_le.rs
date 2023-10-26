/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

#[cfg(feature = "fuzz")]
use anyhow::Result;
#[cfg(feature = "fuzz")]
use webgraph::fuzz::bvcomp_and_read::*;
#[cfg(feature = "fuzz")]
use webgraph::prelude::*;

#[test]
#[cfg(feature = "fuzz")]
fn test_regression_le() -> Result<()> {
    let data = FuzzCase {
        compression_flags: CompFlagsFuzz {
            outdegrees: CodeFuzz::Unary,
            references: CodeFuzz::Unary,
            blocks: CodeFuzz::Unary,
            intervals: CodeFuzz::Unary,
            residuals: CodeFuzz::Unary,
            min_interval_length: 248,
            compression_window: 255,
            max_ref_count: 255,
        },
        edges: vec![(2, 187)],
    };
    dbg!(&data);
    harness(data);
    Ok(())
}
