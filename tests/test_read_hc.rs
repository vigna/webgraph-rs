/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::path::{Path, PathBuf};

use anyhow::Result;
use dsi_bitstream::prelude::*;
use dsi_progress_logger::*;
use lender::*;
use webgraph::{graphs::bvgraph, prelude::*};



#[test]
fn test_hc() -> Result<()> {
    let norm = bvgraph::BVGraphSeq::with_basename("tests/data/cnr-2000").load()?;
    let hc = bvgraph::BVGraphSeq::with_basename("tests/data/cnr-2000-hc").load()?;

    let mut norm_iter = norm.into_lender();
    let mut hc_iter = hc.into_lender();

    while let Some((norm_node, norm_succ)) = norm_iter.next() {
        let succ = norm_succ.collect::<Vec<_>>();
        let (hc_node, hc_succ) = hc_iter.next().unwrap();
        let h_succ = hc_succ.collect::<Vec<_>>();
        assert_eq!(
            norm_node,
            hc_node,
        );
        assert_eq!(
            succ,
            h_succ,
        );
    }

    Ok(())
}