/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod bvcomp;
mod bvcompz;

pub use bvcomp::*;
pub use bvcompz::*;

mod impls;
pub use impls::{BvCompConf, OffsetsWriter};

mod flags;
pub use flags::*;

/// Checks that `succ`, a successor of `node`, is greater than the previous
/// successor, which is `*min_succ - 1`, and sets `*min_succ` to `succ + 1`.
///
/// `*min_succ` must be zero before the first successor of each node.
#[inline(always)]
fn check_succ(node: usize, min_succ: &mut usize, succ: usize) -> anyhow::Result<()> {
    if succ < *min_succ {
        return Err(succ_error(node, succ, *min_succ - 1));
    }
    *min_succ = succ + 1;
    Ok(())
}

/// Returns the error for a successor `succ` of `node` following `prev`.
#[cold]
#[inline(never)]
fn succ_error(node: usize, succ: usize, prev: usize) -> anyhow::Error {
    if succ == prev {
        anyhow::anyhow!("Repeated successor {succ} in successor list of node {node}")
    } else {
        anyhow::anyhow!("Successor list of node {node} is not sorted: {succ} follows {prev}")
    }
}

/// Helpers for the tests of [`BvComp`] and [`BvCompZ`].
#[cfg(all(test, not(miri)))]
mod test_utils {
    use crate::prelude::*;
    use dsi_bitstream::traits::BE;
    use std::path::Path;

    /// Compresses cnr-2000 with the configuration returned by `conf`, the
    /// given compression window, and minimum interval lengths 0, 1, and 2,
    /// checking the results.
    pub(super) fn test_window(
        conf: impl Fn(&Path) -> BvCompConf,
        compression_window: usize,
    ) -> anyhow::Result<()> {
        let cnr_2000 = BvGraphSeq::with_basename("../data/cnr-2000").load()?;
        let tmp_dir = tempfile::Builder::new().prefix("bvcomp_test").tempdir()?;
        let basename = tmp_dir.path().join("cnr-2000");
        for min_interval_length in 0..3 {
            conf(&basename)
                .comp_flags(CompFlags {
                    compression_window,
                    min_interval_length,
                    ..Default::default()
                })
                .comp_graph::<BE>(&cnr_2000)?;
            let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
            labels::eq_sorted(&cnr_2000, &seq_graph)?;
        }
        Ok(())
    }

    /// Compresses cnr-2000 sequentially and in parallel with the
    /// configuration returned by `conf`, checking the results.
    pub(super) fn test_cnr(conf: impl Fn(&Path) -> BvCompConf) -> anyhow::Result<()> {
        let cnr_2000 = BvGraphSeq::with_basename("../data/cnr-2000")
            .endianness::<BE>()
            .load()?;
        let tmp_dir = tempfile::Builder::new().prefix("bvcomp_test").tempdir()?;
        let basename = tmp_dir.path().join("cnr-2000");

        conf(&basename).comp_graph::<BE>(&cnr_2000)?;
        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;

        conf(&basename).par_comp::<BE, _>(&cnr_2000)?;
        let seq_graph = BvGraphSeq::with_basename(&basename).load()?;
        labels::eq_sorted(&cnr_2000, &seq_graph)?;
        Ok(())
    }
}
