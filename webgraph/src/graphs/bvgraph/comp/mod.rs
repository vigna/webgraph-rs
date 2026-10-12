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
