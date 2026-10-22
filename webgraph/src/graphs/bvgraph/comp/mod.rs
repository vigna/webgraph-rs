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

/// An in-memory label store for the tests of [`BvComp`] and [`BvCompZ`].
#[cfg(test)]
mod test_store {
    use crate::traits::StoreLabels;
    use std::cell::RefCell;
    use std::rc::Rc;

    /// A [`StoreLabels`] collecting labels in a vector shared with the
    /// test, so that it can be inspected after the compressor (which owns
    /// the store) has been flushed.
    #[derive(Debug)]
    pub(super) struct VecStore<T>(Rc<RefCell<Vec<T>>>);

    impl<T> Default for VecStore<T> {
        fn default() -> Self {
            Self(Rc::new(RefCell::new(Vec::new())))
        }
    }

    impl<T> VecStore<T> {
        /// Returns a handle to the stored labels.
        pub(super) fn labels(&self) -> Rc<RefCell<Vec<T>>> {
            self.0.clone()
        }
    }

    impl<T: Clone> StoreLabels for VecStore<T> {
        type Label = T;

        fn init(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn push_node(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn push_label(&mut self, label: &T) -> anyhow::Result<()> {
            self.0.borrow_mut().push(label.clone());
            Ok(())
        }

        fn flush(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn label_written_bits(&self) -> u64 {
            0
        }

        fn offsets_written_bits(&self) -> u64 {
            0
        }
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
