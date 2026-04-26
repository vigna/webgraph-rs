/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Traits for storing labels alongside graph compression.

use std::path::Path;

/// A way to store labels alongside a graph compressor.
///
/// Implementations receive labels one arc at a time via [`push_label`],
/// grouped by node via [`push_node`]. The [`init`] method performs any
/// setup (e.g., writing an initial offset), and [`flush`] finalizes the
/// output.
///
/// The unit type implements this trait for the label `()`, making it
/// possible to use labeled compressors as unlabeled ones by wrapping the
/// unlabeled graph in a [`UnitLabelGraph`].
///
/// See also [`StoreLabelsConfig`] (the factory that creates instances of
/// this trait) and [`BitStreamStoreLabels`] (the concrete bitstream
/// implementation).
///
/// [`push_label`]: Self::push_label
/// [`push_node`]: Self::push_node
/// [`init`]: Self::init
/// [`flush`]: Self::flush
/// [`UnitLabelGraph`]: crate::traits::UnitLabelGraph
/// [`BitStreamStoreLabels`]: crate::labels::bitstream::store::BitStreamStoreLabels
pub trait StoreLabels {
    /// The arc-label type that this compressor accepts.
    type Label;

    /// Performs any setup before compression begins.
    fn init(&mut self) -> anyhow::Result<()>;

    /// Signals the start of a new node's labels.
    ///
    /// On every call except the first, implementations typically
    /// record the accumulated bit count for the previous node.
    fn push_node(&mut self) -> anyhow::Result<()>;

    /// Compresses a single arc label.
    fn push_label(&mut self, label: &Self::Label) -> anyhow::Result<()>;

    /// Finalizes compression and flushes all output.
    fn flush(&mut self) -> anyhow::Result<()>;

    /// Returns the number of bits written to the label bitstream so far.
    fn label_written_bits(&self) -> u64;

    /// Returns the number of bits written to the offsets bitstream so far.
    fn offsets_written_bits(&self) -> u64;
}

impl StoreLabels for () {
    type Label = ();

    #[inline(always)]
    fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn push_node(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn push_label(&mut self, _label: &()) -> anyhow::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn label_written_bits(&self) -> u64 {
        0
    }

    #[inline(always)]
    fn offsets_written_bits(&self) -> u64 {
        0
    }
}

/// Configures and spawns [`StoreLabels`] instances.
///
/// This is the factory counterpart to [`StoreLabels`]: it knows *how*
/// to create label writers (via [`new_storage`]) and how to concatenate
/// their output (via [`init_concat`], [`concat_part`], [`flush_concat`]).
///
/// Sequential compression only uses [`new_storage`]; parallel
/// compression additionally uses the concatenation methods to merge
/// per-thread part files into the final output.
///
/// The unit type implements this trait as a no-op factory that spawns
/// `()` stores.
///
/// See also [`BitStreamStoreLabelsConfig`] (the concrete bitstream
/// implementation) and [`BvCompConfig::par_comp_labeled`] /
/// [`BvCompConfig::comp_labeled_graph`] (the consumers).
///
/// [`new_storage`]: Self::new_storage
/// [`init_concat`]: Self::init_concat
/// [`concat_part`]: Self::concat_part
/// [`flush_concat`]: Self::flush_concat
/// [`BitStreamStoreLabelsConfig`]: crate::labels::BitStreamStoreLabelsConfig
/// [`BvCompConfig::par_comp_labeled`]: crate::graphs::bvgraph::BvCompConfig::par_comp_labeled
/// [`BvCompConfig::comp_labeled_graph`]: crate::graphs::bvgraph::BvCompConfig::comp_labeled_graph
pub trait StoreLabelsConfig {
    /// The per-part label writer this factory creates.
    type StoreLabels: StoreLabels;

    /// Creates a [`StoreLabels`] instance writing to the given paths.
    fn new_storage(
        &self,
        labels_path: &Path,
        offsets_path: &Path,
    ) -> anyhow::Result<Self::StoreLabels>;

    /// Opens the final output files for concatenation.
    fn init_concat(&mut self, labels_path: &Path, offsets_path: &Path) -> anyhow::Result<()>;

    /// Appends one part's labels and offsets into the final files.
    fn concat_part(
        &mut self,
        part_labels_path: &Path,
        labels_written_bits: u64,
        part_offsets_path: &Path,
        offsets_written_bits: u64,
    ) -> anyhow::Result<()>;

    /// Finalizes concatenation and flushes output.
    fn flush_concat(&mut self) -> anyhow::Result<()>;

    /// Returns the stable name of the label serializer, or an empty string
    /// for unlabeled graphs.
    ///
    /// The compressor writes this to the `.properties` file; the loader
    /// checks it against the deserializer provided at load time.
    fn label_serializer_name(&self) -> String;
}

impl StoreLabelsConfig for () {
    type StoreLabels = ();

    #[inline(always)]
    fn new_storage(&self, _labels_path: &Path, _offsets_path: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn init_concat(&mut self, _labels_path: &Path, _offsets_path: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn concat_part(
        &mut self,
        _part_labels_path: &Path,
        _labels_written_bits: u64,
        _part_offsets_path: &Path,
        _offsets_written_bits: u64,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn flush_concat(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn label_serializer_name(&self) -> String {
        "()".to_string()
    }
}
