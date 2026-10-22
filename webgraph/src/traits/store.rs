/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
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
/// See also [`StoreLabelsConf`] (the factory that creates instances of
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
/// Sequential compression only uses [`new_storage`], and requires the
/// factory to implement [`SeqStoreLabelsConf`]; parallel compression
/// additionally uses the concatenation methods to merge per-thread part
/// files into the final output.
///
/// The unit type implements this trait as a no-op factory that spawns
/// `()` stores.
///
/// See also [`BitStreamStoreLabelsConf`] (the concrete bitstream
/// implementation) and [`BvCompConf::par_comp_labeled`] /
/// [`BvCompConf::comp_labeled_graph`] (the consumers).
///
/// [`new_storage`]: Self::new_storage
/// [`init_concat`]: Self::init_concat
/// [`concat_part`]: Self::concat_part
/// [`flush_concat`]: Self::flush_concat
/// [`BitStreamStoreLabelsConf`]: crate::labels::BitStreamStoreLabelsConf
/// [`BvCompConf::par_comp_labeled`]: crate::graphs::bvgraph::BvCompConf::par_comp_labeled
/// [`BvCompConf::comp_labeled_graph`]: crate::graphs::bvgraph::BvCompConf::comp_labeled_graph
pub trait StoreLabelsConf {
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

    /// Returns the stable name of the label serializer, or `"()"` for
    /// unlabeled graphs (the value returned by the unit implementation).
    ///
    /// The compressor writes this to the `.properties` file; the loader
    /// checks it against the deserializer provided at load time.
    fn label_serializer_name(&self) -> String;
}

impl StoreLabelsConf for () {
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

/// A marker trait for [`StoreLabelsConf`] implementations whose
/// [`new_storage`] writes labels in their final format, and that can thus be
/// used for sequential compression.
///
/// # Rationale
///
/// Parallel compression ([`BvCompConf::par_comp_labeled`]) has each thread
/// compress a chunk of nodes concurrently. A thread cannot write its labels
/// directly to the final output files, because the position at which they
/// start depends on the number of bits written by the previous chunks, so
/// each thread writes them to temporary part files using a [`StoreLabels`]
/// returned by [`new_storage`]. At the end, the parts are concatenated into
/// the final output files using [`init_concat`], [`concat_part`], and
/// [`flush_concat`].
///
/// The format of the part files is thus an implementation detail of the
/// factory, as long as [`concat_part`] turns it into the final format. For
/// example, a [`BitStreamStoreLabelsConf`] with [`Zstd`] part files writes
/// zstd-compressed parts, reducing the space used on disk by temporary
/// files, and [`concat_part`] decompresses them while copying them into the
/// final files, which are always uncompressed bitstreams (they are
/// memory-mapped and accessed at arbitrary bit offsets).
///
/// Sequential compression ([`BvCompConf::comp_labeled_graph`] and
/// [`BvCompConf::comp_labeled_lender`]), instead, has no need for temporary
/// files: it obtains a [`StoreLabels`] from [`new_storage`] and writes the
/// labels directly to the final output files. This is correct only if
/// [`new_storage`] writes the labels in the same format produced by
/// concatenation; otherwise, compression would complete successfully, but
/// the resulting labels could not be loaded.
///
/// Sequential compression accepts only factories implementing this trait,
/// so using an incompatible factory is a compile-time error rather than a
/// silent corruption of the output. Note that there would be no point in
/// supporting such factories by writing temporary files anyway: the
/// purpose of transforming part files (e.g., compressing them) is to reduce
/// the cost of the temporary files of parallel compression, and sequential
/// compression has none.
///
/// # Implementing this trait
///
/// Implement this trait for your [`StoreLabelsConf`] if and only if the
/// files written by the [`StoreLabels`] instances returned by
/// [`new_storage`] have the same format as the final output files built by
/// concatenation. In particular, do not implement this trait if
/// [`new_storage`] compresses or otherwise transforms the data, and
/// [`concat_part`] undoes the transformation.
///
/// This trait is implemented by the unit type (which stores no labels) and
/// by [`BitStreamStoreLabelsConf`] with [`Uncompressed`] part files, but not
/// by [`BitStreamStoreLabelsConf`] with [`Zstd`] part files.
///
/// # Examples
///
/// A [`BitStreamStoreLabelsConf`] with uncompressed part files (the default)
/// can be used for both sequential and parallel compression:
///
/// ```
/// # if cfg!(miri) { return Ok::<(), Box<dyn std::error::Error>>(()); }
/// # use dsi_bitstream::prelude::BE;
/// # use webgraph::prelude::*;
/// # use webgraph::graphs::vec_graph::LabeledVecGraph;
/// # use webgraph::labels::bitstream::BitStreamStoreLabelsConf;
/// # use webgraph::traits::FixedWidth;
/// let graph = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
/// let dir = tempfile::tempdir()?;
/// let basename = dir.path().join("graph");
/// let conf = || BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u32>::new());
/// BvComp::with_basename(&basename).comp_labeled_graph::<BE, _, _>(&graph, conf())?;
/// BvComp::with_basename(&basename).par_comp_labeled::<BE, _, _, _>(&graph, conf())?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// A [`BitStreamStoreLabelsConf`] with [`Zstd`] part files can be used only
/// for parallel compression:
///
/// ```compile_fail
/// # use dsi_bitstream::prelude::BE;
/// # use webgraph::prelude::*;
/// # use webgraph::graphs::vec_graph::LabeledVecGraph;
/// # use webgraph::labels::bitstream::BitStreamStoreLabelsConf;
/// # use webgraph::traits::FixedWidth;
/// let graph = LabeledVecGraph::<u32>::from_arcs([((0, 1), 10), ((1, 0), 20)]);
/// let dir = tempfile::tempdir()?;
/// let basename = dir.path().join("graph");
/// let conf = || BitStreamStoreLabelsConf::<BE, _>::new(FixedWidth::<u32>::new()).zstd();
/// BvComp::with_basename(&basename).par_comp_labeled::<BE, _, _, _>(&graph, conf())?;
/// // Does not compile: the factory does not implement SeqStoreLabelsConf
/// BvComp::with_basename(&basename).comp_labeled_graph::<BE, _, _>(&graph, conf())?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// [`new_storage`]: StoreLabelsConf::new_storage
/// [`init_concat`]: StoreLabelsConf::init_concat
/// [`concat_part`]: StoreLabelsConf::concat_part
/// [`flush_concat`]: StoreLabelsConf::flush_concat
/// [`BitStreamStoreLabelsConf`]: crate::labels::BitStreamStoreLabelsConf
/// [`Uncompressed`]: crate::labels::bitstream::Uncompressed
/// [`Zstd`]: crate::labels::bitstream::Zstd
/// [`BvCompConf::par_comp_labeled`]: crate::graphs::bvgraph::BvCompConf::par_comp_labeled
/// [`BvCompConf::comp_labeled_graph`]: crate::graphs::bvgraph::BvCompConf::comp_labeled_graph
/// [`BvCompConf::comp_labeled_lender`]: crate::graphs::bvgraph::BvCompConf::comp_labeled_lender
pub trait SeqStoreLabelsConf: StoreLabelsConf {}

/// The unit type stores no labels, so there is no format to get wrong.
impl SeqStoreLabelsConf for () {}
