/*
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

//! Traits related to compression.

use anyhow::Result;

/// Compresses arc labels written alongside a graph compressor.
///
/// Implementations receive labels one arc at a time via [`push_label`],
/// grouped by node via [`push_node`]. The [`init`] method performs any
/// setup (e.g., writing an initial offset), and [`flush`] finalizes the
/// output.
///
/// The unit type `()` implements this trait with `Label = ()`, making
/// every method a no-op that is compiled away by monomorphization.
///
/// [`push_label`]: Self::push_label
/// [`push_node`]: Self::push_node
/// [`init`]: Self::init
/// [`flush`]: Self::flush
pub trait LabelComp {
    /// The arc-label type that this compressor accepts.
    type Label;

    /// Performs any setup before compression begins.
    fn init(&mut self) -> Result<()>;

    /// Signals the start of a new node's labels.
    ///
    /// On every call except the first, implementations typically
    /// record the accumulated bit count for the previous node.
    fn push_node(&mut self) -> Result<()>;

    /// Compresses a single arc label.
    fn push_label(&mut self, label: &Self::Label) -> Result<()>;

    /// Finalizes compression and flushes all output.
    fn flush(&mut self) -> Result<()>;
}

impl LabelComp for () {
    type Label = ();

    #[inline(always)]
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn push_node(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn push_label(&mut self, _label: &()) -> Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
