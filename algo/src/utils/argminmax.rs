/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// Extension trait for Iterator that provides methods to find the index of the minimum
/// and maximum elements. If the maximum or minimum appears several times, the
/// position of the first instance is returned.
pub trait ArgMinMax: Iterator {
    /// Returns the index of the maximum value in the iterator, or [`None`] if the
    /// iterator is empty.
    ///
    /// If the maximum appears several times, this methods returns the position of
    /// the first instance.
    ///
    /// # Panics
    ///
    /// If a comparison returns [`None`].
    ///
    ///
    /// # Examples
    /// ```
    /// # use webgraph_algo::utils::ArgMinMax;
    /// let v = vec![1, 2, 5, 2, 1, 5];
    /// let index = v.iter().argmax();
    /// assert_eq!(index, Some(2));
    /// ```
    fn argmax(self) -> Option<usize>
    where
        Self::Item: std::cmp::PartialOrd + Copy,
        Self: Sized,
    {
        self.enumerate()
            .min_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap())
            .map(|(idx, _)| idx)
    }

    /// Returns the index of the minimum value in a slice, or [`None`] if the slice
    /// is empty.
    ///
    /// If the minimum appears several times, this methods returns the
    /// position of the first instance.
    ///
    /// # Panics
    ///
    /// If a comparison returns [`None`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use webgraph_algo::utils::ArgMinMax;
    /// let v = vec![4, 3, 1, 0, 5, 0];
    /// let index = v.iter().argmin();
    /// assert_eq!(index, Some(3));
    /// ```
    fn argmin(self) -> Option<usize>
    where
        Self::Item: std::cmp::PartialOrd + Copy,
        Self: Sized,
    {
        self.enumerate()
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(idx, _)| idx)
    }
}

/// Blanket implementation for any iterator
impl<I: Iterator> ArgMinMax for I {}
