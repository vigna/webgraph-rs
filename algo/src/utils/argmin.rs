/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// Returns the index of the minimum value in an iterator, or [`None`] if the
/// iterator is empty.
///
/// If the minimum appears several times, this methods returns the position of
/// the first instance.
///
/// # Arguments
///
/// * `iter`: the iterator.
///
/// # Panics
///
/// If a comparison returns [`None`].
///
/// # Examples
///
/// ```rust
/// # use webgraph_algo::utils::math::argmin;
/// let v = vec![4, 3, 1, 0, 5, 0];
/// let index = argmin(&v);
/// assert_eq!(index, Some(3));
/// ```
pub fn argmin<I: IntoIterator>(iter: I) -> Option<usize>
where
    I::Item: core::cmp::PartialOrd + Copy,
{
    iter.into_iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
        .map(|(idx, _)| idx)
}

/// Returns the index of the minimum value approved by a filter in an iterator,
/// or [`None`] if no element is approved by the filter.
///
/// In case of ties, this method returns the index for which the corresponding
/// element in `tie_break` is minimized.
///
/// If the minimum appears several times with the same tie break, this methods
/// returns the position of the first instance.
///
/// # Arguments
///
/// * `iter`: the iterator.
///
/// * `tie_break`: in case two elements of `iter` are the same, the
///   corresponding elements in this iterator are used as secondary order.
///
/// * `filter`: a closure that takes as arguments the index of the element and
///   the element itself and returns true if the element is approved.
///
/// # Panics
///
/// If a comparison returns [`None`].
///
/// # Examples
///
/// ```rust
/// # use webgraph_algo::utils::math::argmin_filtered;
/// let v = vec![3, 2, 5, 2, 3, 2];
/// let tie = vec![5, 4, 3, 2, 1, 1];
/// let index = argmin_filtered(&v, &tie, |_, &element| element > 1);
/// // Tie break wins
/// assert_eq!(index, Some(5));
///
/// let v = vec![3, 2, 5, 2, 3, 2];
/// let tie = vec![5, 4, 3, 2, 1, 2];
/// // Enumeration order wins
/// let index = argmin_filtered(&v, &tie, |_, &element| element > 1);
/// assert_eq!(index, Some(3));
/// ```
pub fn argmin_filtered<I: IntoIterator, J: IntoIterator>(
    iter: I,
    tie_break: J,
    filter: impl Fn(usize, I::Item) -> bool,
) -> Option<usize>
where
    I::Item: core::cmp::PartialOrd + Copy,
    J::Item: core::cmp::PartialOrd + Copy,
{
    iter.into_iter()
        .zip(tie_break)
        .enumerate()
        .filter(|(idx, (v, _tie))| filter(*idx, *v))
        .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
        .map(|(idx, _)| idx)
}
