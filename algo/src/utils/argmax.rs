/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// Returns the index of the maximum value in an iterator, or [`None`] if the
/// iterator is empty.
///
/// If the maximum appears several times, this methods returns the position of
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
///
/// # Examples
/// ```
/// # use webgraph_algo::utils::math::argmax;
/// let v = vec![1, 2, 5, 2, 1, 5];
/// let index = argmax(&v);
/// assert_eq!(index, Some(2));
/// ```
pub fn argmax<T: std::cmp::PartialOrd + Copy>(iter: impl IntoIterator<Item = T>) -> Option<usize> {
    iter.into_iter()
        .enumerate()
        .min_by(|a, b| b.1.partial_cmp(&a.1).unwrap())
        .map(|m| m.0)
}

/// Returns the index of the maximum value approved by a filter in an iterator,
/// or [`None`] if no element is approved by the filter.
///
/// In case of ties, this method returns the index for which the corresponding
/// element in `tie_break` is minimized.
///
/// If the maximum appears several times with the same tie break, this methods
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
/// ```
/// # use webgraph_algo::utils::math::argmax_filtered;
/// let v = vec![1, 2, 5, 2, 1, 2];
/// let tie = vec![1, 2, 3, 4, 5, 2];
/// let index = argmax_filtered(&v, &tie, |_, element| element < 4);
/// // Tie break wins
/// assert_eq!(index, Some(3));
///
/// let v = vec![1, 2, 5, 2, 1, 2];
/// let tie = vec![1, 1, 3, 2, 5, 2];
/// let index = argmax_filtered(&v, &tie, |_, element| element < 4);
/// // Enumeration order wins
/// assert_eq!(index, Some(3));
/// ```
pub fn argmax_filtered<
    T: std::cmp::PartialOrd + Copy,
    N: std::cmp::PartialOrd + Copy,
    F: Fn(usize, T) -> bool,
>(
    iter: impl IntoIterator<Item = T>,
    tie_break: impl IntoIterator<Item = N>,
    filter: F,
) -> Option<usize> {
    iter.into_iter()
        .zip(tie_break)
        .enumerate()
        .filter(|v| filter(v.0, v.1 .0))
        .min_by(|a, b| {
            let (value_a, tie_a) = a.1;
            let (value_b, tie_b) = b.1;
            value_b
                .partial_cmp(&value_a)
                .unwrap()
                .then(tie_b.partial_cmp(&tie_a).unwrap())
        })
        .map(|m| m.0)
}
