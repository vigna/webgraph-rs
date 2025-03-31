/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// Returns the index of the minimum value in a slice, or [`None`] if the slice
/// is empty.
///
/// If the minimum appears several times, this methods returns the
/// position of the first instance.
///
/// # Arguments
/// * `slice`: the slice of elements.
///
/// # Panics
///
/// If a comparison returns [`None`].
///
/// # Examples
///
/// ```
/// # use webgraph_algo::utils::math::argmin;
/// let v = vec![4, 3, 1, 0, 5, 0];
/// let index = argmin(&v);
/// assert_eq!(index, Some(3));
/// ```
pub fn argmin<T: std::cmp::PartialOrd + Copy>(slice: &[T]) -> Option<usize> {
    slice
        .iter()
        .enumerate()
        .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|m| m.0)
}

/// Returns the index of the minimum value approved by a filter in a slice, or
/// [`None`] if no element is approved by the filter.
///
/// In case of ties, this method returns the index for which `tie_break` is
/// minimized.
///
/// If the minimum appears several times with the same tie break, this methods
/// returns the position of the first instance.
///
/// # Panics
///
/// If a comparison returns [`None`].
///
/// # Arguments
/// * `slice`: the slice of elements.
///
/// * `tie_break`: in case two elements of `slice` are the same, this slice
///   is used as secondary order.
///
/// * `filter`: a closure that takes as arguments the index of the element and
///   the element itself and returns true if the element is approved.
///
/// # Examples
/// ```
/// # use webgraph_algo::utils::math::argmin_filtered;
/// let v = vec![3, 2, 5, 2, 3, 2];
/// let tie = vec![5, 4, 3, 2, 1, 1];
/// let index = argmin_filtered(&v, &tie, |_, element| element > 1);
/// // Tie break wins
/// assert_eq!(index, Some(5));
///
/// let v = vec![3, 2, 5, 2, 3, 2];
/// let tie = vec![5, 4, 3, 2, 1, 2];
/// // Enumeration order wins
/// let index = argmin_filtered(&v, &tie, |_, element| element > 1);
/// assert_eq!(index, Some(3));
/// ```
pub fn argmin_filtered<
    T: std::cmp::PartialOrd + Copy,
    N: std::cmp::PartialOrd + Copy,
    F: Fn(usize, T) -> bool,
>(
    slice: &[T],
    tie_break: &[N],
    filter: F,
) -> Option<usize> {
    slice
        .iter()
        .zip(tie_break.iter())
        .enumerate()
        .filter(|v| filter(v.0, *v.1 .0))
        .min_by(|a, b| {
            let (value_a, tie_a) = a.1;
            let (value_b, tie_b) = b.1;
            value_a
                .partial_cmp(value_b)
                .unwrap()
                .then(tie_a.partial_cmp(tie_b).unwrap())
        })
        .map(|m| m.0)
}
