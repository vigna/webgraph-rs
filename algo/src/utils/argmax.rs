/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// Returns the index of the maximum value in a slice, or [`None`] if the slice
/// is empty.
///
/// If the maximum appears several times, this methods returns the
/// position of the first instance.
///
/// # Arguments
/// * `slice`: the slice of elements.
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
pub fn argmax<T: std::cmp::PartialOrd + Copy>(slice: &[T]) -> Option<usize> {
    slice
        .iter()
        .enumerate()
        .rev()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|m| m.0)
}

/// Returns the index of the maximum value approved by a filter in a slice, or
/// [`None`] if no element is approved by the filter.
///
/// In case of ties, this method returns the index for which `tie_break` is
/// maximized.
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
    slice: &[T],
    tie_break: &[N],
    filter: F,
) -> Option<usize> {
    slice
        .iter()
        .zip(tie_break.iter())
        .enumerate()
        .rev()
        .filter(|v| filter(v.0, *v.1 .0))
        .max_by(|a, b| {
            let (value_a, tie_a) = a.1;
            let (value_b, tie_b) = b.1;
            value_a
                .partial_cmp(value_b)
                .unwrap()
                .then(tie_a.partial_cmp(tie_b).unwrap())
        })
        .map(|m| m.0)
}
