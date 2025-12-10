/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// A [ragged array](https://en.wikipedia.org/wiki/Ragged_array) that can be
/// only appended to.
///
/// This structure keeps track of a list of vectors of different lengths in a
/// compact form: a vector of values contains all the values concatenated, and
/// an vector of offsets contains the starting index of each vector in the
/// values list.
///
/// We support appending new vectors, indexing to retrieve existing vectors,
/// clearing the array while preserving allocated memory, and shrinking the
/// allocated memory.
///
/// # Examples
///
/// ```
/// use webgraph::utils::JaggedArray;
///
/// let mut jagged = JaggedArray::new();
/// jagged.push(vec![1, 2, 3]);
/// jagged.push(vec![4, 5]);
/// assert_eq!(jagged.len(), 2);
/// assert_eq!(&jagged[0], &[1, 2, 3]);
/// assert_eq!(&jagged[1], &[4, 5]);
/// jagged.push(vec![]);
/// assert_eq!(jagged.len(), 3);
/// assert_eq!(&jagged[2], &[]);
/// jagged.clear();
/// assert_eq!(jagged.len(), 0);
/// ```
#[derive(Debug, Clone)]
pub struct JaggedArray<T> {
    /// The first offset is always zero, and offsets contains one more element
    /// than the number of vectors.
    offsets: Vec<usize>,
    /// The concatenation of all vectors.
    values: Vec<T>,
}

impl<T> Default for JaggedArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JaggedArray<T> {
    /// Creates a new empty jagged array.
    pub fn new() -> Self {
        Self {
            offsets: vec![0],
            values: Vec::new(),
        }
    }

    /// Adds a vector, provided as an [`IntoIterator`], to the jagged array.
    pub fn push(&mut self, values: impl IntoIterator<Item = T>) {
        self.values.extend(values);
        self.offsets.push(self.values.len());
    }

    /// Resets the jagged array to an empty state, preserving allocated memory.
    pub fn clear(&mut self) {
        self.offsets.truncate(1);
        self.values.clear();
    }

    /// Gets the number of vectors in the jagged array.
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Shrinks the capacity of the vectors of values and offsets to fit their
    /// current length.
    pub fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.values.shrink_to_fit();
    }

    /// Shrinks the capacity of the vector of values to `min_capacity`, or to
    /// the [overall number of values](Self::num_values) if it is greater.
    ///
    /// Note that this method does not affect the offsets capacity.
    pub fn shrink_values_to(&mut self, min_capacity: usize) {
        self.values.shrink_to(min_capacity);
    }

    /// Returns the capacity of the vector storing values.
    pub fn values_capacity(&self) -> usize {
        self.values.capacity()
    }

    /// Returns the overall number of values in the jagged array.
    pub fn num_values(&self) -> usize {
        self.values.len()
    }
}

impl<T> core::ops::Index<usize> for JaggedArray<T> {
    type Output = [T];
    /// Retrieves the vector at the given index.
    fn index(&self, row: usize) -> &Self::Output {
        let start = self.offsets[row];
        let end = self.offsets[row + 1];
        &self.values[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_array_is_empty() {
        let matrix: JaggedArray<i32> = JaggedArray::new();
        assert_eq!(matrix.len(), 0);
    }

    #[test]
    fn test_push_rows_and_indexing() {
        let mut matrix = JaggedArray::new();
        matrix.push(vec![1, 2, 3]);
        matrix.push(vec![4, 5]);
        assert_eq!(matrix.len(), 2);
        assert_eq!(&matrix[0], &[1, 2, 3]);
        assert_eq!(&matrix[1], &[4, 5]);
    }

    #[test]
    fn test_ragged_rows_and_empty_rows() {
        let mut matrix = JaggedArray::new();
        matrix.push(vec![1]);
        matrix.push(vec![]);
        matrix.push(vec![2, 3, 4]);
        assert_eq!(matrix.len(), 3);
        assert_eq!(&matrix[0], &[1]);
        assert_eq!(&matrix[1], &[]);
        assert_eq!(&matrix[2], &[2, 3, 4]);
    }

    #[test]
    fn test_clear_and_reuse() {
        let mut matrix = JaggedArray::new();
        matrix.push(vec![1, 2]);
        matrix.push(vec![3]);
        assert_eq!(matrix.len(), 2);
        matrix.clear();
        assert_eq!(matrix.len(), 0);
        matrix.push(vec![10, 20]);
        matrix.push(vec![]);
        matrix.push(vec![30]);
        assert_eq!(matrix.len(), 3);
        assert_eq!(&matrix[0], &[10, 20]);
        assert_eq!(&matrix[1], &[]);
        assert_eq!(&matrix[2], &[30]);
    }

    #[test]
    #[should_panic]
    fn test_index_out_of_bounds() {
        let mut matrix = JaggedArray::new();
        matrix.push(vec![1]);
        let _ = &matrix[1];
    }
}
