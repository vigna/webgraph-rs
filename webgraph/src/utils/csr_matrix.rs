/// A Compressed Sparse Row (CSR) matrix representation for storing successors lists,
/// the goal is to minimize memory usage while allowing efficient access to the successors of each node.
///
/// It's like a Vec<Vec<T>>, where you can only append new rows and you can't modify existing rows.
#[derive(Debug, Clone)]
pub struct CSRMatrix<T> {
    /// The first offset is always zero, and offsets contains one more element than the number of rows.
    offsets: Vec<usize>,
    values: Vec<T>,
}

impl<T> Default for CSRMatrix<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CSRMatrix<T> {
    /// Create a new empty CSR matrix.
    pub fn new() -> Self {
        Self {
            offsets: vec![0],
            values: Vec::new(),
        }
    }

    /// Push a new row to the matrix.
    pub fn push(&mut self, values: impl IntoIterator<Item = T>) {
        self.values.extend(values);
        self.offsets.push(self.values.len());
    }

    /// Reset the matrix to empty, keeping the allocated capacity.
    pub fn clear(&mut self) {
        self.offsets.truncate(1);
        self.values.clear();
    }

    /// Get the number of rows in the matrix.
    pub fn num_rows(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Shrink the capacity of the matrix to fit its current size.
    pub fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.values.shrink_to_fit();
    }

    /// Shrink the capacity of the matrix to at least `min_capacity` elements.
    /// This does not affect the offsets capacity, but that's usually much smaller.
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.values.shrink_to(min_capacity);
    }

    /// Get the capacity of the values vector, i.e. the number of elements it can
    /// hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.values.capacity()
    }

    /// Get the total number of elements in the matrix.
    pub fn num_elements(&self) -> usize {
        self.values.len()
    }
}

impl<T> core::ops::Index<usize> for CSRMatrix<T> {
    type Output = [T];

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
    fn test_new_matrix_is_empty() {
        let matrix: CSRMatrix<i32> = CSRMatrix::new();
        assert_eq!(matrix.num_rows(), 0);
    }

    #[test]
    fn test_push_rows_and_indexing() {
        let mut matrix = CSRMatrix::new();
        matrix.push(vec![1, 2, 3]);
        matrix.push(vec![4, 5]);
        assert_eq!(matrix.num_rows(), 2);
        assert_eq!(&matrix[0], &[1, 2, 3]);
        assert_eq!(&matrix[1], &[4, 5]);
    }

    #[test]
    fn test_ragged_rows_and_empty_rows() {
        let mut matrix = CSRMatrix::new();
        matrix.push(vec![1]);
        matrix.push(vec![]);
        matrix.push(vec![2, 3, 4]);
        assert_eq!(matrix.num_rows(), 3);
        assert_eq!(&matrix[0], &[1]);
        assert_eq!(&matrix[1], &[]);
        assert_eq!(&matrix[2], &[2, 3, 4]);
    }

    #[test]
    fn test_clear_and_reuse() {
        let mut matrix = CSRMatrix::new();
        matrix.push(vec![1, 2]);
        matrix.push(vec![3]);
        assert_eq!(matrix.num_rows(), 2);
        matrix.clear();
        assert_eq!(matrix.num_rows(), 0);
        matrix.push(vec![10, 20]);
        matrix.push(vec![]);
        matrix.push(vec![30]);
        assert_eq!(matrix.num_rows(), 3);
        assert_eq!(&matrix[0], &[10, 20]);
        assert_eq!(&matrix[1], &[]);
        assert_eq!(&matrix[2], &[30]);
    }

    #[test]
    #[should_panic]
    fn test_index_out_of_bounds() {
        let mut matrix = CSRMatrix::new();
        matrix.push(vec![1]);
        let _ = &matrix[1];
    }
}
