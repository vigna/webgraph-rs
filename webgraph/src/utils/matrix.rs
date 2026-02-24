use std::ops::{Index, IndexMut};

/// A generic dense matrix stored as a flat Vec in row-major order.
/// `cols` is the number of columns; the number of rows equals data.len() / cols.
#[derive(Debug, Clone)]
pub struct Matrix<T> {
    data: Vec<T>,
    cols: usize,
}

impl<T: Clone + Default> Matrix<T> {
    /// Creates a new n x m matrix with all elements set to T::default()
    /// Guaranteed to be contiguous in memory.
    pub fn new(rows: usize, cols: usize) -> Self {
        let len = rows * cols;
        let data = vec![T::default(); len];
        Matrix { data, cols }
    }
}

impl<T> Index<(usize, usize)> for Matrix<T> {
    type Output = T;

    fn index(&self, index: (usize, usize)) -> &Self::Output {
        let (row, col) = index;
        &self.data[row * self.cols + col]
    }
}

impl<T> IndexMut<(usize, usize)> for Matrix<T> {
    fn index_mut(&mut self, index: (usize, usize)) -> &mut Self::Output {
        let (row, col) = index;
        &mut self.data[row * self.cols + col]
    }
}
