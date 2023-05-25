/// A circular buffer which is used to keep the backreferences both in
/// sequential reads and for compressing during writes.
/// For efficency reasons, we re-use the allocated buffers to avoid pressure
/// over the allocator.
pub(crate) struct CircularBuffer {
    data: Vec<Vec<usize>>,
    end_node_id: usize,
}

impl CircularBuffer {
    /// Create a new circular buffer that can hold `len` values. This should be
    /// equal to the compression windows + 1 so there is space for the new data.
    pub(crate) fn new(len: usize) -> Self {
        Self {
            end_node_id: 0,
            data: (0..len)
                .map(|_| Vec::with_capacity(100))
                .collect::<Vec<_>>(),
        }
    }

    #[inline]
    #[must_use]
    pub(crate) fn get_end_node_id(&self) -> usize {
        self.end_node_id
    }

    /// Get a backreference, it **has to be ** in the compression windows
    pub(crate) fn get(&self, node_id: usize) -> &[usize] {
        debug_assert_ne!(self.end_node_id, node_id);
        debug_assert!(
            (self.end_node_id - node_id) < self.data.len(),
            "The circular buffer was called with a node_id not in bound"
        );
        let idx = node_id % self.data.len();
        &self.data[idx]
    }

    /// Take the buffer to write the neighbours of the new node
    pub(crate) fn take(&mut self) -> Vec<usize> {
        let idx = self.end_node_id % self.data.len();
        let mut res = core::mem::take(&mut self.data[idx]);
        res.clear();
        res
    }

    /// Put it back in the buffer so it can be read
    pub(crate) fn push(&mut self, data: Vec<usize>) -> &[usize] {
        let idx = self.end_node_id % self.data.len();
        self.end_node_id += 1;
        self.data[idx] = data;
        &self.data[idx]
    }
}
