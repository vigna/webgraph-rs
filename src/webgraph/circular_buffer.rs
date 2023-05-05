/// A circular buffer which is used to keep the backreferences both in
/// sequential reads and for compressing during writes.
/// For efficency reasons, we re-use the allocated buffers to avoid pressure
/// over the allocator.
pub(crate) struct CircularBuffer {
    data: Vec<Vec<u64>>,
    end_node_id: u64,
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

    /// Get a backreference, it **has to be ** in the compression windows
    pub(crate) fn get(&self, node_id: u64) -> &[u64] {
        debug_assert_ne!(self.end_node_id, node_id);
        debug_assert!(
            (self.end_node_id - node_id) < self.data.len() as u64,
            "The circular buffer was called with a node_id not in bound"
        );
        let idx = node_id % self.data.len() as u64;
        &self.data[idx as usize]
    }

    /// Take the buffer to write the neighbours of the new node
    pub(crate) fn take(&mut self) -> Vec<u64> {
        let idx = self.end_node_id % self.data.len() as u64;
        let mut res = core::mem::take(&mut self.data[idx as usize]);
        res.clear();
        res
    }

    /// Put it back in the buffer so it can be read
    pub(crate) fn push(&mut self, data: Vec<u64>) -> &[u64] {
        let idx = self.end_node_id % self.data.len() as u64;
        self.end_node_id += 1;
        self.data[idx as usize] = data;
        &self.data[idx as usize]
    }
}
