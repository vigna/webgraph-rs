
pub struct CricularBuffer {
    data: Vec<Vec<u64>>,
    end_node_id: u64,
}

impl CricularBuffer {
    pub fn new(len: usize) -> Self {
        Self {
            end_node_id: 0,
            data: (0..len)
                .map(|_| Vec::with_capacity(100))
                .collect::<Vec<_>>(),
        }
    }

    pub fn get(&self, node_id: u64) -> &[u64] {
        debug_assert!(
            (self.end_node_id - node_id) <= self.data.len() as u64,
            "The circular buffer was called with a node_id not in bound"
        );
        let idx = node_id % self.data.len() as u64;
        &self.data[idx as usize]
    }
    
    pub fn push(&mut self) -> &mut Vec<u64> {
        self.end_node_id += 1;
        let idx = self.end_node_id % self.data.len() as u64;
        let res = &mut self.data[idx as usize];
        res.clear();
        res
    }
}