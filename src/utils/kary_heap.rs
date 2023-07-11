#[inline]
pub fn unchecked_swap<T: Copy>(vec: &mut [T], a: usize, b: usize) {
    unsafe {
        let pa: *mut T = vec.get_unchecked_mut(a);
        let pb: *mut T = vec.get_unchecked_mut(b);
        std::ptr::swap(pa, pb);
    }
}

/// A k-ary heap implementation
#[derive(Clone, Debug)]
pub struct KAryHeap<T: PartialOrd, const ARITY: usize = 4> {
    values: Vec<T>,
    heap: Vec<usize>,
}

impl<const ARITY: usize, T: PartialOrd> Default for KAryHeap<T, ARITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const ARITY: usize, T: PartialOrd> KAryHeap<T, ARITY> {
    /// Initialize a new empty heap
    pub fn new() -> Self {
        KAryHeap {
            values: Vec::new(),
            heap: Vec::new(),
        }
    }

    /// Initialize a new empty heap which is guaranteed to hold at least
    /// `capacity` elements without triggering a re-allocation.
    pub fn with_capacity(capacity: usize) -> Self {
        KAryHeap {
            values: Vec::with_capacity(capacity),
            heap: Vec::with_capacity(capacity),
        }
    }

    /// Get the index of the father of the given node
    #[inline(always)]
    fn parent(node: usize) -> usize {
        node.saturating_sub(1) / ARITY
    }

    /// Get the index of the first child of the current node
    #[inline(always)]
    fn first_child(node: usize) -> usize {
        (node * ARITY) + 1
    }

    // If the heap is empty or not
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// add a value to the heap
    #[inline]
    pub fn push(&mut self, value: T) {
        // Insert the value and get its index
        let mut idx = self.values.len();
        self.values.push(value);
        self.heap.push(idx);
        let value = &self.values[idx];

        // bubble up the value until the heap property holds
        loop {
            let parent_idx = Self::parent(idx);

            // The heap condition is respected so we can stop.
            // This also handles the case of the node at the root since
            // self.parent(0) == 0 => current_value == parent_value
            if value >= &self.values[self.heap[parent_idx]] {
                break;
            }

            // swap the parent and the child
            unchecked_swap(&mut self.heap, idx, parent_idx);

            // Update the mutables
            idx = parent_idx;
        }
    }

    #[inline]
    pub fn peek(&self) -> &T {
        &self.values[self.heap[0]]
    }

    #[inline]
    pub fn peek_mut(&mut self) -> &mut T {
        &mut self.values[self.heap[0]]
    }

    /// remove and return the smallest value
    #[inline]
    pub fn pop(&mut self) {
        // if the queue is empty we can early-stop.
        if self.values.is_empty() {
            return;
        }

        // remove the minimum from the tree and put the last value as the head
        self.heap.swap_remove(0);

        // if there are values left, bubble down the new head to fix the heap
        if !self.heap.is_empty() {
            self.bubble_down(0);
        }
    }

    #[inline(always)]
    pub fn bubble_down(&mut self, idx: usize) {
        // fix the heap by bubbling down the value
        let mut idx = idx;
        let value = &self.values[self.heap[idx]];
        loop {
            // get the indices of the right and left child
            let mut min_idx = Self::first_child(idx);
            if min_idx >= self.heap.len() {
                break;
            }
            let mut min_value = &self.values[self.heap[min_idx]];

            let end_idx = (Self::first_child(idx) + ARITY).min(self.heap.len());

            for i in min_idx + 1..end_idx {
                let v = &self.values[self.heap[i]];

                if min_value > v {
                    min_idx = i;
                    min_value = v;
                }
            }

            // and the heap rule is violated
            if min_value < value {
                // fix it and keep bubbling down
                unchecked_swap(&mut self.heap, idx, min_idx);
                idx = min_idx;
                continue;
            }

            // the min heap rule holds for both childs so we can exit.
            break;
        }
    }
}

#[cfg_attr(test, test)]
#[cfg(test)]
fn test_kary_heap() {}
