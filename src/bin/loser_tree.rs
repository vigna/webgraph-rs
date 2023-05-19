/*!
An inplementation of loser trees.
*/

use std::cmp::max_by_key;
use std::cmp::min_by_key;
use std::mem::swap;

use alloc::vec;

#[derive(Debug, Clone)]
pub struct LoserTree<T> {
    tree: Vec<usize>,
    data: Vec<T>,
    exhausted: Vec<bool>,
}

impl<T: Ord + Copy> Iterator for LoserTree<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let tree = &mut self.tree;
        let data = &mut self.data;
        let exhausted = &mut self.exhausted;
        let len = data.len();
        dbg!(len);

        let mut winner = tree[0];
        let result = data[winner];
        exhausted[winner] = true;
        let mut parent = (winner + len) / 2;
        dbg!(winner, parent);

        while parent != 0 {
            if exhausted[winner] || data[tree[parent]] < data[winner] {
                swap(&mut tree[parent], &mut winner);
            }
            parent = parent / 2;
        }
        tree[0] = winner;
        Some(result)
    }
}

impl<T: Ord + Copy> LoserTree<T> {
    fn new(data: Vec<T>) -> Self {
        let mut tree = vec![0_usize; data.len()];
        let len = data.len();
        // Winner tree
        // Safe pairs of data
        let mut safe = (len + 1) / 2;
        for i in (safe..len).rev() {
            tree[i] = min_by_key(2 * i - len, 2 * i - len + 1, |x| &data[*x]);
        }
        // Obnoxious edge case
        if len % 2 != 0 {
            safe -= 1;
            tree[safe] = min_by_key(tree[2 * safe], 2 * safe + 1 - len, |x| &data[*x]);
        }
        // Safe pairs in the tree
        for i in (1..safe).rev() {
            tree[i] = min_by_key(tree[2 * i], tree[2 * i + 1], |x| &data[*x]);
        }

        // Loser tree
        // Safe pairs in the tree
        let mut safe = len / 2;
        tree[0] = tree[1]; // winner
        for i in 1..safe {
            tree[i] = max_by_key(tree[i * 2], tree[i * 2 + 1], |x| &data[*x]);
        }
        // Obnoxious edge case
        if len % 2 != 0 {
            tree[safe] = max_by_key(tree[2 * safe], 2 * safe + 1 - len, |x| &data[*x]);
            safe += 1;
        }
        // Safe pairs of data
        for i in safe..len {
            tree[i] = max_by_key(2 * i - len, 2 * i - len + 1, |x| &data[*x]);
        }

        Self {
            data: data,
            tree: tree,
            exhausted: vec![false; len],
        }
    }
}

fn main() {
    v = vec![2, 4, 7, 0, 1];
    let len = v.len();
    let mut tree = LoserTree::new(v);
    dbg!(&tree);
    for i in 0..len {
        dbg!(tree.next());
    }
}
