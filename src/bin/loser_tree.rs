/*!
An inplementation of loser trees.
*/

use std::cmp::max_by_key;
use std::cmp::min_by_key;
use std::mem::swap;
#[derive(Debug, Clone)]
pub struct LoserTree<T, I>
where
    T: Ord + Copy,
    I: Iterator<Item = T>,
{
    tree: Vec<usize>,
    data: Vec<T>,
    exhausted: Vec<bool>,
    iterators: Vec<I>,
}

impl<T: Ord + Copy, I: Iterator<Item = T>> Iterator for LoserTree<T, I> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let tree = &mut self.tree;
        let data = &mut self.data;
        let exhausted = &mut self.exhausted;
        let len = data.len();

        let mut winner = tree[0];
        let result = data[winner];
        if let Some(next) = self.iterators[winner].next() {
            data[winner] = next;
        } else {
            exhausted[winner] = true;
        }

        let mut parent = (winner + len) / 2;

        while parent != 0 {
            if !exhausted[tree[parent]] && (exhausted[winner] || data[tree[parent]] < data[winner])
            {
                swap(&mut tree[parent], &mut winner);
            }
            parent = parent / 2;
        }
        tree[0] = winner;
        Some(result)
    }
}

impl<T: Ord + Copy, I: Iterator<Item = T>> LoserTree<T, I> {
    fn new(mut iterators: Vec<I>) -> Self {
        let len = iterators.len();
        let mut tree = vec![0_usize; len];
        let mut data = vec![];
        for iterator in &mut iterators {
            data.push(iterator.next().unwrap());
        }
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
            iterators: iterators,
        }
    }
}

fn main() {
    let mut v = vec![];
    for _ in 0..3 {
        v.push((0..1_000_000_000).into_iter());
    }
    //let mut m = itertools::kmerge(v);
    let mut m = LoserTree::new(v);
    let start = std::time::Instant::now();
    for _ in 0..3_000_000_000_usize {
        std::hint::black_box(m.next());
    }
    println!("{}", start.elapsed().as_secs_f64());
}
