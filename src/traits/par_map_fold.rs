/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use rayon::ThreadPool;

impl<I: Iterator> ParMapFoldIter for I where I::Item: Send {}

pub trait ParMapFoldIter: Iterator
where
    Self::Item: Send,
{
    #[inline(always)]
    fn par_map_fold<M, F, R>(&mut self, map: M, fold: F, thread_pool: &ThreadPool) -> R
    where
        M: Fn(Self::Item) -> R + Send + Sync,
        F: Fn(R, R) -> R + Send + Sync,
        R: Send + Default,
    {
        self.par_map_fold_with((), |_, i| map(i), fold, thread_pool)
    }

    #[inline(always)]
    fn par_map_fold_with<T, M, F, R>(
        &mut self,
        init: T,
        map: M,
        fold: F,
        thread_pool: &ThreadPool,
    ) -> R
    where
        T: Clone + Send,
        M: Fn(&mut T, Self::Item) -> R + Send + Sync,
        F: Fn(R, R) -> R + Send + Sync,
        R: Send + Default,
    {
        self.par_map_fold2_with(init, map, &fold, &fold, thread_pool)
    }

    #[inline(always)]
    fn par_map_fold2<M, IF, OF, A, R>(
        &mut self,
        map: M,
        inner_fold: IF,
        outer_fold: OF,
        thread_pool: &ThreadPool,
    ) -> A
    where
        M: Fn(Self::Item) -> R + Send + Sync,
        IF: Fn(A, R) -> A + Send + Sync,
        OF: Fn(A, A) -> A + Send + Sync,
        A: Send + Default,
    {
        self.par_map_fold2_with((), |_, i| map(i), inner_fold, outer_fold, thread_pool)
    }

    fn par_map_fold2_with<T, M, IF, OF, A, R>(
        &mut self,
        init: T,
        map: M,
        inner_fold: IF,
        outer_fold: OF,
        thread_pool: &ThreadPool,
    ) -> A
    where
        T: Clone + Send,
        M: Fn(&mut T, Self::Item) -> R + Send + Sync,
        IF: Fn(A, R) -> A + Send + Sync,
        OF: Fn(A, A) -> A + Send + Sync,
        A: Send + Default,
    {
        let (_min_len, max_len) = self.size_hint();

        let mut num_scoped_threads = thread_pool.current_num_threads();
        if let Some(max_len) = max_len {
            num_scoped_threads = num_scoped_threads.min(max_len);
        }
        num_scoped_threads = num_scoped_threads.max(1);

        // create a channel to receive the result
        let (out_tx, out_rx) = crossbeam_channel::bounded::<A>(num_scoped_threads);
        let (in_tx, in_rx) = crossbeam_channel::bounded::<Self::Item>(2 * num_scoped_threads);

        thread_pool.in_place_scope(|scope| {
            for _thread_id in 0..num_scoped_threads {
                // create some references so that we can share them across threads
                let mut init = init.clone();
                let map = &map;
                let inner_fold = &inner_fold;
                let out_tx = out_tx.clone();
                let in_rx = in_rx.clone();

                scope.spawn(move |_| {
                    let mut res = A::default();
                    loop {
                        match in_rx.recv() {
                            Ok(val) => {
                                // apply the function and send the result
                                res = inner_fold(res, map(&mut init, val));
                            }
                            Err(_e) => {
                                out_tx.send(res).unwrap();
                                break;
                            }
                        }
                    }
                });
            }
            // these are for the threads to listen to, we don't need them anymore
            drop(out_tx);
            drop(in_rx);
            for val in self {
                in_tx.send(val).unwrap();
            }
            drop(in_tx); // close the channel so the threads will exit when done
                         // listen on the output channel for results
            out_rx.into_iter().fold(A::default(), outer_fold)
        })
    }
}
