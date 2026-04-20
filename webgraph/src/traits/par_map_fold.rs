/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use std::collections::VecDeque;

/// Parallel mapping and folding for iterators.
///
/// This trait extends the [`Iterator`] trait with methods that map values and
/// fold them. Differently from the [Rayon] approach, elements of the iterator
/// are submitted for processing to a thread pool in the order in which they are
/// emitted. Each thread performs internal folding of the results: at the end,
/// all results provided by the threads are folded together.
///
/// Inputs and outputs of the threads are managed through buffered channels,
/// which helps when the original iterator is somewhat CPU bound.
///
/// The more generic method is [`par_map_fold2_with`], which makes it
/// possible to specify a different function for the inner and outer fold,
/// and to pass an initial value to the map function. The other methods are
/// convenience methods delegating to this one.
///
/// The `_ord` variants ([`par_map_fold_ord`], [`par_map_fold_ord_with`])
/// guarantee that the fold function receives results in the same order as
/// the input iterator emitted them. This is useful when the fold operation
/// is not commutative or associative (e.g., building an Elias–Fano
/// representation that requires monotonically non-decreasing pushes). The
/// implementation uses a [`VecDeque`]-based reorder buffer that starts
/// empty and grows on demand; in the common case (results arrive roughly
/// in order) it stays small.
///
/// [Rayon]: rayon
/// [`par_map_fold2_with`]: ParMapFold::par_map_fold2_with
/// [`par_map_fold_ord`]: ParMapFold::par_map_fold_ord
/// [`par_map_fold_ord_with`]: ParMapFold::par_map_fold_ord_with
pub trait ParMapFold: Iterator
where
    Self::Item: Send,
{
    /// Map and fold in parallel the items returned by an iterator.
    ///
    /// This method is a simplified convenience version of
    /// [`par_map_fold2_with`] that uses the
    /// same fold function for the inner and outer fold and does not provide an
    /// init value for the map function.
    ///
    /// If you need to process items in the order in which they are emitted,
    /// consider using [`par_map_fold_ord`]
    /// instead.
    ///
    /// # Arguments
    ///
    /// * `map` - a function that maps an item to a result.
    ///
    /// * `fold` - a function that folds the results of the map function.
    ///
    /// [`par_map_fold2_with`]: ParMapFold::par_map_fold2_with
    /// [`par_map_fold_ord`]: ParMapFold::par_map_fold_ord
    #[inline(always)]
    fn par_map_fold<
        R: Send + Default,
        M: Fn(Self::Item) -> R + Send + Sync,
        F: Fn(R, R) -> R + Sync,
    >(
        &mut self,
        map: M,
        fold: F,
    ) -> R {
        self.par_map_fold_with((), |_, i| map(i), fold)
    }

    /// Map and fold in parallel the items returned by an iterator.
    ///
    /// This method is a simplified convenience version of
    /// [`par_map_fold2_with`] that uses the
    /// same fold function for the inner and outer fold.
    ///
    /// If you need to process items in the order in which they are emitted,
    /// consider using [`par_map_fold_ord_with`]
    /// instead.
    ///
    /// # Arguments
    ///
    /// * `map_init` - an init value for the map function; it will be cloned
    ///   as needed.
    ///
    /// * `map` - a function that maps an item to a result.
    ///
    /// * `fold` - a function that folds the results of the map function.
    ///
    /// [`par_map_fold2_with`]: ParMapFold::par_map_fold2_with
    /// [`par_map_fold_ord_with`]: ParMapFold::par_map_fold_ord_with
    #[inline(always)]
    fn par_map_fold_with<
        T: Clone + Send,
        R: Send + Default,
        M: Fn(&mut T, Self::Item) -> R + Send + Sync,
        F: Fn(R, R) -> R + Sync,
    >(
        &mut self,
        map_init: T,
        map: M,
        fold: F,
    ) -> R {
        self.par_map_fold2_with(map_init, map, &fold, &fold)
    }

    /// Map and fold in parallel the items returned by an iterator.
    ///
    /// This method is a simplified convenience version of
    /// [`par_map_fold2_with`] that does not
    /// provide an init value for the map function.
    ///
    /// If you need to process items in the order in which they are emitted,
    /// consider using [`par_map_fold_ord`]
    /// instead.
    ///
    /// # Arguments
    ///
    /// * `map` - a function that maps an item to a result.
    ///
    /// * `inner_fold` - a function that folds the results of the map function.
    ///
    /// * `outer_fold` - a function that folds the results of the inner fold.
    ///
    /// [`par_map_fold2_with`]: ParMapFold::par_map_fold2_with
    /// [`par_map_fold_ord`]: ParMapFold::par_map_fold_ord
    #[inline(always)]
    fn par_map_fold2<
        R,
        M: Fn(Self::Item) -> R + Send + Sync,
        A: Send + Default,
        IF: Fn(A, R) -> A + Sync,
        OF: Fn(A, A) -> A,
    >(
        &mut self,
        map: M,
        inner_fold: IF,
        outer_fold: OF,
    ) -> A {
        self.par_map_fold2_with((), |_, i| map(i), inner_fold, outer_fold)
    }

    /// Map and fold in parallel the items returned by an iterator.
    ///
    /// This method is the most generic one, making it possible to specify
    /// different functions for the inner and outer fold so that the return
    /// type of the map function can be different from the type of the fold
    /// accumulator.
    ///
    /// If you need to process items in the order in which they are emitted,
    /// consider using [`par_map_fold_ord_with`]
    /// instead.
    ///
    /// Moreover, you can pass an init value for the map function that will
    /// be cloned as needed.
    ///
    /// # Arguments
    ///
    /// * `map_init` - an init value for the map function; it will be cloned
    ///   as needed.
    ///
    /// * `map` - a function that maps an item to a result.
    ///
    /// * `inner_fold` - a function that folds the results of the map function.
    ///
    /// * `outer_fold` - a function that folds the results of the inner fold.
    ///
    /// [`par_map_fold_ord_with`]: ParMapFold::par_map_fold_ord_with
    fn par_map_fold2_with<
        T: Clone + Send,
        R,
        M: Fn(&mut T, Self::Item) -> R + Send + Sync,
        A: Send + Default,
        IF: Fn(A, R) -> A + Sync,
        OF: Fn(A, A) -> A,
    >(
        &mut self,
        map_init: T,
        map: M,
        inner_fold: IF,
        outer_fold: OF,
    ) -> A {
        let (_min_len, max_len) = self.size_hint();

        let mut num_scoped_threads = rayon::current_num_threads();
        if let Some(max_len) = max_len {
            num_scoped_threads = num_scoped_threads.min(max_len);
        }
        num_scoped_threads = num_scoped_threads.max(1);

        // create a channel to receive the result
        let (out_tx, out_rx) = crossbeam_channel::bounded(num_scoped_threads);
        let (in_tx, in_rx) = crossbeam_channel::bounded(2 * num_scoped_threads);

        rayon::in_place_scope(|scope| {
            for _thread_id in 0..num_scoped_threads {
                // create some references so that we can share them across threads
                let mut init = map_init.clone();
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
            out_rx.into_rayon_iter().fold(A::default(), outer_fold)
        })
    }

    /// Maps in parallel and folds in order the items returned by an iterator.
    ///
    /// This method is a simplified convenience version of
    /// [`par_map_fold_ord_with`] that does
    /// not provide an init value for the map function.
    ///
    /// If you don't need the order guarantee, consider using
    /// [`par_map_fold`] instead.
    ///
    /// # Arguments
    ///
    /// * `map` - a function that maps an item to a result.
    ///
    /// * `fold_init` - the initial value for the fold accumulator.
    ///
    /// * `fold` - a function that folds the results of the map function,
    ///   called in the original iterator order.
    ///
    /// [`par_map_fold_ord_with`]: ParMapFold::par_map_fold_ord_with
    /// [`par_map_fold`]: ParMapFold::par_map_fold
    #[inline(always)]
    fn par_map_fold_ord<R: Send, A, M: Fn(Self::Item) -> R + Send + Sync, F: FnMut(A, R) -> A>(
        &mut self,
        map: M,
        fold_init: A,
        fold: F,
    ) -> A
    where
        Self: Send,
    {
        self.par_map_fold_ord_with((), |_, i| map(i), fold_init, fold)
    }

    /// Maps in parallel and folds in order the items returned by an iterator.
    ///
    /// Items are dispatched to a thread pool, mapped in parallel, and then
    /// folded sequentially in the original iterator order. A
    /// [`VecDeque`]-based reorder buffer is used to reassemble results.
    ///
    /// The maximum size of the buffer will be the maximum lag between the order
    /// in which items are emitted by the iterator and the order in which they
    /// are processed by the threads. In the common case (results arrive roughly
    /// in order) it stays small, but in the worst case (e.g., if the first item
    /// is very slow to process) it can grow up to the number of returned
    /// elements. With any reasonable granularity, however, the buffer should
    /// occupy negligible memory. Most of the memory occupation, in any case,
    /// will be due to the items completed but not yet processed because of
    /// some slow item that is still being computed.
    ///
    /// Note that `map_init` must be [`Sync`] (in addition to [`Clone`] and
    /// [`Send`]) because the implementation uses [`std::thread::scope`] to
    /// run the drain loop concurrently with the Rayon workers.
    ///
    /// If you don't need the order guarantee, consider using
    /// [`par_map_fold_with`] instead.
    ///
    /// # Arguments
    ///
    /// * `map_init` - an init value for the map function; it will be cloned
    ///   as needed.
    ///
    /// * `map` - a function that maps an item to a result.
    ///
    /// * `fold_init` - the initial value for the fold accumulator.
    ///
    /// * `fold` - a function that folds the results of the map function,
    ///   called in the original iterator order.
    ///
    /// [`par_map_fold_with`]: ParMapFold::par_map_fold_with
    fn par_map_fold_ord_with<
        T: Clone + Send + Sync,
        R: Send,
        A,
        M: Fn(&mut T, Self::Item) -> R + Send + Sync,
        F: FnMut(A, R) -> A,
    >(
        &mut self,
        map_init: T,
        map: M,
        fold_init: A,
        mut fold: F,
    ) -> A
    where
        Self: Send,
    {
        let (_, max_len) = self.size_hint();

        let mut num_scoped_threads = rayon::current_num_threads();
        if let Some(max_len) = max_len {
            num_scoped_threads = num_scoped_threads.min(max_len);
        }
        num_scoped_threads = num_scoped_threads.max(1);

        let (in_tx, in_rx) = crossbeam_channel::bounded(2 * num_scoped_threads);
        let (out_tx, out_rx) = crossbeam_channel::bounded::<(usize, R)>(2 * num_scoped_threads);

        // We use std::thread::scope to run three concurrent activities:
        // 1. A std thread that feeds input from the iterator
        // 2. Rayon workers (inside the feeder thread) that map items
        // 3. The calling thread that drains results in order
        //
        // We cannot use rayon::in_place_scope alone because the calling
        // thread must block on recv (to drain results), but in_place_scope
        // requires it to participate in work-stealing. Using a separate
        // std thread for feeding + rayon workers leaves all rayon threads
        // available for mapping.
        std::thread::scope(|s| {
            s.spawn(move || {
                rayon::in_place_scope(|scope| {
                    for _thread_id in 0..num_scoped_threads {
                        let mut init = map_init.clone();
                        let map = &map;
                        let out_tx = out_tx.clone();
                        let in_rx = in_rx.clone();

                        scope.spawn(move |_| {
                            for (idx, val) in in_rx {
                                out_tx.send((idx, map(&mut init, val))).unwrap();
                            }
                        });
                    }

                    drop(out_tx);
                    drop(in_rx);

                    for (idx, val) in self.enumerate() {
                        in_tx.send((idx, val)).unwrap();
                    }
                    drop(in_tx);
                });
            });

            // Drain results in order using a reorder buffer concurrently with
            // the Rayon processing threads.
            let mut next = 0;
            let mut buffer: VecDeque<Option<R>> = VecDeque::new();
            let mut acc = fold_init;

            for (idx, result) in out_rx {
                let offset = idx - next;
                if offset >= buffer.len() {
                    buffer.resize_with(offset + 1, || None);
                }
                buffer[offset] = Some(result);
                while let Some(Some(result)) = buffer.front_mut().map(Option::take) {
                    buffer.pop_front();
                    acc = fold(acc, result);
                    next += 1;
                }
            }
            acc
        })
    }
}

impl<I: Iterator> ParMapFold for I where I::Item: Send {}

#[doc(hidden)]
#[derive(Debug)]
pub struct RayonChannelIter<T> {
    // Note that we use crossbeam channels here, as they provide multiple senders.
    // When multiple-senders channels will be stabilized we will be able to switch
    // to std channels.
    channel: crossbeam_channel::Receiver<T>,
}

impl<T> Iterator for RayonChannelIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.channel.try_recv() {
                Ok(item) => return Some(item),
                Err(crossbeam_channel::TryRecvError::Disconnected) => return None,
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    rayon::yield_now();
                }
            }
        }
    }
}

#[doc(hidden)]
pub trait RayonChannelIterExt<T>: Sized {
    fn into_rayon_iter(self) -> RayonChannelIter<T>;
}

impl<T> RayonChannelIterExt<T> for crossbeam_channel::Receiver<T> {
    fn into_rayon_iter(self) -> RayonChannelIter<T> {
        RayonChannelIter { channel: self }
    }
}
