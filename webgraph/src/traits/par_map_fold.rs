/*
 * SPDX-FileCopyrightText: 2025 Tommaso Fontana
 * SPDX-FileCopyrightText: 2025 Inria
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

impl<I: Iterator> ParMapFold for I where I::Item: Send {}

/// Parallel mapping and folding for iterators.
///
/// This trait extends the [`Iterator`] trait with methods that map values and
/// fold them. Differently from the [Rayon](rayon) approach, elements of the iterator
/// are submitted for processing to a thread pool in the order in which the are
/// emitted. Each thread performs internal folding of the results: at the end,
/// all results provided by the threads are folded together.
///
/// Inputs and outputs of the threads are managed through buffered channels,
/// which helps when the original iterator is somewhat CPU bound.
///
/// The more generic method is
/// [`par_map_fold2_with`](ParMapFold::par_map_fold2_with), which allows to
/// specify a different function for the inner and outer fold, and to pass an
/// initial value to the map function. The other methods are convenience
/// methods delegating to this one.
pub trait ParMapFold: Iterator
where
    Self::Item: Send,
{
    /// Map and fold in parallel the items returned by an iterator.
    ///
    /// This method is a simplified convenience version of
    /// [`par_map_fold2_with`](ParMapFold::par_map_fold2_with) that uses the
    /// same fold function for the inner and outer fold and does not provide an
    /// init value for the map function.
    ///
    /// # Arguments
    ///
    /// * `fold`: a function that folds the results of the map function.
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
    /// [`par_map_fold2_with`](ParMapFold::par_map_fold2_with) that uses the
    /// same fold function for the inner and outer fold.
    ///
    /// # Arguments
    ///
    /// * `init`: an init value for the map function; it will cloned as needed.
    ///
    /// * `map`: a function that maps an item to a result.
    ///
    /// * `fold`: a function that folds the results of the map function.
    #[inline(always)]
    fn par_map_fold_with<
        T: Clone + Send,
        R: Send + Default,
        M: Fn(&mut T, Self::Item) -> R + Send + Sync,
        F: Fn(R, R) -> R + Sync,
    >(
        &mut self,
        init: T,
        map: M,
        fold: F,
    ) -> R {
        self.par_map_fold2_with(init, map, &fold, &fold)
    }

    /// Map and fold in parallel the items returned by an iterator.
    ///
    /// This method is a simplified convenience version of
    /// [`par_map_fold2_with`](ParMapFold::par_map_fold2_with) that do not
    /// provides an init value for the map function.
    ///
    /// # Arguments
    ///
    /// * `map`: a function that maps an item to a result.
    ///
    /// * `inner_fold`: a function that folds the results of the map function.
    ///
    /// * `outer_fold`: a function that folds the results of the inner fold.
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
    /// This method is the most generic one, allowing to specify different
    /// functions for the inner and outer fold, which makes it possible to
    /// have the return type of the map function to be different from the
    /// type of the fold accumulator.
    ///
    /// Moreover, you can pass an init value that will be cloned as needed.
    ///
    /// # Arguments
    ///
    /// * `init`: an init value for the map function; it will cloned as needed.
    ///
    /// * `map`: a function that maps an item to a result.
    ///
    /// * `inner_fold`: a function that folds the results of the map function.
    ///
    /// * `outer_fold`: a function that folds the results of the inner fold.
    fn par_map_fold2_with<
        T: Clone + Send,
        R,
        M: Fn(&mut T, Self::Item) -> R + Send + Sync,
        A: Send + Default,
        IF: Fn(A, R) -> A + Sync,
        OF: Fn(A, A) -> A,
    >(
        &mut self,
        init: T,
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
            out_rx.into_rayon_iter().fold(A::default(), outer_fold)
        })
    }
}

#[doc(hidden)]
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

/// Turns `self` into a Rayon-friendly iterator.
///
/// Mainly used through the extension trait implemented for
/// [`Receiver`](crossbeam_channel::Receiver).
pub trait RayonChannelIterExt<T>: Sized {
    fn into_rayon_iter(self) -> RayonChannelIter<T>;
}

impl<T> RayonChannelIterExt<T> for crossbeam_channel::Receiver<T> {
    fn into_rayon_iter(self) -> RayonChannelIter<T> {
        RayonChannelIter { channel: self }
    }
}
