/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!

Traits for lending iterators.

The design of [LendingIterator] was proposed by
[Daniel Henry Mantilla](https://github.com/danielhenrymantilla/lending-iterator.rs/issues/13);
also [Yoav Tzfati](https://github.com/Crazytieguy/gat-lending-iterator/issues/12) participated to
the discussion, providing information and code.

Note that the design is significantly more complex than
```rust
pub trait LendingIterator {
    type Item<'b> where Self: 'b;
    fn next(&mut self) -> Option<Self::Item<'_>>;
}
```
However, the previous design proved to be too restrictive, and would have made it impossible to
write types such as [PermutedGraph](crate::graph::permuted_graph::PermutedGraph)
or [PairsGraph](crate::graph::pairs_graph::PairsGraph).

*/

/// A trait specifying the type of the items of a [LendingIterator].
///
/// Note that the trait specifies that `Self` must outlive `'b`
/// in a way that is inherited by implementations.
pub trait LendingIteratorItem<'b, WhereSelfOutlivesB = &'b Self> {
    type T;
}

/// A readable shorthand for the type of the items of a [LendingIterator] `I`.
pub type Item<'a, I> = <I as LendingIteratorItem<'a>>::T;

/// The main trait: an iterator that borrows its items mutably from
/// `self`, which implies that you cannot own at the same time two returned
/// items.
///
/// The trait depends on the trait [LendingIteratorItem], via
/// higher-kind trait bounds.
pub trait LendingIterator: for<'a> LendingIteratorItem<'a> {
    fn next(&mut self) -> Option<Item<'_, Self>>;

    /// Like [`Iterator::take`], creates an iterator that yields the first n elements,
    /// or fewer if the underlying iterator ends sooner.
    fn take(self, n: usize) -> Take<Self>
    where
        Self: Sized,
    {
        Take {
            iter: self,
            remaining: n,
        }
    }

    /// Like [`Iterator::take_while`], creates an iterator that yields elements based
    /// on a predicate.
    fn take_while<P>(self, predicate: P) -> TakeWhile<Self, P>
    where
        Self: Sized,
        P: FnMut(&'_ Item<'_, Self>) -> bool,
    {
        TakeWhile { iter: self, predicate, ended: false }
    }

    /// Like [`Iterator::inspect`], does something with each element of an iterator,
    /// passing the value on.
    fn inspect<F>(self, f: F) -> Inspect<Self, F>
    where
        Self: Sized,
        for<'any> F: FnMut(&'_ Item<'_, Self>),
    {
        Inspect { iter: self, f }
    }

    /// Like [`Iterator::map`], takes a closure and creates an iterator which calls
    /// that closure on each element.
    fn map<NewItemType, F>(self, map: F) -> Map<Self, F, NewItemType>
    where
        Self: Sized,
        for<'any> F: FnMut(Item<'_, Self>) -> NewItemType,
    {
        Map { iter: self, map }
    }

    /// Like [`Iterator::fold`], folds every element into an accumulator by applying
    /// an operation, returning the final result.
    fn fold<B, F>(mut self, init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Item<'_, Self>) -> B,
    {
        let mut accum = init;
        while let Some(x) = self.next() {
            accum = f(accum, x);
        }
        accum
    }

    /// Like [`Iterator::for_each`], calls a closure on each element of an iterator.
    fn for_each(self, mut f: impl FnMut(Item<'_, Self>))
    where
        Self: Sized,
    {
        self.fold((), |(), item| f(item))
    }

    /// Turns this `LendingIterator` into a regular [`Iterator`]
    fn into_iter<Item>(self) -> IntoIter<Self>
    where
        Self: for<'any> LendingIteratorItem<'any, T = Item>,
        Self: Sized,
    {
        IntoIter(self)
    }

    /// Like [`Iterator::enumerate`], creates an iterator which gives the current
    /// iteration count as well as the next value.
    fn enumerate(self) -> Enumerate<Self>
    where
        Self: Sized,
    {
        Enumerate::new(self)
    }
}

/// This struct is returned by [`LendingIterator::take`]
#[derive(Clone, Debug)]
pub struct Take<I: LendingIterator> {
    pub(crate) iter: I,
    pub(crate) remaining: usize,
}

impl<'succ, I: LendingIterator> LendingIteratorItem<'succ> for Take<I> {
    type T = <I as LendingIteratorItem<'succ>>::T;
}

impl<I: LendingIterator> LendingIterator for Take<I> {
    fn next(&'_ mut self) -> Option<Item<'_, I>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            self.iter.next()
        } else {
            None
        }
    }
}

/// This struct is returned by [`LendingIterator::map`]
#[derive(Clone, Debug)]
pub struct TakeWhile<I: LendingIterator, F>
where
    for<'any> F: FnMut(&'_ <I as LendingIteratorItem>::T) -> bool,
{
    pub(crate) iter: I,
    pub(crate) predicate: F,
    pub(crate) ended: bool,
}

impl<'succ, I: LendingIterator, F> LendingIteratorItem<'succ>
    for TakeWhile<I, F>
where
    for<'any> F: FnMut(&'_ <I as LendingIteratorItem>::T) -> bool,
{
    type T = <I as LendingIteratorItem<'succ>>::T;
}

impl<I, F> LendingIterator for TakeWhile<I, F>
where
    I: LendingIterator,
    for<'any> F: FnMut(&'_ <I as LendingIteratorItem>::T) -> bool,
{
    fn next(&mut self) -> Option<Item<'_, Self>> {
        if self.ended {
            None
        } else {
            let next_item = self.iter.next()?;
            if (self.predicate)(&next_item) {
                Some(next_item)
            } else {
                self.ended = true;
                None
            }
        }
    }
}

/// This struct is returned by [`LendingIterator::inspect`]
#[derive(Clone, Debug)]
pub struct Inspect<I: LendingIterator, F>
where
    for<'any> F: FnMut(&'_ <I as LendingIteratorItem>::T),
{
    pub(crate) iter: I,
    pub(crate) f: F,
}

impl<'succ, I: LendingIterator, F> LendingIteratorItem<'succ> for Inspect<I, F>
where
    for<'any> F: FnMut(&'_ <I as LendingIteratorItem>::T),
{
    type T = <I as LendingIteratorItem<'succ>>::T;
}

impl<I, F> LendingIterator for Inspect<I, F>
where
    I: LendingIterator,
    for<'any> F: FnMut(&'_ <I as LendingIteratorItem>::T),
{
    fn next(&mut self) -> Option<Item<'_, Self>> {
        self.iter.next().map(|item| {
            (self.f)(&item);
            item
        })
    }
}

/// This struct is returned by [`LendingIterator::map`]
#[derive(Clone, Debug)]
pub struct Map<I: LendingIterator, F, NewItemType>
where
    for<'any> F: FnMut(<I as LendingIteratorItem>::T) -> NewItemType,
{
    pub(crate) iter: I,
    pub(crate) map: F,
}

impl<'succ, I: LendingIterator, NewItemType, F> LendingIteratorItem<'succ>
    for Map<I, F, NewItemType>
where
    for<'any> F: FnMut(<I as LendingIteratorItem>::T) -> NewItemType,
{
    type T = NewItemType;
}

impl<I, NewItemType, F> LendingIterator for Map<I, F, NewItemType>
where
    I: LendingIterator,
    for<'any> F: FnMut(<I as LendingIteratorItem>::T) -> NewItemType,
{
    fn next(&mut self) -> Option<Item<'_, Self>> {
        self.iter.next().map(|item| (self.map)(item))
    }
}

/// This struct is returned by [`LendingIterator::into_iter`]
#[derive(Clone, Debug)]
pub struct IntoIter<I: ?Sized + LendingIterator>(pub I);

impl<Item, I: ?Sized + LendingIterator> Iterator for IntoIter<I>
where
    for<'any> I: LendingIteratorItem<'any, T = Item>,
{
    type Item = Item;

    fn next(self: &'_ mut IntoIter<I>) -> Option<Item> {
        self.0.next()
    }
}

/// This struct is returned by [`LendingIterator::enumerate`]
#[derive(Clone, Debug)]
pub struct Enumerate<I> {
    iter: I,
    count: usize,
}
impl<I> Enumerate<I> {
    pub fn new(iter: I) -> Enumerate<I> {
        Enumerate { iter, count: 0 }
    }
}

impl<'succ, I: LendingIterator> LendingIteratorItem<'succ> for Enumerate<I> {
    type T = (usize, <I as LendingIteratorItem<'succ>>::T);
}

impl<I: LendingIterator> LendingIterator for Enumerate<I> {
    fn next(&mut self) -> Option<Item<'_, Self>> {
        let a = self.iter.next()?;
        let i = self.count;
        self.count += 1;
        Some((i, a))
    }
}
