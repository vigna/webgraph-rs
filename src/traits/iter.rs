/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/*!


Iterator traits.

*/

pub trait Tuple2 {
    type _0;
    type _1;

    fn is_tuple(self) -> (Self::_0, Self::_1);
}

impl<T, U> Tuple2 for (T, U) {
    type _0 = T;
    type _1 = U;

    fn is_tuple(self) -> (Self::_0, Self::_1) {
        self
    }
}

pub trait LendingIteratorItem<'b, WhereSelfOutlivesB = &'b Self> {
    type T;
}

pub type Item<'b, I> = <I as LendingIteratorItem<'b>>::T;

pub trait LendingIterator: for<'b> LendingIteratorItem<'b> {
    fn next(&mut self) -> Option<Item<'_, Self>>;

    fn take(self, n: usize) -> Take<Self>
    where
        Self: Sized,
    {
        Take {
            iter: self,
            remaining: n,
        }
    }

    fn map<NewItemType, F>(self, map: F) -> Map<Self, F, NewItemType>
    where
        Self: Sized,
        for<'any> F: FnMut(Item<'_, Self>) -> NewItemType,
    {
        Map { iter: self, map }
    }

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

    fn for_each(self: Self, mut f: impl FnMut(Item<'_, Self>))
    where
        Self: Sized,
    {
        self.fold((), |(), item| f(item))
    }

    fn into_iter<Item>(self: Self) -> IntoIter<Self>
    where
        Self: for<'any> LendingIteratorItem<'any, T = Item>,
        Self: Sized,
    {
        IntoIter(self)
    }
}

pub struct Take<I: LendingIterator> {
    pub(crate) iter: I,
    pub(crate) remaining: usize,
}

impl<'succ, I: LendingIterator> LendingIteratorItem<'succ> for Take<I> {
    type T = <I as LendingIteratorItem<'succ>>::T;
}

impl<I: LendingIterator> LendingIterator for Take<I> {
    fn next(self: &'_ mut Self) -> Option<Item<'_, I>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            self.iter.next()
        } else {
            None
        }
    }
}

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

struct GroupByFirst<I: Iterator<Item = (usize, usize)>> {
    iter: std::iter::Peekable<I>,
}

impl<I: Iterator<Item = (usize, usize)>> GroupByFirst<I> {
    fn new(iter: I) -> Self {
        Self {
            iter: iter.peekable(),
        }
    }
}

struct Group<'a, I: Iterator<Item = (usize, usize)>> {
    iter: &'a mut std::iter::Peekable<I>,
    first: usize,
}

impl<'a, I: Iterator<Item = (usize, usize)>> Iterator for Group<'a, I> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next_if(|&(first, _)| first == self.first)
            .map(|(_, second)| second)
    }
}

impl<'succ, I: Iterator<Item = (usize, usize)>> LendingIteratorItem<'succ> for GroupByFirst<I> {
    type T = (usize, Group<'succ, I>);
}

impl<I: Iterator<Item = (usize, usize)>> LendingIterator for GroupByFirst<I> {
    fn next(&mut self) -> Option<Item<'_, Self>> {
        let &(first, _) = self.iter.peek()?;
        Some((
            first,
            Group {
                iter: &mut self.iter,
                first,
            },
        ))
    }
}

#[test]
fn test_group_by() {
    let iter = [0, 0, 1, 1, 2, 2].into_iter().zip(0..6);
    let mut groupby = GroupByFirst::new(iter);
    if let Some((first, mut group)) = groupby.next() {
        assert_eq!(first, 0);
        assert_eq!(group.next(), Some(0));
        assert_eq!(group.next(), Some(1));
    }
    if let Some((first, mut group)) = groupby.next() {
        assert_eq!(first, 1);
        assert_eq!(group.next(), Some(2));
        assert_eq!(group.next(), Some(3));
    }
}