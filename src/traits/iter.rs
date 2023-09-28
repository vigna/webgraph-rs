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
}
