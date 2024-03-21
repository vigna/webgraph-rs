/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

pub trait SplitIterator {
    type InnerIter: Iterator;
    type SplitIter: IntoIterator<Item = Self::InnerIter>;
    fn split(self, n: usize) -> Self::SplitIter;
}
