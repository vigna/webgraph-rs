/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

use lender::prelude::*;

pub trait SplitLabeling {
    type InnerLender: Lender;
    type SplitIter: IntoIterator<Item = Self::InnerLender>;
    fn split(self, n: usize) -> Self::SplitIter;
}
