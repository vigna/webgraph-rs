/*
 * SPDX-FileCopyrightText: 2024 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// A hasher that mixes `usize` values.
///
/// This can only be used to hash `usize` values and it's not a general purpose
/// hasher. It is used by the label hash maps.
#[derive(Debug, Clone, Default)]
pub(crate) struct Mix64 {
    state: u64,
}

impl core::hash::Hasher for Mix64 {
    #[inline(always)]
    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!()
    }
    #[inline(always)]
    fn write_usize(&mut self, i: usize) {
        self.state = i as u64;
        self.state ^= self.state >> 33;
        self.state = self.state.overflowing_mul(0xff51_afd7_ed55_8ccd).0;
        self.state ^= self.state >> 33;
        self.state = self.state.overflowing_mul(0xc4ce_b9fe_1a85_ec53).0;
        self.state ^= self.state >> 33;
    }
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.state
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Mix64Builder;

impl core::hash::BuildHasher for Mix64Builder {
    type Hasher = Mix64;

    fn build_hasher(&self) -> Self::Hasher {
        Mix64::default()
    }
}
