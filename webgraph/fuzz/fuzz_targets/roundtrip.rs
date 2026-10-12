/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![no_main]
use libfuzzer_sys::fuzz_target;
use webgraph::fuzz::roundtrip::*;

fuzz_target!(|data: FuzzCase| harness(data));
