/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#![no_main]
use libfuzzer_sys::fuzz_target;
use webgraph::fuzz::bvcomp_and_read::*;

fuzz_target!(|data: FuzzCase| harness(data));
