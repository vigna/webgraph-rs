#![no_main]
use webgraph::fuzz::bvcomp_and_read::*;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: FuzzCase| harness(data));
