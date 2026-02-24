#![no_main]
use libfuzzer_sys::fuzz_target;
use webgraph::fuzz::bvcomp_and_read::*;

fuzz_target!(|data: FuzzCase| harness(data));
