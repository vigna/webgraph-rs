#![no_main]
use libfuzzer_sys::fuzz_target;
use webgraph::fuzz::roundtrip::*;

fuzz_target!(|data: FuzzCase| harness(data));
