#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: webgraph::fuzz::bvcomp_and_read::FuzzCase| {
    webgraph::fuzz::bvcomp_and_read::harness(data)
});
