#!/bin/bash -e

# Generates EF/DCF files for 64-bit and 32-bit targets

cargo run -r build ef data/cnr-2000
cargo run -r build ef data/cnr-2000-t
cargo run -r build dcf data/cnr-2000
cargo run -r build dcf data/cnr-2000-t

RUSTFLAGS="-C target-cpu=generic -C target-feature=+sse2,+bmi2" cross run -j1 -r --target i686-unknown-linux-gnu -- build ef data/cnr-2000_32/cnr-2000
RUSTFLAGS="-C target-cpu=generic -C target-feature=+sse2,+bmi2" cross run -j1 -r --target i686-unknown-linux-gnu -- build ef data/cnr-2000_32/cnr-2000-t
RUSTFLAGS="-C target-cpu=generic -C target-feature=+sse2,+bmi2" cross run -j1 -r --target i686-unknown-linux-gnu -- build dcf data/cnr-2000_32/cnr-2000
RUSTFLAGS="-C target-cpu=generic -C target-feature=+sse2,+bmi2" cross run -j1 -r --target i686-unknown-linux-gnu -- build dcf data/cnr-2000_32/cnr-2000-t
