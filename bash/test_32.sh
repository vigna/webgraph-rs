#!/bin/bash -e

# Run tests on a 32-bit platform

RUSTFLAGS="-C target-cpu=generic -C target-feature=+sse2,+bmi2" cross test -j1 --features slow_tests --target i686-unknown-linux-gnu
