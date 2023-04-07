#!/usr/bin/python3
"""
This file generate the `./src/codes/gamma_tables.rs` file.
This is not a `build.rs` because it will mainly generated once and adding it to
`build.rs` would cause a big slowdown of compilation because it would invalidate
the cache.
To run just execute `$ python ./gen_code_tables.py`
"""
import os
from math import log2, ceil, floor

MISSING_VALUE_LEN = 255 # any value > 64 is fine

def read_unary(bitstream, m2l):
    if m2l:
        l = len(bitstream) - len(bitstream.lstrip("0"))
        if l == len(bitstream):
            raise ValueError()
        return l, bitstream[l + 1:]
    else:
        l = len(bitstream) - len(bitstream.rstrip("0"))
        if l == len(bitstream):
            raise ValueError()
        return l, bitstream[:-l - 1]

def write_unary(value, bitstream, m2l):
    if m2l:
        return bitstream + "0" * value + "1"
    else:
        return "1" + "0" * value + bitstream

def read_fixed(n_bits, bitstream, m2l):
    if len(bitstream) < n_bits:
        raise ValueError()
    if m2l:
        return int(bitstream[:n_bits], 2), bitstream[n_bits:]
    else:
        return int(bitstream[-n_bits:], 2), bitstream[:-n_bits]

def write_fixed(value, n_bits, bitstream, m2l):
    if m2l:
        return bitstream + ("{:0%sb}"%n_bits).format(value)
    else:
        return ("{:0%sb}"%n_bits).format(value) + bitstream

def read_gamma(bitstream, m2l):
    l, bitstream = read_unary(bitstream, m2l)
    f, bitstream = read_fixed(l, bitstream, m2l)
    v = f + (1 << l) - 1
    return v, bitstream

def write_gamma(value, bitstream, m2l):
    l = floor(log2(value))
    s = value - (1 << l)
    bitstream = write_unary(l, bitstream, m2l)
    bitstream = write_fixed(s, l, bitstream, m2l)
    return bitstream

def gen_unary(read_bits, write_max_val):
    with open("./src/codes/unary_tables.rs", "w") as f:
        f.write("//! Pre-computed constants used to speedup the reading and writing of unary codes\n")

        f.write("/// How many bits are needed to read the tables in this\n")
        f.write("pub const READ_BITS: u8 = {};\n".format(read_bits))

        f.write("/// THe len we assign to a code that cannot be decoded through the table\n")
        f.write("pub const MISSING_VALUE_LEN: u8 = {};\n".format(MISSING_VALUE_LEN))
        
        for bo in ["M2L", "L2M"]:
            f.write("///Table used to speed up the reading of unary codes\n")
            f.write("pub const READ_%s: &[(u8, u8)] = &["%bo)
            for value in range(2**read_bits):
                bits = ("{:0%sb}"%read_bits).format(value)
                try:
                    value, bits_left = read_unary(bits, bo=="M2L")
                    f.write("({}, {}),".format(value, read_bits  - len(bits_left)))
                except ValueError:
                    f.write("({}, {}),".format(0, MISSING_VALUE_LEN))
            f.write("];\n")

        for bo in ["M2L", "L2M"]:
            f.write("///Table used to speed up the reading of unary codes\n")
            f.write("pub const WRITE_%s: &[(u64, u8)] = &["%bo)
            for value in range(write_max_val + 1):
                bits = write_unary(value, "", bo=="M2L")
                f.write("({}, {}),".format(int(bits, 2), len(bits)))
            f.write("];\n")

if __name__ == "__main__":
    gen_unary(read_bits=8, write_max_val=63)