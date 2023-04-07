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

def get_best_fitting_type(n_bits):
    if n_bits <= 8:
        return "u8"
    if n_bits <= 16:
        return "u16"
    if n_bits <= 32:
        return "u32"
    if n_bits <= 64:
        return "u64"
    if n_bits <= 128:
        return "u128"
    raise ValueError()

################################################################################

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
    
################################################################################

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

def len_unary(value):
    return value + 1

# Test that the impl is reasonable
assert write_unary(0, "", True)  == "1"
assert write_unary(0, "", False) == "1"
assert write_unary(1, "", True)  == "01"
assert write_unary(1, "", False) == "10"
assert write_unary(2, "", True)  == "001"
assert write_unary(2, "", False) == "100"
assert write_unary(3, "", True)  == "0001"
assert write_unary(3, "", False) == "1000"

for i in range(256):
    wm2l = write_unary(i, "", True)
    rm2l = read_unary(wm2l, True)[0]
    wl2m = write_unary(i, "", False)
    rl2m = read_unary(wl2m, False)[0]
    l = len_unary(i)
    assert i == rm2l
    assert i == rl2m
    assert len(wm2l) == l
    assert len(wl2m) == l

def gen_unary(read_bits, write_max_val):
    with open("./src/codes/unary_tables.rs", "w") as f:
        f.write("//! Pre-computed constants used to speedup the reading and writing of unary codes\n")

        f.write("/// How many bits are needed to read the tables in this\n")
        f.write("pub const READ_BITS: u8 = {};\n".format(read_bits))

        len_ty = get_best_fitting_type(ceil(log2(read_bits)))
        f.write("/// The len we assign to a code that cannot be decoded through the table\n")
        f.write("pub const MISSING_VALUE_LEN: {} = {};\n".format(len_ty, MISSING_VALUE_LEN))
        
        for bo in ["M2L", "L2M"]:
            f.write("///Table used to speed up the reading of unary codes\n")
            f.write("pub const READ_%s: &[(%s, %s)] = &["%(
                bo, 
                get_best_fitting_type(read_bits),
                len_ty,
            ))
            for value in range(2**read_bits):
                bits = ("{:0%sb}"%read_bits).format(value)
                try:
                    value, bits_left = read_unary(bits, bo=="M2L")
                    f.write("({}, {}),".format(value, read_bits  - len(bits_left)))
                except ValueError:
                    f.write("({}, {}),".format(0, MISSING_VALUE_LEN))
            f.write("];\n")

        for bo in ["M2L", "L2M"]:
            f.write("///Table used to speed up the writing of unary codes\n")
            f.write("pub const WRITE_%s: &[(u64, u8)] = &["%bo)
            for value in range(write_max_val + 1):
                bits = write_unary(value, "", bo=="M2L")
                f.write("({}, {}),".format(int(bits, 2), len(bits)))
            f.write("];\n")

################################################################################

def read_gamma(bitstream, m2l):
    l, bitstream = read_unary(bitstream, m2l)
    if l == 0:
        return 0, bitstream
    f, bitstream = read_fixed(l, bitstream, m2l)
    v = f + (1 << l) - 1
    return v, bitstream

def write_gamma(value, bitstream, m2l):
    value += 1
    l = floor(log2(value))
    s = value - (1 << l)
    bitstream = write_unary(l, bitstream, m2l)
    if l != 0:
        bitstream = write_fixed(s, l, bitstream, m2l)
    return bitstream

def len_gamma(value):
    value += 1
    l = floor(log2(value))
    return 2*l + 1

# Test that the impl is reasonable
assert write_gamma(0, "", True)  == "1"
assert write_gamma(0, "", False) == "1"
assert write_gamma(1, "", True)  == "010"
assert write_gamma(1, "", False) == "010"
assert write_gamma(2, "", True)  == "011"
assert write_gamma(2, "", False) == "110"
assert write_gamma(3, "", True)  == "00100"
assert write_gamma(3, "", False) == "00100"
assert write_gamma(4, "", True)  == "00101"
assert write_gamma(4, "", False) == "01100"
assert write_gamma(5, "", True)  == "00110"
assert write_gamma(5, "", False) == "10100"

for i in range(256):
    wm2l = write_gamma(i, "", True)
    rm2l = read_gamma(wm2l, True)[0]
    wl2m = write_gamma(i, "", False)
    rl2m = read_gamma(wl2m, False)[0]
    l = len_gamma(i)
    assert i == rm2l
    assert i == rl2m
    assert len(wm2l) == l
    assert len(wl2m) == l

def gen_gamma(read_bits, write_max_val):
    with open("./src/codes/gamma_tables.rs", "w") as f:
        f.write("//! Pre-computed constants used to speedup the reading and writing of gamma codes\n")

        f.write("/// How many bits are needed to read the tables in this\n")
        f.write("pub const READ_BITS: u8 = {};\n".format(read_bits))
        len_ty = get_best_fitting_type(len_gamma(2**read_bits - 1))
        f.write("/// The len we assign to a code that cannot be decoded through the table\n")
        f.write("pub const MISSING_VALUE_LEN: {} = {};\n".format(len_ty, MISSING_VALUE_LEN))
        
        for bo in ["M2L", "L2M"]:
            f.write("///Table used to speed up the reading of gamma codes\n")
            f.write("pub const READ_%s: &[(%s, %s)] = &["%(
                bo, 
                get_best_fitting_type(read_bits),
                len_ty,
            ))
            for value in range(0, 2**read_bits):
                bits = ("{:0%sb}"%read_bits).format(value)
                try:
                    value, bits_left = read_gamma(bits, bo=="M2L")
                    f.write("({}, {}),".format(value, read_bits  - len(bits_left)))
                except ValueError:
                    f.write("({}, {}),".format(0, MISSING_VALUE_LEN))
            f.write("];\n")

        for bo in ["M2L", "L2M"]:
            f.write("///Table used to speed up the writing of gamma codes\n")
            f.write("pub const WRITE_%s: &[(%s, u8)] = &["%(
                bo,
                get_best_fitting_type(len_gamma(write_max_val))
            ))
            for value in range(write_max_val + 1):
                bits = write_gamma(value, "", bo=="M2L")
                f.write("({}, {}),".format(int(bits, 2), len(bits)))
            f.write("];\n")

################################################################################

if __name__ == "__main__":
    gen_unary(read_bits=8, write_max_val=63)
    gen_gamma(read_bits=8, write_max_val=256)