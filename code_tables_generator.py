#!/usr/bin/python3
"""
This file generate the `./src/codes/*_tables.rs` files.
This is not a `build.rs` because it will mainly generated once and adding it to
`build.rs` would cause a big slowdown of compilation because it would invalidate
the cache.
To run just execute `$ python ./gen_code_tables.py`
"""
from math import log2, ceil, floor

# Value we will use for values we cannot read using the current tables
MISSING_VALUE_LEN = 255 # any value > 64 is fine

def get_best_fitting_type(n_bits):
    """Find the smallest rust type that can fit n_bits"""
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

def gen_table(read_bits, write_max_val, len_max_val, code_name, len_func, read_func, write_func):
    """Main routine that generates the tables for a given code."""

    with open("./src/codes/{}_tables.rs".format(code_name), "w") as f:
        f.write("//! Pre-computed constants used to speedup the reading and writing of {} codes\n".format(code_name))

        f.write("/// How many bits are needed to read the tables in this\n")
        f.write("pub const READ_BITS: u8 = {};\n".format(read_bits))
        len_ty = get_best_fitting_type(ceil(log2(len_func(2**read_bits - 1))))
        f.write("/// The len we assign to a code that cannot be decoded through the table\n")
        f.write("pub const MISSING_VALUE_LEN: {} = {};\n".format(len_ty, MISSING_VALUE_LEN))
        
        # Write the read tables
        for bo in ["M2L", "L2M"]:
            f.write("///Table used to speed up the reading of {} codes\n".format(code_name))
            f.write("pub const READ_%s: &[(%s, %s)] = &["%(
                bo, 
                get_best_fitting_type(read_bits),
                len_ty,
            ))
            for value in range(0, 2**read_bits):
                bits = ("{:0%sb}"%read_bits).format(value)
                try:
                    value, bits_left = read_func(bits, bo=="M2L")
                    f.write("({}, {}),".format(value, read_bits  - len(bits_left)))
                except ValueError:
                    f.write("({}, {}),".format(0, MISSING_VALUE_LEN))
            f.write("];\n")

        # Write the write tables
        for bo in ["M2L", "L2M"]:
            f.write("///Table used to speed up the writing of {} codes\n".format(code_name))
            f.write("pub const WRITE_%s: &[(%s, u8)] = &["%(
                bo,
                get_best_fitting_type(len_func(write_max_val))
            ))
            for value in range(write_max_val + 1):
                bits = write_func(value, "", bo=="M2L")
                f.write("({}, {}),".format(int(bits, 2), len(bits)))
            f.write("];\n")

        # Write the len table
        f.write("///Table used to speed up the skipping of {} codes\n".format(code_name))
        f.write("pub const LEN: &[%s] = &["%(
            get_best_fitting_type(ceil(log2(len_func(len_max_val))))
        ))
        for value in range(write_max_val + 1):
            f.write("{}, ".format(len_func(value)))
        f.write("];\n")

################################################################################

def read_fixed(n_bits, bitstream, m2l):
    """Read a fixed number of bits"""
    if len(bitstream) < n_bits:
        raise ValueError()
    
    if m2l:
        return int(bitstream[:n_bits], 2), bitstream[n_bits:]
    else:
        return int(bitstream[-n_bits:], 2), bitstream[:-n_bits]

def write_fixed(value, n_bits, bitstream, m2l):
    """Write a fixed number of bits"""
    if m2l:
        return bitstream + ("{:0%sb}"%n_bits).format(value)
    else:
        return ("{:0%sb}"%n_bits).format(value) + bitstream
    
################################################################################

def read_unary(bitstream, m2l):
    """Read an unary code"""
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
    """Write an unary code"""
    if m2l:
        return bitstream + "0" * value + "1"
    else:
        return "1" + "0" * value + bitstream

def len_unary(value):
    """The len of an unary code for value"""
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

# Little consistency check
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

def gen_unary(read_bits, write_max_val, len_max_val=None):
    """Configuration of `gen_table` for unary"""
    len_max_val = len_max_val or write_max_val
    return gen_table(
        read_bits, write_max_val, len_max_val, 
        "unary",
        len_unary, read_unary, write_unary,
    )

################################################################################

def read_gamma(bitstream, m2l):
    """Read a gamma code"""
    l, bitstream = read_unary(bitstream, m2l)
    if l == 0:
        return 0, bitstream
    f, bitstream = read_fixed(l, bitstream, m2l)
    v = f + (1 << l) - 1
    return v, bitstream

def write_gamma(value, bitstream, m2l):
    """Write a gamma code"""
    value += 1
    l = floor(log2(value))
    s = value - (1 << l)
    bitstream = write_unary(l, bitstream, m2l)
    if l != 0:
        bitstream = write_fixed(s, l, bitstream, m2l)
    return bitstream

def len_gamma(value):
    """Length of the gamma code of `value`"""
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

# Little consistency check
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

def gen_gamma(read_bits, write_max_val, len_max_val=None):
    """Configuration of `gen_table` for gamma"""
    len_max_val = len_max_val or write_max_val
    return gen_table(
        read_bits, write_max_val, len_max_val,
        "gamma",
        len_gamma, read_gamma, write_gamma,
    )

################################################################################

def read_delta(bitstream, m2l):
    """Read a delta code"""
    l, bitstream = read_gamma(bitstream, m2l)
    if l == 0:
        return 0, bitstream
    f, bitstream = read_fixed(l, bitstream, m2l)
    v = f + (1 << l) - 1
    return v, bitstream

def write_delta(value, bitstream, m2l):
    """Write a delta code"""
    value += 1
    l = floor(log2(value))
    s = value - (1 << l)
    bitstream = write_gamma(l, bitstream, m2l)
    if l != 0:
        bitstream = write_fixed(s, l, bitstream, m2l)
    return bitstream

def len_delta(value):
    """Length of the delta code of `value`"""
    value += 1
    l = floor(log2(value))
    return l + len_gamma(l)

# Test that the impl is reasonable
assert write_delta(0, "", True)  == "1"
assert write_delta(0, "", False) == "1"
assert write_delta(1, "", True)  == "0100"
assert write_delta(1, "", False) == "0010"
assert write_delta(2, "", True)  == "0101"
assert write_delta(2, "", False) == "1010"
assert write_delta(3, "", True)  == "01100"
assert write_delta(3, "", False) == "00110"
assert write_delta(4, "", True)  == "01101"
assert write_delta(4, "", False) == "01110"
assert write_delta(5, "", True)  == "01110"
assert write_delta(5, "", False) == "10110"

# Little consistency check
for i in range(256):
    wm2l = write_delta(i, "", True)
    rm2l = read_delta(wm2l, True)[0]
    wl2m = write_delta(i, "", False)
    rl2m = read_delta(wl2m, False)[0]
    l = len_delta(i)
    assert i == rm2l
    assert i == rl2m
    assert len(wm2l) == l
    assert len(wl2m) == l

def gen_delta(read_bits, write_max_val, len_max_val=None):
    """Configuration of `gen_table` for delta"""
    len_max_val = len_max_val or write_max_val
    return gen_table(
        read_bits, write_max_val, len_max_val,
        "delta",
        len_delta, read_delta, write_delta,
    )

################################################################################

if __name__ == "__main__":
    # Generate the default tables
    gen_unary(read_bits=8, write_max_val=63)
    gen_gamma(read_bits=8, write_max_val=256)
    gen_delta(read_bits=8, write_max_val=256)