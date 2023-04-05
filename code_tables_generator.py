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