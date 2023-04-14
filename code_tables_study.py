#!/usr/bin/python3
"""Benchmark the code with different number of bits for the codes tables and
create a `table.csv` file with all the results
"""
import os
import sys
import subprocess
from code_tables_generator import *

for bits in range(1, 18):
    for tables_num in [1, 2]:
        # Create the tables
        gen_unary(bits, 63, merged_table=tables_num == 1)
        gen_gamma(bits, 256, merged_table=tables_num == 1)
        gen_delta(bits, 256, merged_table=tables_num == 1)
        gen_zeta(bits, 256, merged_table=tables_num == 1)

        # Run the benchmark with native cpu optimizations
        stdout = subprocess.check_output(
            "cargo run --release", shell=True,
            env={
                **os.environ,
                "RUSTFLAGS":"-C target-cpu=native",
            },
            cwd="benchmarks",
        ).decode()

        # Dump the header only the first time
        if bits == 1:
            print("n_bits,tables_num," + stdout.split('\n')[0])
        # Dump all lines and add the `n_bits` column
        for line in stdout.split("\n")[1:]:
            if len(line.strip()) != 0:
                print("{},{},{}".format(bits, tables_num, line))
        
        sys.stdout.flush()

# Reset the tables to the original state
generate_default_tables()