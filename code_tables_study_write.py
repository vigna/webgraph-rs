#!/usr/bin/python3
"""Benchmark the code with different number of bits for the codes tables and
create a `table.csv` file with all the results
"""
import os
import sys
import subprocess
from code_tables_generator import generate_default_tables

for bits in range(1, 18):
    value_max = 2**bits
    print("Table bits:", bits, file=sys.stderr)
    for tables_num in [1, 2]:
        # Clean the target to force the recreation of the tables
        subprocess.check_call(
            "cargo clean", shell=True,
            cwd="benchmarks",
        )
        # Run the benchmark with native cpu optimizations
        stdout = subprocess.check_output(
            "cargo run --release",
            shell=True,
            env={
                **os.environ,
                "UNARY_CODE_TABLE_MAX":str(value_max),
                "GAMMA_CODE_TABLE_MAX":str(value_max),
                "DELTA_CODE_TABLE_MAX":str(value_max),
                "ZETA_CODE_TABLE_MAX":str(value_max),
                "MERGED_TABLES":str(2 - tables_num),
                "RUSTFLAGS":"-C target-cpu=native",
            },
            cwd="benchmarks",
        ).decode()

        # Dump the header only the first time
        if bits == 1 and tables_num == 1:
            print("max,tables_num," + stdout.split("\n")[0])
        # Dump all lines and add the `max` column
        for line in stdout.split("\n")[1:]:
            if len(line.strip()) != 0:
                print("{},{},{}".format(value_max, tables_num, line))

        sys.stdout.flush()

# Reset the tables to the original state
generate_default_tables()
