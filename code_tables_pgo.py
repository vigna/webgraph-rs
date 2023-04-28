#!/usr/bin/python3
"""Benchmark the code with different number of bits for the codes tables and
create a `table.csv` file with all the results
"""
import re
import os
import sys
import subprocess
from code_tables_generator import *

ROOT = os.path.dirname(os.path.abspath(__file__))
target_folder = os.path.join(ROOT, "target")
test_cov_path = os.path.join(target_folder, "test_cov.profraw")
rustup_info =  subprocess.check_output("rustup show", shell=True).decode()
arch = re.findall(r"Default host: (.+)", rustup_info)[0]
# To run it needs the following:
# cargo install rustfilt
# rustup component add --toolchain nightly llvm-tools-preview

# Get where it's installed
sysroot = subprocess.check_output("rustc --print sysroot", shell=True).decode().strip()
llvm_path = os.path.join(sysroot, "lib", "rustlib", arch, "bin")
final_cov_path = os.path.join(target_folder, "total_coverage.profdata")
test_cov_path = os.path.join(target_folder, "test_cov.profdata")

for bits in range(1, 18):
    print("Table bits:", bits, file=sys.stderr)
    for tables_num in [1, 2]:
        pgo_folder = "/tmp/pgo-data-{}-{}".format(bits, tables_num)
        pgo_merged = pgo_folder + "/merged.profdata"

        # Clean the target to force the recreation of the tables
        subprocess.check_call(
            "cargo clean", shell=True,
            cwd="benchmarks",
        )
        # Run the benchmark with native cpu optimizations and collect pgo
        _stdout = subprocess.check_output(
            "cargo run --release --target=x86_64-unknown-linux-gnu", shell=True,
            env={
                **os.environ,
                "UNARY_CODE_TABLE_BITS":str(bits),
                "GAMMA_CODE_TABLE_BITS":str(bits),
                "DELTA_CODE_TABLE_BITS":str(bits),
                "ZETA_CODE_TABLE_BITS":str(bits),
                "MERGED_TABLES":str(2 - tables_num),
                "RUSTFLAGS":"-Ctarget-cpu=native -Cprofile-generate={}".format(pgo_folder),
            },
            cwd="benchmarks",
        ).decode()

        # Merge the raw data
        subprocess.check_call(
            "{}/llvm-profdata merge {} -o {}".format(
                llvm_path, pgo_folder, pgo_merged,
            ),
            shell=True, cwd=ROOT,
        )

        # Run the benchmark with native cpu optimizations and pgo
        stdout = subprocess.check_output(
            "cargo run --release --target=x86_64-unknown-linux-gnu", shell=True,
            env={
                **os.environ,
                "UNARY_CODE_TABLE_BITS":str(bits),
                "GAMMA_CODE_TABLE_BITS":str(bits),
                "DELTA_CODE_TABLE_BITS":str(bits),
                "ZETA_CODE_TABLE_BITS":str(bits),
                "MERGED_TABLES":str(2 - tables_num),
                "RUSTFLAGS":"-Ctarget-cpu=native -Cprofile-use={}".format(pgo_merged),
            },
            cwd="benchmarks",
        ).decode()

        # Dump the header only the first time
        if bits == 1 and tables_num == 1:
            print("n_bits,tables_num," + stdout.split('\n')[0])
        # Dump all lines and add the `n_bits` column
        for line in stdout.split("\n")[1:]:
            if len(line.strip()) != 0:
                print("{},{},{}".format(bits, tables_num, line))
        
        sys.stdout.flush()

# Reset the tables to the original state
generate_default_tables()