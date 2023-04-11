import os, re
import subprocess
from code_tables_generator import *

with open("tables.csv", "w") as f:
    for bits in range(1, 20):
        gen_unary(bits, 63)
        gen_gamma(bits, 256)
        gen_delta(bits, 256)
        
        stdout = subprocess.check_output(
            "cargo run --release", shell=True,
            env={
                **os.environ,
                "RUSTFLAGS":"-C target-cpu=native",
            },
            cwd="benchmarks",
        ).decode()

        if bits == 1:
            f.write("n_bits,")
            f.write(stdout.split('\n')[0])
            f.write("\n")

        for line in stdout.split("\n")[1:]:
            if len(line.strip()) != 0:
                f.write("{},".format(bits))
                f.write(line)
                f.write("\n")
        f.flush()