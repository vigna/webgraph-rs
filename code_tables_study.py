import os, re
import subprocess
from code_tables_generator import *

with open("tables.csv", "w") as f:
    for bits in range(1, 16):
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

        for line in stdout.split("\n")[1:]:
            f.write("{},".format(bits))
            f.write(line)
            f.write("\n")
        f.flush()    

import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("tables.csv", header=None)
df.columns = "bits,pat,read_cycles,write_cycles,read_seconds,write_seconds,read_ns,write_ns,read_bs,write_bs".split(",")

for code in ["unary", "gamma", "delta"]:
    plt.figure(figsize=(10, 8), dpi=200, facecolor="white")
    for pat in [
        "buffered::%s::L2M::Table"%code,
        "buffered::%s::M2L::Table"%code,
        "buffered::%s::L2M::NoTable"%code,
        "buffered::%s::M2L::NoTable"%code,
    ]:
        plt.plot(*zip(*[
            (x[0], x[list(df.columns).index("read_ns")])
            for x in df.values
            if x[1] == pat
        ]), label=pat)
        
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.title("Performances of %s codes read and writes\nin function of the table size"%(code.capitalize()))
    plt.xlabel("Table Bits")
    plt.ylabel("ns")
    plt.savefig("tables_%s.png"%code)
