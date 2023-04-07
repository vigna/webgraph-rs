import os, re
import subprocess
from code_tables_generator import *

with open("tables.csv", "w") as f:
    for bits in range(1, 16):
        gen_unary(bits, 63)
        gen_gamma(bits, 256)
        
        stdout = subprocess.check_output(
            "cargo bench", shell=True,
            env={
                **os.environ,
                "RUSTFLAGS":"-C target-cpu=native",
            }
        ).decode()

        for pattern, tp in re.findall(r"test (\S+)\s*.+?(\d+) MB/s", stdout):
            f.write("{},{},{}\n".format(bits, pattern, tp))

        
