import os
import time
import json
import hashlib
import subprocess
from tqdm.auto import tqdm

# ~40 sec to run, 1 m to build
GRAPH = "~/graphs/twitter-2010"

# defaults:
# [profile.release]
# opt-level = 3
# debug = false
# split-debuginfo = '...'  # Platform-specific.
# strip = "none"
# debug-assertions = false
# overflow-checks = false
# lto = false
# panic = 'unwind'
# incremental = false
# codegen-units = 16
# rpath = false

FLAGS = {
    #"-Clinker=":["gcc", "ld", "mold", "lld", "gold"],
    "-Cembed-bitcode=":["yes", "no"],
    "-Clto=":[
        "thin -Clinker-plugin-lto=yes -Cembed-bitcode=yes -Zdylib-lto", 
        "fat -Clinker-plugin-lto=yes -Cembed-bitcode=yes -Zdylib-lto"
    ],
    "-Coverflow-checks=":["yes"],
    "-Ctarget-cpu=":["x86-64", "x86-64-v2", "x86-64-v3", "x86-64-v4", "native"],
    # these are the default for -Os, -Oz, -O3, and just a big number
    # https://llvm.org/doxygen/InlineCost_8h_source.html
    "-Cllvm-args=--inline-threshold=":[5, 50, 250, 100_000],
    # 16 is default, 256 is for incremental builds and debug builds
    "-Ccodegen-units=":[1, 16, 256], 
    "-Cdebuginfo=":["0", "1", "2"],
    "-Cdebug-assertions=":["yes"],
    "-Copt-level=":["s", "z", "1", "2", "3"],
    "-Cpanic=":["abort"],
    "-Cstrip=":["debuginfo", "symbols"],
    "-Ccode-model=":["small", "medium", "large"],
}

def hash_flags(flags):
    return hashlib.sha256(json.dumps(sorted(flags.items())))

def generate_flag_combinations(flags):
    total_combinations = 1
    for values in flags.values():
        total_combinations += len(values)
        
    print(f"Total combinations: {total_combinations}")
    
    # Generate all combinations using itertools.product
    with tqdm(total=total_combinations) as pbar:
        for flag, vals in flags.items():
            for val in vals:
                yield f"{flag}{val}"
                pbar.update(1)
            
taskset_build = "taskset -c 0-7,16-23"
taskset_run = "taskset -c 7"

def bench(f, flags = None, name = None):
    if name is not None:
        f.write(f"{name}\t")
    else:
        f.write(json.dumps(flags) + "\t")
        
    if flags is not None:
        env = {
            **os.environ,
            "RUSTFLAGS":flags,
        }
    else:
        env = {**os.environ}
        env.pop("RUSTFLAGS", None)
    f.flush()
    
    subprocess.check_call(
        "cargo clean",
        shell=True
    )
    
    start = time.time()
    subprocess.check_call(
        f"{taskset_build} cargo build --release",
        env=env,
        shell=True,
    )
    build_time = time.time() - start

    f.write(str(build_time) + "\t")
    f.flush()

    start = time.time()
    subprocess.check_call(
        f"{taskset_run} target/release/webgraph bench bf-visit {GRAPH}",
        shell=True
    )
    runtime = time.time() - start
    f.write(str(runtime))
    f.write("\n")
    f.flush()

with open("results.csv", "a") as f:
    f.write("\n")
    for _ in range(101):
        bench(f, name="fullfat", flags=" ".join([
            #"-Cllvm-args=--inline-threshold=100000",
            "-Ccodegen-units=1",
            "-Ctarget-cpu=native",
            "-Copt-level=3",
            "-Cpanic=abort",
            "-Clto=fat",
            "-Clinker-plugin-lto=yes",
            "-Cembed-bitcode=yes",
            "-Zdylib-lto",
        ])) # Full release flags
        bench(f, name="fullthin", flags=" ".join([
            #"-Cllvm-args=--inline-threshold=100000",
            "-Ctarget-cpu=native",
            "-Copt-level=3",
            "-Cpanic=abort",
            "-Clto=thin",
            "-Clinker-plugin-lto=yes",
            "-Cembed-bitcode=yes",
            "-Zdylib-lto",
        ])) # Full release flags
        
        bench(f, name="default") # Default flags
        for combination in generate_flag_combinations(FLAGS):
            try:
                bench(f, combination)
            except Exception as e:
                f.write("ERROR\n")
                f.flush()
