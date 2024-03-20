import os
import subprocess

def oracle(stack_size, capacity):
    try:
        res = subprocess.check_call(
            "./target/release/examples/bisect",
            shell=True,
            env={
                **os.environ,
                "RUST_MIN_STACK": str(int(stack_size)),
                "CAPACITY": str(int(capacity))
            },
            stdout=subprocess.PIPE,
        )
        return res == 0
    except subprocess.CalledProcessError:
        return False
with open("data.csv", "w") as f:
    for capacity_log in range(27, 33): 
        capacity = 1 << capacity_log
        print(capacity)
        guess = 1024
        m = guess
        while True:
            o = True
            for i in range(4):
                if not oracle(guess, capacity):
                    o = False
                    break
            if o:
                break
            m = guess
            guess *= 2
            print(f"{o}, {guess}, {m}, {i}")
        M = guess
        print(f"{capacity}, {M}, {m}")
        while (M - m) > 8:
            o = True
            for i in range(40):
                if not oracle(guess, capacity):
                    o = False
                    break
            print(f"{o}, {guess}, {M}, {m}, {i}")
            if o:
                M = min(guess, M)
                guess = m + ((M - m) // 2)
            else:
                m = max(guess, m)
                guess = m + ((M - m) // 2) 
        f.write(f"{capacity_log},{M}\n")
        f.flush()