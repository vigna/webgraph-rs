#!/bin/usr/python3
"""This script takes the data generated from `code_tables_study.py` and plots them"""

import sys
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv(sys.stdin, index_col=None, header=0)

for code in ["unary", "gamma", "delta", "zeta3"]:
    plt.figure(figsize=(10, 8), dpi=200, facecolor="white")
    for ty in ["read_buff", "read_unbuff"]:
        for pat in [
            "%s::L2M::Table"%code,
            "%s::M2L::Table"%code,
            "%s::L2M::NoTable"%code,
            "%s::M2L::NoTable"%code,
        ]:
            values = df[(df.pat == pat) & (df.type == ty)]
            plt.errorbar(
                values.n_bits, values.ns_median, values.ns_std,
                label=pat+"::"+ty+"_median_std", fmt="-o",
            )
            plt.fill_between(
                values.n_bits, values.ns_perc25, values.ns_perc75, 
                label=pat+"::"+ty+"_25%_75%", alpha=0.3,
            )

    ratios = df[df.type == "ratios"].groupby("n_bits").mean()
    bars = plt.bar(
        ratios.index,
        ratios.ns_median,
        yerr=[ratios.ns_perc25, ratios.ns_perc75],
        label="table hit ratio",
        color="blue",
        alpha=0.1,
    )
    for ratio,rect in zip(ratios.ns_median, bars):
        height = rect.get_height()
        plt.text(
            rect.get_x() + rect.get_width() / 2.0, 1.2, 
            "{:.2f}%".format(ratio * 100), 
            ha='center', 
            va='bottom',
            rotation=90,
        )

    left = min(ratios.index) - 1
    right = max(ratios.index) + 1

    plt.plot(
        [left - 1, right + 1],
        [1, 1],
        "--",
        alpha=0.3,
        color="gray",
        label="table hit ratio 100% line"
    )
        
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.ylim(bottom=0) #ymin is your value
    plt.xlim([left, right]) #ymin is your value
    plt.xticks(ratios.index)
    plt.title("Performances of %s codes read and writes\nin function of the table size"%(code.capitalize()))
    plt.xlabel("Table Bits")
    plt.ylabel("ns")
    plt.savefig("%s_tables.png"%code, bbox_inches="tight")
