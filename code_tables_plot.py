#!/bin/usr/python3
"""This script takes the data generated from `code_tables_study.py` and plots them"""

import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv(sys.stdin, index_col=None, header=0)

for code in ["unary", "gamma", "delta", "zeta3"]:
    plt.figure(figsize=(10, 8), dpi=200, facecolor="white")
    for ty in ["read_buff", "read_unbuff"]:
        for tables_n in [1, 2]:
            if tables_n == 1:
                table_txt = "merged"
                marker = "o"
            else:
                table_txt = "sep"
                marker = "s"

            for pat in [
                "%s::L2M::Table" % code,
                "%s::M2L::Table" % code,
            ]:
                values = df[
                    (df.pat == pat) & (df.type == ty) & (df.tables_num == tables_n)
                ]
                m = min(values.ns_median)
                i = np.argmin(values.ns_median)
                plt.errorbar(
                    values.n_bits,
                    values.ns_median,  # values.ns_std,
                    label="{}::{}::{} (min: {:.3f}ns {} bits)".format(
                        "::".join(pat.split("::")[1:]), table_txt, ty, m, i
                    ),
                    marker=marker,
                )
                plt.fill_between(
                    values.n_bits,
                    values.ns_perc25,
                    values.ns_perc75,
                    alpha=0.3,
                )

        for pat in [
            "%s::L2M::NoTable" % code,
            "%s::M2L::NoTable" % code,
        ]:
            values = df[(df.pat == pat) & (df.type == ty)].groupby("n_bits").mean()
            m = min(values.ns_median)
            plt.errorbar(
                values.index,
                values.ns_median,  # values.ns_std,
                label="{}::{} (min: {:.3f}ns)".format(
                    "::".join(pat.split("::")[1:]), ty, m
                ),
                marker="^",
            )
            plt.fill_between(
                values.index,
                values.ns_perc25,
                values.ns_perc75,
                alpha=0.3,
            )

    ratios = (
        df[df.pat.str.contains(code) & (df.tables_num == tables_n)]
        .groupby("n_bits")
        .mean()
    )
    bars = plt.bar(
        ratios.index,
        ratios.ratio,
        label="table hit ratio",
        fc=(0, 0, 1, 0.3),
        linewidth=1,
        edgecolor="black",
    )
    for ratio, rect in zip(ratios.ratio, bars):
        height = rect.get_height()
        plt.text(
            rect.get_x() + rect.get_width() / 2.0,
            1.2,
            "{:.2f}%".format(ratio * 100),
            ha="center",
            va="bottom",
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
        label="table hit ratio 100% line",
    )

    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    plt.ylim(bottom=0)  # ymin is your value
    plt.xlim([left, right])  # ymin is your value
    plt.xticks(ratios.index)
    plt.title(
        (
            "Performances of %s codes read and writes in function of the table size\n"
            "Shaded areas are the 25%% and 75%% percentiles and the plots "
            "are medians with stds."
        )
        % (code.capitalize())
    )
    plt.xlabel("Table Bits")
    plt.ylabel("ns")
    plt.savefig("%s_tables.png" % code, bbox_inches="tight")
