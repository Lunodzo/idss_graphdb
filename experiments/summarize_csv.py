#!/usr/bin/env python3
"""Print per-group means from an experiment results CSV.

Usage: summarize_csv.py <csv_file> --group col1,col2,... --value col
"""
import argparse
import csv
import statistics
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_file")
    parser.add_argument("--group", required=True, help="Comma-separated grouping columns")
    parser.add_argument("--value", required=True, help="Column to average")
    args = parser.parse_args()

    group_cols = args.group.split(",")
    buckets = defaultdict(list)
    with open(args.csv_file, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            try:
                value = float(row[args.value])
            except (KeyError, ValueError, TypeError):
                continue
            key = tuple(row[col] for col in group_cols)
            buckets[key].append(value)

    print(",".join(group_cols + [f"mean_{args.value}", "n"]))
    for key in sorted(buckets):
        values = buckets[key]
        if not values:
            continue
        print(",".join(list(key) + [f"{statistics.fmean(values):.6f}", str(len(values))]))


if __name__ == "__main__":
    main()
