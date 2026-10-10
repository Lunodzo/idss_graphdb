#!/usr/bin/env python3
"""Print tab-separated column values from the first row of a client result JSON.

Usage: extract_row_field.py [--last] <json_file> <column_name> [<column_name> ...]

Column names are matched case-insensitively against the response header.
Traversal queries concatenate the source node's columns with the destination
node's columns, so duplicate names such as "Key" or "Mrid" can appear more
than once; pass --last to select the final (destination-node) occurrence
instead of the default first (source-node) occurrence.
Exits non-zero and prints an error to stderr if the file has no rows or a
requested column is not present.
"""
import json
import sys


def main():
    args = sys.argv[1:]
    use_last = False
    if args and args[0] == "--last":
        use_last = True
        args = args[1:]
    if len(args) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    json_file, columns = args[0], args[1:]
    with open(json_file, encoding="utf-8") as handle:
        response = json.load(handle)

    header = [label.lower() for label in response.get("header", [])]
    rows = response.get("rows", [])
    if not rows:
        print(f"No rows in {json_file}", file=sys.stderr)
        return 1

    row = rows[0]
    values = []
    for column in columns:
        indices = [i for i, label in enumerate(header) if label == column.lower()]
        if not indices:
            print(f"Column {column!r} not found in header {response.get('header')}", file=sys.stderr)
            return 1
        index = indices[-1] if use_last else indices[0]
        values.append(row[index])

    print("\t".join(values))
    return 0


if __name__ == "__main__":
    sys.exit(main())
