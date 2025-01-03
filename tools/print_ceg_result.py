#!/usr/bin/env python
import sys
import json
import argparse

parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description=
    "Print the result of CEG in the same manner as GCard and GLogS")
parser.add_argument("-i",
                    "--input",
                    help="Specify the input csv",
                    required=True)
parser.add_argument("-t",
                    "--type",
                    help="Specify the pattern type",
                    choices=["acyclic", "cyclic"],
                    required=True)
args = parser.parse_args()

with open(args.input) as f:
    line = f.readline()
    results = line.split(",")[2:-1]
    line = f.readline()
    time = float(line)

# Order: all-min, all-max, all-avg, min-min, min-max, min-avg, max-min, max-max, max-avg
# Choose max-max estimator on on acyclic queries and cyclic queries with only triangles,
# and max-min estimator on queries with larger cycles.
result = float(results[7]) if args.type == "acyclic" else float(results[6])
print(f"{result},{time}")