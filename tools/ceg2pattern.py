#!/usr/bin/env python
import sys
import json
import argparse
import csv

parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description=
    "Convert an input CEG result (in CSV format) to JSON pattern with 'count'")
parser.add_argument("-i",
                    "--input",
                    help="Specify the input csv",
                    required=True)
parser.add_argument("-p",
                    "--pattern",
                    help="Specify the input pattern",
                    required=True)
parser.add_argument("-t",
                    "--type",
                    help="Specify the pattern type",
                    choices=["acyclic", "cyclic"],
                    required=True)
parser.add_argument("-o",
                    "--output",
                    help="Specify the output path",
                    required=True)
args = parser.parse_args()

with open(args.pattern) as f:
    pattern = json.load(f)
with open(args.input) as f:
    reader = csv.reader(f)
    line = next(reader)
    results = line[2:-1]

# Order: all-min, all-max, all-avg, min-min, min-max, min-avg, max-min, max-max, max-avg
# Choose max-max estimator on on acyclic queries and cyclic queries with only triangles,
# and max-min estimator on queries with larger cycles.
result = float(results[7]) if args.type == "acyclic" else float(results[6])
output = open(args.output, "w+")
pattern["count"] = result
json.dump(pattern, output, indent=4)
