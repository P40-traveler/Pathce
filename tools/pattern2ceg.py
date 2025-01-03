#!/usr/bin/env python
import sys
import json
import argparse
import csv

parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description="Convert an input pattern (in JSON format) to CEG format")
parser.add_argument("-p",
                    "--pattern",
                    help="Specify the pattern path",
                    required=True)
parser.add_argument("-o",
                    "--output",
                    help="Specify the output path",
                    required=True)
args = parser.parse_args()

with open(args.pattern) as f:
    pattern = json.load(f)

output = open(args.output, "w+")
writer = csv.writer(output, lineterminator="\n")

pattern_edges = sorted(pattern["edges"], key=lambda e: (e["src"], e["dst"]))

edges = []
for e in pattern_edges:
    src = e["src"]
    dst = e["dst"]
    edges.append(f"{src}-{dst}")
edges = ";".join(edges)
labels = []
for e in pattern_edges:
    label = e["label_id"]
    labels.append(f"{label}")
labels = "->".join(labels)
writer.writerow((edges, labels, 0))
